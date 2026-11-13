param(
    [string]$RunId = 'task1-5000',
    [string]$Dataset = 'data/wiki-5000.jsonl',
    [int]$Count = 5000,
    [int]$BaselineCount = 2000,
    [int]$MaxOffsets = 100,
    [int]$DuplicateCount = 100,
    [string]$WaitForContainer = '',
    [switch]$UseExistingDataset
)
# Uses the installed image and cached model. No build or destructive reset.
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if ($RunId -cnotmatch '^[a-z0-9][a-z0-9_-]{0,47}$') { throw 'Invalid RunId' }
if ($Count -le 1 -or $BaselineCount -le 0 -or $BaselineCount -ge $Count) { throw 'Invalid split counts' }
if ($MaxOffsets -le 0 -or $DuplicateCount -le 0 -or $DuplicateCount -gt $Count) { throw 'Invalid batch/replay count' }
if ($WaitForContainer -and $UseExistingDataset) { throw 'Choose waiting for capture or an existing dataset' }
$dataRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'data')) + '\'
$datasetPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot $Dataset))
if (-not $datasetPath.StartsWith($dataRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Dataset must be inside the project data directory'
}
$Dataset = 'data/' + $datasetPath.Substring($dataRoot.Length).Replace('\', '/')
$split = "data/$RunId-split"
$run = "runs/$RunId"
$workflow = "runs/workflow-$RunId"
foreach ($path in @($run, $workflow, $split)) {
    if (Test-Path -LiteralPath $path) { throw "Existing output $path; choose a fresh RunId" }
}
New-Item -ItemType Directory -Path $workflow -Force | Out-Null
$utf8 = New-Object System.Text.UTF8Encoding($false)
$script:phase = 'preflight'
$started = [DateTime]::UtcNow.ToString('o')

function Set-State([string]$Phase, [string]$Status = 'RUNNING', [string]$Detail = '') {
    $script:phase = $Phase
    $state = [ordered]@{ status = $Status; phase = $Phase; run_id = $RunId
        started_utc = $started; updated_utc = [DateTime]::UtcNow.ToString('o')
        dataset = $Dataset; target_records = $Count; baseline_records = $BaselineCount
        detail = $Detail }
    $temp = Join-Path $workflow 'status.json.tmp'
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot $temp), ($state | ConvertTo-Json), $utf8)
    Move-Item -LiteralPath $temp -Destination (Join-Path $workflow 'status.json') -Force
}

function Invoke-Docker {
    $disk = [IO.DriveInfo]::new($env:SystemDrive + '\')
    if ($disk.AvailableFreeSpace -lt 750MB) { throw 'Less than 750 MB free; stopped before next phase' }
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue' # Native stderr contains ordinary Spark/Docker progress.
    try {
        & docker @args 2>&1 | ForEach-Object {
            $line = "$_"
            [IO.File]::AppendAllText((Join-Path $PSScriptRoot "$workflow/workflow.log"), $line + "`n", $utf8)
            Write-Output $line
        }
        $exitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    if ($exitCode -ne 0) { throw "Docker command failed in $script:phase (exit $exitCode)" }
}

function Read-Report([string]$Name) {
    $report = Get-Content -LiteralPath "$run/$Name.json" -Raw | ConvertFrom-Json
    if ($report.status -ne 'PASS') { throw "Verification $Name failed" }
    return $report
}

try {
    Set-State 'preflight'
    Invoke-Docker compose up -d --wait kafka chroma
    $sources = Get-ChildItem -File *.py,*.ps1,Dockerfile,docker-compose.yml,requirements.txt |
        ForEach-Object { [ordered]@{ file = $_.Name; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLower() } }
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot "$workflow/source-hashes.json"), ($sources | ConvertTo-Json), $utf8)
    Set-State 'capture'
    if ($WaitForContainer) {
        # Keep a heartbeat while the detached collector runs. Do not replay partial files.
        while ($true) {
            $inspect = & docker inspect $WaitForContainer
            if ($LASTEXITCODE -ne 0) { throw "Cannot inspect collector $WaitForContainer" }
            $container = ($inspect | ConvertFrom-Json)[0]
            if ($container.Config.Cmd -notcontains $Dataset -or $container.Config.Cmd -notcontains '--record-only') {
                throw 'Collector does not match the requested record-only dataset'
            }
            if ($container.State.Status -eq 'exited') {
                $captureInfo = [ordered]@{ container_id = $container.Id; image_id = $container.Image
                    command = $container.Config.Cmd; started_utc = $container.State.StartedAt
                    finished_utc = $container.State.FinishedAt; exit_code = $container.State.ExitCode }
                [IO.File]::WriteAllText((Join-Path $PSScriptRoot "$workflow/capture-config.json"), ($captureInfo | ConvertTo-Json), $utf8)
                & docker logs $WaitForContainer 2>&1 | ForEach-Object {
                    [IO.File]::AppendAllText((Join-Path $PSScriptRoot "$workflow/capture.log"), "$_`n", $utf8)
                }
                if ($container.State.ExitCode -ne 0) { throw "Collector failed: exit $($container.State.ExitCode)" }
                break
            }
            if ($container.State.Status -ne 'running') { throw "Collector is $($container.State.Status)" }
            $accepted = 0
            if (Test-Path -LiteralPath $datasetPath) {
                $accepted = (Get-Content -LiteralPath $datasetPath | Measure-Object -Line).Lines
            }
            Set-State 'capture' 'RUNNING' "Collected $accepted of $Count records; waiting for collector to exit successfully"
            Start-Sleep -Seconds 15
        }
    } elseif (-not $UseExistingDataset) {
        Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py live --record $Dataset --record-only --max-events $Count --max-seconds 0
    }
    Set-State 'prepare-dataset'
    Invoke-Docker compose run --rm --no-deps --volume './prepare_dataset.py:/app/prepare_dataset.py:ro' --entrypoint python stream-job prepare_dataset.py --dataset $Dataset --out-dir $split --expected-count $Count --baseline-count $BaselineCount
    $dataManifest = Get-Content -LiteralPath "$split/manifest.json" -Raw | ConvertFrom-Json
    $topic = "wiki-edits-$RunId"
    Set-State 'baseline-replay'
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file "$split/baseline.jsonl" --topic $topic --rate 50
    Set-State 'baseline-ingestion'
    Invoke-Docker compose run --rm --no-deps stream-job --run-id $RunId --max-offsets $MaxOffsets --available-now
    Set-State 'baseline-verification'
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id $RunId --dataset "$split/baseline.jsonl" --query-check --out "$run/baseline-verification.json"
    $baseline = Read-Report 'baseline-verification'
    if ($baseline.events_processed -ne $BaselineCount -or $baseline.unique_vectors -ne $dataManifest.splits.baseline.vectors) { throw 'Baseline count mismatch' }

    Set-State 'incremental-replay'
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file "$split/incremental.jsonl" --topic $topic --rate 50
    Set-State 'incremental-ingestion'
    Invoke-Docker compose run --rm --no-deps stream-job --run-id $RunId --max-offsets $MaxOffsets --available-now
    Set-State 'full-verification'
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id $RunId --dataset $Dataset --query-check --out "$run/verification.json"
    $full = Read-Report 'verification'
    if ($full.events_processed -ne $Count -or $full.unique_vectors -ne $dataManifest.expected_vectors -or $full.truncated_chunks -ne 0) { throw 'Full dataset count mismatch or truncated chunks' }

    Set-State 'checkpoint-restart'
    Invoke-Docker compose run --rm --no-deps stream-job --run-id $RunId --max-offsets $MaxOffsets --available-now
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id $RunId --dataset $Dataset --out "$run/restart-verification.json"
    $restart = Read-Report 'restart-verification'
    if ($restart.events_processed -ne $full.events_processed -or $restart.batches -ne $full.batches -or $restart.unique_vectors -ne $full.unique_vectors) { throw 'Checkpoint restart changed the run' }

    Set-State 'duplicate-replay'
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file $Dataset --limit $DuplicateCount --topic $topic --rate 50
    Invoke-Docker compose run --rm --no-deps stream-job --run-id $RunId --max-offsets $MaxOffsets --available-now
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id $RunId --dataset $Dataset --query-check --out "$run/replay-verification.json"
    $replay = Read-Report 'replay-verification'
    if ($replay.events_processed -ne ($Count + $DuplicateCount) -or $replay.unique_vectors -ne $full.unique_vectors) { throw 'Duplicate replay changed vector count or lost events' }

    Set-State 'save-evidence'
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job -c "import pathlib,subprocess; pathlib.Path('$run/requirements.freeze.txt').write_bytes(subprocess.check_output(['python','-m','pip','freeze']))"
    $summary = [ordered]@{ status = 'PASS'; run_id = $RunId; completed_utc = [DateTime]::UtcNow.ToString('o')
        dataset = $Dataset; dataset_sha256 = $dataManifest.sha256; dataset_records = $Count
        baseline_records = $BaselineCount; incremental_records = ($Count - $BaselineCount)
        unique_vectors = $full.unique_vectors; initial_batches = $full.batches
        duplicate_events_tested = $DuplicateCount; final_events_processed = $replay.events_processed
        truncated_chunks = $replay.truncated_chunks; maximum_chunk_tokens = $dataManifest.maximum_chunk_tokens
        checkpoint_restart_passed = $true; duplicate_replay_passed = $true; self_retrieval_passed = $replay.query_checked
        collection = "wiki_$RunId"; chroma_host = 'localhost:8001'; kafka_topic = $topic
        scope = 'Task 1 correctness validation; no concurrent-query or CRD performance claim' }
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot "$run/task1-summary.json"), ($summary | ConvertTo-Json), $utf8)
    Set-State 'complete' 'PASS' "Validated $Count records and $($full.unique_vectors) unique vectors, including restart and duplicate replay"
    Write-Output "Task 1 validation PASS: $run/task1-summary.json"
} catch {
    Set-State $script:phase 'FAIL' $_.Exception.Message
    Write-Error $_
    exit 1
}
