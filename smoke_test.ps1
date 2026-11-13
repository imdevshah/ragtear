param(
    [string]$RunId = ('smoke-' + (Get-Date -Format 'yyyyMMdd-HHmmss')),
    [string]$Dataset = '',
    [int]$Count = 50,
    [ValidateSet('sse', 'poll')][string]$Source = 'sse',
    [switch]$SkipBuild
)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if ($RunId -cnotmatch '^[a-z0-9][a-z0-9_-]{0,47}$') { throw 'Invalid RunId' }
if ($Count -le 0) { throw 'Count must be positive' }
if (-not $SkipBuild) {
    $systemDisk = [System.IO.DriveInfo]::new($env:SystemDrive + '\')
    if ($systemDisk.AvailableFreeSpace -lt 8GB) {
        throw 'Free at least 8 GB on the system drive before the first Docker build.'
    }
}
if (Test-Path -LiteralPath (Join-Path 'runs' $RunId)) {
    throw 'RunId already exists. Choose a fresh ID; existing results are preserved.'
}
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed (exit $LASTEXITCODE)" }
}
New-Item -ItemType Directory -Force -Path data,runs | Out-Null
if (-not $SkipBuild) { Invoke-Docker compose build stream-job }
Invoke-Docker compose up -d --wait kafka chroma
if (-not $Dataset) {
    $Dataset = "data/$RunId.jsonl"
    Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py live --record $Dataset --record-only --max-events $Count --max-seconds 600 --source $Source
} else {
    $datasetPath = (Resolve-Path -LiteralPath $Dataset).Path
    $dataRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'data')) + '\'
    if (-not $datasetPath.StartsWith($dataRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Dataset must be inside this project data directory (mounted in Docker).'
    }
    $Dataset = 'data/' + $datasetPath.Substring($dataRoot.Length).Replace('\', '/')
}
$topic = "wiki-edits-$RunId"
Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file $Dataset --topic $topic --rate 20
Invoke-Docker compose run --rm --no-deps stream-job --run-id $RunId --max-offsets 10 --available-now
Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id $RunId --dataset $Dataset --query-check --out "runs/$RunId/verification.json"
# Reusing the same checkpoint must not ingest the backlog again.
Invoke-Docker compose run --rm --no-deps stream-job --run-id $RunId --max-offsets 10 --available-now
Invoke-Docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id $RunId --dataset $Dataset --out "runs/$RunId/restart-verification.json"
Write-Output "Validated run: runs/$RunId. Shared Chroma: localhost:8001, collection wiki_$RunId"
