# Task 1: Wikipedia ingestion handoff

Task 1 provides the Wikipedia producer, Kafka delivery, Spark Structured Streaming
writer, and reproducible input data for the team's Chroma experiments.

## Current work and completion evidence

**Completed November 13, 2026:** `task1-5000` reports `PASS`. All 5,000 distinct
revisions were ingested into 32,663 unique vectors, with zero truncated chunks.
The 2,000/3,000 split, exact dataset verification, self-retrieval, checkpoint
restart and 100-event duplicate replay all passed. No collection or validation
work remains for this Task 1 run. The instructions below also explain future runs.

The real 50-edit run `final-50` passed ingestion, exact content verification,
retrieval, checkpoint restart, and duplicate replay: 339 unique vectors and zero
truncated chunks. The regression suite now has 15 passing tests.

The completed live capture contains **5,000 distinct Wikipedia revision events**.
These are not necessarily 5,000 different articles. The full workflow creates a
2,000-record baseline and a disjoint 3,000-record incremental split in capture
order. The same article can appear in both splits with different revisions.

Both completion evidence files report `PASS`:

- `runs/workflow-task1-5000/status.json`: workflow phase and current status.
- `runs/task1-5000/task1-summary.json`: final results, dataset hash, and counts.

While the status is `RUNNING`, the capture/validation is still in progress. A
`FAIL` status records the phase and error; do not treat a partial file as the
finished dataset. Keep this computer awake and Docker Desktop running until the
workflow finishes. Live collection can take several hours.

```powershell
# Progress; commands are read-only.
Get-Content runs/workflow-task1-5000/status.json
docker logs --tail 5 task1-wiki-5000
Get-Content runs/workflow-task1-5000/workflow.log -Tail 10
```

## Files to give your teammates

| Artifact | Meaning |
|---|---|
| `data/wiki-5000.jsonl` | Fixed, validated source records after capture completes |
| `data/task1-5000-split/manifest.json` | SHA-256 hashes, record/vector counts, time ranges, pinned model/chunk settings |
| `data/task1-5000-split/baseline.jsonl` | First 2,000 records |
| `data/task1-5000-split/incremental.jsonl` | Remaining 3,000 records |
| `runs/task1-5000/manifest.json` | Kafka topic, Chroma collection, pipeline settings and package versions |
| `runs/task1-5000/batches/*.json` | Successful Spark batches, Kafka offsets, vector identities and write timing |
| `runs/task1-5000/*verification.json` | Baseline, full dataset, restart and duplicate-replay verification |
| `runs/task1-5000/task1-summary.json` | Final machine-readable completion report |
| `runs/task1-5000/requirements.freeze.txt` | Exact installed Python packages |
| `runs/workflow-task1-5000/` | Progress, workflow/capture logs and source hashes |

Share the source code, Docker files, README and this handoff together with those
artifacts. Git now includes the fixed Task 1 datasets and saved validation results;
new captures and experiment runs remain ignored by default. Dataset line endings
are preserved so their SHA-256 hashes remain valid after cloning on another OS.
Spark checkpoints, Python environments and local databases are excluded. Kafka
and Chroma storage live in Docker volumes; teammates must build the Docker image
and replay the saved JSONL with a fresh run ID to create their own index. Saved
PASS reports document the original run; cloning them does not populate a database.

## Data and service contract

- Source: Wikimedia's live recent-change SSE stream, filtered to English Wikipedia,
  main namespace, human nonminor edits. Each record fetches the **exact revision**
  from the MediaWiki API; unavailable revisions are skipped and logged.
- Each record contains the actual `page_id`, `rev_id`, `event_id`, `rc_id`, title,
  revision timestamp, capture timestamp, text, text SHA-256 and revision URL.
  These are revision snapshots, not edit diffs. Wikitext templates are omitted;
  the default text cap is 6,000 characters per record.
- Chunk IDs: `{page_id}-{rev_id}-{chunk_index}`. Different revisions coexist.
  Replaying the same revision upserts its existing IDs.
- Chunking: 200 model tokens, 40-token overlap. Encoder:
  `sentence-transformers/all-MiniLM-L6-v2`, revision
  `c9745ed1d9f207416be6d2e6f8de32d1f16199bf`, 384 dimensions, normalized embeddings,
  cosine distance. Query clients must use the same encoder and normalization.
- Host endpoints: Kafka `localhost:9092`, Chroma `localhost:8001`.
  Container endpoints: Kafka `kafka:9092`, Chroma `chroma:8000`.
  `localhost` only connects to this same machine; teammates on their own computers
  should run their own Compose stack and replay the dataset.
- Validation run: topic `wiki-edits-task1-5000`, collection `wiki_task1-5000`.
  This collection already contains both phases after validation; create a fresh
  experiment run for baseline-versus-concurrent comparisons.

## Teammate experiment workflow

On a new computer, complete **Teammate setup from GitHub** in `README.md` first,
including `docker compose build stream-job`. The commands below assume the image
is already built. The original validation collections exist on the author's
computer; a fresh clone creates its own collections when these commands run.

After the large dataset manifest reports `PASS`, choose a fresh run ID (`trial01`
below). Use identical writer settings in baseline and incremental phases.

```powershell
docker compose up -d --wait kafka chroma

# Baseline: preload 2,000 records and wait for Spark to exit.
docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file data/task1-5000-split/baseline.jsonl --topic wiki-edits-trial01 --rate 50
docker compose run --rm --no-deps stream-job --run-id trial01 --max-offsets 100 --available-now
docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id trial01 --dataset data/task1-5000-split/baseline.jsonl --query-check --out runs/trial01/baseline-verification.json

# Terminal 1: continuously consume the incremental phase.
docker compose run --rm --no-deps stream-job --run-id trial01 --max-offsets 100 --trigger-seconds 10

# Terminal 2: after teammates start their query workload against wiki_trial01.
docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file data/task1-5000-split/incremental.jsonl --topic wiki-edits-trial01 --rate 50
```

Wait until successful batch logs total 5,000 events, then stop the continuous
writer with Ctrl+C. Drain any final backlog and verify with no concurrent writes:

```powershell
docker compose run --rm --no-deps stream-job --run-id trial01 --max-offsets 100 --available-now
docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id trial01 --dataset data/wiki-5000.jsonl --query-check --out runs/trial01/verification.json
```

The replay rate is a target, not measured sustained ingestion throughput.
`--max-offsets` limits events per batch, not vectors. Keep checkpoints to resume a
run. Change the run ID when changing model, chunking, batch or HNSW settings.

## Ownership boundary

Member 2 can instrument `sink.py::ChromaSink.upsert` and Chroma internals. The
batch timestamps only bound **client upsert calls**; they do not establish HNSW
visibility or internal index commit times. Member 3 supplies Locust and CRD
evaluation. Members 4 and 5 supply analysis/mitigations and Ollama integration.
Self-retrieval verification demonstrates integration, not recall or answer quality.

## Repeat the complete Task 1 validation

Use a fresh run ID; existing outputs are never reset automatically. This reuses
the installed Docker image and mounted helper script, so no rebuild is needed:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\complete_task1.ps1 -RunId task1-repeat -Dataset data/wiki-5000.jsonl -UseExistingDataset
```

The script prepares the split, ingests/verifies both phases, checks that a
checkpoint restart adds no events, then replays 100 existing records and checks
that the number of unique vectors stays unchanged. It saves a final summary only
after every check passes. A failed run retains its data and error log for diagnosis.
