# Validated ingestion pipeline - November 13, 2026

The Member 1 ingestion pipeline passed the full 5,000-edit validation, retrieval
check, checkpoint restart, and duplicate replay check. The completed large run is
**`task1-5000`**. The earlier `final-50` smoke-test evidence is retained below.

## Completed 5,000-edit run

| Check | Result |
|---|---|
| Fixed real dataset | 5,000 distinct revisions across 3,275 articles |
| Baseline | 2,000 records; 12,866 vectors |
| Incremental split | 3,000 records; 19,797 vectors |
| Full ingestion | 50 batches; 32,663 unique vectors |
| IDs, content hashes and metadata vs. dataset/logs | PASS |
| Self-retrieval | PASS |
| Embedding truncation | Zero; maximum chunk length 204 tokens |
| Checkpoint restart | Still 5,000 events / 50 batches / 32,663 vectors |
| Duplicate replay of 100 records | 5,100 processed events / 51 batches / still 32,663 vectors |
| Final workflow status | PASS, completed November 13, 2026 |

Evidence: `runs/task1-5000/task1-summary.json`, the four verification reports in
that directory, and `data/task1-5000-split/manifest.json`. The dataset SHA-256 is
`055acaef9ef8c9033db81b0479509c4505feea2ef3d7035cada32cdbd472bf70`.
The final collection is `wiki_task1-5000` on `localhost:8001`; the Kafka topic is
`wiki-edits-task1-5000`. Use a fresh run for teammates' concurrent experiments.

## Confirmed results

| Check | Result |
|---|---|
| Regression tests using the Docker image and mounted current tests/helper | 15 passed |
| Docker build and `pip check` | Passed |
| Live Wikipedia SSE capture | 50 accepted edits, zero skipped revision fetches |
| Dataset integrity | 50 unique revision IDs; every record validates |
| Initial Kafka replay | 50 acknowledgments |
| Initial Spark ingestion | 5 batches of 10 events |
| Exact unique chunks in Chroma | 339 |
| Embedding truncation | Zero; largest tokenized chunk in this dataset was 203 tokens |
| Stored IDs, content hashes, and metadata vs. logs/dataset | Passed |
| Self-retrieval using the same embedding model | Passed |
| Restart with the same Spark checkpoint | Remained at 50 events / 5 batches / 339 vectors |
| Replay the same 50 edits again | 100 total processed events / 10 batches / still 339 vectors |
| Retrieval and content verification after replay | Passed |

The increase from the earlier 226-chunk trial is intentional. That trial used
200-word chunks and exposed 159 silently truncated embeddings. The final shared
chunker uses 200 model tokens with 40-token overlap and source-text offsets. The
writer now rejects oversized chunks instead of truncating them. The earlier
`runs/validated-50/` results remain as historical evidence; use `final-50` for
the current implementation and a fresh run ID for any future experiment.

## Saved evidence and teammate connection

- Dataset: `data/wiki-50.jsonl`
- Initial validation: `runs/final-50/verification.json`
- Restart validation: `runs/final-50/restart-verification.json`
- Replay validation: `runs/final-50/replay-verification.json`
- Settings and model revision: `runs/final-50/manifest.json`
- Exact Python package versions: `runs/final-50/requirements.freeze.txt`
- Successful batch records: `runs/final-50/batches/*.json`
- Recovery state: `runs/final-50/checkpoint/`
- Host connection: `localhost:8001`, collection **`wiki_final-50`**
- Container connection: `chroma:8000`, collection **`wiki_final-50`**
- Kafka topic: `wiki-edits-final-50`

Dataset SHA-256:

```text
184d017843e89f4854b22c59157f9db49423a3b50833e0d20e49a3e6e809949c
```

Validated Docker image ID:

```text
sha256:eea440ca267857bbe4929cf79a9d4052c54356d1cddb30185eb1422ead710059
```

## Repeat without rebuilding

The image and model are already installed. From this project directory:

```powershell
powershell -ExecutionPolicy Bypass -File .\smoke_test.ps1 -RunId demo-01 -Dataset data/wiki-50.jsonl -SkipBuild
```

Choose a new RunId each time. This command verifies ingestion, retrieval, and
checkpoint restart. To inspect the existing final collection without changing it:

```powershell
docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id final-50 --dataset data/wiki-50.jsonl --query-check
```

## Runtime recovery completed

With permission, cleared 5.3 GB of disposable Python caches and 5.5 GB of Docker
build cache. An administrator-approved WSL service restart recovered Docker after
the disk-full incident. Removed the incomplete application image and rebuilt it
cleanly. The Dockerfile now avoids duplicating the dependency tree in a separate
build stage and includes Spark's `ps` utility. Project datasets and data volumes
were preserved. Keep adequate free disk space for future builds; the smoke script
checks for 8 GB before a build, and `-SkipBuild` uses the installed image.

## Scope of these results

This proves correctness and repeatability on the tested real datasets. It does not
measure large-scale throughput, concurrent retrieval degradation, HNSW internal
visibility, or answer quality. The retrieval check is a self-retrieval smoke test,
not a recall benchmark. Batch timestamps measure client upsert calls; Members 2
and 3 still need their instrumentation and query workloads for the experiments.

## Full workflow automation

`complete_task1.ps1` was also tested end to end on the same real 50-edit dataset
using run `task1-check-50`. It prepared a 20/30 baseline/incremental split,
verified both ingestion phases, restarted the checkpoint without adding events,
and replayed 10 duplicate events. Final result: **PASS**, 60 processed events,
339 unique vectors, zero truncated chunks, successful self-retrieval. Evidence:
`runs/task1-check-50/task1-summary.json` and its four verification reports.

The larger 5,000-edit SSE capture and automatic validation completed successfully.
Both `runs/workflow-task1-5000/status.json` and
`runs/task1-5000/task1-summary.json` report `PASS`. The 2,000/3,000 split and the
100-event duplicate replay passed all checks. See `TASK1_HANDOFF.md` for the saved
artifacts and instructions for teammates' fresh experiment runs.
