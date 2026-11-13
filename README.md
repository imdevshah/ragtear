# Ragtear

Retrieval Degradation Under Concurrent Write Load in Streaming RAG Systems.
Task 1 implements Wikipedia -> Kafka -> Spark -> Chroma ingestion.

The **5,000-record run is complete and validated**: 32,663 unique vectors, zero
truncated chunks, successful retrieval, checkpoint restart and duplicate replay.
See `runs/task1-5000/task1-summary.json` for the PASS report and
[TASK1_HANDOFF.md](TASK1_HANDOFF.md) for the 2,000/3,000 split and teammate instructions.

## Teammate setup from GitHub

Install Git and Docker Desktop with Linux containers, start Docker Desktop, and
allow at least 8 GB free for the first build. The fixed datasets and saved Task 1
results are included in this repository. You do not need to collect edits again.

```powershell
git clone https://github.com/imdevshah/ragtear.git
cd ragtear
git switch -c task2/hnsw-instrumentation
docker compose build stream-job
docker compose up -d --wait kafka chroma

# Run the 15 regression tests using the project's container environment.
docker compose run --rm --no-deps --volume ./tests:/app/tests:ro --entrypoint python stream-job -m unittest discover -s tests -v

# Optional quick ingestion/retrieval/restart check on the saved 50-edit sample.
powershell -ExecutionPolicy Bypass -File .\smoke_test.ps1 -RunId teammate-smoke-01 -Dataset data/wiki-50.jsonl -SkipBuild
```

Use a new run ID for each new experiment. Cloning saved PASS reports does not
copy a running database or Kafka topic. Follow the **Teammate experiment workflow**
in [TASK1_HANDOFF.md](TASK1_HANDOFF.md) to load the 2,000-record baseline before
Task 2 instrumentation and the 3,000-record incremental phase. The Task 2 entry
point is `sink.py::ChromaSink.upsert`; internal HNSW measurements require changes
inside Chroma as described in the handoff.

If you already cloned the repository, save or commit your local changes, then run
`git switch main` and `git pull --ff-only origin main` before creating your work
branch. After making changes, commit them, push your branch with
`git push -u origin task2/hnsw-instrumentation`, and open a pull request into `main`.

Local environments, secrets, Docker volumes, Spark checkpoints and new experiment
runs are excluded from Git. Add selected future experiment results explicitly when
they are ready to share; keep the original Task 1 datasets and evidence unchanged.

Validated on a real 50-edit dataset: 339 token-safe chunks, successful retrieval,
checkpoint restart, and duplicate replay with no vector-count growth. See
[VALIDATION.md](VALIDATION.md) for evidence. On this computer, reuse the installed
image with `-SkipBuild`:

```powershell
powershell -ExecutionPolicy Bypass -File .\smoke_test.ps1 -RunId demo-01 -Dataset data/wiki-50.jsonl -SkipBuild
```

Member 1 owns reliable Wikipedia ingestion and the Spark streaming pipeline.
This project provides that pipeline and its handoff to Members 2 and 3. It does
not implement their HNSW instrumentation, Locust workload, CRD calculation, or
the team's Ollama answering service.

## Recommended setup: Docker Desktop + PowerShell

Run commands from this project directory. Docker supplies Python 3.11, Java 17,
Spark 3.5.1 and the matching Chroma 1.5.9 client/server. No host Java installation
is required. The first build downloads CPU PyTorch and Spark; the first run
downloads the embedding model and Kafka connector jars.
Keep at least 8 GB free on Docker's storage drive before building; the script
checks the Windows system drive. A full drive can stall Docker and Chroma even
while the Chroma heartbeat still responds.

```powershell
# Build, record 50 real edits, replay them, drain Spark, verify, and test restart.
powershell -ExecutionPolicy Bypass -File .\smoke_test.ps1

# Reuse a recorded dataset in a NEW isolated run (after the image is built).
powershell -ExecutionPolicy Bypass -File .\smoke_test.ps1 -RunId smoke-second -Dataset data/wiki-50.jsonl -SkipBuild
```

Use `-Source poll` only if your network blocks Wikimedia's SSE connection. Polling
samples recent changes and can miss edits; it is a smoke-test fallback, not a
lossless stream. The normal source is SSE. A timed-out partial recording exits
with an error and keeps the accepted records for inspection. New recordings use
exclusive file creation, so choose a new filename if a previous attempt exists.

Docker services: Kafka at `localhost:9092`, Chroma at **`localhost:8001`**.
Container clients use `kafka:9092` and `chroma:8000`. Port 8001 avoids the older
locally launched Chroma service on port 8000; all teammates must use the same
new server and collection. Do not run the old README's second `chroma run` command.

## Manual workflow

```powershell
docker compose build stream-job
docker compose up -d --wait kafka chroma

# Capture once, without simultaneously writing to the experimental topic.
docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py live --record data/edits.jsonl --record-only --max-events 5000 --max-seconds 0

# Each experiment gets a fresh run ID, topic, collection, checkpoint and logs.
docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file data/edits.jsonl --topic wiki-edits-exp01 --rate 100
docker compose run --rm --no-deps stream-job --run-id exp01 --max-offsets 1000 --available-now
docker compose run --rm --no-deps --entrypoint python stream-job verify.py --run-id exp01 --dataset data/edits.jsonl --query-check --out runs/exp01/verification.json
```

For **concurrent experiments**, start Spark without `--available-now`, then start
the query workload and producer replay from other terminals. `--available-now`
drains an existing backlog and exits; it is for validation and baseline preloads.

```powershell
# Terminal 1: continuous writer. Ctrl+C stops the foreground job.
docker compose run --rm --no-deps stream-job --run-id exp02 --max-offsets 1000 --trigger-seconds 10

# Terminal 2: feed this run's topic. Start teammates' query load on wiki_exp02.
docker compose run --rm --no-deps --entrypoint python stream-job wiki_producer.py replay --file data/edits.jsonl --topic wiki-edits-exp02 --rate 100
```

Create the topic before starting a continuous writer if it does not yet exist:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic wiki-edits-exp02 --partitions 1 --replication-factor 1
```

For a baseline split, replay `--limit 2000`, drain Spark, then replay `--skip 2000`
while the continuous writer and query load run. Reuse the SAME run ID and writer
settings for these phases. Start a new run ID when changing parameters. Verification
against the full dataset is meaningful only after both phases have drained.

## Correctness and experiment contract

- **Source identity:** SSE `id` is a recent-change ID, stored as `rc_id`. The API
  receives `revision.new` via `revids`; its response supplies the real `page_id`,
  revision timestamp, title, and revision content. `event_title` preserves the
  stream title if the page has subsequently moved.
- **Text:** strip wikitext with mwparserfromhell; templates are omitted rather
  than expanded from current template versions. Keep the first 6000 text
  characters by default (`MAX_TEXT_CHARS=0` disables the cap). These records are
  article revision snapshots, not edit diffs. A changed fact may lie outside the
  retained text; select evaluation questions from the actual recorded passages.
- **Record format:** `schema_version=2`, `event_id`, `rc_id`, `page_id`, `rev_id`,
  `title`, `event_title`, `user`, `edit_ts` (epoch seconds), `ingest_ts` (epoch
  seconds), `text`, `text_sha256`, and revision-specific `source_url`. Old datasets
  produced by the incorrect page-ID mapping are rejected; record them again.
- **Chunks:** shared `common.chunk_text`, 200 model tokens with 40-token overlap. IDs
  are `{page_id}-{rev_id}-{chunk}`. Revisions coexist; this is an append-revisions
  experiment, not a current-version-only Wikipedia index.
- **Embeddings:** `sentence-transformers/all-MiniLM-L6-v2`, pinned model revision
  in `common.py`, 384 dimensions, normalized, cosine distance, CPU. Use that same
  model/revision/normalization for queries. Source offsets preserve the original
  text. The writer checks each chunk against the model's 256-token limit and fails
  instead of silently truncating it. Keep chunking fixed between experiment
  conditions and question generation. Use a new run ID after chunking changes.
- **Delivery:** Kafka acknowledgments are checked. Spark uses checkpoints and
  fails on missing Kafka offsets or malformed records. Writes are at least once;
  deterministic IDs and upserts make retries idempotent. Duplicate IDs within a
  batch are collapsed; conflicting content for an ID is an error.
- **Isolation:** each `--run-id` maps to topic `wiki-edits-ID`, collection
  `wiki_ID`, and `runs/ID/`. A manifest pins settings and library versions; changing
  them requires a new ID. Run only one writer per run ID. Reusing an ID resumes
  its checkpoint; `startingOffsets` does not reset an existing checkpoint.

## Handoff and evidence

| Output | Purpose |
|---|---|
| `data/*.jsonl` | Immutable recorded dataset, replayable at controlled rates |
| `runs/ID/manifest.json` | Topic, collection, model revision, chunk settings, HNSW settings, dependency versions |
| `runs/ID/batches/*.json` | One atomic successful record per Spark batch; retries replace the same batch file |
| `runs/ID/checkpoint/` | Spark recovery state; preserve when resuming |
| `runs/ID/verification.json` | Strict verification result including optional retrieval check |
| `sink.py::ChromaSink.upsert` | Instrumentation hook for Member 2 |

Batch records include event/vector counts, IDs and content hashes, Kafka offset
ranges, embedding time, batch time, and write-call start/end in wall-clock and
monotonic nanoseconds. **These are client upsert-call windows, not proof of HNSW
visibility, durability, or an atomic index commit.** Member 2 must measure those
inside Chroma. Setup and model loading happen before measured writes. Multiple
500-vector API calls may make up one write window. Empty batches are not writes.
Use synchronized wall clocks between machines; do not compare monotonic clocks
across machines or host/container clock domains without validation.

`verify.py` checks exact unique IDs, content hashes, identity metadata, latest
batch ownership, dataset coverage, and timestamp ordering. It exits nonzero on
connection failures, missing/extra vectors, or mismatches. Stop writes before
verification; pagination is not a snapshot under concurrent mutation.
`--skip-chroma` explicitly reports `LOGS_ONLY`, not a full pipeline pass.
The self-retrieval test checks integration, not recall quality or CRD.

`--max-offsets` caps Kafka EVENTS per trigger, not vectors. Replay `--rate` is a
target events/second; actual acknowledged throughput is printed. The trigger
interval is not a guaranteed write rate when processing takes longer than it.
HNSW environment variables are `HNSW_M`, `HNSW_EF_CONSTRUCTION`, and
`HNSW_EF_SEARCH`. Pass them with `docker compose run -e NAME=value ...` and use a
fresh run ID; the sink checks actual collection settings.

## Tests and optional question generation

For local Python tests (Python 3.11):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install torch==2.6.0 --index-url https://download.pytorch.org/whl/cpu
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Optional question generation is a helper for the evaluation team. It uses the
same chunker and validates the dataset. Start Ollama separately, choose a distinct
question-generation model from the answering model, and inspect accepted questions.
Temperature zero is not a substitute for saving the generated question set.

```powershell
docker compose run --rm --no-deps -e OLLAMA_URL=http://host.docker.internal:11434 --entrypoint python stream-job make_questions.py --edits data/edits.jsonl --out data/questions.jsonl --skip-first 2000 --n 300 --model llama3.1:8b
```

## Troubleshooting

- `docker compose ps` and `docker compose logs --tail 100 kafka chroma` show service status.
- If port 9092 is occupied by the previous Kafka container, run `docker compose up -d kafka`
  to reconcile this project's container with the current Compose definition.
- The default Python sink connects to `localhost:8001`; container clients are configured
  automatically. An unset host never silently creates a separate embedded index.
- Model and Kafka connector downloads require network access on the first run.
- `docker compose stop` stops services while preserving named volumes. Avoid removing
  volumes or checkpoints when you want to resume or preserve experiment evidence.

API references: [MediaWiki event fields](https://www.mediawiki.org/wiki/Manual:RCFeed),
[exact revisions](https://www.mediawiki.org/wiki/API:Revisions), and
[Chroma collection configuration](https://docs.trychroma.com/docs/collections/configure).
