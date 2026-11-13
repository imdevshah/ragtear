"""Kafka -> Spark micro-batches -> embeddings -> shared Chroma server."""
import argparse
import json
import os
import time
from importlib.metadata import version
from pathlib import Path

from common import (CHUNK_OVERLAP, CHUNK_SIZE, MODEL, MODEL_REVISION, chunk_text,
                    load_embedder, run_paths, text_hash, validate_record)
from sink import ChromaSink, hnsw_config


def write_json_atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as out:
        json.dump(data, out, indent=2)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)


def prepare_run(args):
    root, collection, default_topic = run_paths(args.run_id)
    topic = args.topic or default_topic
    config = {"schema_version": 2, "run_id": args.run_id, "collection": collection,
              "topic": topic, "max_offsets": args.max_offsets,
              "trigger_seconds": args.trigger_seconds, "starting": args.starting,
              "embedding_model": MODEL, "model_revision": MODEL_REVISION,
              "chunk_size": CHUNK_SIZE, "chunk_overlap": CHUNK_OVERLAP,
              "chunk_unit": "model_tokens", "chunking_version": 2,
              "hnsw": hnsw_config(), "versions": {p: version(p) for p in
                  ("chromadb", "pyspark", "sentence-transformers", "transformers", "torch")}}
    manifest = root / "manifest.json"
    if manifest.exists():
        old = json.loads(manifest.read_text(encoding="utf-8"))
        if old != config:
            raise ValueError("Run configuration changed. Use a new --run-id for a new experiment.")
    else:
        if root.exists() and any(root.iterdir()):
            raise ValueError("Run directory has data but no manifest; use a new --run-id")
        write_json_atomic(manifest, config)
    return root, collection, topic


def build_payload(rows, batch_id, run_id, tokenizer=None):
    payload = {}
    for row in rows:
        raw = row.asDict() if hasattr(row, "asDict") else row
        record = validate_record(json.loads(raw["value"]))
        for index, doc in enumerate(chunk_text(record["text"], tokenizer=tokenizer)):
            chunk_id = f"{record['page_id']}-{record['rev_id']}-{index}"
            meta = {"page_id": record["page_id"], "rev_id": record["rev_id"],
                    "title": record["title"], "chunk": index, "edit_ts": record["edit_ts"],
                    "event_id": record["event_id"], "ingest_ts": record["ingest_ts"],
                    "batch_id": batch_id, "run_id": run_id, "text_sha256": text_hash(doc)}
            if chunk_id in payload and payload[chunk_id][0] != doc:
                raise ValueError(f"Conflicting content for {chunk_id}")
            payload[chunk_id] = (doc, meta)
    ids = list(payload)
    return ids, [payload[i][0] for i in ids], [payload[i][1] for i in ids]


def make_batch_writer(root, run_id, embedder, sink):
    root = Path(root)

    def process_batch(df, batch_id):
        start_wall, start_mono = time.time_ns(), time.monotonic_ns()
        rows = df.collect()  # Deliberately bounded by maxOffsetsPerTrigger.
        ids, docs, metas = build_payload(rows, batch_id, run_id, embedder.tokenizer)
        embed_start = time.monotonic_ns()
        truncated = 0
        if ids:
            lengths = embedder.tokenizer(docs, truncation=False, padding=False,
                                        return_length=True, verbose=False)["length"]
            truncated = sum(n > embedder.max_seq_length for n in lengths)
            if truncated:
                raise ValueError("A chunk exceeds the embedding token limit; refusing silent truncation")
            embeddings = embedder.encode(docs, batch_size=64, normalize_embeddings=True,
                                         show_progress_bar=False).tolist()
        embed_done = time.monotonic_ns()
        write_wall, write_mono = time.time_ns(), time.monotonic_ns()
        if ids:
            sink.upsert(ids, embeddings, docs, metas)
        commit_wall, commit_mono = time.time_ns(), time.monotonic_ns()
        offsets = {}
        for row in rows:
            r = row.asDict() if hasattr(row, "asDict") else row
            key = f"{r['topic']}:{r['partition']}"
            span = offsets.setdefault(key, {"min": r["offset"], "max": r["offset"]})
            span["min"], span["max"] = min(span["min"], r["offset"]), max(span["max"], r["offset"])
        entry = {"schema_version": 2, "run_id": run_id, "batch_id": batch_id,
                 "n_events": len(rows), "n_vectors": len(ids), "vector_ids": ids,
                 "text_hashes": {i: m["text_sha256"] for i, m in zip(ids, metas)},
                 "offsets": offsets, "truncated_chunks": truncated,
                 "t_start_wall_ns": start_wall, "t_start_mono_ns": start_mono,
                 "t_embed_done_mono_ns": embed_done,
                 "t_write_start_wall_ns": write_wall, "t_write_start_mono_ns": write_mono,
                 "t_commit_wall_ns": commit_wall, "t_commit_mono_ns": commit_mono,
                 "embed_s": (embed_done - embed_start) / 1e9,
                 "write_s": (commit_mono - write_mono) / 1e9,
                 "batch_s": (commit_mono - start_mono) / 1e9,
                 "window_kind": "client_upsert_calls"}
        # One atomic result per batch: retrying a batch replaces its record.
        # If a write or logging fails, Spark retries using the same deterministic IDs.
        write_json_atomic(root / "batches" / f"{batch_id:020d}.json", entry)
        print(f"[batch {batch_id}] events={len(rows)} unique_vectors={len(ids)} "
              f"write_s={entry['write_s']:.3f} truncated_chunks={truncated}", flush=True)
    return process_batch


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--topic")
    parser.add_argument("--max-offsets", type=int, default=1000)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--starting", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--available-now", action="store_true",
                        help="Drain current Kafka backlog and exit (smoke test / preload)")
    args = parser.parse_args()
    if args.max_offsets <= 0 or args.trigger_seconds <= 0:
        parser.error("max-offsets and trigger-seconds must be positive")
    root, collection, topic = prepare_run(args)
    spark = (SparkSession.builder.appName(f"wiki-ingest-{args.run_id}")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Connection, collection creation and model loading precede all write windows.
        sink = ChromaSink(collection, args.run_id)
        embedder = load_embedder()
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
               .option("subscribe", topic).option("startingOffsets", args.starting)
               .option("maxOffsetsPerTrigger", args.max_offsets)
               .option("failOnDataLoss", "true").load()
               .selectExpr("CAST(value AS STRING) AS value", "topic", "partition", "offset"))
        writer = (raw.writeStream.foreachBatch(make_batch_writer(root, args.run_id, embedder, sink))
                  .option("checkpointLocation", str(root / "checkpoint")))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(
            processingTime=f"{args.trigger_seconds} seconds")
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
