"""Verify a stopped/quiescent run against its batch records and optional dataset."""
import argparse
import json
from pathlib import Path

from common import chunk_text, load_embedder, read_records, run_paths, text_hash
from sink import connect


def load_batches(root, run_id):
    batches = [json.loads(p.read_text(encoding="utf-8")) for p in sorted((root / "batches").glob("*.json"))]
    if not batches or not any(b["n_vectors"] for b in batches):
        raise ValueError("No nonempty completed batches")
    seen, previous = set(), None
    for b in batches:
        if b["run_id"] != run_id or b["batch_id"] in seen:
            raise ValueError("Mixed runs or duplicate batch IDs")
        seen.add(b["batch_id"])
        if b["n_vectors"] != len(set(b["vector_ids"])):
            raise ValueError("Invalid logged vector count")
        if not (b["t_start_wall_ns"] <= b["t_write_start_wall_ns"] <= b["t_commit_wall_ns"]):
            raise ValueError("Invalid wall-clock write window")
        if not (b["t_start_mono_ns"] <= b["t_embed_done_mono_ns"] <=
                b["t_write_start_mono_ns"] <= b["t_commit_mono_ns"]):
            raise ValueError("Invalid monotonic write window")
        if b["n_vectors"]:
            if previous and previous["t_commit_wall_ns"] > b["t_write_start_wall_ns"]:
                raise ValueError("Write windows overlap or wall clock moved backwards")
            previous = b
    return batches


def expected_vectors(batches):
    expected = {}
    for b in batches:
        for chunk_id in b["vector_ids"]:
            digest = b["text_hashes"][chunk_id]
            if chunk_id in expected and expected[chunk_id][1] != digest:
                raise ValueError(f"Content changed for immutable chunk {chunk_id}")
            expected[chunk_id] = (b["batch_id"], digest)
    return expected


def verify(run_id, dataset=None, query_check=False, skip_chroma=False):
    root, collection, _ = run_paths(run_id)
    batches = load_batches(root, run_id)
    expected = expected_vectors(batches)
    if dataset:
        wanted = {}
        for record in read_records(dataset):
            for index, doc in enumerate(chunk_text(record["text"])):
                key = f"{record['page_id']}-{record['rev_id']}-{index}"
                digest = text_hash(doc)
                if key in wanted and wanted[key] != digest:
                    raise ValueError(f"Dataset has conflicting content for {key}")
                wanted[key] = digest
        if wanted != {key: value[1] for key, value in expected.items()}:
            raise ValueError("Dataset IDs/content do not match completed batches")
    summary = {"run_id": run_id, "batches": len(batches),
               "events_processed": sum(b["n_events"] for b in batches),
               "unique_vectors": len(expected), "dataset_checked": bool(dataset),
               "chroma_checked": not skip_chroma, "query_checked": False,
               "truncated_chunks": sum(b.get("truncated_chunks", 0) for b in batches)}
    if skip_chroma:
        if query_check:
            raise ValueError("--query-check cannot be combined with --skip-chroma")
        summary["status"] = "LOGS_ONLY"
        return summary
    col = connect().get_collection(collection, embedding_function=None)
    actual = {}
    offset = 0
    while True:
        result = col.get(limit=1000, offset=offset, include=["metadatas", "documents"])
        if not result["ids"]:
            break
        for key, meta, doc in zip(result["ids"], result["metadatas"], result["documents"]):
            if meta.get("run_id") != run_id or meta.get("text_sha256") != text_hash(doc):
                raise ValueError(f"Wrong run or corrupted content: {key}")
            if key != f"{meta['page_id']}-{meta['rev_id']}-{meta['chunk']}":
                raise ValueError(f"Identity metadata mismatch: {key}")
            actual[key] = (meta["batch_id"], text_hash(doc))
        offset += len(result["ids"])
    if actual != expected or col.count() != len(expected):
        missing = sorted(set(expected) - set(actual))[:5]
        extra = sorted(set(actual) - set(expected))[:5]
        raise ValueError(f"Chroma does not match logs: missing={missing}, extra={extra}; "
                         "also check batch IDs and content hashes")
    if query_check:
        sample_id = next(iter(expected))
        sample = col.get(ids=[sample_id], include=["documents"])["documents"][0]
        model = load_embedder()
        embedding = model.encode([sample], normalize_embeddings=True).tolist()
        result = col.query(query_embeddings=embedding, n_results=min(5, len(expected)),
                           include=["documents", "distances"])
        # Identical documents from separate revisions may tie. Accept equal content.
        if sample not in result["documents"][0]:
            raise ValueError("Self-retrieval failed: sample document absent from top-5")
        summary["query_checked"] = True
        summary["query_sample_id"] = sample_id
        summary["query_top_ids"] = result["ids"][0]
    summary["status"] = "PASS"
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dataset")
    parser.add_argument("--query-check", action="store_true")
    parser.add_argument("--skip-chroma", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args()
    try:
        summary = verify(args.run_id, args.dataset, args.query_check, args.skip_chroma)
    except Exception as exc:
        summary = {"run_id": args.run_id, "status": "FAIL", "error": str(exc)}
    print(json.dumps(summary, indent=2))
    if args.out:
        from stream_job import write_json_atomic
        write_json_atomic(Path(args.out), summary)
    raise SystemExit(1 if summary["status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
