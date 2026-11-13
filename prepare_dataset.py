"""Validate a fixed capture and save disjoint, reproducible experiment splits."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from common import (CHUNK_OVERLAP, CHUNK_SIZE, MODEL, MODEL_REVISION,
                    chunk_text, get_tokenizer, read_records)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare(dataset, output, expected_count=5000, baseline_count=2000, tokenizer=None):
    dataset, output = Path(dataset), Path(output)
    if not 0 < baseline_count < expected_count:
        raise ValueError("Baseline count must be between zero and the total count")
    records = list(read_records(dataset))
    if len(records) != expected_count:
        raise ValueError(f"Expected {expected_count} records, found {len(records)}")
    if len({r["rev_id"] for r in records}) != len(records):
        raise ValueError("Duplicate revision IDs in fixed dataset")
    if len({r["event_id"] for r in records}) != len(records):
        raise ValueError("Duplicate event IDs in fixed dataset")
    tokenizer = tokenizer or get_tokenizer()
    counts, maximum_tokens = [], 0
    for record in records:
        docs = chunk_text(record["text"], tokenizer=tokenizer)
        if not docs:
            raise ValueError(f"No chunks for revision {record['rev_id']}")
        lengths = tokenizer(docs, truncation=False, padding=False,
                            return_length=True, verbose=False)["length"]
        maximum_tokens = max(maximum_tokens, *lengths)
        if max(lengths) > 256:
            raise ValueError(f"Oversized chunk for revision {record['rev_id']}")
        counts.append(len(docs))

    # All validation precedes creation. Never overwrite a previous prepared dataset.
    output.mkdir(parents=True, exist_ok=False)
    splits = {}
    for name, start, end in (("baseline", 0, baseline_count),
                             ("incremental", baseline_count, len(records))):
        path = output / f"{name}.jsonl"
        with path.open("x", encoding="utf-8", newline="\n") as target:
            for record in records[start:end]:
                target.write(json.dumps(record, ensure_ascii=False) + "\n")
        splits[name] = {"file": str(path), "sha256": sha256(path),
                        "start_index": start, "end_index_exclusive": end,
                        "records": end - start, "vectors": sum(counts[start:end])}
    manifest = {
        "status": "PASS", "created_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": str(dataset), "sha256": sha256(dataset),
        "records": len(records), "unique_revisions": len(records),
        "unique_pages": len({r["page_id"] for r in records}),
        "unique_texts": len({r["text_sha256"] for r in records}),
        "expected_vectors": sum(counts), "maximum_chunk_tokens": maximum_tokens,
        "edit_time_range_utc": [datetime.fromtimestamp(t, timezone.utc).isoformat()
                                for t in (min(r["edit_ts"] for r in records),
                                          max(r["edit_ts"] for r in records))],
        "recorded_time_range_utc": [datetime.fromtimestamp(t, timezone.utc).isoformat()
                                    for t in (min(r["ingest_ts"] for r in records),
                                              max(r["ingest_ts"] for r in records))],
        "split_order": "capture order; disjoint revision IDs, pages may overlap",
        "embedding_model": MODEL, "model_revision": MODEL_REVISION,
        "chunk_size": CHUNK_SIZE, "chunk_overlap": CHUNK_OVERLAP,
        "chunk_unit": "model_tokens", "splits": splits,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--expected-count", type=int, default=5000)
    parser.add_argument("--baseline-count", type=int, default=2000)
    args = parser.parse_args()
    print(json.dumps(prepare(args.dataset, args.out_dir, args.expected_count,
                             args.baseline_count), indent=2))


if __name__ == "__main__":
    main()
