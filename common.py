"""Shared dataset, chunking, and experiment contracts."""
import hashlib
import json
import math
import re
from pathlib import Path
from functools import lru_cache

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "c9745ed1d9f207416be6d2e6f8de32d1f16199bf"
CHUNK_SIZE = 200
CHUNK_OVERLAP = 40


def text_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@lru_cache(maxsize=1)
def get_tokenizer():
    from transformers import AutoTokenizer
    return AutoTokenizer.from_pretrained(MODEL, revision=MODEL_REVISION, use_fast=True)


def chunk_text(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP, tokenizer=None):
    """Preserve source text while forming overlapping model-token windows."""
    if size <= 0 or not 0 <= overlap < size:
        raise ValueError("Require size > 0 and 0 <= overlap < size")
    if not text.strip():
        return []
    tokenizer = tokenizer or get_tokenizer()
    if size + tokenizer.num_special_tokens_to_add(pair=False) > 256:
        raise ValueError("Chunk size exceeds the embedding model's 256-token limit")
    offsets = tokenizer(text, add_special_tokens=False, return_offsets_mapping=True,
                        truncation=False, verbose=False)["offset_mapping"]
    return [text[offsets[i][0]:offsets[min(i + size, len(offsets)) - 1][1]].strip()
            for i in range(0, max(len(offsets) - overlap, 1), size - overlap)] if offsets else []


def validate_record(rec):
    if rec.get("schema_version") != 2:
        raise ValueError("Expected schema_version=2; re-record datasets made by the old page-ID producer")
    for key in ("page_id", "rev_id", "edit_ts"):
        if type(rec.get(key)) is not int or rec[key] <= 0:
            raise ValueError(f"Invalid {key}")
    for key in ("event_id", "title", "text"):
        if not isinstance(rec.get(key), str) or not rec[key].strip():
            raise ValueError(f"Missing {key}")
    if not isinstance(rec.get("ingest_ts"), (int, float)) or not math.isfinite(rec["ingest_ts"]):
        raise ValueError("Invalid ingest_ts")
    if rec.get("text_sha256") != text_hash(rec["text"]):
        raise ValueError("text_sha256 mismatch")
    return rec


def read_records(path):
    with open(path, encoding="utf-8") as source:
        for line_no, line in enumerate(source, 1):
            if line.strip():
                try:
                    yield validate_record(json.loads(line))
                except (ValueError, TypeError, KeyError) as exc:
                    raise ValueError(f"{path}:{line_no}: {exc}") from exc


def run_paths(run_id):
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,47}", run_id):
        raise ValueError("run-id must be 1-48 lowercase letters, digits, underscores or hyphens")
    root = Path("runs") / run_id
    return root, f"wiki_{run_id}", f"wiki-edits-{run_id}"


def load_embedder():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(MODEL, revision=MODEL_REVISION, device="cpu")
