"""
Generate an answer-level evaluation set from recorded Wikipedia edits using a local Ollama model.

Each question is written from ONE chunk of a page revision, so the gold chunk id
({page_id}-{rev_id}-{chunk}) is known. For the CRD study, questions should come from
documents that are ingested DURING the concurrent-write phase, not from the preloaded
baseline index. Use --skip-first N to ignore the first N edits (the baseline preload).

  python make_questions.py --edits data/edits.jsonl --out data/questions.jsonl \
      --skip-first 2000 --n 300 --model llama3.1:8b

Output line:
  {"qid", "question", "answer", "gold_chunk_id", "page_id", "rev_id", "title", "edit_index"}

The question LLM is separate from the RAG answering LLM; pin both model versions and
use temperature 0 so the set is reproducible. Review a sample by hand before trusting it.
"""
import argparse
import json
import os
import random
import re

import requests
from pathlib import Path
from common import chunk_text, read_records

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")

PROMPT = """You write exam questions from a passage.

Passage (from the Wikipedia article "{title}"):
\"\"\"{passage}\"\"\"

Write ONE specific factual question that can be answered ONLY from this passage, plus its short answer.
Rules:
- The question must name its subject explicitly (no "this article", "the passage", or pronouns).
- The answer must be a short phrase or number stated in the passage.
- Do not ask about trivia that is not in the passage.
Reply with JSON only: {{"question": "...", "answer": "..."}}"""


def ask(model, title, passage, timeout=120):
    r = requests.post(f"{OLLAMA_URL}/api/generate", timeout=timeout, json={
        "model": model,
        "prompt": PROMPT.format(title=title, passage=passage),
        "stream": False,
        "format": "json",
        "options": {"temperature": 0, "seed": 42},
    })
    r.raise_for_status()
    obj = json.loads(r.json()["response"])
    q, a = str(obj.get("question", "")).strip(), str(obj.get("answer", "")).strip()
    return q, a


def valid(q, a, passage):
    if not q.endswith("?") or len(q.split()) < 5 or not a:
        return False
    if re.search(r"\b(this article|the passage|the text|this passage)\b", q, re.I):
        return False
    # the answer should actually appear in the passage (case-insensitive substring)
    return a.lower() in passage.lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--edits", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--skip-first", type=int, default=0,
                    help="ignore the first N edits (baseline preload)")
    ap.add_argument("--model", default=os.getenv("QUESTION_MODEL", "llama3.1:8b"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-chunk-words", type=int, default=80)
    args = ap.parse_args()

    candidates = []
    if args.n <= 0 or args.skip_first < 0:
        ap.error("n must be positive and skip-first must be nonnegative")
    for idx, rec in enumerate(read_records(args.edits)):
        if idx < args.skip_first:
            continue
        chunks = chunk_text(rec.get("text") or "")
        usable = [(j, c) for j, c in enumerate(chunks) if len(c.split()) >= args.min_chunk_words]
        if usable:
            candidates.append((idx, rec, usable))
    if not candidates:
        raise SystemExit("no usable edits after filtering; record more data or lower --skip-first")

    rng = random.Random(args.seed)
    rng.shuffle(candidates)

    seen_pages, made, tried = set(), 0, 0
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "x", encoding="utf-8") as out:
        for idx, rec, usable in candidates:
            if made >= args.n:
                break
            if rec["page_id"] in seen_pages:  # one question per page keeps questions independent
                continue
            j, passage = rng.choice(usable)
            tried += 1
            try:
                q, a = ask(args.model, rec["title"], passage)
            except Exception as e:
                print(f"[warn] ollama call failed: {e}")
                continue
            if not valid(q, a, passage):
                continue
            seen_pages.add(rec["page_id"])
            out.write(json.dumps({
                "qid": f"q{made:04d}",
                "question": q,
                "answer": a,
                "gold_chunk_id": f"{rec['page_id']}-{rev_id(rec)}-{j}",
                "page_id": rec["page_id"],
                "rev_id": rec["rev_id"],
                "title": rec["title"],
                "edit_index": idx,
            }) + "\n")
            out.flush()
            made += 1
            if made % 25 == 0:
                print(f"[questions] {made}/{args.n} (tried {tried})")
    print(f"done: {made} questions written to {args.out} (accepted {made}/{tried} attempts)")
    if made < args.n:
        raise SystemExit("Insufficient accepted questions; review the partial output and collect more data")


def rev_id(rec):
    return rec["rev_id"]


if __name__ == "__main__":
    main()
