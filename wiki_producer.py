"""Record exact Wikipedia revisions, or publish acknowledged records to Kafka."""
import argparse
import json
import os
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import read_records, text_hash, validate_record

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
API_URL = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": os.getenv(
    "WIKI_UA", "AMS560-Group4-Ingestion/1.0 (Stony Brook University course project)")}
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "6000"))


def api_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
    return session


def api_get(session, params):
    response = session.get(API_URL, params={"format": "json", "formatversion": 2,
                                          "maxlag": 5, **params}, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise ValueError(f"MediaWiki API error: {payload['error']}")
    return payload


def make_producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                         key_serializer=lambda k: k.encode("utf-8"),
                         linger_ms=5, acks="all", retries=3,
                         max_in_flight_requests_per_connection=1)


def keep(change):
    return (change.get("wiki") == "enwiki" and change.get("type") == "edit"
            and change.get("namespace") == 0 and not change.get("bot", False)
            and not change.get("minor", False))


def fetch_revision(rev_id, session):
    """Get the page identity AND content from the requested revision, never rcid."""
    import mwparserfromhell
    payload = api_get(session, {"action": "query", "prop": "revisions",
                               "revids": rev_id, "rvslots": "main",
                               "rvprop": "ids|timestamp|content|contentmodel"})
    pages = payload.get("query", {}).get("pages", [])
    if len(pages) != 1 or "missing" in pages[0]:
        raise ValueError(f"Unavailable revision {rev_id}")
    page = pages[0]
    revisions = page.get("revisions", [])
    if len(revisions) != 1 or revisions[0].get("revid") != rev_id:
        raise ValueError(f"Revision mismatch for {rev_id}")
    revision = revisions[0]
    slot = revision.get("slots", {}).get("main", {})
    if slot.get("contentmodel") != "wikitext" or not slot.get("content"):
        raise ValueError(f"Hidden, empty, or non-wikitext revision {rev_id}")
    # Templates are omitted, not expanded using their current versions.
    text = mwparserfromhell.parse(slot["content"]).strip_code(normalize=True, collapse=True)
    text = " ".join(text.split())
    if MAX_TEXT_CHARS > 0:
        text = text[:MAX_TEXT_CHARS].strip()
    if not text:
        raise ValueError(f"No usable text for revision {rev_id}")
    return {"page_id": page["pageid"], "title": page["title"], "rev_id": rev_id,
            "edit_ts": int(datetime.fromisoformat(revision["timestamp"].replace("Z", "+00:00")).timestamp()),
            "text": text}


def to_record(change, revision):
    if revision["rev_id"] != change["revision"]["new"]:
        raise ValueError("Event and fetched revision disagree")
    return validate_record({"schema_version": 2, **revision,
                            "event_id": change["meta"]["id"],
                            "rc_id": change.get("id"), "event_title": change["title"],
                            "user": change.get("user"), "ingest_ts": time.time(),
                            "text_sha256": text_hash(revision["text"]),
                            "source_url": f"https://en.wikipedia.org/w/index.php?oldid={revision['rev_id']}"})


def sse_events(deadline=None):
    last_id = None
    while deadline is None or time.monotonic() < deadline:
        try:
            headers = {**HEADERS, **({"Last-Event-ID": last_id} if last_id else {})}
            with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 30)) as resp:
                resp.raise_for_status()
                resp.encoding = "utf-8"
                data, event_id = [], None
                for line in resp.iter_lines(decode_unicode=True):
                    if deadline is not None and time.monotonic() >= deadline:
                        return
                    if not line:
                        if data:
                            try:
                                yield json.loads("\n".join(data))
                                last_id = event_id or last_id
                            except json.JSONDecodeError:
                                pass
                        data, event_id = [], None
                    elif line.startswith("data:"):
                        data.append(line[5:].lstrip())
                    elif line.startswith("id:"):
                        event_id = line[3:].lstrip()
        except requests.RequestException as exc:
            print(f"[warn] stream disconnected: {exc}; reconnecting", flush=True)
            time.sleep(3)


def poll_events(session, deadline=None):
    """Bounded smoke-test alternative when a network blocks long-lived SSE."""
    seen, order = set(), deque()
    while deadline is None or time.monotonic() < deadline:
        payload = api_get(session, {"action": "query", "list": "recentchanges",
                                   "rcnamespace": 0, "rctype": "edit", "rcshow": "!bot|!minor",
                                   "rcprop": "title|ids|timestamp|user|flags", "rclimit": 100})
        for row in reversed(payload["query"]["recentchanges"]):
            if row["rcid"] in seen:
                continue
            seen.add(row["rcid"])
            order.append(row["rcid"])
            if len(order) > 10000:
                seen.remove(order.popleft())
            yield {"wiki": "enwiki", "type": "edit", "namespace": 0,
                   "id": row["rcid"], "title": row["title"], "user": row.get("user"),
                   "revision": {"new": row["revid"]},
                   "meta": {"id": f"enwiki-revision-{row['revid']}"}}
        time.sleep(2)


def publish(producer, topic, record):
    # Future.get propagates broker errors; flush alone does not check each send.
    return producer.send(topic, key=str(record["page_id"]), value=record)


def run_live(args):
    if args.record_only and not args.record:
        raise ValueError("--record-only requires --record")
    if args.record:
        Path(args.record).parent.mkdir(parents=True, exist_ok=True)
    out = open(args.record, "x", encoding="utf-8") if args.record else None
    producer = None
    sent, failed, seen = 0, 0, set()
    deadline = time.monotonic() + args.max_seconds if args.max_seconds else None
    try:
        producer = None if args.record_only else make_producer()
        with api_session() as session:
            events = sse_events(deadline) if args.source == "sse" else poll_events(session, deadline)
            for change in events:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                if not keep(change):
                    continue
                rev_id = change.get("revision", {}).get("new")
                if not rev_id or rev_id in seen:
                    continue
                try:
                    record = to_record(change, fetch_revision(rev_id, session))
                except (requests.RequestException, ValueError, KeyError) as exc:
                    failed += 1
                    print(f"[warn] skipping revision {rev_id}: {exc}", flush=True)
                    continue
                if producer:
                    publish(producer, args.topic, record).get(timeout=30)
                if out:
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    out.flush()
                seen.add(rev_id)
                sent += 1
                print(f"[live] accepted={sent} page={record['page_id']} revision={rev_id}", flush=True)
                if args.max_events and sent >= args.max_events:
                    break
                time.sleep(args.min_interval)
    finally:
        if producer:
            producer.close(timeout=30)
        if out:
            out.close()
        print(f"[live] accepted={sent} skipped={failed}", flush=True)
    if sent == 0 or (args.max_events and sent < args.max_events):
        raise RuntimeError(f"Only collected {sent}/{args.max_events or 'requested'} records")


def run_replay(args):
    records = list(read_records(args.file))  # Validate before sending anything.
    records = records[args.skip:args.skip + args.limit if args.limit else None]
    if not records:
        raise ValueError("No records selected for replay")
    producer = make_producer()
    pending = deque()
    start = time.monotonic()
    acknowledged = 0
    try:
        for index, original in enumerate(records):
            if args.rate:
                time.sleep(max(0, start + index / args.rate - time.monotonic()))
            record = {**original, "ingest_ts": time.time()}
            pending.append(publish(producer, args.topic, record))
            if len(pending) >= 100:
                pending.popleft().get(timeout=30)
                acknowledged += 1
        while pending:
            pending.popleft().get(timeout=30)
            acknowledged += 1
    finally:
        producer.close(timeout=30)
    elapsed = time.monotonic() - start
    print(f"[replay] acknowledged={acknowledged} elapsed_s={elapsed:.3f} "
          f"actual_events_per_s={acknowledged / max(elapsed, 1e-9):.2f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    live = sub.add_parser("live")
    live.add_argument("--record")
    live.add_argument("--record-only", action="store_true")
    live.add_argument("--source", choices=["sse", "poll"], default="sse")
    live.add_argument("--max-events", type=int, default=50)
    live.add_argument("--max-seconds", type=float, default=600)
    live.add_argument("--min-interval", type=float, default=0.2)
    live.set_defaults(func=run_live)
    replay = sub.add_parser("replay")
    replay.add_argument("--file", required=True)
    replay.add_argument("--rate", type=float, default=50)
    replay.add_argument("--skip", type=int, default=0)
    replay.add_argument("--limit", type=int, default=0)
    replay.set_defaults(func=run_replay)
    for p in (live, replay):
        p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "wiki-edits"))
    args = parser.parse_args()
    for name in ("rate", "skip", "limit", "max_events", "max_seconds", "min_interval"):
        if hasattr(args, name) and getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} must be nonnegative")
    args.func(args)


if __name__ == "__main__":
    main()
