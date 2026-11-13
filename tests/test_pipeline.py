"""Regression tests for identity, retry safety, and strict verification."""
import json
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import common
import stream_job
import verify
import wiki_producer


def record(text="A reliable test document about astronomy."):
    return {"schema_version": 2, "page_id": 123, "rev_id": 456,
            "edit_ts": 1700000000, "event_id": "event-1", "title": "Astronomy",
            "text": text, "ingest_ts": 1700000001.0, "text_sha256": common.text_hash(text)}


class TestTokenizer:
    """Small offline tokenizer double; real-model coverage is checked in integration."""
    def num_special_tokens_to_add(self, pair=False):
        return 2

    def __call__(self, text, **kwargs):
        if isinstance(text, list):
            return {"length": [len(re.findall(r"\w+|[^\w\s]", item)) + 2 for item in text]}
        return {"offset_mapping": [m.span() for m in re.finditer(r"\w+|[^\w\s]", text)]}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tokenizer = TestTokenizer()
    def test_exact_revision_and_real_page_id(self):
        session = Mock()
        session.get.return_value.json.return_value = {"query": {"pages": [{
            "pageid": 123, "title": "Astronomy", "revisions": [{
                "revid": 456, "timestamp": "2026-11-12T10:00:00Z",
                "slots": {"main": {"contentmodel": "wikitext",
                                   "content": "Astronomy studies [[stars]]."}}}]}]}}
        result = wiki_producer.fetch_revision(456, session)
        change = {"id": 999999, "revision": {"new": 456}, "title": "Astronomy",
                  "meta": {"id": "test-event"}}
        rec = wiki_producer.to_record(change, result)
        self.assertEqual(rec["page_id"], 123)
        self.assertEqual(rec["rc_id"], 999999)
        self.assertEqual(rec["text"], "Astronomy studies stars.")
        params = session.get.call_args.kwargs["params"]
        self.assertEqual(params["revids"], 456)
        self.assertNotIn("pageids", params)

    def test_revision_mismatch_rejected(self):
        session = Mock()
        session.get.return_value.json.return_value = {"query": {"pages": [{
            "pageid": 123, "revisions": [{"revid": 789}]}]}}
        with self.assertRaisesRegex(ValueError, "Revision mismatch"):
            wiki_producer.fetch_revision(456, session)

    def test_dataset_integrity_and_legacy_rejected(self):
        bad = record()
        bad["text"] = "Modified without updating hash"
        with self.assertRaisesRegex(ValueError, "sha256"):
            common.validate_record(bad)
        bad = record()
        del bad["schema_version"]
        with self.assertRaisesRegex(ValueError, "re-record"):
            common.validate_record(bad)

    def test_chunks_cover_tail_and_validate_overlap(self):
        words = [str(i) for i in range(401)]
        chunks = common.chunk_text(" ".join(words), tokenizer=self.tokenizer)
        self.assertEqual(chunks[-1].split()[-1], "400")
        self.assertEqual(chunks[0].split()[-40:], chunks[1].split()[:40])
        with self.assertRaises(ValueError):
            common.chunk_text("text", size=10, overlap=10, tokenizer=self.tokenizer)

    def test_chunks_preserve_unicode_and_respect_token_limit(self):
        text = ("Caf\u00e9, stars! " * 100) + "END"
        chunks = common.chunk_text(text, tokenizer=self.tokenizer)
        self.assertTrue(chunks[-1].endswith("END"))
        self.assertTrue(all(c in text for c in chunks))
        self.assertTrue(all(n <= 202 for n in self.tokenizer(chunks)["length"]))
        with self.assertRaisesRegex(ValueError, "256-token"):
            common.chunk_text(text, size=255, tokenizer=self.tokenizer)

    def test_duplicate_records_are_upserted_once_in_batch(self):
        row = {"value": json.dumps(record())}
        ids, docs, _ = stream_job.build_payload([row, row], 0, "test", self.tokenizer)
        self.assertEqual(ids, ["123-456-0"])
        self.assertEqual(len(docs), 1)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            stream_job.build_payload([row, {"value": json.dumps(record("Different content"))}],
                                     0, "test", self.tokenizer)

    def test_failed_kafka_send_propagates(self):
        producer = Mock()
        producer.send.return_value.get.side_effect = RuntimeError("broker rejected")
        args = SimpleNamespace(file="unused", skip=0, limit=0, rate=0, topic="test")
        with patch.object(wiki_producer, "read_records", return_value=iter([record()])), \
             patch.object(wiki_producer, "make_producer", return_value=producer):
            with self.assertRaisesRegex(RuntimeError, "broker rejected"):
                wiki_producer.run_replay(args)
        producer.close.assert_called_once()

    def test_batch_retry_and_failed_sink_logging(self):
        with tempfile.TemporaryDirectory() as folder:
            model, sink = Mock(), Mock()
            model.max_seq_length = 256
            model.tokenizer = self.tokenizer
            model.encode.return_value.tolist.return_value = [[1.0, 0.0]]
            df = Mock()
            df.collect.return_value = [{"value": json.dumps(record()), "topic": "test",
                                        "partition": 0, "offset": 0}]
            writer = stream_job.make_batch_writer(folder, "test", model, sink)
            sink.upsert.side_effect = RuntimeError("write failed")
            with self.assertRaises(RuntimeError):
                writer(df, 0)
            self.assertFalse(list(Path(folder).rglob("*.json")))
            sink.upsert.side_effect = None
            writer(df, 0)
            writer(df, 0)
            self.assertEqual(len(list((Path(folder) / "batches").glob("*.json"))), 1)
            batches = verify.load_batches(Path(folder), "test")
            self.assertEqual(len(verify.expected_vectors(batches)), 1)

    def test_verifier_rejects_missing_vectors_and_unreachable_server(self):
        batch = {"batch_id": 0, "vector_ids": ["123-456-0"],
                 "text_hashes": {"123-456-0": common.text_hash(record()["text"])},
                 "n_events": 1}
        with patch.object(verify, "load_batches", return_value=[batch]), \
             patch.object(verify, "connect", side_effect=ConnectionError("unreachable")):
            with self.assertRaises(ConnectionError):
                verify.verify("test")
        client = Mock()
        client.get_collection.return_value.get.return_value = {"ids": []}
        with patch.object(verify, "load_batches", return_value=[batch]), \
             patch.object(verify, "connect", return_value=client):
            with self.assertRaisesRegex(ValueError, "Chroma does not match"):
                verify.verify("test")

    def test_writer_rejects_truncation_before_writing(self):
        with tempfile.TemporaryDirectory() as folder:
            model, sink, df = Mock(), Mock(), Mock()
            model.tokenizer = self.tokenizer
            model.max_seq_length = 3
            df.collect.return_value = [{"value": json.dumps(record()), "topic": "test",
                                        "partition": 0, "offset": 0}]
            with self.assertRaisesRegex(ValueError, "refusing silent truncation"):
                stream_job.make_batch_writer(folder, "test", model, sink)(df, 0)
            sink.upsert.assert_not_called()
            model.encode.assert_not_called()
            self.assertFalse(list(Path(folder).rglob("*.json")))

    def test_replay_metadata_uses_last_batch(self):
        rows = [{"batch_id": i, "vector_ids": ["x"], "text_hashes": {"x": "digest"}}
                for i in [0, 1]]
        self.assertEqual(verify.expected_vectors(rows), {"x": (1, "digest")})

    def test_run_id_cannot_escape_workspace(self):
        for name in ("../other", "x/y", "UPPER", ""):
            with self.assertRaises(ValueError):
                common.run_paths(name)


if __name__ == "__main__":
    unittest.main()
