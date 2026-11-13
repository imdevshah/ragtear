"""Dataset handoff must reject partial captures and preserve disjoint splits."""
import json
import tempfile
import unittest
from pathlib import Path

import prepare_dataset
from common import read_records
from test_pipeline import TestTokenizer, record


class DatasetTests(unittest.TestCase):
    def write_capture(self, folder, duplicate=False):
        rows = []
        for i in range(4):
            rows.append({**record(), "rev_id": 456 + (0 if duplicate else i),
                         "event_id": f"event-{i}"})
        path = Path(folder) / "capture.jsonl"
        path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        return path

    def test_splits_preserve_records_and_hashes(self):
        with tempfile.TemporaryDirectory() as folder:
            source = self.write_capture(folder)
            output = Path(folder) / "split"
            report = prepare_dataset.prepare(source, output, 4, 2, TestTokenizer())
            baseline = list(read_records(output / "baseline.jsonl"))
            incremental = list(read_records(output / "incremental.jsonl"))
            self.assertEqual(baseline + incremental, list(read_records(source)))
            self.assertFalse({r["rev_id"] for r in baseline} & {r["rev_id"] for r in incremental})
            self.assertEqual(report["sha256"], prepare_dataset.sha256(source))
            self.assertEqual(report["expected_vectors"], 4)
            self.assertEqual(report["splits"]["baseline"]["vectors"], 2)
            with self.assertRaises(FileExistsError):
                prepare_dataset.prepare(source, output, 4, 2, TestTokenizer())

    def test_partial_capture_has_no_handoff_output(self):
        with tempfile.TemporaryDirectory() as folder:
            source = self.write_capture(folder)
            output = Path(folder) / "split"
            with self.assertRaisesRegex(ValueError, "Expected 5 records"):
                prepare_dataset.prepare(source, output, 5, 2, TestTokenizer())
            self.assertFalse(output.exists())

    def test_duplicate_revisions_rejected_before_split(self):
        with tempfile.TemporaryDirectory() as folder:
            source = self.write_capture(folder, duplicate=True)
            output = Path(folder) / "split"
            with self.assertRaisesRegex(ValueError, "Duplicate revision"):
                prepare_dataset.prepare(source, output, 4, 2, TestTokenizer())
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
