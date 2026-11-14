"""Receiver storage contract tests (SQLite, no HTTP).

Run: python -I tests/test_receiver_store.py
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, "/workspace/receiver")

os.environ.setdefault("RECEIVER_DB_PATH", tempfile.mktemp(suffix=".db"))

from app.main import BatchConflict, BatchStore, content_hash  # noqa: E402


def make_content(batch_id="B-r1", status="partial", records=None):
    records = records if records is not None else [
        {"announcement_code": "A-1", "title": "t", "procurement_unit": "u",
         "deadline": "2026-01-01", "section": "招标公告", "detail_url": "http://x/1"}
    ]
    unfinished = [] if status == "completed" else [
        {"section": "招标公告", "page_type": "detail", "page": "A-2",
         "url": "http://x/2", "reason": "parse failed", "attempts": 3}
    ]
    content = {
        "batch_id": batch_id,
        "round_id": "r1",
        "status": status,
        "complete": status == "completed",
        "base_url": "http://site",
        "started_at": "2026-11-14T00:00:00+00:00",
        "finished_at": "2026-11-14T00:01:00+00:00",
        "records_count": len(records),
        "unfinished_count": len(unfinished),
        "records": records,
        "unfinished_pages": unfinished,
    }
    content["content_hash"] = content_hash(content)
    return content


class ReceiverStoreTests(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mktemp(suffix=".db")
        self.store = BatchStore(self.path)

    def tearDown(self):
        self.store._conn.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass

    def test_first_insert_then_replay_returns_same_receipt(self):
        content = make_content()
        status, receipt = self.store.receive(content)
        self.assertEqual(status, 201)
        status2, receipt2 = self.store.receive(dict(content))
        self.assertEqual(status2, 200)
        self.assertEqual(receipt, receipt2)
        self.assertEqual(len(self.store.list()), 1)
        # Original receipt id is generated once and never rewritten.
        self.assertEqual(self.store.get("B-r1")["receipt"], receipt)

    def test_same_id_different_content_is_rejected(self):
        self.store.receive(make_content())
        altered = make_content(records=[{"announcement_code": "A-9"}])
        altered["records_count"] = 1
        altered["content_hash"] = content_hash(altered)
        with self.assertRaises(BatchConflict):
            self.store.receive(altered)
        # Nothing was overwritten.
        stored = self.store.get("B-r1")
        self.assertEqual(stored["records_count"], 1)
        self.assertEqual(stored["content_hash"], make_content()["content_hash"])

    def test_partial_batch_stored_as_incomplete_with_unfinished_pages(self):
        status, _ = self.store.receive(make_content(status="partial"))
        self.assertEqual(status, 201)
        stored = self.store.get("B-r1")
        self.assertFalse(stored["complete"])
        self.assertEqual(stored["status"], "partial")
        self.assertEqual(stored["unfinished_count"], 1)
        self.assertEqual(stored["payload"]["unfinished_pages"][0]["page"], "A-2")
        self.assertFalse(stored["receipt"]["complete"])

    def test_receipt_survives_reopen(self):
        _, receipt = self.store.receive(make_content())
        self.store._conn.close()
        reopened = BatchStore(self.path)
        try:
            stored = reopened.get("B-r1")
            self.assertEqual(stored["receipt"], receipt)
            self.assertEqual(reopened.receive(make_content())[0], 200)
        finally:
            reopened._conn.close()

    def test_hash_changes_with_content(self):
        a = make_content()
        b = make_content(records=[{"announcement_code": "other"}])
        self.assertNotEqual(a["content_hash"], b["content_hash"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
