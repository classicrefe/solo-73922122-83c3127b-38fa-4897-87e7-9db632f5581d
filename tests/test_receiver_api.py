"""Receiver HTTP contract tests via FastAPI's TestClient.

Run: python -I tests/test_receiver_api.py
"""

import itertools
import os
import sys
import tempfile
import unittest

sys.path.insert(0, "/workspace/receiver")

os.environ["RECEIVER_DB_PATH"] = tempfile.mktemp(suffix=".db")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import content_hash  # noqa: E402
from app import main as receiver_main  # noqa: E402


_ids = itertools.count(1)


def payload(batch_id=None, status="partial", records=None, unfinished=None):
    # The module-level store is shared, so every call gets a fresh batch id
    # unless the test explicitly reuses one.
    batch_id = batch_id or f"B-api-{next(_ids)}"
    records = records if records is not None else [{"announcement_code": "A-1"}]
    if unfinished is None:
        unfinished = [] if status == "completed" else [{"page": "A-2", "reason": "boom"}]
    body = {
        "batch_id": batch_id,
        "round_id": "r-" + batch_id,
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
    body["content_hash"] = content_hash(body)
    return body


class ReceiverApiTests(unittest.TestCase):
    def setUp(self):
        receiver_main.runtime["drop_response"] = False
        self.client = TestClient(receiver_main.app)

    def test_completed_batch_201_and_replay_200(self):
        body = payload(status="completed")
        first = self.client.post("/api/batches", json=body)
        second = self.client.post("/api/batches", json=body)
        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json(), second.json())
        self.assertTrue(first.json()["complete"])

        stored = self.client.get(f"/api/batches/{body['batch_id']}").json()
        self.assertEqual(stored["receipt"], first.json())
        self.assertTrue(stored["complete"])
        self.assertEqual(stored["payload"]["records"], body["records"])

    def test_partial_batch_marked_incomplete(self):
        body = payload(status="partial")
        response = self.client.post("/api/batches", json=body)
        self.assertEqual(response.status_code, 201)
        self.assertFalse(response.json()["complete"])
        stored = self.client.get(f"/api/batches/{body['batch_id']}").json()
        self.assertFalse(stored["complete"])
        self.assertEqual(stored["unfinished_count"], 1)
        self.assertEqual(stored["payload"]["unfinished_pages"][0]["page"], "A-2")

    def test_conflicting_content_409_and_nothing_written(self):
        body = payload()
        self.assertEqual(self.client.post("/api/batches", json=body).status_code, 201)
        conflicting = payload(batch_id=body["batch_id"], records=[{"announcement_code": "Z"}])
        response = self.client.post("/api/batches", json=conflicting)
        self.assertEqual(response.status_code, 409)
        stored = self.client.get(f"/api/batches/{body['batch_id']}").json()
        self.assertEqual(stored["payload"]["records"], body["records"])

    def test_hash_mismatch_400_and_nothing_written(self):
        body = payload(batch_id="B-api-hash")
        body["content_hash"] = "0" * 64
        response = self.client.post("/api/batches", json=body)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.get("/api/batches/B-api-hash").status_code, 404)

    def test_count_mismatch_400(self):
        body = payload(batch_id="B-api-count")
        body["records_count"] = 99
        body["content_hash"] = content_hash(body)
        self.assertEqual(self.client.post("/api/batches", json=body).status_code, 400)

    def test_complete_flag_contradiction_400(self):
        body = payload(batch_id="B-api-flag", status="partial")
        body["complete"] = True
        body["content_hash"] = content_hash(body)
        self.assertEqual(self.client.post("/api/batches", json=body).status_code, 400)

    def test_missing_fields_400(self):
        body = payload(batch_id="B-api-missing")
        del body["records"]
        body["content_hash"] = content_hash(body)
        self.assertEqual(self.client.post("/api/batches", json=body).status_code, 400)

    def test_listing_exposes_receipt_and_completeness(self):
        self.client.post("/api/batches", json=payload(batch_id="B-api-list"))
        listing = self.client.get("/api/batches").json()
        row = next(b for b in listing["batches"] if b["batch_id"] == "B-api-list")
        self.assertIn("receipt_id", row["receipt"])
        self.assertFalse(row["complete"])

    def test_batch_detail_page_shows_records_and_unfinished(self):
        body = payload()
        self.client.post("/api/batches", json=body)
        page = self.client.get(f"/batches/{body['batch_id']}")
        self.assertEqual(page.status_code, 200)
        html = page.text
        self.assertIn("不完整 partial", html)
        self.assertIn("A-1", html)
        self.assertIn("A-2", html)
        self.assertIn("boom", html)
        self.assertEqual(self.client.get("/batches/nope").status_code, 404)

    def test_drop_switch_toggle(self):
        self.assertEqual(self.client.get("/api/admin/drop-response").json(),
                         {"drop_response": False})
        self.assertEqual(
            self.client.post("/api/admin/drop-response", json={"enabled": True}).json(),
            {"drop_response": True},
        )
        self.assertTrue(self.client.get("/api/admin/drop-response").json()["drop_response"])
        self.assertEqual(
            self.client.post("/api/admin/drop-response", json={"enabled": "yes"}).status_code,
            400,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
