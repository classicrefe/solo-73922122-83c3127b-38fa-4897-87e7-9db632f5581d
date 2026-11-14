"""End-to-end delivery contract tests.

Runs the real receiver (uvicorn subprocess, SQLite) and the real collector
DeliveryManager over HTTP.  Covers:

* freezing is idempotent per round and immutable (new collection cannot
  change a pending batch);
* delivery is reported as done only after the stored receipt is verified;
* replay returns the original receipt and does not duplicate rows;
* same batch id + different content is rejected;
* network failure leaves the batch pending, and a restarted manager settles
  it automatically;
* commit-then-drop response is tolerated (verified, not duplicated).

Run: python -I tests/test_delivery_contract.py
"""

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

sys.path.insert(0, "/workspace/collector")

from app.delivery import (  # noqa: E402
    DELIVERY_DELIVERED,
    DELIVERY_PENDING,
    DELIVERY_REJECTED,
    DeliveryManager,
    DeliveryStore,
    content_hash,
)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_health(url: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"service at {url} did not become healthy")


def round_snapshot(status="partial", round_id="round1", unfinished=None):
    return {
        "round_id": round_id,
        "status": status,
        "base_url": "http://procurement-site:8080",
        "started_at": "2026-11-14T01:00:00+00:00",
        "finished_at": "2026-11-14T01:01:00+00:00",
        "unfinished": unfinished
        if unfinished is not None
        else [
            {
                "section": "招标公告",
                "page_type": "detail",
                "page": "ZB-2026-010",
                "url": "http://procurement-site:8080/notice/ZB-2026-010",
                "reason": "detail page missing required field(s): 采购单位, 截止日期",
                "attempts": 3,
            }
        ],
    }


RECORDS = [
    {
        "announcement_code": "ZB-2026-001",
        "title": "公告 1",
        "procurement_unit": "测试单位",
        "deadline": "2026-12-31 17:00",
        "section": "招标公告",
        "detail_url": "http://procurement-site:8080/notice/ZB-2026-001",
    },
    {
        "announcement_code": "ZB-2026-002",
        "title": "公告 2",
        "procurement_unit": "测试单位",
        "deadline": "2026-12-31 17:00",
        "section": "中标公告",
        "detail_url": "http://procurement-site:8080/notice/ZB-2026-002",
    },
]


class DeliveryContractTests(unittest.TestCase):
    receiver: subprocess.Popen | None = None
    receiver_url: str = ""
    db_paths: list[str] = []

    @classmethod
    def setUpClass(cls):
        cls.receiver_db = tempfile.mktemp(suffix=".db")
        cls.port = free_port()
        cls.receiver_url = f"http://127.0.0.1:{cls.port}"
        cls.receiver = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(cls.port), "--log-level", "warning"],
            cwd="/workspace/receiver",
            env={**os.environ, "RECEIVER_DB_PATH": cls.receiver_db},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        wait_health(cls.receiver_url + "/api/health")

    @classmethod
    def tearDownClass(cls):
        if cls.receiver is not None:
            try:
                cls.receiver.terminate()
                cls.receiver.wait(timeout=10)
            except (ProcessLookupError, PermissionError):
                pass

    def setUp(self):
        # Unique round ids keep tests isolated inside the shared receiver DB.
        self.round_suffix = f"{self._testMethodName}"
        self.db = tempfile.mktemp(suffix=".db")
        self.store = DeliveryStore(self.db)
        self.manager = DeliveryManager(self.store, self.receiver_url, retry_seconds=0.2)
        self._receiver_http("POST", "/api/admin/drop-response", {"enabled": False})

    def tearDown(self):
        self.store._conn.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.db + suffix)
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------------ utils
    def _receiver_http(self, method, path, body=None):
        import httpx

        async def go():
            async with httpx.AsyncClient(base_url=self.receiver_url, timeout=10) as client:
                return await client.request(method, path, json=body)

        return asyncio.run(go())

    def snapshot(self, status="partial", unfinished=None):
        return round_snapshot(status, f"round-{self.round_suffix}", unfinished)

    def _receiver_batches(self):
        return self._receiver_http("GET", "/api/batches").json()["batches"]

    # ------------------------------------------------------------------ tests
    def test_freeze_is_idempotent_and_immutable(self):
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        self.assertEqual(batch["delivery_state"], DELIVERY_PENDING)
        self.assertFalse(batch["complete"])

        # Repeated click: same batch id, same frozen content.
        again = self.manager.freeze(self.snapshot(), RECORDS)
        self.assertEqual(batch["batch_id"], again["batch_id"])
        self.assertEqual(batch["content_hash"], again["content_hash"])

        # A later collection round (new round id, different records) must not
        # alter the batch frozen for the earlier round.
        changed_records = RECORDS + [{"announcement_code": "ZB-2026-999"}]
        newer = self.manager.freeze(
            round_snapshot("partial", f"round-{self.round_suffix}-new"), changed_records
        )
        self.assertNotEqual(newer["batch_id"], batch["batch_id"])
        frozen = self.store.get(batch["batch_id"])
        self.assertEqual(frozen["payload"]["records"], RECORDS)
        self.assertEqual(newer["payload"]["records"], changed_records)
        self.assertEqual(frozen["payload"].get("content_hash"), None)
        self.assertEqual(frozen["content_hash"], batch["content_hash"])

    def test_delivery_verifies_stored_receipt(self):
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        result = asyncio.run(self.manager.deliver(batch["batch_id"]))
        self.assertEqual(result["delivery_state"], DELIVERY_DELIVERED)
        self.assertIsNotNone(result["delivered_at"])
        receipt = result["receipt"]
        self.assertEqual(receipt["batch_id"], batch["batch_id"])
        self.assertEqual(receipt["content_hash"], batch["content_hash"])
        self.assertEqual(receipt["records_count"], 2)

        stored = self._receiver_http("GET", f"/api/batches/{batch['batch_id']}").json()
        self.assertFalse(stored["complete"])
        self.assertEqual(stored["status"], "partial")
        self.assertEqual(len(stored["payload"]["records"]), 2)
        self.assertEqual(stored["payload"]["unfinished_pages"][0]["page"], "ZB-2026-010")
        self.assertEqual(stored["receipt"]["receipt_id"], receipt["receipt_id"])

    def test_replay_returns_original_receipt_without_duplicate(self):
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        payload = dict(batch["payload"], content_hash=batch["content_hash"])
        first = self._receiver_http("POST", "/api/batches", payload)
        second = self._receiver_http("POST", "/api/batches", payload)
        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json(), second.json())
        matching = [
            row for row in self._receiver_batches() if row["batch_id"] == batch["batch_id"]
        ]
        self.assertEqual(len(matching), 1)

    def test_same_batch_id_different_content_is_rejected(self):
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        # The receiver already holds this batch id with the original content.
        original = dict(batch["payload"], content_hash=batch["content_hash"])
        self.assertEqual(self._receiver_http("POST", "/api/batches", original).status_code, 201)

        # Simulate corrupted/different frozen content for the same batch id.
        tampered = dict(batch["payload"])
        tampered["records"] = RECORDS[:1]
        tampered["records_count"] = 1
        digest = content_hash(tampered)
        with self.store._conn:
            self.store._conn.execute(
                "UPDATE delivery_batches SET payload = ?, content_hash = ? WHERE batch_id = ?",
                (
                    json.dumps(tampered, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                    digest,
                    batch["batch_id"],
                ),
            )
        result = asyncio.run(self.manager.deliver(batch["batch_id"]))
        self.assertEqual(result["delivery_state"], DELIVERY_REJECTED)
        self.assertIn("different content", result["last_error"])
        # The receiver keeps the original row and stores nothing from the
        # conflicting submission.
        matching = [
            row for row in self._receiver_batches() if row["batch_id"] == batch["batch_id"]
        ]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["receipt"]["content_hash"], batch["content_hash"])

    def test_network_failure_stays_pending_and_resumes_after_restart(self):
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        dead = DeliveryManager(
            self.store, f"http://127.0.0.1:{free_port()}", retry_seconds=0.2, request_timeout=1.0
        )
        result = asyncio.run(dead.deliver(batch["batch_id"]))
        self.assertEqual(result["delivery_state"], DELIVERY_PENDING)
        self.assertIn("network failure", result["last_error"])
        self.assertEqual(result["attempts"], 1)
        # No receipt yet -> UI must not show "delivered".
        self.assertIsNone(result["receipt"])

        # Simulate process restart: a fresh manager over the same SQLite file
        # resumes the pending batch and verifies the stored receipt.
        restarted_store = DeliveryStore(self.db)
        restarted = DeliveryManager(restarted_store, self.receiver_url, retry_seconds=0.2)
        asyncio.run(restarted.deliver_pending_once())
        settled = restarted_store.get(batch["batch_id"])
        restarted_store._conn.close()
        self.assertEqual(settled["delivery_state"], DELIVERY_DELIVERED)
        self.assertIsNotNone(settled["receipt"])

    def test_dropped_response_is_verified_not_duplicated(self):
        self._receiver_http("POST", "/api/admin/drop-response", {"enabled": True})
        batch = self.manager.freeze(self.snapshot("completed", unfinished=[]), RECORDS)
        result = asyncio.run(self.manager.deliver(batch["batch_id"]))
        self.assertEqual(result["delivery_state"], DELIVERY_DELIVERED)
        stored = self._receiver_http("GET", f"/api/batches/{batch['batch_id']}").json()
        self.assertTrue(stored["complete"])
        matching = [
            row for row in self._receiver_batches() if row["batch_id"] == batch["batch_id"]
        ]
        self.assertEqual(len(matching), 1)

    def test_crash_before_confirmation_resumes(self):
        """Batch committed at the receiver, collector died before confirming."""
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        payload = dict(batch["payload"], content_hash=batch["content_hash"])
        # Commit directly at the receiver, as if the collector crashed right
        # after POST but before reading/verifying the response.
        self._receiver_http("POST", "/api/batches", payload)
        self.store.begin_attempt(batch["batch_id"])
        self.store.mark_pending(batch["batch_id"], "collector crashed before confirmation")
        self.assertEqual(self.store.get(batch["batch_id"])["delivery_state"], DELIVERY_PENDING)

        restarted_store = DeliveryStore(self.db)
        restarted = DeliveryManager(restarted_store, self.receiver_url, retry_seconds=0.2)
        asyncio.run(restarted.deliver_pending_once())
        settled = restarted_store.get(batch["batch_id"])
        restarted_store._conn.close()
        self.assertEqual(settled["delivery_state"], DELIVERY_DELIVERED)
        self.assertEqual(settled["receipt"]["batch_id"], batch["batch_id"])
        matching = [
            row for row in self._receiver_batches() if row["batch_id"] == batch["batch_id"]
        ]
        self.assertEqual(len(matching), 1)

    def test_reconcile_does_not_invent_delivery(self):
        batch = self.manager.freeze(self.snapshot(), RECORDS)
        result = asyncio.run(self.manager.reconcile(batch["batch_id"]))
        self.assertEqual(result["delivery_state"], DELIVERY_PENDING)
        self.assertIsNone(result["receipt"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
