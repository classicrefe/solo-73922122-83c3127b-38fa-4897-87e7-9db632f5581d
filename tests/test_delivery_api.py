"""Full-stack delivery test: real procurement site + collector + receiver.

Starts the three services the same way docker-compose does (uvicorn in
subprocesses, SQLite files in temp dirs) and drives the collector's HTTP API,
so the browser-visible flow is what is being verified:

* only completed/partial rounds can be delivered; repeated clicks reuse the
  frozen batch;
* partial batches arrive at the receiver marked incomplete with their
  unfinished pages;
* a new collection round does not change an existing batch;
* the receiver's drop-response switch cannot make the collector lie;
* a pending batch is resumed automatically after a collector restart.

Run: python -I tests/test_delivery_api.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT = "/workspace"
RUNNER = f"{ROOT}/tests/_service_runner.py"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(method: str, url: str, body=None, timeout=10.0):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw) if raw else None


def wait_health(url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = http("GET", url, timeout=1.0)
            if status == 200:
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"service at {url} did not become healthy")


class Service:
    """A uvicorn subprocess stopped via a sentinel file (no signals needed)."""

    def __init__(self, package_dir: str, module: str, port: int, env: dict):
        self.stop_file = tempfile.mktemp(suffix=".stop")
        self.proc = subprocess.Popen(
            [sys.executable, "-I", RUNNER, str(port), package_dir, module, self.stop_file],
            env={**os.environ, **env},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def stop(self) -> None:
        open(self.stop_file, "w").close()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)


class FullStackDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.site_port = free_port()
        cls.receiver_port = free_port()
        cls.collector_port = free_port()
        cls.site_url = f"http://127.0.0.1:{cls.site_port}"
        cls.receiver_url = f"http://127.0.0.1:{cls.receiver_port}"
        cls.collector_url = f"http://127.0.0.1:{cls.collector_port}"
        cls.collector_db = tempfile.mktemp(suffix=".db")
        cls.receiver_db = tempfile.mktemp(suffix=".db")

        cls.site = Service(
            f"{ROOT}/site", "app.main:app", cls.site_port,
            {"SITE_RATE_ENABLED": "false", "SITE_LATENCY_MS": "0"},
        )
        cls.receiver = Service(
            f"{ROOT}/receiver", "app.main:app", cls.receiver_port,
            {"RECEIVER_DB_PATH": cls.receiver_db},
        )
        cls.collector_env = {
            "TARGET_BASE_URL": cls.site_url,
            "RECEIVER_BASE_URL": cls.receiver_url,
            "COLLECTOR_DB_PATH": cls.collector_db,
            "DELIVERY_RETRY_SECONDS": "1",
            "DELIVERY_REQUEST_TIMEOUT": "3",
        }
        cls.collector = Service(
            f"{ROOT}/collector", "app.main:app", cls.collector_port, cls.collector_env
        )

        try:
            wait_health(cls.site_url + "/healthz")
            wait_health(cls.receiver_url + "/api/health")
            wait_health(cls.collector_url + "/api/health")
        except Exception:
            cls._kill_all()
            raise

    @classmethod
    def _kill_all(cls):
        for attr in ("collector", "receiver", "site"):
            service = getattr(cls, attr, None)
            if service is not None:
                service.stop()

    @classmethod
    def tearDownClass(cls):
        cls._kill_all()

    # ------------------------------------------------------------------ utils
    def run_round(self) -> dict:
        status, body = http("POST", self.collector_url + "/api/rounds")
        self.assertEqual(status, 200, body)
        round_id = body["round_id"]
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            _, state = http("GET", self.collector_url + "/api/state")
            if state["status"] in ("completed", "partial", "stopped") and state["round_id"] == round_id:
                return state
            time.sleep(0.2)
        self.fail("round did not finish")

    def receiver_batches(self):
        return http("GET", self.receiver_url + "/api/batches")[1]["batches"]

    def set_drop(self, enabled: bool) -> None:
        http("POST", self.receiver_url + "/api/admin/drop-response", {"enabled": enabled})

    def start_collector(self) -> Service:
        start_health = self.collector_url + "/api/health"
        service = Service(
            f"{ROOT}/collector", "app.main:app", self.collector_port, self.collector_env
        )
        wait_health(start_health)
        return service

    def setUp(self):
        self.set_drop(False)

    # ------------------------------------------------------------------ tests
    def test_delivery_end_to_end(self):
        # Not deliverable before a round finishes.
        status, _ = http("POST", self.collector_url + "/api/deliveries")
        self.assertEqual(status, 404)

        state = self.run_round()
        self.assertEqual(state["status"], "partial")
        self.assertEqual(len(state["unfinished"]), 1)

        status, batch = http("POST", self.collector_url + "/api/deliveries")
        self.assertEqual(status, 200, batch)
        self.assertEqual(round(batch["attempts"]), 1)
        batches_before = len(self.receiver_batches())
        self.assertEqual(batch["delivery_state"], "delivered")
        self.assertFalse(batch["complete"])
        self.assertEqual(batch["round_status"], "partial")
        self.assertEqual(batch["records_count"], 35)
        self.assertEqual(batch["unfinished_count"], 1)
        self.assertIsNotNone(batch["receipt"])

        # Whole batch + completeness flag + receipt are stored together.
        stored = http("GET", self.receiver_url + f"/api/batches/{batch['batch_id']}")[1]
        self.assertFalse(stored["complete"])
        self.assertEqual(stored["payload"]["records_count"], 35)
        self.assertEqual(stored["payload"]["unfinished_pages"][0]["page"], "ZB-2026-010")
        self.assertEqual(stored["receipt"], batch["receipt"])

        # Repeated click: same batch, same receipt, still one stored row and
        # no new delivery attempt.
        status, again = http("POST", self.collector_url + "/api/deliveries")
        self.assertEqual(status, 200)
        self.assertEqual(again["batch_id"], batch["batch_id"])
        self.assertEqual(again["receipt"], batch["receipt"])
        self.assertEqual(again["attempts"], batch["attempts"])
        rows = [r for r in self.receiver_batches() if r["batch_id"] == batch["batch_id"]]
        self.assertEqual(len(rows), 1)

        # A new collection round does not alter the frozen batch.
        new_state = self.run_round()
        self.assertNotEqual(new_state["round_id"], state["round_id"])
        old = http("GET", self.collector_url + f"/api/deliveries/{batch['batch_id']}")[1]
        self.assertEqual(old["content_hash"], batch["content_hash"])
        self.assertEqual(old["receipt"], batch["receipt"])
        self.assertEqual(old["delivery_state"], "delivered")
        frozen = http("GET", self.collector_url + f"/api/deliveries/{batch['batch_id']}/content")[1]
        self.assertEqual(frozen["records_count"], 35)
        self.assertEqual(frozen["round_id"], state["round_id"])

        # New round is a distinct batch; the delivered one is untouched.
        status, second = http("POST", self.collector_url + "/api/deliveries")
        self.assertEqual(status, 200)
        self.assertNotEqual(second["batch_id"], batch["batch_id"])
        self.assertEqual(second["delivery_state"], "delivered")
        self.assertEqual(len(self.receiver_batches()), batches_before + 1)

        # State/list endpoints back the browser UI.
        listing = http("GET", self.collector_url + "/api/deliveries")[1]
        self.assertEqual(listing["count"], len(self.receiver_batches()))
        self.assertEqual(http("GET", self.collector_url + "/api/deliveries/pending")[1]["count"], 0)

    def test_dropped_response_is_confirmed_through_receipt(self):
        self.run_round()
        self.set_drop(True)
        status, batch = http("POST", self.collector_url + "/api/deliveries", timeout=30)
        self.assertEqual(status, 200, batch)
        # The POST response was lost, yet the batch must be reported delivered
        # only because the stored receipt was read back and matched.
        self.assertEqual(batch["delivery_state"], "delivered")
        stored = http("GET", self.receiver_url + f"/api/batches/{batch['batch_id']}")[1]
        self.assertEqual(stored["receipt"]["receipt_id"], batch["receipt"]["receipt_id"])
        self.assertEqual(stored["content_hash"], batch["content_hash"])

    def test_pending_batch_survives_collector_restart(self):
        self.run_round()

        # Receiver goes away: delivery fails and stays pending, not delivered.
        self.receiver.stop()
        try:
            status, batch = http("POST", self.collector_url + "/api/deliveries", timeout=30)
            self.assertEqual(status, 200, batch)
            self.assertEqual(batch["delivery_state"], "pending")
            self.assertIsNone(batch["receipt"])
            self.assertIn("network failure", batch["last_error"])

            # Restart the collector; the frozen batch is still pending there.
            self.collector.stop()
            type(self).collector = self.start_collector()
            persisted = http("GET", self.collector_url + f"/api/deliveries/{batch['batch_id']}")[1]
            self.assertEqual(persisted["delivery_state"], "pending")
            self.assertEqual(persisted["content_hash"], batch["content_hash"])
        finally:
            type(self).receiver = Service(
                f"{ROOT}/receiver", "app.main:app", self.receiver_port,
                {"RECEIVER_DB_PATH": self.receiver_db},
            )
            wait_health(self.receiver_url + "/api/health")

        # The resume loop settles the batch without another click.
        deadline = time.monotonic() + 30
        row = persisted
        while time.monotonic() < deadline:
            row = http("GET", self.collector_url + f"/api/deliveries/{batch['batch_id']}")[1]
            if row["delivery_state"] == "delivered":
                break
            time.sleep(0.5)
        self.assertEqual(row["delivery_state"], "delivered")
        self.assertIsNotNone(row["receipt"])
        rows = [r for r in self.receiver_batches() if r["batch_id"] == batch["batch_id"]]
        self.assertEqual(len(rows), 1)



if __name__ == "__main__":
    unittest.main(verbosity=2)
