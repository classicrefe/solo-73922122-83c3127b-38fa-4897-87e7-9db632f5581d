"""Delivery of frozen collection rounds to the local analysis receiver.

Design contract
---------------

* A batch is frozen from a *finished* round whose status is ``completed`` or
  ``partial``.  Freezing serialises the records and the unfinished-page list
  into SQLite, so a later collection round can never alter data that is
  waiting for delivery.
* The batch id is derived from the round id, and the round id is unique in the
  collector database: repeated clicks, retries and process restarts therefore
  reuse exactly the same batch and the same bytes.
* Delivery state is only reported as ``delivered`` after the receiver's
  *stored receipt* for that batch id has been read back and its content hash
  matches this batch.  A POST that commits at the receiver but whose response
  is lost, or that fails on the network, leaves the batch ``pending``; the
  background resume loop and the retry button can settle it later.
* A receiver ``409`` (same batch id, different content) is permanent and is
  recorded as ``rejected``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx

DELIVERY_PENDING = "pending"
DELIVERY_DELIVERED = "delivered"
DELIVERY_REJECTED = "rejected"

DEFAULT_RETRY_SECONDS = 5.0


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value) -> str:
    """Stable JSON encoding shared with the receiver's hash computation."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(content: dict) -> str:
    return hashlib.sha256(canonical_json(content).encode("utf-8")).hexdigest()


def build_batch_content(
    *,
    batch_id: str,
    round_snapshot: dict,
    records: list[dict],
    unfinished_pages: list[dict],
) -> dict:
    """Canonical, immutable content of a frozen round.

    Both the collector and the receiver compute the hash over exactly these
    fields (see ``README.md``); adding a field here changes the hash and must
    be mirrored in ``receiver/app/main.py``.
    """
    status = round_snapshot["status"]
    return {
        "batch_id": batch_id,
        "round_id": round_snapshot["round_id"],
        "status": status,
        "complete": status == "completed",
        "base_url": round_snapshot["base_url"],
        "started_at": round_snapshot.get("started_at"),
        "finished_at": round_snapshot.get("finished_at"),
        "records_count": len(records),
        "unfinished_count": len(unfinished_pages),
        "records": records,
        "unfinished_pages": unfinished_pages,
    }


class DeliveryStore:
    """SQLite persistence for frozen batches (collector side)."""

    def __init__(self, path: str):
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS delivery_batches (
                    batch_id        TEXT PRIMARY KEY,
                    round_id        TEXT NOT NULL UNIQUE,
                    round_status    TEXT NOT NULL,
                    complete        INTEGER NOT NULL,
                    content_hash    TEXT NOT NULL,
                    source_url      TEXT NOT NULL,
                    payload         TEXT NOT NULL,
                    created_at      TEXT NOT NULL,
                    delivery_state  TEXT NOT NULL,
                    attempts        INTEGER NOT NULL DEFAULT 0,
                    last_error      TEXT,
                    last_attempt_at TEXT,
                    delivered_at    TEXT,
                    receipt         TEXT
                )
                """
            )

    # ------------------------------------------------------------------ reads
    def get(self, batch_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM delivery_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def get_by_round(self, round_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM delivery_batches WHERE round_id = ?", (round_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def list_batches(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM delivery_batches ORDER BY created_at DESC, batch_id"
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def pending_batch_ids(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT batch_id FROM delivery_batches "
                "WHERE delivery_state = ? ORDER BY created_at",
                (DELIVERY_PENDING,),
            ).fetchall()
        return [row["batch_id"] for row in rows]

    # ----------------------------------------------------------------- writes
    def create_if_absent(
        self,
        *,
        batch_id: str,
        round_id: str,
        round_status: str,
        complete: bool,
        content_hash_value: str,
        source_url: str,
        payload: dict,
    ) -> dict:
        """Freeze a batch, or return the existing one for this round.

        The unique constraint on ``round_id`` plus the transaction makes
        repeated clicks for the same round converge on one immutable batch.
        """
        payload_text = canonical_json(payload)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM delivery_batches WHERE round_id = ?", (round_id,)
            ).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO delivery_batches (
                        batch_id, round_id, round_status, complete, content_hash,
                        source_url, payload, created_at, delivery_state, attempts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        batch_id,
                        round_id,
                        round_status,
                        1 if complete else 0,
                        content_hash_value,
                        source_url,
                        payload_text,
                        utcnow(),
                        DELIVERY_PENDING,
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM delivery_batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
            elif row["content_hash"] != content_hash_value:
                # Should be impossible: a round's frozen content never changes.
                raise RuntimeError(
                    f"round {round_id} already frozen with different content"
                )
        return self._row_to_dict(row)

    def begin_attempt(self, batch_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE delivery_batches SET attempts = attempts + 1, "
                "last_attempt_at = ? WHERE batch_id = ?",
                (utcnow(), batch_id),
            )

    def mark_delivered(self, batch_id: str, receipt: dict) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE delivery_batches SET delivery_state = ?, last_error = NULL, "
                "delivered_at = ?, receipt = ? WHERE batch_id = ?",
                (
                    DELIVERY_DELIVERED,
                    utcnow(),
                    canonical_json(receipt),
                    batch_id,
                ),
            )

    def mark_pending(self, batch_id: str, error: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE delivery_batches SET delivery_state = ?, last_error = ? "
                "WHERE batch_id = ?",
                (DELIVERY_PENDING, error, batch_id),
            )

    def mark_rejected(self, batch_id: str, error: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE delivery_batches SET delivery_state = ?, last_error = ? "
                "WHERE batch_id = ?",
                (DELIVERY_REJECTED, error, batch_id),
            )

    # ----------------------------------------------------------------- utils
    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        data = dict(row)
        data["payload"] = json.loads(data["payload"])
        data["receipt"] = json.loads(data["receipt"]) if data["receipt"] else None
        data["complete"] = bool(data["complete"])
        return data


class DeliveryManager:
    """Freeze, persist and deliver batches; verify receipts when confirming."""

    def __init__(
        self,
        store: DeliveryStore,
        receiver_url: str,
        *,
        retry_seconds: float = DEFAULT_RETRY_SECONDS,
        request_timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.store = store
        self.receiver_url = receiver_url.rstrip("/")
        self.retry_seconds = retry_seconds
        self.request_timeout = request_timeout
        # Injectable transport for tests; production uses real HTTP.
        self._transport = transport
        self._deliver_lock = asyncio.Lock()
        self._resume_task: asyncio.Task | None = None
        self._stopping = False

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.receiver_url,
            timeout=self.request_timeout,
            headers={"User-Agent": "procurement-collector/1.0"},
            transport=self._transport,
        )

    # --------------------------------------------------------------- freezing
    def freeze(self, round_snapshot: dict, records: list[dict]) -> dict:
        round_id = round_snapshot["round_id"]
        if round_snapshot["status"] not in ("completed", "partial"):
            raise ValueError("only a finished completed/partial round can be frozen")
        batch_id = f"B-{round_id}"
        # Deep-copy through JSON so live crawler objects cannot leak in.
        records = json.loads(json.dumps(records, ensure_ascii=False))
        unfinished = json.loads(
            json.dumps(round_snapshot.get("unfinished", []), ensure_ascii=False)
        )
        content = build_batch_content(
            batch_id=batch_id,
            round_snapshot=round_snapshot,
            records=records,
            unfinished_pages=unfinished,
        )
        return self.store.create_if_absent(
            batch_id=batch_id,
            round_id=round_id,
            round_status=round_snapshot["status"],
            complete=round_snapshot["status"] == "completed",
            content_hash_value=content_hash(content),
            source_url=round_snapshot["base_url"],
            payload=content,
        )

    # --------------------------------------------------------------- delivery
    async def deliver(self, batch_id: str) -> dict:
        """Try once to deliver; return the persisted batch afterwards."""
        async with self._deliver_lock:
            row = self.store.get(batch_id)
            if row is None:
                raise KeyError(batch_id)
            if row["delivery_state"] == DELIVERY_DELIVERED:
                return row

            self.store.begin_attempt(batch_id)
            payload = dict(row["payload"], content_hash=row["content_hash"])
            receipt: dict | None = None
            error = ""

            try:
                async with self._client() as client:
                    try:
                        response = await client.post("/api/batches", json=payload)
                    except httpx.HTTPError as exc:
                        # The receiver may have committed before the response
                        # was lost: verify the stored receipt before deciding.
                        verified, receipt, error = await self._verify(client, row)
                        if verified:
                            self.store.mark_delivered(batch_id, receipt)
                        else:
                            self.store.mark_pending(
                                batch_id, f"network failure, receipt not verified: {exc!s}"
                            )
                        return self.store.get(batch_id)

                    if response.status_code == 409:
                        detail = _detail_text(response)
                        self.store.mark_rejected(
                            batch_id,
                            "receiver rejected batch id with different content: " + detail,
                        )
                        return self.store.get(batch_id)

                    if response.status_code not in (200, 201):
                        self.store.mark_pending(
                            batch_id,
                            f"receiver returned HTTP {response.status_code}: "
                            + _detail_text(response),
                        )
                        return self.store.get(batch_id)

                    # A response alone is not enough: read back the stored
                    # receipt and only then call the batch delivered.
                    verified, receipt, error = await self._verify(client, row)
                    if verified:
                        self.store.mark_delivered(batch_id, receipt)
                    else:
                        self.store.mark_pending(
                            batch_id, f"receiver accepted but receipt verification failed: {error}"
                        )
                    return self.store.get(batch_id)
            except httpx.HTTPError as exc:  # client construction/other transport error
                self.store.mark_pending(batch_id, f"network failure: {exc!s}")
                return self.store.get(batch_id)

    async def _verify(self, client: httpx.AsyncClient, row: dict) -> tuple[bool, dict | None, str]:
        """Read the receiver's stored receipt and confirm it belongs to us."""
        try:
            response = await client.get(f"/api/batches/{row['batch_id']}")
        except httpx.HTTPError as exc:
            return False, None, f"verification request failed: {exc!s}"

        if response.status_code == 404:
            return False, None, "receiver has no stored batch for this id"
        if response.status_code != 200:
            return False, None, f"verification returned HTTP {response.status_code}"

        try:
            detail = response.json()
        except ValueError:
            return False, None, "verification response was not JSON"

        stored_hash = detail.get("content_hash")
        if stored_hash != row["content_hash"]:
            return False, None, "stored content hash does not match the frozen batch"

        receipt = detail.get("receipt")
        if not isinstance(receipt, dict) or receipt.get("content_hash") != row["content_hash"]:
            return False, None, "stored receipt is missing or mismatched"

        if detail.get("batch_id") != row["batch_id"]:
            return False, None, "stored batch id does not match"

        return True, receipt, ""

    async def reconcile(self, batch_id: str) -> dict:
        """Verify the receiver's stored receipt without re-posting the batch.

        Used when the outcome of an earlier POST is unknown (dropped response,
        crash after commit) and by the UI's "核实回执" action.  Only a verified
        receiver receipt flips a pending batch to ``delivered``; an already
        delivered batch stays delivered, but a failed re-verification is
        recorded so the discrepancy is visible.
        """
        async with self._deliver_lock:
            row = self.store.get(batch_id)
            if row is None:
                raise KeyError(batch_id)

            async with self._client() as client:
                try:
                    verified, receipt, error = await self._verify(client, row)
                except httpx.HTTPError as exc:
                    verified, receipt, error = False, None, f"network failure: {exc!s}"

            if verified:
                if row["delivery_state"] != DELIVERY_DELIVERED:
                    self.store.mark_delivered(batch_id, receipt)
                elif receipt != row["receipt"]:
                    # Receipts are immutable at the receiver; a difference
                    # means the local record and the store disagree.
                    self.store.mark_pending(
                        batch_id, "stored receipt changed at receiver; needs review"
                    )
            elif error:
                self.store.mark_pending(batch_id, f"receipt not verified: {error}")
            return self.store.get(batch_id)

    # -------------------------------------------------------------- lifecycle
    async def deliver_pending_once(self) -> None:
        for batch_id in self.store.pending_batch_ids():
            if self._stopping:
                return
            try:
                await self.deliver(batch_id)
            except Exception:
                # One bad batch must not stop the resume loop.
                continue

    async def resume_loop(self) -> None:
        """Automatically settle batches left pending by a crash/restart.

        Each tick re-posts a pending batch (the receiver answers a committed
        batch with its original receipt) and then reads the stored receipt
        back before the batch is marked delivered.
        """
        while not self._stopping:
            await self.deliver_pending_once()
            try:
                await asyncio.sleep(self.retry_seconds)
            except asyncio.CancelledError:
                raise

    def start_resume_loop(self) -> None:
        if self._resume_task is None or self._resume_task.done():
            self._stopping = False
            self._resume_task = asyncio.create_task(self.resume_loop())

    async def stop(self) -> None:
        self._stopping = True
        if self._resume_task is not None:
            self._resume_task.cancel()
            try:
                await self._resume_task
            except asyncio.CancelledError:
                pass
            self._resume_task = None


def _detail_text(response: httpx.Response) -> str:
    try:
        body = response.json()
        if isinstance(body, dict) and "detail" in body:
            return str(body["detail"])
        return canonical_json(body)
    except ValueError:
        return response.text[:300]
