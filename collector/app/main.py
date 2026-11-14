"""FastAPI entry point: start / monitor / stop a single collection round,
download its JSON, and hand finished rounds to the analysis receiver."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from .crawler import Crawler
from .delivery import (
    DELIVERY_REJECTED,
    DeliveryManager,
    DeliveryStore,
)

BASE_URL = os.getenv("TARGET_BASE_URL", "http://procurement-site:8080")
SECTIONS = {
    "招标公告": os.getenv("SECTION_TENDER_PATH", "/tenders"),
    "中标公告": os.getenv("SECTION_AWARD_PATH", "/awards"),
}
RECEIVER_URL = os.getenv("RECEIVER_BASE_URL", "http://analysis-receiver:8090")
# Browser-facing address of the receiver (the internal service name is not
# resolvable from the operator's machine).
RECEIVER_PUBLIC_URL = os.getenv("RECEIVER_PUBLIC_URL", "http://localhost:8090")
COLLECTOR_DB_PATH = os.getenv("COLLECTOR_DB_PATH", "./data/collector.db")
DELIVERY_RETRY_SECONDS = float(os.getenv("DELIVERY_RETRY_SECONDS", "5"))

crawler = Crawler(base_url=BASE_URL, sections=SECTIONS)
delivery_store = DeliveryStore(COLLECTOR_DB_PATH)
delivery = DeliveryManager(
    delivery_store,
    RECEIVER_URL,
    retry_seconds=DELIVERY_RETRY_SECONDS,
    request_timeout=float(os.getenv("DELIVERY_REQUEST_TIMEOUT", "10")),
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _PROXY_CLIENT
    _PROXY_CLIENT = httpx.AsyncClient(base_url=BASE_URL, timeout=20.0)
    # Settle batches frozen before a crash/restart. Nothing is reported as
    # delivered until the receiver's stored receipt has been verified.
    delivery.start_resume_loop()
    try:
        yield
    finally:
        await delivery.stop()
        if _PROXY_CLIENT is not None:
            await _PROXY_CLIENT.aclose()
            _PROXY_CLIENT = None


app = FastAPI(
    title="Procurement Announcement Collector", version="1.0.0", lifespan=lifespan
)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/rounds")
async def start_round() -> dict:
    try:
        round_id = await crawler.start_round()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"round_id": round_id, "state": crawler.snapshot()}


@app.post("/api/rounds/stop")
async def stop_round() -> dict:
    await crawler.stop_round()
    return {"state": crawler.snapshot()}


@app.get("/api/state")
async def get_state() -> dict:
    return crawler.snapshot()


@app.get("/api/config")
async def get_config() -> dict:
    """Runtime facts the browser UI needs (receiver entry link)."""
    return {"receiver_url": RECEIVER_PUBLIC_URL}


@app.get("/api/records")
async def get_records() -> dict:
    state = crawler.snapshot()
    return {
        "round_id": state["round_id"],
        "status": state["status"],
        "count": len(crawler.records()),
        "records": crawler.records(),
    }


@app.get("/api/records/download")
async def download_records():
    state = crawler.snapshot()
    if state["round_id"] is None:
        raise HTTPException(status_code=404, detail="no collection round has run yet")
    payload = {
        "round_id": state["round_id"],
        "status": state["status"],
        "base_url": state["base_url"],
        "started_at": state["started_at"],
        "finished_at": state["finished_at"],
        "records_count": len(crawler.records()),
        "unfinished_pages": state["unfinished"],
        "records": crawler.records(),
    }
    headers = {
        "Content-Disposition": f'attachment; filename="procurement-{state["round_id"]}.json"'
    }
    return JSONResponse(content=payload, headers=headers)


# ------------------------------------------------------- delivery to receiver
def _public_batch(row: dict) -> dict:
    """Batch view for the UI/API; excludes the (large) frozen payload."""
    return {
        "batch_id": row["batch_id"],
        "round_id": row["round_id"],
        "round_status": row["round_status"],
        "complete": row["complete"],
        "content_hash": row["content_hash"],
        "created_at": row["created_at"],
        "delivery_state": row["delivery_state"],
        "attempts": row["attempts"],
        "last_error": row["last_error"],
        "last_attempt_at": row["last_attempt_at"],
        "delivered_at": row["delivered_at"],
        "receipt": row["receipt"],
        "records_count": row["payload"]["records_count"],
        "unfinished_count": row["payload"]["unfinished_count"],
    }


def _current_round_or_404() -> dict:
    state = crawler.snapshot()
    if state["round_id"] is None:
        raise HTTPException(status_code=404, detail="no collection round has run yet")
    return state


def _get_batch_or_404(batch_id: str) -> dict:
    row = delivery_store.get(batch_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown batch {batch_id}")
    return row


@app.post("/api/deliveries")
async def create_delivery() -> dict:
    """Freeze the finished current round as a batch and deliver it.

    Idempotent per round: a repeated click reuses the frozen batch and its
    original content, and a batch already delivered is returned unchanged.
    """
    state = _current_round_or_404()
    if state["status"] not in ("completed", "partial"):
        raise HTTPException(
            status_code=409,
            detail=f"round {state['round_id']} is {state['status']}; "
            "only a finished completed/partial round can be delivered",
        )
    row = delivery.freeze(state, crawler.records())
    if row["delivery_state"] != DELIVERY_REJECTED:
        row = await delivery.deliver(row["batch_id"])
    return _public_batch(row)


@app.post("/api/deliveries/{batch_id}/retry")
async def retry_delivery(batch_id: str) -> dict:
    row = _get_batch_or_404(batch_id)
    if row["delivery_state"] == DELIVERY_REJECTED:
        raise HTTPException(
            status_code=409,
            detail="batch was rejected by the receiver for conflicting content",
        )
    row = await delivery.deliver(batch_id)
    return _public_batch(row)


@app.get("/api/deliveries")
async def list_deliveries() -> dict:
    rows = delivery_store.list_batches()
    return {"count": len(rows), "batches": [_public_batch(row) for row in rows]}


@app.get("/api/deliveries/pending")
async def pending_deliveries() -> dict:
    ids = delivery_store.pending_batch_ids()
    return {"count": len(ids), "batch_ids": ids}


@app.get("/api/deliveries/{batch_id}")
async def get_delivery(batch_id: str) -> dict:
    return _public_batch(_get_batch_or_404(batch_id))


@app.get("/api/deliveries/{batch_id}/content")
async def get_delivery_content(batch_id: str):
    row = _get_batch_or_404(batch_id)
    return JSONResponse(
        content=row["payload"],
        headers={
            "Content-Disposition": f'attachment; filename="{batch_id}.json"'
        },
    )


@app.post("/api/deliveries/{batch_id}/refresh")
async def refresh_delivery(batch_id: str) -> dict:
    """Re-check the receiver's stored receipt without re-posting.

    Used when a previous POST's outcome is unknown (dropped response / crash).
    """
    _get_batch_or_404(batch_id)
    return _public_batch(await delivery.reconcile(batch_id))


# Browser entry to the procurement site: same-origin proxy so the operator can
# open both the console and the target site from the collector's port.
_PROXY_CLIENT: httpx.AsyncClient | None = None


@app.api_route("/site/{path:path}", methods=["GET", "POST"])
async def site_proxy(path: str, request: Request):
    assert _PROXY_CLIENT is not None
    # Pass query params exactly once (httpx merges params into the URL).
    proxy_headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    try:
        upstream = await _PROXY_CLIENT.request(
            request.method,
            path,
            params=list(request.query_params.multi_items()),
            content=await request.body(),
            headers=proxy_headers,
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"procurement site unreachable: {exc}") from exc
    excluded = {"content-encoding", "transfer-encoding", "connection"}
    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in excluded}
    content_type = upstream.headers.get("content-type", "")
    if "application/json" in content_type:
        return JSONResponse(upstream.json(), status_code=upstream.status_code, headers=headers)
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=headers,
        media_type=content_type or None,
    )


# Browser UI (mounted last so /api routes take precedence).
_static_dir = Path(__file__).resolve().parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="ui")
