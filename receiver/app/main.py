"""Local analysis receiver.

Receives a frozen batch from the collector, stores the *whole* batch (records,
unfinished pages, completeness flag) and a receipt in SQLite in one
transaction.  Contract:

* identical replay of a batch id returns the originally stored receipt and
  writes nothing;
* the same batch id with different content is rejected with ``409`` and
  nothing is written;
* the receipt is generated once, at insert time, so it is stable across
  replays and restarts;
* when ``RECEIVER_DROP_RESPONSE`` is on, the batch is committed and the HTTP
  connection is then aborted without a usable response.  This simulates a
  receiver that stores data but loses the acknowledgement, which the
  collector must tolerate by verifying the stored receipt before it reports
  the batch as delivered.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

REQUIRED_FIELDS = (
    "batch_id",
    "round_id",
    "status",
    "complete",
    "records",
    "unfinished_pages",
)
HASH_FIELD = "content_hash"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(content: dict) -> str:
    payload = {k: v for k, v in content.items() if k != HASH_FIELD}
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


class BatchStore:
    """SQLite-backed batch + receipt storage."""

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
                CREATE TABLE IF NOT EXISTS received_batches (
                    batch_id        TEXT PRIMARY KEY,
                    round_id        TEXT NOT NULL,
                    status          TEXT NOT NULL,
                    complete        INTEGER NOT NULL,
                    content_hash    TEXT NOT NULL,
                    records_count   INTEGER NOT NULL,
                    unfinished_count INTEGER NOT NULL,
                    payload         TEXT NOT NULL,
                    receipt         TEXT NOT NULL,
                    received_at     TEXT NOT NULL
                )
                """
            )

    def receive(self, content: dict) -> tuple[int, dict]:
        """Store a batch or replay its receipt.

        Returns ``(http_status, receipt)`` where 201 means newly written and
        200 means an identical batch was already stored.  Raises
        :class:`BatchConflict` for the same id with different content.
        """
        digest = content[HASH_FIELD]
        batch_id = content["batch_id"]
        payload_text = canonical_json(
            {k: v for k, v in content.items() if k != HASH_FIELD}
        )
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT * FROM received_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is not None:
                if row["content_hash"] != digest:
                    self._conn.rollback()
                    raise BatchConflict(batch_id)
                self._conn.commit()
                return 200, json.loads(row["receipt"])

            received_at = utcnow()
            receipt = {
                "receipt_id": "R-" + hashlib.sha256(
                    f"{batch_id}:{digest}".encode("utf-8")
                ).hexdigest()[:16],
                "batch_id": batch_id,
                "round_id": content["round_id"],
                "content_hash": digest,
                "status": content["status"],
                "complete": bool(content["complete"]),
                "records_count": len(content["records"]),
                "unfinished_count": len(content["unfinished_pages"]),
                "received_at": received_at,
            }
            self._conn.execute(
                """
                INSERT INTO received_batches (
                    batch_id, round_id, status, complete, content_hash,
                    records_count, unfinished_count, payload, receipt,
                    received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    content["round_id"],
                    content["status"],
                    1 if content["complete"] else 0,
                    digest,
                    len(content["records"]),
                    len(content["unfinished_pages"]),
                    payload_text,
                    canonical_json(receipt),
                    received_at,
                ),
            )
            self._conn.commit()
            return 201, receipt

    def get(self, batch_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM received_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def list(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM received_batches ORDER BY received_at DESC, batch_id"
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        data = dict(row)
        data["payload"] = json.loads(data["payload"])
        data["receipt"] = json.loads(data["receipt"])
        data["complete"] = bool(data["complete"])
        return data


class BatchConflict(Exception):
    """Same batch id, different content."""


class DroppedResponse(Response):
    """Commit-then-disconnect response used by the drop switch.

    The resource headers are sent, but the body never arrives and the server
    closes the connection.  Clients see a transport error rather than an
    acknowledgement even though the batch is already committed.
    """

    async def __call__(self, scope, receive, send) -> None:  # noqa: ANN001
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", b"1024"),
                ],
            }
        )
        # No body is sent on purpose; uvicorn closes the connection when the
        # app returns with the response incomplete.


DB_PATH = os.getenv("RECEIVER_DB_PATH", "./data/receiver.db")
store = BatchStore(DB_PATH)
runtime: dict = {
    "drop_response": os.getenv("RECEIVER_DROP_RESPONSE", "false").lower() == "true"
}


def _receive_response(status: int, receipt: dict):
    if runtime["drop_response"]:
        return DroppedResponse()
    return JSONResponse(receipt, status_code=status)


app = FastAPI(title="Analysis Receiver", version="1.0.0")


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "batches": len(store.list()), "drop_response": runtime["drop_response"]}


@app.post("/api/batches")
async def receive_batch(request: Request):
    try:
        body = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="body must be JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")

    missing = [field for field in REQUIRED_FIELDS + (HASH_FIELD,) if field not in body]
    if missing:
        raise HTTPException(status_code=400, detail="missing field(s): " + ", ".join(missing))
    if not isinstance(body["records"], list) or not isinstance(body["unfinished_pages"], list):
        raise HTTPException(status_code=400, detail="records/unfinished_pages must be lists")
    if body["status"] not in ("completed", "partial"):
        raise HTTPException(status_code=400, detail="status must be completed or partial")
    if bool(body["complete"]) != (body["status"] == "completed"):
        raise HTTPException(status_code=400, detail="complete flag contradicts status")
    # The declared counts must describe the arrays actually delivered in the
    # same batch, so the stored completeness marker cannot drift from data.
    if body.get("records_count") != len(body["records"]):
        raise HTTPException(status_code=400, detail="records_count does not match records")
    if body.get("unfinished_count") != len(body["unfinished_pages"]):
        raise HTTPException(
            status_code=400, detail="unfinished_count does not match unfinished_pages"
        )
    if content_hash(body) != body[HASH_FIELD]:
        raise HTTPException(status_code=400, detail="content_hash does not match body")

    try:
        status, receipt = store.receive(body)
    except BatchConflict:
        raise HTTPException(
            status_code=409,
            detail="batch id already stored with different content",
        ) from None
    return _receive_response(status, receipt)


@app.get("/api/batches")
async def list_batches() -> dict:
    rows = store.list()
    return {
        "count": len(rows),
        "batches": [
            {
                "batch_id": row["batch_id"],
                "round_id": row["round_id"],
                "status": row["status"],
                "complete": row["complete"],
                "records_count": row["records_count"],
                "unfinished_count": row["unfinished_count"],
                "received_at": row["received_at"],
                "receipt": row["receipt"],
            }
            for row in rows
        ],
    }


@app.get("/api/batches/{batch_id}")
async def get_batch(batch_id: str):
    row = store.get(batch_id)
    if row is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return row


@app.get("/api/admin/drop-response")
async def get_drop_response() -> dict:
    return {"drop_response": runtime["drop_response"]}


@app.post("/api/admin/drop-response")
async def set_drop_response(request: Request) -> dict:
    body = await request.json()
    if not isinstance(body, dict) or not isinstance(body.get("enabled"), bool):
        raise HTTPException(status_code=400, detail="expected {\"enabled\": true|false}")
    runtime["drop_response"] = body["enabled"]
    return {"drop_response": runtime["drop_response"]}


@app.get("/", response_class=HTMLResponse)
async def index():
    rows = store.list()
    trs = "".join(
        f"""<tr><td class="mono">{row['batch_id']}</td><td>{row['round_id']}</td>
        <td>{row['status']}</td><td>{'完整' if row['complete'] else '不完整'}</td>
        <td>{row['records_count']}</td><td>{row['unfinished_count']}</td>
        <td class="mono">{row['receipt']['receipt_id']}</td>
        <td>{row['received_at']}</td>
        <td><a href="/batches/{row['batch_id']}">明细</a></td></tr>"""
        for row in rows
    ) or '<tr><td colspan="9" class="muted">尚无已接收批次</td></tr>'
    checked = "checked" if runtime["drop_response"] else ""
    return f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/>
<title>分析接收服务</title><style>
body{{font-family:"PingFang SC","Microsoft YaHei",sans-serif;margin:0;background:#0f172a;color:#e2e8f0}}
main{{max-width:1180px;margin:0 auto;padding:24px}}h1{{font-size:20px}}
.panel{{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:18px 20px;margin-bottom:18px}}
h2{{font-size:15px;color:#38bdf8;margin:0 0 12px}}table{{width:100%;border-collapse:collapse;font-size:13px}}
th,td{{text-align:left;padding:8px 10px;border-bottom:1px solid #334155}}
th{{color:#94a3b8}}.mono{{font-family:ui-monospace,monospace;font-size:12px}}
.muted{{color:#94a3b8}}.notice{{background:#422006;border:1px solid #b45309;padding:10px 14px;
border-radius:8px;margin-bottom:14px}}button{{padding:8px 18px;border:none;border-radius:8px;
background:#38bdf8;color:#08233a;font-weight:700;cursor:pointer}}
</style></head><body><main>
<h1>📥 分析接收服务</h1>
<div class="panel"><h2>提交后断开响应（故障开关）</h2>
<p class="notice">开启后，新提交仍会先写入 SQLite，但服务端会在回执发出前断开连接；
采集端必须核实已入库回执后才能显示“已交付”。</p>
<label><input type="checkbox" id="drop" {checked}/> 提交后断开响应</label>
<p class="muted">当前状态：<b id="dropState">{'开启' if runtime['drop_response'] else '关闭'}</b></p>
</div>
<div class="panel"><h2>已接收批次（含回执）</h2>
<table><thead><tr><th>批次</th><th>轮次</th><th>状态</th><th>完整性</th>
<th>公告数</th><th>未完成页</th><th>回执号</th><th>接收时间</th><th></th></tr></thead>
<tbody>{trs}</tbody></table></div>
</main><script>
const box=document.getElementById('drop'), state=document.getElementById('dropState');
box.addEventListener('change', async ()=>{{
  const resp=await fetch('/api/admin/drop-response',{{method:'POST',
    headers:{{'content-type':'application/json'}},body:JSON.stringify({{enabled:box.checked}})}});
  const data=await resp.json(); state.textContent=data.drop_response?'开启':'关闭';
  if(!resp.ok) alert('切换失败');
}});
</script></body></html>"""


def _esc(value) -> str:
    import html

    return html.escape(str(value))


@app.get("/batches/{batch_id}", response_class=HTMLResponse)
async def batch_detail_page(batch_id: str):
    """Receipt + whole batch (records and unfinished pages) for one batch."""
    row = store.get(batch_id)
    if row is None:
        raise HTTPException(status_code=404, detail="batch not found")
    payload = row["payload"]
    receipt = row["receipt"]
    record_rows = "".join(
        f"<tr><td class='mono'>{_esc(r.get('announcement_code'))}</td>"
        f"<td>{_esc(r.get('title'))}</td><td>{_esc(r.get('procurement_unit'))}</td>"
        f"<td>{_esc(r.get('deadline'))}</td><td>{_esc(r.get('section'))}</td></tr>"
        for r in payload["records"]
    ) or "<tr><td colspan='5' class='muted'>无成功公告</td></tr>"
    unfinished_rows = "".join(
        f"<tr><td>{_esc(u.get('section'))}</td><td>{_esc(u.get('page_type'))}</td>"
        f"<td class='mono'>{_esc(u.get('page'))}</td><td>{_esc(u.get('attempts'))}</td>"
        f"<td>{_esc(u.get('reason'))}</td><td class='mono'>{_esc(u.get('url'))}</td></tr>"
        for u in payload["unfinished_pages"]
    ) or "<tr><td colspan='6' class='muted'>无未完成页</td></tr>"
    return f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/>
<title>批次 {_esc(batch_id)} - 分析接收服务</title><style>
body{{font-family:"PingFang SC","Microsoft YaHei",sans-serif;margin:0;background:#0f172a;color:#e2e8f0}}
main{{max-width:1180px;margin:0 auto;padding:24px}}h1{{font-size:20px}}
.panel{{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:18px 20px;margin-bottom:18px}}
h2{{font-size:15px;color:#38bdf8;margin:0 0 12px}}table{{width:100%;border-collapse:collapse;font-size:13px}}
th,td{{text-align:left;padding:8px 10px;border-bottom:1px solid #334155}}
th{{color:#94a3b8}}.mono{{font-family:ui-monospace,monospace;font-size:12px}}
.muted{{color:#94a3b8}}a{{color:#38bdf8}}.kv{{display:flex;justify-content:space-between;
max-width:520px;padding:4px 0;color:#94a3b8}}.kv b{{color:#e2e8f0}}
.badge{{display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:700;
background:{'#047857' if row['complete'] else '#a16207'}}}
</style></head><body><main>
<p><a href="/">← 返回接收端首页</a></p>
<div class="panel"><h2>批次与回执</h2>
<div class="kv"><span>批次 ID</span><b class="mono">{_esc(row['batch_id'])}</b></div>
<div class="kv"><span>轮次 ID</span><b class="mono">{_esc(row['round_id'])}</b></div>
<div class="kv"><span>完整性</span><b><span class="badge">
{'完整 complete' if row['complete'] else '不完整 partial'}</span></b></div>
<div class="kv"><span>轮次状态</span><b>{_esc(row['status'])}</b></div>
<div class="kv"><span>公告数 / 未完成页</span><b>{row['records_count']} / {row['unfinished_count']}</b></div>
<div class="kv"><span>内容哈希</span><b class="mono">{_esc(row['content_hash'])}</b></div>
<div class="kv"><span>回执号</span><b class="mono">{_esc(receipt['receipt_id'])}</b></div>
<div class="kv"><span>入库时间</span><b>{_esc(row['received_at'])}</b></div>
<p class="muted">JSON：<a href="/api/batches/{_esc(row['batch_id'])}">/api/batches/{_esc(row['batch_id'])}</a></p>
</div>
<div class="panel"><h2>未完成页（{row['unfinished_count']}）</h2>
<table><thead><tr><th>栏目</th><th>类型</th><th>页/编号</th><th>尝试</th><th>原因</th><th>URL</th></tr></thead>
<tbody>{unfinished_rows}</tbody></table></div>
<div class="panel"><h2>成功公告（{row['records_count']}）</h2>
<table><thead><tr><th>公告编号</th><th>标题</th><th>采购单位</th><th>截止日期</th><th>来源栏目</th></tr></thead>
<tbody>{record_rows}</tbody></table></div>
</main></body></html>"""
