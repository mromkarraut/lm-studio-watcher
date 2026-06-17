"""
LM Studio Watcher — FastAPI backend
- Polls /api/v0/models for model state every 5 s
- Proxies /proxy/v1/* to LM Studio, capturing token usage and latency
- Pushes live metrics to dashboard via WebSocket
"""

import asyncio
import json
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

LM_STUDIO_BASE = "http://127.0.0.1:1234"
POLL_INTERVAL = 5  # seconds

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_state: dict[str, Any] = {
    "server_online": False,
    "models": [],
    "last_poll": None,
}

# Circular buffer — keep last 200 requests
_requests: deque = deque(maxlen=200)

# Rolling 60-bucket counter (each bucket = 1 s) for tokens/s chart
_BUCKETS = 60
_token_buckets: deque = deque([0] * _BUCKETS, maxlen=_BUCKETS)
_last_bucket_ts: float = time.time()

_ws_clients: set[WebSocket] = set()

# Cumulative totals
_totals = {"prompt_tokens": 0, "completion_tokens": 0, "requests": 0, "errors": 0}


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

async def _poll_models() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                r = await client.get(f"{LM_STUDIO_BASE}/api/v0/models")
                data = r.json()
                _state["models"] = data.get("data", [])
                _state["server_online"] = True
                _state["last_poll"] = time.time()
            except Exception:
                _state["server_online"] = False
                _state["models"] = []
            await _broadcast_state()
            await asyncio.sleep(POLL_INTERVAL)


async def _broadcast_state() -> None:
    if not _ws_clients:
        return
    payload = json.dumps(_build_ws_payload())
    dead = set()
    for ws in _ws_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)


def _build_ws_payload() -> dict:
    return {
        "server_online": _state["server_online"],
        "last_poll": _state["last_poll"],
        "models": _state["models"],
        "totals": dict(_totals),
        "recent_requests": list(_requests)[-20:],
        "token_timeseries": list(_token_buckets),
    }


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_poll_models())
    yield
    task.cancel()


app = FastAPI(title="LM Studio Watcher", lifespan=lifespan)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    # Send current snapshot immediately on connect
    await ws.send_text(json.dumps(_build_ws_payload()))
    try:
        while True:
            await ws.receive_text()  # keep-alive ping/pong
    except WebSocketDisconnect:
        _ws_clients.discard(ws)


# ---------------------------------------------------------------------------
# Proxy  /proxy/v1/*  →  LM Studio /v1/*
# ---------------------------------------------------------------------------

def _record_request(entry: dict) -> None:
    _requests.appendleft(entry)
    _totals["requests"] += 1
    _totals["prompt_tokens"] += entry.get("prompt_tokens", 0)
    _totals["completion_tokens"] += entry.get("completion_tokens", 0)

    # Advance token-bucket timeline
    global _last_bucket_ts
    now = time.time()
    elapsed = now - _last_bucket_ts
    if elapsed >= 1.0:
        # Fill missed seconds with zero then record current
        missed = min(int(elapsed), _BUCKETS)
        for _ in range(missed):
            _token_buckets.append(0)
        _last_bucket_ts = now
    _token_buckets[-1] += entry.get("completion_tokens", 0)


@app.api_route(
    "/proxy/v1/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
)
async def proxy(path: str, request: Request):
    url = f"{LM_STUDIO_BASE}/v1/{path}"
    body = await request.body()
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }

    # Detect streaming
    streaming = False
    try:
        parsed = json.loads(body) if body else {}
        streaming = bool(parsed.get("stream", False))
    except Exception:
        parsed = {}

    t_start = time.perf_counter()

    if streaming:
        return await _proxy_stream(path, url, headers, body, parsed, t_start)

    # Non-streaming
    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            upstream = await client.request(
                request.method, url, content=body, headers=headers
            )
        except httpx.ConnectError:
            _totals["errors"] += 1
            return JSONResponse({"error": "LM Studio unreachable"}, status_code=503)

    latency = time.perf_counter() - t_start

    # Capture usage from response
    entry: dict[str, Any] = {
        "ts": time.time(),
        "path": f"/v1/{path}",
        "model": parsed.get("model", "—"),
        "latency": round(latency, 3),
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "status": upstream.status_code,
    }
    try:
        resp_json = upstream.json()
        usage = resp_json.get("usage", {})
        entry["prompt_tokens"] = usage.get("prompt_tokens", 0)
        entry["completion_tokens"] = usage.get("completion_tokens", 0)
        entry["total_tokens"] = usage.get("total_tokens", 0)
        if entry["completion_tokens"] > 0 and latency > 0:
            entry["tokens_per_sec"] = round(entry["completion_tokens"] / latency, 1)
    except Exception:
        pass

    _record_request(entry)
    asyncio.create_task(_broadcast_state())

    return JSONResponse(
        content=upstream.json() if upstream.headers.get("content-type", "").startswith("application/json") else {},
        status_code=upstream.status_code,
        headers={"content-type": upstream.headers.get("content-type", "application/json")},
    )


async def _proxy_stream(path, url, headers, body, parsed, t_start):
    """Stream SSE back to caller, tally tokens from the final [DONE] chunk."""
    completion_tokens = 0
    prompt_tokens = 0

    async def generator():
        nonlocal completion_tokens, prompt_tokens
        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream("POST", url, content=body, headers=headers) as r:
                async for line in r.aiter_lines():
                    if line.startswith("data: ") and not line.endswith("[DONE]"):
                        try:
                            chunk = json.loads(line[6:])
                            usage = chunk.get("usage") or {}
                            if usage.get("completion_tokens"):
                                completion_tokens = usage["completion_tokens"]
                                prompt_tokens = usage.get("prompt_tokens", 0)
                        except Exception:
                            pass
                    yield (line + "\n\n").encode()

        latency = time.perf_counter() - t_start
        entry = {
            "ts": time.time(),
            "path": f"/v1/{path}",
            "model": parsed.get("model", "—"),
            "latency": round(latency, 3),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
            "tokens_per_sec": round(completion_tokens / latency, 1) if completion_tokens and latency else 0,
            "status": 200,
        }
        _record_request(entry)
        asyncio.create_task(_broadcast_state())

    return StreamingResponse(generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# REST snapshots (useful for initial load & debugging)
# ---------------------------------------------------------------------------

@app.get("/api/state")
async def get_state():
    return _build_ws_payload()


@app.get("/api/requests")
async def get_requests(limit: int = 50):
    return list(_requests)[:limit]


# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=7860, reload=True)
