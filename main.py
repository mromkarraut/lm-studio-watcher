"""
LM Studio Watcher — FastAPI backend
- Polls /api/v0/models for model state every 5 s
- Proxies /proxy/v1/* to LM Studio, capturing token usage and latency
- Intercepts direct WSL traffic to :1234 via TCP proxy on INTERCEPT_PORT
- Pushes live metrics to dashboard via WebSocket
"""

import asyncio
import json
import socket as _socket
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

LM_STUDIO_HOST = "127.0.0.1"
LM_STUDIO_PORT = 1234
LM_STUDIO_BASE = f"http://{LM_STUDIO_HOST}:{LM_STUDIO_PORT}"
POLL_INTERVAL = 300  # seconds
INTERCEPT_PORT = 1235  # iptables redirects :1234 → here
_BYPASS_MARK = 1       # SO_MARK value that skips the iptables REDIRECT rule

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

_poll_limits = httpx.Limits(max_connections=2, max_keepalive_connections=1)
_poll_lock = asyncio.Lock()


async def _do_poll() -> None:
    async with _poll_lock:
        try:
            async with httpx.AsyncClient(timeout=5.0, limits=_poll_limits) as client:
                r = await client.get(f"{LM_STUDIO_BASE}/api/v0/models")
                data = r.json()
                _state["models"] = data.get("data", [])
                _state["server_online"] = True
                _state["last_poll"] = time.time()
        except Exception:
            _state["server_online"] = False
            _state["models"] = []
        await _broadcast_state()


async def _poll_models() -> None:
    while True:
        await _do_poll()
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
    poll_task = asyncio.create_task(_poll_models())
    intercept_server = await asyncio.start_server(
        _intercept_connection, "0.0.0.0", INTERCEPT_PORT
    )
    async with intercept_server:
        yield
    poll_task.cancel()


app = FastAPI(title="LM Studio Watcher", lifespan=lifespan)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    await ws.send_text(json.dumps(_build_ws_payload()))
    asyncio.create_task(_do_poll())
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        _ws_clients.discard(ws)


# ---------------------------------------------------------------------------
# Shared request recorder
# ---------------------------------------------------------------------------

def _record_request(entry: dict) -> None:
    _requests.appendleft(entry)
    _totals["requests"] += 1
    _totals["prompt_tokens"] += entry.get("prompt_tokens", 0)
    _totals["completion_tokens"] += entry.get("completion_tokens", 0)

    global _last_bucket_ts
    now = time.time()
    elapsed = now - _last_bucket_ts
    if elapsed >= 1.0:
        missed = min(int(elapsed), _BUCKETS)
        for _ in range(missed):
            _token_buckets.append(0)
        _last_bucket_ts = now
    _token_buckets[-1] += entry.get("completion_tokens", 0)


# ---------------------------------------------------------------------------
# TCP intercept proxy  (iptables redirects :1234 → INTERCEPT_PORT)
# ---------------------------------------------------------------------------

async def _bypass_connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Connect to LM Studio using SO_MARK so iptables skips the REDIRECT rule."""
    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.setblocking(False)
    try:
        sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_MARK, _BYPASS_MARK)
    except OSError:
        pass  # needs CAP_NET_ADMIN; see setup_intercept.sh
    await asyncio.get_running_loop().sock_connect(sock, (LM_STUDIO_HOST, LM_STUDIO_PORT))
    return await asyncio.open_connection(sock=sock)


async def _intercept_connection(
    client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
) -> None:
    try:
        await _handle_intercept(client_reader, client_writer)
    except Exception:
        pass
    finally:
        try:
            client_writer.close()
        except Exception:
            pass


async def _handle_intercept(
    client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
) -> None:
    t_start = time.perf_counter()

    # ── Read request line + headers ──────────────────────────────────
    raw_req_head = b""
    while True:
        line = await asyncio.wait_for(client_reader.readline(), timeout=30)
        if not line:
            return
        raw_req_head += line
        if line in (b"\r\n", b"\n"):
            break

    header_text = raw_req_head.decode("utf-8", errors="replace")
    header_lines = header_text.split("\r\n") if "\r\n" in header_text else header_text.split("\n")

    if not header_lines or not header_lines[0].strip():
        return

    parts = header_lines[0].split(" ", 2)
    if len(parts) < 3:
        return
    method, path, proto = parts

    req_headers: dict[str, str] = {}
    for hl in header_lines[1:]:
        if ":" in hl:
            k, _, v = hl.partition(":")
            req_headers[k.strip().lower()] = v.strip()

    # ── Read request body ────────────────────────────────────────────
    content_length = int(req_headers.get("content-length", 0))
    body = b""
    if content_length > 0:
        body = await asyncio.wait_for(
            client_reader.readexactly(content_length), timeout=30
        )

    # ── Parse for model / streaming ──────────────────────────────────
    model = "—"
    streaming = False
    try:
        if body:
            parsed = json.loads(body)
            model = parsed.get("model", "—")
            streaming = bool(parsed.get("stream", False))
    except Exception:
        pass

    # ── Connect to LM Studio bypassing iptables ──────────────────────
    try:
        up_reader, up_writer = await _bypass_connect()
    except Exception:
        _totals["errors"] += 1
        return

    try:
        # Rebuild and forward request (force Connection: close)
        forward_head = f"{method} {path} HTTP/1.1\r\n"
        forward_head += f"Host: {LM_STUDIO_HOST}:{LM_STUDIO_PORT}\r\n"
        forward_head += "Connection: close\r\n"
        for k, v in req_headers.items():
            if k not in ("host", "connection"):
                forward_head += f"{k}: {v}\r\n"
        forward_head += "\r\n"

        up_writer.write(forward_head.encode() + body)
        await up_writer.drain()

        # ── Read response headers ────────────────────────────────────
        raw_resp_head = b""
        while True:
            line = await asyncio.wait_for(up_reader.readline(), timeout=120)
            if not line:
                break
            raw_resp_head += line
            if line in (b"\r\n", b"\n"):
                break

        client_writer.write(raw_resp_head)
        await client_writer.drain()

        resp_text = raw_resp_head.decode("utf-8", errors="replace")
        resp_lines = resp_text.split("\r\n") if "\r\n" in resp_text else resp_text.split("\n")

        status_code = 200
        if resp_lines:
            sp = resp_lines[0].split(" ", 2)
            if len(sp) >= 2:
                try:
                    status_code = int(sp[1])
                except ValueError:
                    pass

        resp_headers: dict[str, str] = {}
        for rl in resp_lines[1:]:
            if ":" in rl:
                k, _, v = rl.partition(":")
                resp_headers[k.strip().lower()] = v.strip()

        # ── Forward response body & capture tokens ───────────────────
        prompt_tokens = completion_tokens = 0

        if streaming:
            while True:
                line = await asyncio.wait_for(up_reader.readline(), timeout=120)
                if not line:
                    break
                client_writer.write(line)
                await client_writer.drain()
                decoded = line.decode("utf-8", errors="replace").rstrip()
                if decoded.startswith("data: ") and not decoded.endswith("[DONE]"):
                    try:
                        chunk = json.loads(decoded[6:])
                        usage = chunk.get("usage") or {}
                        if usage.get("completion_tokens"):
                            completion_tokens = usage["completion_tokens"]
                            prompt_tokens = usage.get("prompt_tokens", 0)
                    except Exception:
                        pass

        else:
            resp_cl = int(resp_headers.get("content-length", 0))
            is_chunked = "chunked" in resp_headers.get("transfer-encoding", "").lower()

            if resp_cl > 0:
                resp_body = await asyncio.wait_for(
                    up_reader.readexactly(resp_cl), timeout=120
                )
                client_writer.write(resp_body)
                await client_writer.drain()
            elif is_chunked:
                resp_body = b""
                while True:
                    size_line = await up_reader.readline()
                    client_writer.write(size_line)
                    chunk_size = int(size_line.strip().split(b";")[0], 16)
                    if chunk_size == 0:
                        crlf = await up_reader.readline()
                        client_writer.write(crlf)
                        break
                    chunk_data = await up_reader.readexactly(chunk_size)
                    resp_body += chunk_data
                    client_writer.write(chunk_data)
                    crlf = await up_reader.readline()
                    client_writer.write(crlf)
                await client_writer.drain()
            else:
                resp_body = await asyncio.wait_for(up_reader.read(-1), timeout=120)
                client_writer.write(resp_body)
                await client_writer.drain()

            try:
                resp_json = json.loads(resp_body)
                usage = resp_json.get("usage", {})
                prompt_tokens = usage.get("prompt_tokens", 0)
                completion_tokens = usage.get("completion_tokens", 0)
            except Exception:
                pass

        latency = time.perf_counter() - t_start
        entry: dict[str, Any] = {
            "ts": time.time(),
            "path": path,
            "model": model,
            "latency": round(latency, 3),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
            "tokens_per_sec": round(completion_tokens / latency, 1)
            if completion_tokens and latency
            else 0,
            "status": status_code,
            "source": "direct",
        }
        _record_request(entry)
        asyncio.create_task(_broadcast_state())

    finally:
        try:
            up_writer.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Proxy  /proxy/v1/*  →  LM Studio /v1/*
# ---------------------------------------------------------------------------

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

    streaming = False
    try:
        parsed = json.loads(body) if body else {}
        streaming = bool(parsed.get("stream", False))
    except Exception:
        parsed = {}

    t_start = time.perf_counter()

    if streaming:
        return await _proxy_stream(path, url, headers, body, parsed, t_start)

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            upstream = await client.request(
                request.method, url, content=body, headers=headers
            )
        except httpx.ConnectError:
            _totals["errors"] += 1
            return JSONResponse({"error": "LM Studio unreachable"}, status_code=503)

    latency = time.perf_counter() - t_start

    entry: dict[str, Any] = {
        "ts": time.time(),
        "path": f"/v1/{path}",
        "model": parsed.get("model", "—"),
        "latency": round(latency, 3),
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "status": upstream.status_code,
        "source": "proxy",
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
        content=upstream.json()
        if upstream.headers.get("content-type", "").startswith("application/json")
        else {},
        status_code=upstream.status_code,
        headers={"content-type": upstream.headers.get("content-type", "application/json")},
    )


async def _proxy_stream(path, url, headers, body, parsed, t_start):
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
            "tokens_per_sec": round(completion_tokens / latency, 1)
            if completion_tokens and latency
            else 0,
            "status": 200,
            "source": "proxy",
        }
        _record_request(entry)
        asyncio.create_task(_broadcast_state())

    return StreamingResponse(generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# REST snapshots
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
