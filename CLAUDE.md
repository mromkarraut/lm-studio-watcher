# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Start / stop

```bash
# Start (requires sudo for iptables)
sudo bash start_lm_watcher.sh

# Stop
kill $(cat /tmp/lm_watcher.pid)
sudo bash teardown_intercept.sh
```

Logs: `/tmp/lm_watcher.log`. PID: `/tmp/lm_watcher.pid`. Tunnel log: `/tmp/lm_watcher_tunnel.log`. Tunnel PID: `/tmp/lm_watcher_tunnel.pid`.

The iptables rule does not survive WSL restarts — the start script must be re-run each boot.

## Run directly (dev)

```bash
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

## Environment

- LM Studio runs on Windows, reachable from WSL2 at `127.0.0.1:1234` (mirrored networking). Never use `host.docker.internal` or `192.168.x.x`.
- The watcher itself runs in WSL on port 8080.
- A separate financial-agent service runs in WSL on port 8000 and makes direct requests to LM Studio — those are captured via iptables.

## Architecture

All logic lives in `main.py` (single-file FastAPI app). Three request paths flow through it:

1. **Background poller** — `_poll_models()` calls `/api/v0/models` on LM Studio every 5 minutes and on every dashboard WebSocket connection, updating `_state`. Uses a fresh `httpx.AsyncClient` per poll (connection-per-poll, not a persistent pool) to avoid leaking connections.

2. **TCP intercept proxy** — listens on `:1235`. `setup_intercept.sh` installs an iptables `REDIRECT` rule that sends all outbound `:1234` traffic here. The proxy reads the raw HTTP request, reconnects to LM Studio using a socket marked with `SO_MARK=1` (which skips the iptables rule to avoid looping), forwards the response, and records token usage + latency. Requires `CAP_NET_ADMIN` on the Python binary (`setcap` in `setup_intercept.sh`).

3. **HTTP proxy** — `/proxy/v1/*` proxies requests to LM Studio `/v1/*` via httpx, capturing the same metrics. Used for clients that can be configured to point at the watcher instead of LM Studio directly.

Both paths call `_record_request()` and `asyncio.create_task(_broadcast_state())` to push updates to all WebSocket clients in real time.

**State** is kept entirely in memory: `_state` (model list + online status), `_requests` (deque, last 200), `_token_buckets` (60-bucket rolling 1 s/bucket timeseries), `_totals` (cumulative counters).

**Frontend** (`static/`) connects via WebSocket `/ws`, receives the full payload on connect and on every state change, and renders with Chart.js. No build step — plain HTML/CSS/JS.
