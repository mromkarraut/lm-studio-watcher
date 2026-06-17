#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/tmp/lm_watcher.log"
PID_FILE="/tmp/lm_watcher.pid"
TUNNEL_LOG="/tmp/lm_watcher_tunnel.log"
TUNNEL_PID_FILE="/tmp/lm_watcher_tunnel.pid"

# Kill any existing tunnel
if [ -f "$TUNNEL_PID_FILE" ]; then
    OLD_TUNNEL_PID=$(cat "$TUNNEL_PID_FILE")
    if kill -0 "$OLD_TUNNEL_PID" 2>/dev/null; then
        echo "Stopping existing tunnel (PID $OLD_TUNNEL_PID)..."
        kill "$OLD_TUNNEL_PID"
    fi
    rm -f "$TUNNEL_PID_FILE" /tmp/lm_watcher_tunnel.url
fi

# Kill any existing instance
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing watcher (PID $OLD_PID)..."
        kill "$OLD_PID"
        sleep 1
    fi
    rm -f "$PID_FILE"
fi

# Set up iptables intercept (requires sudo)
echo "Setting up iptables intercept..."
sudo bash "$SCRIPT_DIR/setup_intercept.sh"

# Start the watcher
echo "Starting LM Studio Watcher..."
cd "$SCRIPT_DIR"
python -m uvicorn main:app --host 0.0.0.0 --port 8080 &> "$LOG_FILE" &
echo $! > "$PID_FILE"

sleep 2

if kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Watcher running on http://localhost:8080/ (PID $(cat "$PID_FILE"), logs: $LOG_FILE)"
else
    echo "Failed to start — check $LOG_FILE"
    exit 1
fi

# Start Cloudflare Tunnel (optional — skip if cloudflared not found)
if command -v cloudflared &>/dev/null; then
    echo "Starting Cloudflare Tunnel..."
    cloudflared tunnel --url http://localhost:8080 --no-autoupdate &>"$TUNNEL_LOG" &
    echo $! > "$TUNNEL_PID_FILE"
    sleep 3
    # Extract the assigned URL from the log
    TUNNEL_URL=$(grep -o 'https://[a-zA-Z0-9-]*\.trycloudflare\.com' "$TUNNEL_LOG" | head -1)
    if [ -n "$TUNNEL_URL" ]; then
        echo "$TUNNEL_URL" > /tmp/lm_watcher_tunnel.url
        echo "Cloudflare Tunnel: $TUNNEL_URL"
    else
        rm -f /tmp/lm_watcher_tunnel.url
        echo "Tunnel started (PID $(cat "$TUNNEL_PID_FILE")) — check $TUNNEL_LOG for URL"
    fi
else
    echo "cloudflared not found — skipping tunnel (install from https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/)"
fi
