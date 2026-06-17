#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/tmp/lm_watcher.log"
PID_FILE="/tmp/lm_watcher.pid"

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
