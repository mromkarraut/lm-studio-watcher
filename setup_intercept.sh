#!/usr/bin/env bash
# Redirect WSL traffic destined for LM Studio (:1234) through the watcher's
# intercept proxy (:1235). Connections from the watcher itself are skipped via
# SO_MARK=1, which requires CAP_NET_ADMIN on the Python process.
#
# Run once after booting WSL. Re-run after restarting WSL (rules don't persist).

set -e

LMSTUDIO_PORT=1234
INTERCEPT_PORT=1235
MARK=1

# Grant CAP_NET_ADMIN to the active Python so SO_MARK works without sudo
PYTHON=$(which python3 || which python)
sudo setcap cap_net_admin=eip "$PYTHON"

# Install iptables redirect rule (idempotent check)
if ! sudo iptables -t nat -C OUTPUT -p tcp --dport $LMSTUDIO_PORT \
        -m mark ! --mark $MARK -j REDIRECT --to-port $INTERCEPT_PORT 2>/dev/null; then
    sudo iptables -t nat -I OUTPUT -p tcp --dport $LMSTUDIO_PORT \
        -m mark ! --mark $MARK -j REDIRECT --to-port $INTERCEPT_PORT
    echo "iptables rule installed."
else
    echo "iptables rule already present."
fi

echo "Intercept active: WSL traffic to :$LMSTUDIO_PORT is captured by watcher on :$INTERCEPT_PORT"
