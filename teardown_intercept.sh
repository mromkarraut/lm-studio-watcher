#!/usr/bin/env bash
set -e

LMSTUDIO_PORT=1234
INTERCEPT_PORT=1235
MARK=1

sudo iptables -t nat -D OUTPUT -p tcp --dport $LMSTUDIO_PORT \
    -m mark ! --mark $MARK -j REDIRECT --to-port $INTERCEPT_PORT 2>/dev/null \
    && echo "iptables rule removed." \
    || echo "Rule not found (already removed)."
