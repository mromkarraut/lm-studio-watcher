# LM Studio Watcher

Monitors LM Studio usage from WSL — tracks requests, token counts, latency, and model state in a live dashboard.

## Startup

### Prerequisites

- LM Studio running on Windows with the local server enabled (port 1234)
- WSL2 with mirrored networking
- `iptables` installed: `sudo apt-get install -y iptables`
- Python dependencies: `pip install -r requirements.txt`

### Start the watcher

```bash
sudo bash ~/repos/lm_studio_watcher/start_lm_watcher.sh
```

This will:
1. Stop any existing watcher instance
2. Set up the iptables rule to intercept WSL traffic to `:1234` (captures requests from any WSL service, e.g. financial-agent on `:8000`)
3. Start the FastAPI server on port 8080

Open the dashboard at **http://localhost:8080/**

### Stop the watcher

```bash
kill $(cat /tmp/lm_watcher.pid)
sudo bash ~/repos/lm_studio_watcher/teardown_intercept.sh
```

## Notes

- The iptables rule does not persist across WSL restarts — re-run the start script after each reboot
- Logs are written to `/tmp/lm_watcher.log`
- The watcher polls LM Studio every 5 minutes and also on every dashboard page refresh
- Requests routed through the watcher's proxy (`http://localhost:8080/proxy/v1/*`) are also tracked
