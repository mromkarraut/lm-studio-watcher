/* ── LM Studio Watcher — frontend ───────────────────────────────── */

const fmtNum = n => typeof n === 'number' ? n.toLocaleString() : '—';
const fmtTs  = ts => new Date(ts * 1000).toLocaleTimeString();
const fmtSec = s  => typeof s === 'number' ? s.toFixed(3) + ' s' : '—';

// ── Charts ──────────────────────────────────────────────────────────

const CHART_DEFAULTS = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 300 },
  plugins: { legend: { display: false } },
  scales: {
    x: { display: false },
    y: {
      ticks: { color: '#8b949e', font: { size: 11 } },
      grid:  { color: '#30363d' },
      border: { color: '#30363d' },
    },
  },
};

// Token timeseries (area)
const tokenCtx = document.getElementById('tokenChart').getContext('2d');
const tokenChart = new Chart(tokenCtx, {
  type: 'line',
  data: {
    labels: Array.from({ length: 60 }, (_, i) => i),
    datasets: [{
      data: Array(60).fill(0),
      borderColor: '#58a6ff',
      backgroundColor: 'rgba(88,166,255,.12)',
      fill: true,
      tension: 0.4,
      pointRadius: 0,
      borderWidth: 2,
    }],
  },
  options: {
    ...CHART_DEFAULTS,
    scales: {
      ...CHART_DEFAULTS.scales,
      y: { ...CHART_DEFAULTS.scales.y, min: 0 },
    },
  },
});

// Latency per-request (bar)
const latencyCtx = document.getElementById('latencyChart').getContext('2d');
const latencyChart = new Chart(latencyCtx, {
  type: 'bar',
  data: {
    labels: [],
    datasets: [{
      data: [],
      backgroundColor: 'rgba(63,185,80,.5)',
      borderColor: '#3fb950',
      borderWidth: 1,
      borderRadius: 3,
    }],
  },
  options: {
    ...CHART_DEFAULTS,
    scales: {
      ...CHART_DEFAULTS.scales,
      y: { ...CHART_DEFAULTS.scales.y, min: 0, title: { display: true, text: 'seconds', color: '#8b949e', font: { size: 10 } } },
    },
  },
});

// ── DOM helpers ─────────────────────────────────────────────────────

function setStatus(online) {
  const dot   = document.getElementById('status-dot');
  const label = document.getElementById('server-status-label');
  dot.className   = 'status-dot ' + (online ? 'online' : 'offline');
  label.textContent = online ? 'Online' : 'Offline';
}

function renderModels(models) {
  const el = document.getElementById('model-list');
  if (!models.length) {
    el.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:4px">No models found</div>';
    return;
  }
  el.innerHTML = models.map(m => {
    const loaded = m.state === 'loaded';
    const ctx = loaded && m.loaded_context_length
      ? `${(m.loaded_context_length / 1024).toFixed(0)}k ctx`
      : (m.max_context_length ? `${(m.max_context_length / 1024).toFixed(0)}k max` : '');
    return `
      <div class="model-card ${loaded ? 'loaded' : ''}">
        <div class="model-name">${m.id}</div>
        <div class="model-tags">
          <span class="tag ${m.state === 'loaded' ? 'loaded' : 'not-loaded'}">${m.state || '?'}</span>
          ${m.quantization ? `<span class="tag">${m.quantization}</span>` : ''}
          ${m.arch        ? `<span class="tag">${m.arch}</span>` : ''}
          ${ctx           ? `<span class="tag">${ctx}</span>` : ''}
          ${m.type        ? `<span class="tag">${m.type}</span>` : ''}
        </div>
      </div>`;
  }).join('');
}

function renderTotals(totals, requests) {
  document.getElementById('stat-requests').textContent         = fmtNum(totals.requests);
  document.getElementById('stat-prompt-tokens').textContent    = fmtNum(totals.prompt_tokens);
  document.getElementById('stat-completion-tokens').textContent= fmtNum(totals.completion_tokens);
  document.getElementById('stat-total-tokens').textContent     = fmtNum(totals.prompt_tokens + totals.completion_tokens);

  if (requests.length) {
    const lats = requests.map(r => r.latency).filter(Number.isFinite);
    const avg  = lats.length ? (lats.reduce((a, b) => a + b, 0) / lats.length).toFixed(3) : '—';
    document.getElementById('stat-avg-latency').textContent = avg;

    const tps = requests.map(r => r.tokens_per_sec).filter(Number.isFinite);
    const avgTps = tps.length ? (tps.reduce((a, b) => a + b, 0) / tps.length).toFixed(1) : '—';
    document.getElementById('stat-tps').textContent = avgTps;
  }
}

function renderTable(requests) {
  document.getElementById('req-count-badge').textContent = requests.length;
  const tbody = document.getElementById('req-tbody');
  tbody.innerHTML = requests.map(r => {
    const ok = r.status >= 200 && r.status < 300;
    return `
      <tr>
        <td>${fmtTs(r.ts)}</td>
        <td class="td-model" title="${r.model}">${r.model}</td>
        <td class="td-path">${r.path}</td>
        <td>${fmtNum(r.prompt_tokens)}</td>
        <td>${fmtNum(r.completion_tokens)}</td>
        <td>${fmtNum(r.total_tokens)}</td>
        <td>${r.tokens_per_sec != null ? r.tokens_per_sec : '—'}</td>
        <td>${fmtSec(r.latency)}</td>
        <td class="${ok ? 'status-ok' : 'status-err'}">${r.status}</td>
      </tr>`;
  }).join('');
}

function renderTokenChart(buckets) {
  tokenChart.data.datasets[0].data = [...buckets];
  tokenChart.update('none');
}

function renderLatencyChart(requests) {
  const recent = [...requests].reverse().slice(0, 20);
  latencyChart.data.labels   = recent.map((_, i) => `#${i + 1}`);
  latencyChart.data.datasets[0].data = recent.map(r => r.latency ?? 0);
  latencyChart.update('none');
}

// ── WebSocket ────────────────────────────────────────────────────────

let ws, reconnectMs = 1000;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onmessage = ({ data }) => {
    const msg = JSON.parse(data);
    setStatus(msg.server_online);

    const pollLabel = document.getElementById('last-poll-label');
    pollLabel.textContent = msg.last_poll
      ? 'Polled ' + fmtTs(msg.last_poll)
      : '—';

    renderModels(msg.models || []);
    renderTotals(msg.totals || {}, msg.recent_requests || []);
    renderTable(msg.recent_requests || []);
    renderTokenChart(msg.token_timeseries || Array(60).fill(0));
    renderLatencyChart(msg.recent_requests || []);

    reconnectMs = 1000; // reset back-off on success
  };

  ws.onclose = () => {
    setStatus(false);
    document.getElementById('last-poll-label').textContent = 'Reconnecting…';
    setTimeout(connect, reconnectMs);
    reconnectMs = Math.min(reconnectMs * 1.5, 15000);
  };

  ws.onerror = () => ws.close();

  // Keep-alive ping every 20 s
  ws.onopen = () => {
    setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send('ping');
    }, 20000);
  };
}

connect();
