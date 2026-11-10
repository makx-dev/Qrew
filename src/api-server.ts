import express, { Request, Response, NextFunction } from 'express';
import { Queue, Job } from 'bullmq';
import { Server } from 'http';
import { redisConnection } from './redis-connection';
import { IdempotencyManager } from './idempotency';
import { isPoisonPill } from './errors';
import { inspectAndReplayFailedJobs } from './replay-service';
import { createMultiRoleBullBoards, enforceReadOnlyMethods } from './monitoring-board';
import { requireUserAuth, requireAdminAuth } from './monitoring-auth';
import {
    JobPriority,
    NotificationPayload,
    EmailPayload,
    ImageProcessingPayload,
    ClickSyncPayload,
} from './JobTypes';

export const app = express();
app.use(express.json());

// Main default queue instance
export const queue = new Queue('my-queue', { connection: redisConnection });
const idempotency = new IdempotencyManager(redisConnection);

// Setup multi-role Bull Board instances (Admin vs User Read-Only)
export const { adminRouter, userRouter } = createMultiRoleBullBoards({
    queues: [queue],
});

// Mount User Read-Only Dashboard (Strict user auth + read-only HTTP method guard)
app.use('/user/queues', requireUserAuth, enforceReadOnlyMethods, userRouter);

// Mount Admin Interactive Dashboard (Strict admin auth + full mutation controls)
app.use('/admin/queues', requireAdminAuth, adminRouter);

/**
 * Root Portfolio Landing Page & Interactive Dispatcher Console
 */
app.get('/', (_req: Request, res: Response) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Qrew — Distributed Asynchronous Job Queue Engine</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <script src="https://unpkg.com/lucide@latest"></script>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: #0f172a;
      --card-subtle: #131d35;
      --border: #1e293b;
      --border-hover: #334155;
      --text: #f1f5f9;
      --text-muted: #94a3b8;
      --text-dim: #64748b;
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.15);
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --p1: #ef4444;
      --p2: #38bdf8;
      --p3: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      padding: 32px 20px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }
    .container { max-width: 1040px; margin: 0 auto; }
    
    /* Header */
    header {
      border-bottom: 1px solid var(--border);
      padding-bottom: 24px;
      margin-bottom: 28px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
    }
    .logo-group { display: flex; align-items: center; gap: 12px; }
    .logo-icon {
      width: 40px;
      height: 40px;
      border-radius: 10px;
      background: linear-gradient(135deg, #0284c7, #38bdf8);
      display: flex;
      align-items: center;
      justify-content: center;
      color: #fff;
      box-shadow: 0 4px 12px rgba(56, 189, 248, 0.25);
    }
    h1 {
      font-size: 22px;
      color: #fff;
      font-weight: 700;
      letter-spacing: -0.02em;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .subhead { font-size: 13px; color: var(--text-muted); margin-top: 4px; }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      font-weight: 600;
      padding: 6px 12px;
      border-radius: 9999px;
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid rgba(16, 185, 129, 0.25);
      color: var(--success);
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--success);
      box-shadow: 0 0 8px var(--success);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.5; transform: scale(0.85); }
    }
    .header-links { display: flex; gap: 12px; }
    .endpoint-link {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: var(--text-muted);
      text-decoration: none;
      padding: 6px 12px;
      border-radius: 6px;
      border: 1px solid var(--border);
      background: var(--card-bg);
      font-family: 'JetBrains Mono', monospace;
      transition: all 0.2s;
    }
    .endpoint-link:hover { color: var(--accent); border-color: var(--accent); }

    /* Telemetry Grid */
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 14px;
      margin-bottom: 28px;
    }
    .stat-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px 18px;
      position: relative;
      transition: transform 0.15s, border-color 0.15s;
    }
    .stat-card:hover { border-color: var(--border-hover); transform: translateY(-1px); }
    .stat-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .stat-lbl { font-size: 12px; font-weight: 500; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; }
    .stat-icon { color: var(--text-dim); }
    .stat-val { font-size: 26px; font-weight: 700; color: #fff; font-family: 'JetBrains Mono', monospace; }
    .stat-val.success { color: var(--success); }
    .stat-val.danger { color: var(--danger); }
    .stat-val.warning { color: var(--warning); }
    .stat-val.accent { color: var(--accent); }

    /* Section Cards */
    .section-title {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 14px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 24px;
      margin-bottom: 28px;
    }

    /* Monitoring Links */
    .dash-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; }
    .dash-card {
      background: var(--card-subtle);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 18px;
      text-decoration: none;
      color: var(--text);
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      transition: all 0.2s;
    }
    .dash-card:hover {
      border-color: var(--accent);
      background: #17233f;
      transform: translateY(-2px);
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.3);
    }
    .dash-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px; }
    .dash-title { font-size: 15px; font-weight: 600; color: #fff; display: flex; align-items: center; gap: 8px; }
    .dash-desc { font-size: 13px; color: var(--text-muted); margin-bottom: 14px; }
    .creds-pill {
      align-self: flex-start;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: #090d16;
      border: 1px solid var(--border);
      padding: 5px 10px;
      border-radius: 6px;
      font-family: 'JetBrains Mono', monospace;
      color: var(--warning);
      font-size: 11px;
    }

    /* Actions Grid */
    .actions-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 14px;
    }
    .action-btn {
      background: var(--card-subtle);
      border: 1px solid var(--border);
      color: #fff;
      padding: 16px;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.2s;
      text-align: left;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .action-btn:hover {
      border-color: var(--accent);
      background: #17233f;
      transform: translateY(-2px);
    }
    .action-btn:active { transform: translateY(0); }
    .btn-header { display: flex; justify-content: space-between; align-items: center; }
    .btn-title { font-size: 14px; font-weight: 600; color: #fff; display: flex; align-items: center; gap: 8px; }
    .tier-badge {
      font-size: 11px;
      font-weight: 700;
      padding: 2px 7px;
      border-radius: 4px;
      text-transform: uppercase;
      font-family: 'JetBrains Mono', monospace;
    }
    .tier-p1 { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .tier-p2 { background: rgba(56, 189, 248, 0.2); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.4); }
    .tier-p3 { background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.4); }
    .tier-dlq { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }
    .btn-desc { font-size: 12px; color: var(--text-muted); line-height: 1.4; }

    /* Execution Terminal */
    .terminal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }
    .terminal-dots { display: flex; gap: 6px; }
    .t-dot { width: 10px; height: 10px; border-radius: 50%; }
    .t-dot.red { background: #ef4444; }
    .t-dot.yellow { background: #f59e0b; }
    .t-dot.green { background: #10b981; }
    .clear-btn {
      background: transparent;
      border: 1px solid var(--border);
      color: var(--text-muted);
      padding: 4px 10px;
      border-radius: 4px;
      font-size: 11px;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .clear-btn:hover { color: #fff; border-color: var(--border-hover); }
    pre {
      background: #060911;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px;
      color: #38bdf8;
      font-family: 'JetBrains Mono', Consolas, monospace;
      font-size: 12px;
      overflow-x: auto;
      max-height: 240px;
      line-height: 1.6;
    }
    .log-ts { color: var(--text-dim); }
    .log-info { color: #a5f3fc; }
    .log-success { color: #34d399; }
    .log-warn { color: #fbbf24; }
    .log-err { color: #f87171; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo-group">
        <div class="logo-icon">
          <i data-lucide="layers" style="width: 22px; height: 22px;"></i>
        </div>
        <div>
          <h1>Qrew Engine <span class="status-pill" id="broker-status"><span class="status-dot"></span>Broker Connected</span></h1>
          <p class="subhead">Production Asynchronous Job Engine &bull; Redis ZSET &bull; Neon Postgres &bull; Express RBAC</p>
        </div>
      </div>
      <div class="header-links">
        <a class="endpoint-link" href="/health" target="_blank">
          <i data-lucide="activity" style="width: 14px; height: 14px;"></i> /health
        </a>
        <a class="endpoint-link" href="/api/v1/metrics" target="_blank">
          <i data-lucide="bar-chart-2" style="width: 14px; height: 14px;"></i> /api/v1/metrics
        </a>
      </div>
    </header>

    <!-- Telemetry -->
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-header">
          <span class="stat-lbl">Active</span>
          <i data-lucide="play" class="stat-icon" style="width: 16px; height: 16px;"></i>
        </div>
        <div class="stat-val accent" id="stat-active">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-header">
          <span class="stat-lbl">Waiting</span>
          <i data-lucide="clock" class="stat-icon" style="width: 16px; height: 16px;"></i>
        </div>
        <div class="stat-val" id="stat-waiting">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-header">
          <span class="stat-lbl">Completed</span>
          <i data-lucide="check-circle-2" class="stat-icon" style="width: 16px; height: 16px;"></i>
        </div>
        <div class="stat-val success" id="stat-completed">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-header">
          <span class="stat-lbl">Dead Letter Queue</span>
          <i data-lucide="shield-alert" class="stat-icon" style="width: 16px; height: 16px;"></i>
        </div>
        <div class="stat-val danger" id="stat-failed">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-header">
          <span class="stat-lbl">Delayed / Sched</span>
          <i data-lucide="timer" class="stat-icon" style="width: 16px; height: 16px;"></i>
        </div>
        <div class="stat-val warning" id="stat-delayed">-</div>
      </div>
    </div>

    <!-- Dashboards -->
    <div class="card">
      <div class="section-title">
        <i data-lucide="layout-dashboard" style="width: 16px; height: 16px;"></i> Observability &amp; Dashboards
      </div>
      <div class="dash-grid">
        <a class="dash-card" href="/user/queues" target="_blank">
          <div>
            <div class="dash-top">
              <span class="dash-title">
                <i data-lucide="eye" style="width: 18px; height: 18px; color: var(--accent);"></i>
                Viewer Dashboard
              </span>
              <i data-lucide="external-link" style="width: 15px; height: 15px; color: var(--text-dim);"></i>
            </div>
            <p class="dash-desc">Read-only role partitioned Bull Board telemetry. Default-deny RBAC prevents mutations.</p>
          </div>
          <span class="creds-pill">
            <i data-lucide="key-round" style="width: 13px; height: 13px;"></i>
            viewer &bull; demo_viewer_pass
          </span>
        </a>
        <a class="dash-card" href="/admin/queues" target="_blank">
          <div>
            <div class="dash-top">
              <span class="dash-title">
                <i data-lucide="shield-check" style="width: 18px; height: 18px; color: #a855f7;"></i>
                Admin Console
              </span>
              <i data-lucide="external-link" style="width: 15px; height: 15px; color: var(--text-dim);"></i>
            </div>
            <p class="dash-desc">Full queue management interface. Supports retry mutations, pause/resume, and DLQ replay.</p>
          </div>
          <span class="creds-pill">
            <i data-lucide="key-round" style="width: 13px; height: 13px;"></i>
            admin &bull; demo_admin_pass
          </span>
        </a>
      </div>
    </div>

    <!-- Job Dispatcher -->
    <div class="card">
      <div class="section-title">
        <i data-lucide="send" style="width: 16px; height: 16px;"></i> Interactive Job Dispatcher
      </div>
      <div class="actions-grid">
        <button class="action-btn" onclick="dispatch('notification', { userId: 'usr_77', message: 'Your OTP is 849201', channel: 'sms' }, 1)">
          <div class="btn-header">
            <span class="btn-title"><i data-lucide="zap" style="width: 16px; height: 16px; color: #f87171;"></i> Critical OTP</span>
            <span class="tier-badge tier-p1">P1</span>
          </div>
          <span class="btn-desc">Preempts lower tiers via Redis ZSET. Fast recovery with low-jitter backoff.</span>
        </button>

        <button class="action-btn" onclick="dispatch('email', { to: 'recruiter@example.com', subject: 'Welcome to Qrew', body: 'Test asynchronous batch email' }, 2)">
          <div class="btn-header">
            <span class="btn-title"><i data-lucide="mail" style="width: 16px; height: 16px; color: #38bdf8;"></i> Transactional Email</span>
            <span class="tier-badge tier-p2">P2</span>
          </div>
          <span class="btn-desc">Tiered exponential backoff with wide jitter to mitigate downstream rate limits.</span>
        </button>

        <button class="action-btn" onclick="dispatch('image-processing', { imageUrl: 'https://example.com/banner.png', dimensions: { width: 1920, height: 1080 }, format: 'webp' }, 3)">
          <div class="btn-header">
            <span class="btn-title"><i data-lucide="image" style="width: 16px; height: 16px; color: #c084fc;"></i> Image Processing</span>
            <span class="tier-badge tier-p3">P3</span>
          </div>
          <span class="btn-desc">Heavy computation with dual-gate throttled progress reporting (0% &rarr; 100%).</span>
        </button>

        <button class="action-btn" onclick="dispatch('email', { to: 'invalid-email-address', subject: 'Simulated Poison Pill', body: 'Payload deliberately triggers PoisonPillError' }, 2)">
          <div class="btn-header">
            <span class="btn-title"><i data-lucide="alert-triangle" style="width: 16px; height: 16px; color: #fbbf24;"></i> Poison Pill</span>
            <span class="tier-badge tier-dlq">DLQ</span>
          </div>
          <span class="btn-desc">UnrecoverableError quarantine. Skips retries directly into Dead Letter Queue.</span>
        </button>
      </div>
    </div>

    <!-- Terminal -->
    <div class="card">
      <div class="terminal-header">
        <div style="display: flex; align-items: center; gap: 10px;">
          <div class="terminal-dots">
            <div class="t-dot red"></div>
            <div class="t-dot yellow"></div>
            <div class="t-dot green"></div>
          </div>
          <span style="font-size: 13px; font-weight: 600; color: var(--text-muted); display: flex; align-items: center; gap: 6px;">
            <i data-lucide="terminal" style="width: 14px; height: 14px;"></i> Live Dispatch Console
          </span>
        </div>
        <button class="clear-btn" onclick="clearLogs()">
          <i data-lucide="trash-2" style="width: 12px; height: 12px;"></i> Clear
        </button>
      </div>
      <pre id="console-log"><span class="log-ts">[Ready]</span> Dispatcher initialized. Trigger any job above to observe live ingestion.</pre>
    </div>
  </div>

  <script>
    lucide.createIcons();

    const logEl = document.getElementById('console-log');
    function log(msg, type = 'info') {
      const ts = new Date().toLocaleTimeString();
      let colorClass = 'log-info';
      if (type === 'success') colorClass = 'log-success';
      if (type === 'warn') colorClass = 'log-warn';
      if (type === 'err') colorClass = 'log-err';

      const line = '<span class="log-ts">[' + ts + ']</span> <span class="' + colorClass + '">' + msg + '</span>\\n';
      logEl.innerHTML = line + logEl.innerHTML;
    }

    function clearLogs() {
      logEl.innerHTML = '<span class="log-ts">[Cleared]</span> Console buffer reset.';
    }

    async function fetchMetrics() {
      try {
        const res = await fetch('/api/v1/metrics');
        if (!res.ok) throw new Error('Status ' + res.status);
        const data = await res.json();
        const statusEl = document.getElementById('broker-status');
        statusEl.innerHTML = '<span class="status-dot"></span>Broker Connected';
        statusEl.style.color = '#10b981';
        statusEl.style.borderColor = 'rgba(16, 185, 129, 0.25)';

        document.getElementById('stat-active').textContent = data.metrics.active;
        document.getElementById('stat-waiting').textContent = data.metrics.waiting;
        document.getElementById('stat-completed').textContent = data.metrics.completed;
        document.getElementById('stat-failed').textContent = data.metrics.failed;
        document.getElementById('stat-delayed').textContent = data.metrics.delayed;
      } catch (err) {
        const statusEl = document.getElementById('broker-status');
        statusEl.innerHTML = '<span class="status-dot" style="background: #ef4444; box-shadow: 0 0 8px #ef4444;"></span>Broker Offline';
        statusEl.style.color = '#ef4444';
        statusEl.style.borderColor = 'rgba(239, 68, 68, 0.25)';
      }
    }

    async function dispatch(type, payload, priority) {
      log("Enqueueing " + type + " (Priority " + priority + ")...", 'info');
      try {
        const res = await fetch('/api/v1/jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type, data: payload, priority })
        });
        const json = await res.json();
        if (res.status === 202) {
          log("Accepted (202) &bull; Job ID: #" + json.jobId + " &bull; State: " + json.status, 'success');
        } else {
          log("Rejected (" + res.status + "): " + (json.message || JSON.stringify(json)), 'warn');
        }
        setTimeout(fetchMetrics, 700);
      } catch (err) {
        log("Network Error: " + err.message, 'err');
      }
    }

    fetchMetrics();
    setInterval(fetchMetrics, 3000);
  </script>
</body>
</html>`);
});

/**
 * Health check endpoint: verifies Redis connection readiness
 */
app.get('/health', async (_req: Request, res: Response) => {
    try {
        const ping = await redisConnection.ping();
        return res.json({
            status: 'ok',
            broker: ping === 'PONG' ? 'healthy' : 'degraded',
            queue: queue.name,
            timestamp: new Date().toISOString(),
        });
    } catch (err: any) {
        return res.status(503).json({
            status: 'unhealthy',
            error: err.message || 'Cannot communicate with queue broker',
        });
    }
});

/**
 * Helper to validate job input payloads according to their type
 */
function validateJobPayload(type: string, data: any): { valid: boolean; error?: string } {
    if (!data || typeof data !== 'object') {
        return { valid: false, error: 'Request body must contain a valid "data" object.' };
    }

    switch (type) {
        case 'notification': {
            const p = data as NotificationPayload;
            if (!p.userId || !p.message || !['sms', 'push'].includes(p.channel)) {
                return { valid: false, error: 'Invalid notification data: userId, message, and channel ("sms" | "push") are required.' };
            }
            break;
        }
        case 'email': {
            const p = data as EmailPayload;
            if (!p.to || !p.subject || !p.body) {
                return { valid: false, error: 'Invalid email data: "to", "subject", and "body" are required.' };
            }
            break;
        }
        case 'image-processing': {
            const p = data as ImageProcessingPayload;
            if (!p.imageUrl || !p.dimensions?.width || !p.dimensions?.height || !['jpg', 'png', 'webp'].includes(p.format)) {
                return { valid: false, error: 'Invalid image-processing data: "imageUrl", "dimensions" (width, height), and format ("jpg" | "png" | "webp") are required.' };
            }
            break;
        }
        case 'click-sync': {
            const p = data as ClickSyncPayload;
            if (p.batchSize !== undefined && (typeof p.batchSize !== 'number' || p.batchSize <= 0)) {
                return { valid: false, error: 'Invalid click-sync data: batchSize must be a positive integer.' };
            }
            break;
        }
        default:
            // Custom or generic tasks allowed
            break;
    }

    return { valid: true };
}

// ==========================================
// 1. SUBMIT JOB ENDPOINT
// ==========================================
async function handleSubmitJob(req: Request, res: Response) {
    const idempotencyKey = req.header('Idempotency-Key');
    const { type, data, priority, options = {} } = req.body;

    if (!type || typeof type !== 'string') {
        return res.status(400).json({
            error: 'Bad Request',
            message: 'A valid "type" field (e.g., "notification", "email", "image-processing") is required.',
        });
    }

    // Payload validation
    const validation = validateJobPayload(type, data);
    if (!validation.valid) {
        return res.status(400).json({
            error: 'Bad Request',
            message: validation.error,
        });
    }

    // Handle Idempotency if key is supplied
    let bodyHash = '';
    if (idempotencyKey) {
        bodyHash = idempotency.computeHash(req.body);
        const check = await idempotency.checkOrLock(idempotencyKey, bodyHash);

        if (check.state === 'PAYLOAD_MISMATCH') {
            return res.status(422).json({
                error: 'Unprocessable Entity',
                message: 'Idempotency-Key already used with a different request payload.',
            });
        }
        if (check.state === 'IN_FLIGHT') {
            return res.status(409).json({
                error: 'Conflict',
                message: 'A request with this Idempotency-Key is currently in flight. Please retry shortly.',
            });
        }
        if (check.state === 'CACHED') {
            return res.status(check.response.statusCode).json(check.response.body);
        }
    }

    try {
        // Enqueue Job into BullMQ
        const job = await queue.add(type, data, {
            priority: priority ?? JobPriority.MEDIUM,
            attempts: options.attempts ?? 3,
            delay: options.delayMs ?? 0,
            backoff: options.backoff ?? {
                type: 'exponential',
                delay: 1000,
            },
            removeOnComplete: 1000,
            removeOnFail: { count: 1000 },
        });

        const responsePayload = {
            jobId: job.id,
            queue: queue.name,
            type: job.name,
            status: 'waiting',
            enqueuedAt: new Date(job.timestamp).toISOString(),
            _links: {
                self: `/api/v1/queues/${queue.name}/jobs/${job.id}`,
                status: `/api/v1/queues/${queue.name}/jobs/${job.id}/status`,
            },
        };

        if (idempotencyKey) {
            await idempotency.saveResponse(idempotencyKey, bodyHash, {
                statusCode: 202,
                body: responsePayload,
            });
        }

        res.setHeader('Location', `/api/v1/queues/${queue.name}/jobs/${job.id}`);
        return res.status(202).json(responsePayload);
    } catch (error: any) {
        if (idempotencyKey) {
            await idempotency.release(idempotencyKey);
        }
        return res.status(503).json({
            error: 'Service Unavailable',
            message: 'Failed to enqueue job onto broker.',
            details: error.message,
        });
    }
}

app.post('/api/v1/jobs', handleSubmitJob);
app.post('/api/v1/queues/:queueName/jobs', handleSubmitJob);

// 2. CHECK STATUS / PROGRESS ENDPOINTS

async function handleGetJob(req: Request, res: Response) {
    try {
        const rawId = req.params.id;
        const jobId = Array.isArray(rawId) ? rawId[0] : rawId;

        if (!jobId) {
            return res.status(400).json({
                error: 'Bad Request',
                message: 'Job ID parameter is required.',
            });
        }

        const job = await queue.getJob(jobId);
        if (!job) {
            return res.status(404).json({
                error: 'Not Found',
                message: `Job ${jobId} does not exist or has expired.`,
            });
        }

        const state = await job.getState();

        return res.json({
            jobId: job.id,
            name: job.name,
            queue: queue.name,
            type: job.name,
            state,
            progress: job.progress ?? 0,
            attemptsMade: job.attemptsMade,
            data: job.data,
            returnValue: job.returnvalue ?? null,
            failedReason: job.failedReason ?? null,
            stackTrace: job.stacktrace ?? [],
            timestamps: {
                enqueuedAt: new Date(job.timestamp).toISOString(),
                processedOn: job.processedOn ? new Date(job.processedOn).toISOString() : null,
                finishedOn: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
            },
        });
    } catch (error: any) {
        return res.status(500).json({ error: 'Internal Server Error', details: error.message });
    }
}

app.get('/api/v1/jobs/:id', handleGetJob);
app.get('/api/v1/jobs/:id/status', handleGetJob);
app.get('/api/v1/jobs/:id/progress', handleGetJob);
app.get('/api/v1/queues/:queueName/jobs/:id', handleGetJob);
app.get('/api/v1/queues/:queueName/jobs/:id/status', handleGetJob);
app.get('/api/v1/queues/:queueName/jobs/:id/progress', handleGetJob);
app.get('/jobs/:id/progress', handleGetJob);

// 3. LIST FAILED JOBS (DLQ AUDIT)

async function handleListFailedJobs(req: Request, res: Response) {
    try {
        const page = Math.max(1, parseInt(req.query.page as string, 10) || 1);
        const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string, 10) || 20));
        const typeFilter = req.query.type as string | undefined;

        const totalFailed = await queue.getFailedCount();
        const start = (page - 1) * limit;
        const end = start + limit - 1;

        const failedJobs: Job[] = await queue.getFailed(start, end);

        const data = failedJobs
            .filter((j) => !typeFilter || j.name === typeFilter)
            .map((j) => {
                const poison = isPoisonPill(j);
                return {
                    jobId: j.id,
                    name: j.name,
                    attemptsMade: j.attemptsMade,
                    failedReason: j.failedReason,
                    stackTrace: j.stacktrace?.[0] ?? null,
                    data: j.data,
                    classification: poison ? 'POISON_PILL' : 'TRANSIENT_EXHAUSTED',
                    enqueuedAt: new Date(j.timestamp).toISOString(),
                    failedAt: j.finishedOn ? new Date(j.finishedOn).toISOString() : null,
                };
            });

        return res.json({
            queue: queue.name,
            pagination: {
                total: totalFailed,
                page,
                limit,
                totalPages: Math.ceil(totalFailed / limit),
            },
            data,
        });
    } catch (error: any) {
        return res.status(500).json({ error: 'Internal Server Error', details: error.message });
    }
}

app.get('/api/v1/dlq/jobs', handleListFailedJobs);
app.get('/api/v1/queues/:queueName/dlq/jobs', handleListFailedJobs);


// 4. MANUALLY RETRY FROM DLQ

// 4A. Single Job Retry
async function handleRetryJob(req: Request, res: Response) {
    try {
        const rawId = req.params.id;
        const jobId = Array.isArray(rawId) ? rawId[0] : rawId;

        const job = await queue.getJob(jobId);
        if (!job) {
            return res.status(404).json({
                error: 'Not Found',
                message: `Job ${jobId} not found.`,
            });
        }

        const state = await job.getState();
        if (state !== 'failed') {
            return res.status(400).json({
                error: 'Bad Request',
                message: `Job ${jobId} is currently in "${state}" state, not "failed". Only failed jobs can be replayed.`,
            });
        }

        // BullMQ native retry transitions job back to wait/prioritized
        await job.retry();

        return res.json({
            jobId: job.id,
            previousState: 'failed',
            currentState: 'waiting',
            reEnqueuedAt: new Date().toISOString(),
        });
    } catch (error: any) {
        return res.status(500).json({ error: 'Internal Server Error', details: error.message });
    }
}

app.post('/api/v1/dlq/jobs/:id/retry', handleRetryJob);
app.post('/api/v1/queues/:queueName/dlq/jobs/:id/retry', handleRetryJob);

// 4B. Batch DLQ Replay via ReplayService (Redlock protected)
async function handleBatchRetry(req: Request, res: Response) {
    try {
        const { maxReplays = 3, batchSize = 100 } = req.body || {};

        const report = await inspectAndReplayFailedJobs(queue, {
            maxReplays,
            batchSize,
        });

        return res.status(200).json({
            status: 'COMPLETED',
            report,
        });
    } catch (error: any) {
        return res.status(500).json({
            error: 'Batch Replay Failed',
            details: error.message,
        });
    }
}

app.post('/api/v1/dlq/retry-batch', handleBatchRetry);
app.post('/api/v1/queues/:queueName/dlq/retry-batch', handleBatchRetry);

// 5. QUEUE METRICS & DEPTH TELEMETRY
async function handleGetQueueMetrics(_req: Request, res: Response) {
    try {
        if (redisConnection.status !== 'ready') {
            return res.status(503).json({
                error: 'Service Unavailable',
                message: 'Queue broker is offline or reconnecting.',
            });
        }

        const counts = await queue.getJobCounts(
            'waiting',
            'active',
            'completed',
            'failed',
            'delayed',
            'prioritized'
        );

        const totalWaiting = (counts.waiting || 0) + (counts.prioritized || 0);
        const queueDepth = totalWaiting + (counts.delayed || 0);

        return res.json({
            queue: queue.name,
            timestamp: new Date().toISOString(),
            queueDepth,
            metrics: {
                active: counts.active || 0,
                waiting: totalWaiting,
                delayed: counts.delayed || 0,
                failed: counts.failed || 0,
                completed: counts.completed || 0,
                paused: counts.paused || 0,
            },
        });
    } catch (error: any) {
        return res.status(500).json({
            error: 'Internal Server Error',
            message: 'Failed to retrieve queue metrics',
            details: error.message,
        });
    }
}

app.get('/api/v1/metrics', handleGetQueueMetrics);
app.get('/api/v1/queues/:queueName/metrics', handleGetQueueMetrics);

// Global unhandled error handler
app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    console.error('Unhandled API Error:', err);
    res.status(500).json({
        error: 'Internal Server Error',
        message: err.message || 'An unexpected error occurred.',
    });
});

export function startServer(port: number = 3000): Server {
    return app.listen(port, () => {
        console.log(`[Qrew API] REST API layer listening on http://localhost:${port}`);
    });
}

// Start standalone if executed directly
if (require.main === module) {
    startServer(3000);
}
