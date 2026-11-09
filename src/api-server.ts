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
  <title>Qrew — Distributed Job Queue Engine</title>
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --border: #30363d;
      --text: #c9d1d9;
      --text-muted: #8b949e;
      --accent: #58a6ff;
      --success: #3fb950;
      --warning: #d29922;
      --danger: #f85149;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
    body { background-color: var(--bg); color: var(--text); padding: 24px 16px; line-height: 1.5; }
    .container { max-width: 1000px; margin: 0 auto; }
    header { border-bottom: 1px solid var(--border); padding-bottom: 20px; margin-bottom: 24px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px; }
    h1 { font-size: 24px; color: #fff; font-weight: 700; display: flex; align-items: center; gap: 10px; }
    .badge { font-size: 12px; padding: 4px 10px; border-radius: 12px; font-weight: 600; text-transform: uppercase; background: #238636; color: #fff; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 12px; margin-bottom: 24px; }
    .stat-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 14px; text-align: center; }
    .stat-val { font-size: 24px; font-weight: 700; color: #fff; }
    .stat-lbl { font-size: 12px; color: var(--text-muted); text-transform: uppercase; margin-top: 4px; }
    .section-title { font-size: 16px; font-weight: 600; color: #fff; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 24px; }
    .actions-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
    button { background: #21262d; border: 1px solid var(--border); color: #fff; padding: 12px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: all 0.2s; text-align: left; }
    button:hover { background: #30363d; border-color: var(--accent); }
    button:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-desc { display: block; font-size: 11px; font-weight: normal; color: var(--text-muted); margin-top: 4px; }
    .dash-links { display: flex; gap: 12px; flex-wrap: wrap; }
    .dash-btn { flex: 1; min-width: 220px; background: #21262d; border: 1px solid var(--border); border-radius: 6px; padding: 14px; text-decoration: none; color: #fff; display: block; }
    .dash-btn:hover { border-color: var(--accent); background: #30363d; }
    .dash-btn strong { display: block; font-size: 14px; color: var(--accent); }
    .dash-btn span { display: block; font-size: 12px; color: var(--text-muted); margin-top: 4px; }
    .creds { display: inline-block; background: #000; padding: 2px 6px; border-radius: 4px; font-family: monospace; color: var(--warning); margin-top: 4px; font-size: 11px; }
    pre { background: #000; border: 1px solid var(--border); border-radius: 6px; padding: 14px; color: #7ee787; font-family: Consolas, monospace; font-size: 12px; overflow-x: auto; max-height: 240px; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>⚡ Qrew Engine <span class="badge" id="broker-status">Checking...</span></h1>
        <p style="font-size: 13px; color: var(--text-muted); margin-top: 4px;">
          Distributed BullMQ Queue &bull; Redis Broker &bull; Neon Postgres &bull; Express RBAC
        </p>
      </div>
      <div>
        <a href="/health" target="_blank" style="color: var(--accent); text-decoration: none; font-size: 13px;">/health</a> &bull;
        <a href="/api/v1/metrics" target="_blank" style="color: var(--accent); text-decoration: none; font-size: 13px;">/api/v1/metrics</a>
      </div>
    </header>

    <div class="stats-grid">
      <div class="stat-card"><div class="stat-val" id="stat-active">-</div><div class="stat-lbl">Active</div></div>
      <div class="stat-card"><div class="stat-val" id="stat-waiting">-</div><div class="stat-lbl">Waiting</div></div>
      <div class="stat-card"><div class="stat-val" id="stat-completed" style="color: var(--success);">-</div><div class="stat-lbl">Completed</div></div>
      <div class="stat-card"><div class="stat-val" id="stat-failed" style="color: var(--danger);">-</div><div class="stat-lbl">Failed (DLQ)</div></div>
      <div class="stat-card"><div class="stat-val" id="stat-delayed">-</div><div class="stat-lbl">Delayed</div></div>
    </div>

    <div class="card">
      <div class="section-title">📊 Live Monitoring Dashboards</div>
      <div class="dash-links">
        <a class="dash-btn" href="/user/queues" target="_blank">
          <strong>User Dashboard (Read-Only)</strong>
          <span>Observer view for queues & telemetry</span>
          <span class="creds">User: viewer &bull; Pass: demo_viewer_pass</span>
        </a>
        <a class="dash-btn" href="/admin/queues" target="_blank">
          <strong>Admin Console (Mutating)</strong>
          <span>Full queue mutation & DLQ replay</span>
          <span class="creds">User: admin &bull; Pass: demo_admin_pass</span>
        </a>
      </div>
    </div>

    <div class="card">
      <div class="section-title">🚀 Interactive Job Dispatcher (1-Click Test Bench)</div>
      <div class="actions-grid">
        <button onclick="dispatch('notification', { userId: 'usr_77', message: 'Your OTP is 849201', channel: 'sms' }, 1)">
          ⚡ Push P1 Critical Job
          <span class="btn-desc">Priority 1 &bull; Fast recovery &bull; OTP/SMS</span>
        </button>
        <button onclick="dispatch('email', { to: 'recruiter@example.com', subject: 'Welcome to Qrew', body: 'Test asynchronous batch email' }, 2)">
          📧 Push P2 Email Job
          <span class="btn-desc">Priority 2 &bull; Tiered jitter backoff</span>
        </button>
        <button onclick="dispatch('image-processing', { imageUrl: 'https://example.com/banner.png', dimensions: { width: 1920, height: 1080 }, format: 'webp' }, 3)">
          🖼️ Push P3 Heavy Job
          <span class="btn-desc">Priority 3 &bull; Throttled dual-gate progress</span>
        </button>
        <button onclick="dispatch('notification', { userId: 'usr_fail', message: 'FAIL_SIMULATION', channel: 'sms' }, 2)">
          💥 Trigger Poison Pill
          <span class="btn-desc">UnrecoverableError &bull; DLQ Quarantine</span>
        </button>
      </div>
    </div>

    <div class="card">
      <div class="section-title">📟 Live Execution Terminal</div>
      <pre id="console-log">// Click any button above to enqueue a real job into the Redis broker.
// Metrics and queue states will update automatically.</pre>
    </div>
  </div>

  <script>
    const logEl = document.getElementById('console-log');
    function log(msg) {
      const ts = new Date().toLocaleTimeString();
      logEl.textContent = "[" + ts + "] " + msg + "\\n" + logEl.textContent;
    }

    async function fetchMetrics() {
      try {
        const res = await fetch('/api/v1/metrics');
        if (!res.ok) throw new Error('Status ' + res.status);
        const data = await res.json();
        document.getElementById('broker-status').textContent = 'Online';
        document.getElementById('broker-status').style.background = '#238636';
        document.getElementById('stat-active').textContent = data.metrics.active;
        document.getElementById('stat-waiting').textContent = data.metrics.waiting;
        document.getElementById('stat-completed').textContent = data.metrics.completed;
        document.getElementById('stat-failed').textContent = data.metrics.failed;
        document.getElementById('stat-delayed').textContent = data.metrics.delayed;
      } catch (err) {
        document.getElementById('broker-status').textContent = 'Offline';
        document.getElementById('broker-status').style.background = '#da3633';
      }
    }

    async function dispatch(type, payload, priority) {
      log("Dispatching " + type + " (Priority " + priority + ")...");
      try {
        const res = await fetch('/api/v1/jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type, data: payload, priority })
        });
        const json = await res.json();
        log("Enqueued -> Job ID: " + json.jobId + " (Status: " + json.status + ")");
        setTimeout(fetchMetrics, 1000);
      } catch (err) {
        log("Error enqueuing job: " + err.message);
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

        const queueDepth = (counts.waiting || 0) + (counts.delayed || 0);

        return res.json({
            queue: queue.name,
            timestamp: new Date().toISOString(),
            queueDepth,
            metrics: {
                active: counts.active || 0,
                waiting: counts.waiting || 0,
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
