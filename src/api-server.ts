import express, { Request, Response, NextFunction } from 'express';
import { Queue, Job } from 'bullmq';
import { Server } from 'http';
import { redisConnection } from './redis-connection';
import { IdempotencyManager } from './idempotency';
import { isPoisonPill } from './errors';
import { inspectAndReplayFailedJobs } from './replay-service';
import {
    JobPriority,
    NotificationPayload,
    EmailPayload,
    ImageProcessingPayload,
} from './JobTypes';

export const app = express();
app.use(express.json());

// Main default queue instance
export const queue = new Queue('my-queue', { connection: redisConnection });
const idempotency = new IdempotencyManager(redisConnection);

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
