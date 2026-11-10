import { Worker, Job } from 'bullmq';
import { redisConnection } from './redis-connection';
import { NotificationPayload, EmailPayload, ImageProcessingPayload, ClickSyncPayload } from './JobTypes';
import { ThrottledProgress } from './throttled-progress';
import { PoisonPillError, isPoisonPill } from './errors';
import { pgPool } from './db';

function isValidEmail(email: string): boolean {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return typeof email === 'string' && emailRegex.test(email.trim());
}

function isValidImage(url: string): boolean {
    const urlRegex = /^(https?:\/\/)?([\da-z.-]+)\.([a-z.]{2,6})([/\w .-]*)*\/?$/;
    return typeof url === 'string' && urlRegex.test(url.trim());
}

export const worker = new Worker(
    'my-queue',
    async (job: Job) => {
        const priority = job.opts.priority ?? 'default';
        console.log(`[START] Processing ${job.name} (Priority: ${priority}, ID: ${job.id})`);

        switch (job.name) {
            case 'notification': {
                const data = job.data as NotificationPayload;
                // Fast execution: simulating 200ms
                await new Promise((r) => setTimeout(r, 200));
                console.log(`[DONE] Sent ${data.channel} notification to user ${data.userId}`);
                return { sent: true };
            }

            case 'email': {
                if (!isValidEmail(job.data.to)) {
                    // Fatal validation error -> Poison Pill
                    throw new PoisonPillError(`Invalid email address: ${job.data.to}`, 'INVALID_EMAIL');
                }

                const data = job.data as EmailPayload;
                // Moderate execution: simulating 500ms
                await new Promise((r) => setTimeout(r, 500));
                console.log(`[DONE] Sent email to ${data.to}`);
                return { sent: true };
            }

            case 'image-processing': {
                if (!isValidImage(job.data.imageUrl)) {
                    // Fatal validation error -> Poison Pill
                    throw new PoisonPillError(`Invalid image URL: ${job.data.imageUrl}`, 'INVALID_IMAGE_URL');
                }

                const data = job.data as ImageProcessingPayload;

                // Instantiate per-job throttler: min 800ms between Redis writes, >= 5% delta
                const progressTracker = new ThrottledProgress(job, {
                    minIntervalMs: 800,
                    minDelta: 5,
                });

                // Simulate multi-chunk processing (18 chunks x 250ms = 4.5s duration)
                const totalChunks = 18;
                for (let chunk = 1; chunk <= totalChunks; chunk++) {
                    await new Promise((r) => setTimeout(r, 250));
                    const percent = Math.floor((chunk / totalChunks) * 100);
                    // Throttler handles intermediate gates
                    await progressTracker.update(percent);
                }

                // Guaranteed terminal flush to bypass throttle and force strictly 100%
                await progressTracker.flush(100);

                console.log(`[DONE] Resized image ${data.imageUrl} to ${data.dimensions.width}x${data.dimensions.height}`);
                return { processed: true };
            }

            case 'click-sync': {
                const data = (job.data as ClickSyncPayload) || {};
                const batchLimit = data.batchSize || 100;

                // 1. Pop dirty codes from Redis dirty set
                const rawDirty = await redisConnection.spop('dirty:clicks', batchLimit);
                const codes = Array.isArray(rawDirty) ? rawDirty : rawDirty ? [rawDirty] : [];

                if (codes.length === 0) {
                    console.log(`[CLICK-SYNC] No dirty click keys to synchronize.`);
                    return { syncedCodes: 0, totalClicks: 0 };
                }

                let syncedCodes = 0;
                let totalClicks = 0;

                for (const code of codes) {
                    // Atomic GETDEL ensures clicks accumulating after this point start a fresh counter
                    const clicksStr = await redisConnection.getdel(`clicks:${code}`);
                    const clicks = Number(clicksStr) || 0;

                    if (clicks > 0) {
                        try {
                            await pgPool.query(
                                'UPDATE urls SET click_cnt = click_cnt + $1 WHERE short_code = $2',
                                [clicks, code]
                            );
                            totalClicks += clicks;
                            syncedCodes++;
                        } catch (dbErr: any) {
                            console.error(`[CLICK-SYNC ERROR] Failed updating DB for code ${code}:`, dbErr.message);
                            // Restore counts in Redis so no clicks are lost
                            await redisConnection.incrby(`clicks:${code}`, clicks);
                            await redisConnection.sadd('dirty:clicks', code);
                            throw dbErr; // Let BullMQ retry/backoff handle transient errors
                        }
                    }
                }

                console.log(`[DONE] [CLICK-SYNC] Synced ${totalClicks} clicks across ${syncedCodes} URLs`);
                return { syncedCodes, totalClicks };
            }

            default:
                throw new Error(`Unknown job type: ${job.name}`);
        }
    },
    {
        connection: redisConnection,
        concurrency: 1, // Keep concurrency limit to 1
    }
);

worker.on('completed', (job: Job) => {
    console.log(`[COMPLETED] Job ${job.id} completed successfully`);
});

worker.on('failed', (job: Job | undefined, err: Error) => {
    if (!job) {
        console.error('Job failed with unknown context:', err);
        return;
    }

    if (isPoisonPill(job, err)) {
        console.error(`[POISON PILL] Job ${job.id} (${job.name}) permanently failed: ${err.message}. Retries immediately skipped.`);
    } else {
        const attemptsLeft = (job.opts.attempts || 1) - job.attemptsMade;
        if (attemptsLeft > 0) {
            console.warn(`[TRANSIENT RETRY] Job ${job.id} (${job.name}) failed: ${err.message}. Backoff scheduled (${attemptsLeft} retries remaining)...`);
        } else {
            console.error(`[TRANSIENT EXHAUSTED] Job ${job.id} (${job.name}) exhausted all ${job.opts.attempts} attempts: ${err.message}. Preserved in native failed state.`);
        }
    }
});

// Graceful shutdown
export async function shutdownWorker() {
    console.log('Shutting down worker...');
    await worker.close();
    await pgPool.end();
}
