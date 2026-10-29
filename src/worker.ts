import { Worker, Job } from 'bullmq';
import { redisConnection } from './redis-connection';
import { NotificationPayload, EmailPayload, ImageProcessingPayload } from './JobTypes';
import { ThrottledProgress } from './throttled-progress';
import { PoisonPillError, isPoisonPill } from './errors';

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

                // Instantiate per-job throttler: max 1 write/sec or >= 5% change
                const progressTracker = new ThrottledProgress(job, {
                    minIntervalMs: 1000,
                    minDelta: 5,
                });

                // Simulate multi-chunk processing (20 chunks x 150ms = ~3000ms duration)
                const totalChunks = 20;
                for (let chunk = 1; chunk <= totalChunks; chunk++) {
                    await new Promise((r) => setTimeout(r, 150));
                    const percent = Math.floor((chunk / totalChunks) * 100);
                    // High-frequency call: throttler drops redundant Redis writes in memory
                    await progressTracker.update(percent);
                }

                // Guaranteed terminal flush to ensure Redis state is strictly 100%
                await progressTracker.flush(100);

                console.log(`[DONE] Resized image ${data.imageUrl} to ${data.dimensions.width}x${data.dimensions.height}`);
                return { processed: true };
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
}
