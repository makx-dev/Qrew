import { Worker, Job, UnrecoverableError } from 'bullmq';
import { redisConnection } from './redis-connection';
import { NotificationPayload, EmailPayload, ImageProcessingPayload } from './JobTypes';

function isValidEmail(email: string): boolean {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return typeof email === 'string' && emailRegex.test(email.trim());
}

function isValidImage(url: string): boolean {
    const urlRegex = /^(https?:\/\/)?([\da-z.-]+)\.([a-z.]{2,6})([/\w .-]*)*\/?$/;
    return typeof url === 'string' && urlRegex.test(url.trim());
}

// Map to track the timestamp of the previous attempt per job ID
const attemptTimestamps = new Map<string, number>();

export const worker = new Worker(
    'my-queue',
    async (job: Job) => {
        const now = Date.now();
        const isoTime = new Date(now).toISOString();
        const attempt = job.attemptsMade + 1;
        const totalAttempts = job.opts.attempts ?? 1;
        const priority = job.opts.priority ?? 'default';
        // Calculate delta since last attempt (if retried)
        const lastAttempt = attemptTimestamps.get(job.id!);
        const deltaMs = lastAttempt ? now - lastAttempt : 0;
        attemptTimestamps.set(job.id!, now);
        console.log(
            `[${isoTime}] [ATTEMPT ${attempt}/${totalAttempts}] Job ${job.name} (ID: ${job.id}, Priority: ${priority})` +
            (lastAttempt ? ` -> Delay since last attempt: ${deltaMs}ms` : ' -> Initial execution')
        );
        console.log(`[START] Processing ${job.name} (Priority: ${priority}, ID: ${job.id})`);
        switch (job.name) {
            case 'notification': {

                // Simulate transient failure (e.g., HTTP 429 rate limit) on attempts 1 to 3
                if (job.data.simulateTransientFailure && attempt < 4) {
                    console.log(`[${isoTime}] [FAILING TRANSIENTLY] Simulating HTTP 429 for job ${job.id}`);
                    throw new Error(`Simulated transient rate limit error (HTTP 429) on attempt ${attempt}`);
                }

                const data = job.data as NotificationPayload;
                // Fast execution: simulating 200ms
                await new Promise((r) => setTimeout(r, 200));
                console.log(`[DONE] Sent ${data.channel} notification to user ${data.userId}`);
                console.log(
                    `[START] Processing ${job.name} (Priority: ${priority}, ID: ${job.id}, Attempt: ${job.attemptsMade + 1}/${job.opts.attempts})`
                );
                return { sent: true };
            }
            case 'email': {

                if (!isValidEmail(job.data.to)) {
                    throw new UnrecoverableError(`Invalid email address ${job.data.to}`);
                }

                // Simulate transient failure (e.g., HTTP 429 rate limit) on attempts 1 to 3
                if (job.data.simulateTransientFailure && attempt < 4) {
                    console.log(`[${isoTime}] [FAILING TRANSIENTLY] Simulating HTTP 429 for job ${job.id}`);
                    throw new Error(`Simulated transient rate limit error (HTTP 429) on attempt ${attempt}`);
                }

                const data = job.data as EmailPayload;
                // Moderate execution: simulating 500ms
                await new Promise((r) => setTimeout(r, 500));
                console.log(`[DONE] Sent email to ${data.to}`);
                console.log(
                    `[START] Processing ${job.name} (Priority: ${priority}, ID: ${job.id}, Attempt: ${job.attemptsMade + 1}/${job.opts.attempts})`
                );
                return { sent: true };
            }
            case 'image-processing': {
                if (!isValidImage(job.data.imageUrl)) {
                    throw new UnrecoverableError(`Invalid image URL ${job.data.imageUrl}`);
                }

                // Simulate transient failure (e.g., HTTP 429 rate limit) on attempts 1 to 3
                if (job.data.simulateTransientFailure && attempt < 4) {
                    console.log(`[${isoTime}] [FAILING TRANSIENTLY] Simulating HTTP 429 for job ${job.id}`);
                    throw new Error(`Simulated transient rate limit error (HTTP 429) on attempt ${attempt}`);
                }

                const data = job.data as ImageProcessingPayload;
                // Heavy execution: simulating 1000ms
                await new Promise((r) => setTimeout(r, 1000));
                console.log(`[DONE] Resized image ${data.imageUrl} to ${data.dimensions.width}x${data.dimensions.height}`);
                console.log(
                    `[START] Processing ${job.name} (Priority: ${priority}, ID: ${job.id}, Attempt: ${job.attemptsMade + 1}/${job.opts.attempts})`
                );
                return { processed: true };
            }
            default:
                throw new Error(`Unknown job type: ${job.name}`);
        }
    },
    {
        connection: redisConnection,
        concurrency: 1,
    }
);

worker.on('completed', (job: Job) => {
    console.log(`Job ${job.id} completed`);
});

worker.on('failed', (job: Job | undefined, err: Error) => {
    console.error(`Job ${job?.id} failed:`, err);
});

// Graceful shutdown
export async function shutdownWorker() {
    console.log('Shutting down worker...');
    await worker.close(); // closes worker's own connections
    // Note: do NOT quit redisConnection here if other parts of app still use it
}
