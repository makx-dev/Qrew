import { Worker, Job } from 'bullmq';
import { redisConnection } from './redis-connection';
import { NotificationPayload, EmailPayload, ImageProcessingPayload } from './JobTypes';

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
                const data = job.data as EmailPayload;
                // Moderate execution: simulating 500ms
                await new Promise((r) => setTimeout(r, 500));
                console.log(`[DONE] Sent email to ${data.to}`);
                return { sent: true };
            }
            case 'image-processing': {
                const data = job.data as ImageProcessingPayload;
                // Heavy execution: simulating 1000ms
                await new Promise((r) => setTimeout(r, 1000));
                console.log(`[DONE] Resized image ${data.imageUrl} to ${data.dimensions.width}x${data.dimensions.height}`);
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
