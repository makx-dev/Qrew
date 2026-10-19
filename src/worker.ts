import { Worker, Job } from 'bullmq';
import { redisConnection } from './redis-connection';

const queueName = 'my-queue';

export const worker = new Worker(
    queueName,
    async (job: Job) => {
        switch (job.name) {
            case 'paint': {
                const data = job.data as { color: string };
                // idempotent handler, can be retried
                console.log(`Painting color=${data.color}, jobId=${job.id}`);
                // perform async work...
                return { ok: true };
            }
            default:
                throw new Error(`Unknown job type: ${job.name}`);
        }
    },
    { connection: redisConnection, concurrency: 5 }
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
