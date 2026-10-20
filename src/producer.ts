import { Queue } from 'bullmq';
import { redisConnection } from './redis-connection';

export type PaintJob = { color: string };

const queue = new Queue('my-queue', { connection: redisConnection });

export async function addPaintJob(payload: PaintJob) {
    return queue.add('paint', payload, {
        attempts: 3,
        backoff: { type: 'exponential', delay: 500 },
        removeOnComplete: 1000, //keeps the last 1000 jobs completed
        removeOnFail: false, //keeps the last 1000 jobs failed
        delay: 1000,
    });
}

// If you need to close the queue explicitly (for graceful shutdown)
export async function closeProducer() {
    await queue.close();
}