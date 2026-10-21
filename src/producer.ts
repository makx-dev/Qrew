import { Queue } from 'bullmq';
import { redisConnection } from './redis-connection';
import {
    JobPriority,
    NotificationPayload,
    EmailPayload,
    ImageProcessingPayload,
} from './JobTypes';

const queue = new Queue('my-queue', { connection: redisConnection });

export async function addNotificationJob(
    data: NotificationPayload,
    priority: JobPriority = JobPriority.CRITICAL
) {
    return queue.add('notification', data, {
        priority,
        attempts: 3, // Retry 3 times before failing
        backoff: { type: 'exponential', delay: 500 },
        removeOnComplete: 1000, //keeps the last 1000 jobs completed
        removeOnFail: false, //keeps the last 1000 jobs failed
    });
}

export async function addEmailJob(
    data: EmailPayload,
    priority: JobPriority = JobPriority.HIGH
) {
    return queue.add('email', data, {
        priority,
        attempts: 3, // Retry 3 times before failing
        backoff: { type: 'exponential', delay: 500 },
        removeOnComplete: 1000, //keeps the last 1000 jobs completed
        removeOnFail: false, //keeps the last 1000 jobs failed
    });
}

export async function addImageJob(
    data: ImageProcessingPayload,
    priority: JobPriority = JobPriority.MEDIUM
) {
    return queue.add('image-processing', data, {
        priority,
        attempts: 2, // Retry 2 times before failing
        removeOnComplete: 1000, //keeps the last 1000 jobs completed
        removeOnFail: false, //keeps the last 1000 jobs failed
    });
}

// If you need to close the queue explicitly (for graceful shutdown)
export async function closeProducer() {
    await queue.close();
}