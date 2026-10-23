import { Queue } from 'bullmq';
import { redisConnection } from './redis-connection';
import {
    JobPriority,
    NotificationPayload,
    EmailPayload,
    ImageProcessingPayload,
} from './JobTypes';

const queue = new Queue('my-queue', { connection: redisConnection });

// 1. Critical notification: tight 20% jitter
export async function addNotificationJob(
    data: NotificationPayload,
    priority: JobPriority = JobPriority.CRITICAL
) {
    return queue.add('notification', data, {
        priority,
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 500,
            jitter: 0.2, // 20% variance (400ms - 500ms for attempt 1)
        },
        removeOnComplete: 1000,
        removeOnFail: false,
    });
}

// 2. High priority email: 50% jitter to absorb 429 rate limits
export async function addEmailJob(
    data: EmailPayload,
    priority: JobPriority = JobPriority.HIGH
) {
    return queue.add('email', data, {
        priority,
        attempts: 5,
        backoff: {
            type: 'exponential',
            delay: 2000,
            jitter: 0.5, // 50% variance (1.5s - 3s for attempt 1)
        },
        removeOnComplete: 1000,
        removeOnFail: false,
    });
}
// 3. Medium priority image processing: fixed 5s delay with 30% jitter
export async function addImageJob(
    data: ImageProcessingPayload,
    priority: JobPriority = JobPriority.MEDIUM
) {
    return queue.add('image-processing', data, {
        priority,
        attempts: 3,
        backoff: {
            type: 'fixed',
            delay: 10000,
            jitter: 0.3, // 30% variance (3.5s - 5.0s)
        },
        removeOnComplete: 1000,
        removeOnFail: false,
    });
}

// If you need to close the queue explicitly (for graceful shutdown)
export async function closeProducer() {
    await queue.close();
}