import { addNotificationJob, addEmailJob, addImageJob, closeProducer } from './producer';
import { redisConnection } from './redis-connection';
import { JobPriority, ImageProcessingPayload, EmailPayload, NotificationPayload } from './JobTypes';

// Sample verification flow in producer-runner.ts
async function main() {
    console.log('1. Enqueueing LOW priority jobs (image-processing, Priority: 3)...');
    for (let i = 1; i <= 5; i++) {
        await addImageJob({ imageUrl: `img-${i}.jpg`, dimensions: { width: 800, height: 600 }, format: 'webp' });
    }

    console.log('2. Enqueueing MEDIUM priority jobs (email, Priority: 2)...');
    for (let i = 1; i <= 5; i++) {
        await addEmailJob({ to: `user${i}@example.com`, subject: 'Hello', body: '...' });
    }

    console.log('3. Enqueueing HIGH priority jobs (notification, Priority: 1)...');
    for (let i = 1; i <= 5; i++) {
        await addNotificationJob({ userId: `user-${i}`, message: 'OTP alert', channel: 'sms' });
    }

    console.log('All jobs enqueued!');
    await closeProducer();
    await redisConnection.quit();
    process.exit(0);
}

main().catch(console.error);