import { Worker, Job, QueueEvents } from 'bullmq';
import { redisConnection } from './redis-connection';
import { addEmailJob, addNotificationJob, addImageJob, closeProducer } from './producer';

/**
 * 1. Verify Notification Retries (P1 / Critical)
 * - attempts: 3
 * - backoff: exponential, base: 500ms, jitter: 20%
 * - Expected delays:
 *     Retry #1: ~500ms (400ms - 600ms)
 *     Retry #2: ~1000ms (800ms - 1200ms)
 */


function verifyWindows(
    delays: number[],
    {
        base,
        jitter,
        retries,
        type = 'exponential',
        buffer = 300,
    }: {
        base: number;
        jitter: number;
        retries: number;
        type?: 'exponential' | 'fixed';
        buffer?: number;
    }
): boolean {
    const windows = Array.from({ length: retries }, (_, i) => {
        const nominal = type === 'exponential' ? base * 2 ** i : base;
        return {
            min: nominal * (1 - jitter) - buffer,
            max: nominal + buffer,
        };
    });

    const ok =
        delays.length === retries &&
        delays.every((d, i) => d >= windows[i].min && d <= windows[i].max);

    if (!ok) {
        process.exitCode = 1;
    }
    return ok;
}


export async function verifyNotificationRetries(queueEvents: QueueEvents) {
    console.log('1. Verifying Notification Retries (Exponential: 500ms, Jitter: 20%)');

    const delays: number[] = [];
    let lastTime = 0;

    const testWorker = new Worker(
        'my-queue',
        async (job: Job) => {
            if (job.name !== 'notification') return;

            const now = Date.now();
            const attempt = job.attemptsMade + 1;
            const iso = new Date(now).toISOString();

            if (lastTime > 0) {
                const delta = now - lastTime;
                delays.push(delta);
                console.log(`[${iso}] [Notification] Attempt ${attempt}/${job.opts.attempts} | Delay: ${delta}ms`);
            } else {
                console.log(`[${iso}] [Notification] Attempt ${attempt}/${job.opts.attempts} | Initial run`);
            }
            lastTime = now;

            // Fail attempts 1 and 2, succeed on attempt 3
            if (attempt < 3) {
                throw new Error(`Transient notification error on attempt ${attempt}`);
            }

            console.log(`[${iso}] [Notification] Attempt ${attempt} succeeded!`);
            return { sent: true };
        },
        { connection: redisConnection }
    );

    console.log('Enqueuing test notification job (attempts: 3, base 500ms exponential)...');
    const job = await addNotificationJob({
        userId: 'verify-user-1',
        message: 'OTP 9999',
        channel: 'sms',
    });

    await job.waitUntilFinished(queueEvents);

    console.log('\n##Notification Backoff Summary##');
    delays.forEach((delay, idx) => {
        console.log(`Retry #${idx + 1}: ${delay}ms`);
    });

    const ok = verifyWindows(delays, { base: 500, jitter: 0.2, retries: 2, type: 'exponential', buffer: 75 });
    console.log(`Result: Delays within expected windows: ${ok ? 'PASSED' : 'FAILED'}\n`);
    process.exitCode = ok ? 0 : 1;

    await testWorker.close();
}

/**
 * 2. Verify Email Retries (P2 / High)
 * - attempts: 
 * - backoff: exponential, base: 2000ms, jitter: 50%
 * - Expected delays:
 *     Retry #1: ~2000ms (1000ms - 3000ms)
 *     Retry #2: ~4000ms (2000ms - 6000ms)
 *     Retry #3: ~8000ms (4000ms - 12000ms)
 */
export async function verifyEmailRetries(queueEvents: QueueEvents) {
    console.log('2. Verifying Email Retries (Exponential: 2000ms, Jitter: 50%)');

    const delays: number[] = [];
    let lastTime = 0;

    const testWorker = new Worker(
        'my-queue',
        async (job: Job) => {
            if (job.name !== 'email') return;

            const now = Date.now();
            const attempt = job.attemptsMade + 1;
            const iso = new Date(now).toISOString();

            if (lastTime > 0) {
                const delta = now - lastTime;
                delays.push(delta);
                console.log(`[${iso}] [Email] Attempt ${attempt}/${job.opts.attempts} | Delay: ${delta}ms`);
            } else {
                console.log(`[${iso}] [Email] Attempt ${attempt}/${job.opts.attempts} | Initial run`);
            }
            lastTime = now;

            // Fail attempts 1 through 3, succeed on attempt 4
            if (attempt < 4) {
                throw new Error(`Transient email error on attempt ${attempt}`);
            }

            console.log(`[${iso}] [Email] Attempt ${attempt} succeeded!`);
            return { sent: true };
        },
        { connection: redisConnection }
    );

    console.log('Enqueuing test email job (attempts: 5, base 2000ms exponential)...');
    const job = await addEmailJob({
        to: 'verify@example.com',
        subject: 'Retry Test',
        body: 'Testing email backoff intervals',
    });

    await job.waitUntilFinished(queueEvents);

    console.log('\n##Email Backoff Summary##');
    delays.forEach((delay, idx) => {
        console.log(`Retry #${idx + 1}: ${delay}ms`);
    });

    const ok = verifyWindows(delays, { base: 2000, jitter: 0.5, retries: 3, type: 'exponential', buffer: 300 });
    console.log(`Result: Delays within expected windows: ${ok ? 'PASSED' : 'FAILED'}\n`);
    process.exitCode = ok ? 0 : 1;

    await testWorker.close();
}

/**
 * 3. Verify Image Processing Retries (P3 / Medium)
 * - attempts: 3
 * - backoff: fixed, base: 10000ms, jitter: 30%
 * - Expected delays:
 *     Retry #1: ~10000ms (7000ms - 13000ms)
 *     Retry #2: ~10000ms (7000ms - 13000ms)
 */
export async function verifyImageProcessingRetries(queueEvents: QueueEvents) {
    console.log('3. Verifying Image Processing Retries (Fixed: 10000ms, Jitter: 30%)');
    const delays: number[] = [];
    let lastTime = 0;

    const testWorker = new Worker(
        'my-queue',
        async (job: Job) => {
            if (job.name !== 'image-processing') return;

            const now = Date.now();
            const attempt = job.attemptsMade + 1;
            const iso = new Date(now).toISOString();

            if (lastTime > 0) {
                const delta = now - lastTime;
                delays.push(delta);
                console.log(`[${iso}] [ImageProcessing] Attempt ${attempt}/${job.opts.attempts} | Delay: ${delta}ms`);
            } else {
                console.log(`[${iso}] [ImageProcessing] Attempt ${attempt}/${job.opts.attempts} | Initial run`);
            }
            lastTime = now;

            // Fail attempts 1 and 2, succeed on attempt 3
            if (attempt < 3) {
                throw new Error(`Transient image processing error on attempt ${attempt}`);
            }

            console.log(`[${iso}] [ImageProcessing] Attempt ${attempt} succeeded!`);
            return { processed: true };
        },
        { connection: redisConnection }
    );

    console.log('Enqueuing test image processing job (attempts: 3, fixed 10000ms)...');
    const job = await addImageJob({
        imageUrl: 'https://example.com/test-image.png',
        dimensions: { width: 800, height: 600 },
        format: 'png',
    });

    await job.waitUntilFinished(queueEvents);

    console.log('\n##Image Processing Backoff Summary##');
    delays.forEach((delay, idx) => {
        console.log(`Retry #${idx + 1}: ${delay}ms`);
    });


    const ok = verifyWindows(delays, { base: 10000, jitter: 0.3, retries: 2, type: 'fixed', buffer: 300 });
    console.log(`Result: Delays within expected windows: ${ok ? 'PASSED' : 'FAILED'}\n`);
    process.exitCode = ok ? 0 : 1;

    await testWorker.close();
}

/**
 * Main test orchestrator
 */
async function runAllRetriesVerification() {
    console.log('### Starting Comprehensive Retry & Backoff Verification Suite ###');

    const queueEvents = new QueueEvents('my-queue', { connection: redisConnection });

    try {
        await verifyNotificationRetries(queueEvents);
        await verifyEmailRetries(queueEvents);
        await verifyImageProcessingRetries(queueEvents);
    } finally {
        console.log('\nCleaning up verification resources...');
        await queueEvents.close();
        await closeProducer();
        await redisConnection.quit();
        console.log('All verification tests completed.');
        process.exit(0);
    }
}

runAllRetriesVerification().catch(console.error);
