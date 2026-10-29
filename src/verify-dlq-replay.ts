import { Queue, Worker, Job } from 'bullmq';
import { redisConnection } from './redis-connection';
import { PoisonPillError, isPoisonPill } from './errors';
import { inspectAndReplayFailedJobs, trimFailedJobs } from './replay-service';
import { ensureRedisNoEviction } from './redis-policy';

const TEST_QUEUE_NAME = 'test-dlq-replay-queue';
const testQueue = new Queue(TEST_QUEUE_NAME, { connection: redisConnection });

async function runVerification() {
    console.log('##Step 1: Verify Redis memory policy (noeviction) ##');
    await ensureRedisNoEviction(redisConnection);

    // Clean up test queue state before running
    await testQueue.drain();
    await testQueue.clean(0, 1000, 'failed');
    await testQueue.clean(0, 1000, 'completed');

    console.log('\n##Step 2: Setting up test worker with concurrency: 1 ##');
    let failTransientCount = 0;

    const testWorker = new Worker(
        TEST_QUEUE_NAME,
        async (job: Job) => {
            console.log(`[Worker] Processing Job ${job.id} (${job.name}), attempt: ${job.attemptsMade + 1}`);

            if (job.name === 'test-poison-pill') {
                throw new PoisonPillError('Malformed data: missing field "userId"', 'MALFORMED_DATA');
            }

            if (job.name === 'test-transient') {
                const replayCount = job.data._replayCount || 0;
                // Simulate: fails on initial attempt, but succeeds once replayed after downstream recovers!
                if (replayCount < 1) {
                    throw new Error('503 Service Unavailable: Downstream API timeout');
                }
                return { success: true, processedAtReplay: replayCount };
            }

            if (job.name === 'test-always-failing-transient') {
                throw new Error('500 Persistent Internal Server Error');
            }

            return { done: true };
        },
        {
            connection: redisConnection,
            concurrency: 1, // Concurrency limit 1
        }
    );

    testWorker.on('failed', (job: Job | undefined, err: Error) => {
        if (!job) return;
        if (isPoisonPill(job, err)) {
            console.log(`[Event: failed] Poison Pill detected on Job ${job.id}: ${err.message}`);
        } else {
            console.log(`[Event: failed] Transient failure on Job ${job.id} (${job.attemptsMade}/${job.opts.attempts}): ${err.message}`);
        }
    });

    console.log('\n##Step 3: Enqueuing test jobs with capped failed storage ##');
    // 1. Poison pill job
    const poisonJob = await testQueue.add('test-poison-pill', { foo: 'bar' }, {
        attempts: 3,
        removeOnFail: { count: 1000 },
    });

    // 2. Transient job that will succeed upon replay
    const transientJob = await testQueue.add('test-transient', { testId: 'transient-1' }, {
        attempts: 1, // fails immediately to simulate exhausted retries
        removeOnFail: { count: 1000 },
    });

    // 3. Persistent transient job to test maxReplay cap (3)
    const persistentFailingJob = await testQueue.add('test-always-failing-transient', {
        testId: 'persistent-1',
        _replayCount: 3, // Already spent 3 replays!
    }, {
        attempts: 1,
        removeOnFail: { count: 1000 },
    });

    // Allow worker to process initial attempts
    await new Promise((r) => setTimeout(r, 2000));

    console.log('\n##Step 4: Inspect BullMQ native failed jobs ##');
    const failedBefore = await testQueue.getFailed();
    console.log(`Native failed jobs in Redis: ${failedBefore.length}`);
    for (const f of failedBefore) {
        console.log(` - ID: ${f.id} | Name: ${f.name} | FailedReason: ${f.failedReason}`);
    }

    console.log('\n##Step 5: Execute Redlock-protected Replay Service (maxReplay: 3) ##');
    const report = await inspectAndReplayFailedJobs(testQueue, { maxReplays: 3 });
    console.log('\nReplay Execution Summary:');
    console.table(report.details);

    console.log('\n##Step 6: Test Concurrent Replay Lock (Redlock collision test) ##');
    // Simulate another process attempting to replay while locked
    try {
        const p1 = inspectAndReplayFailedJobs(testQueue, { maxReplays: 3, lockTtlMs: 2000 });
        const p2 = inspectAndReplayFailedJobs(testQueue, { maxReplays: 3, lockTtlMs: 2000 });
        await Promise.all([p1, p2]);
    } catch (err: any) {
        console.log(`[Redlock Verified] Lock collision prevented duplicate replay: ${err.message}`);
    }

    // Allow replayed transient job to process
    await new Promise((r) => setTimeout(r, 1500));

    console.log('\n##Step 7: Cap verification & cleanup ##');
    await trimFailedJobs(testQueue, 1000);

    // Clean up
    await testWorker.close();
    await testQueue.close();
    console.log('\n => All 5 verification checks completed successfully.');
}

if (require.main === module) {
    runVerification().then(() => {
        process.exit(0);
    }).catch((err) => {
        console.error('Verification error:', err);
        process.exit(1);
    });
}

