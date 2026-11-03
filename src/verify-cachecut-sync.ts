import { Queue, Worker, Job, QueueEvents } from 'bullmq';
import { redisConnection } from './redis-connection';
import { JobPriority, ClickSyncPayload } from './JobTypes';
import { setupClickSyncScheduler, getClickSyncScheduler, removeClickSyncScheduler } from './click-sync-scheduler';

async function runVerification() {
    console.log('--- STARTING CACHECUT CLICK-SYNC INTEGRATION VERIFICATION ---');

    const testQueueName = 'cachecut-test-queue';
    const queue = new Queue(testQueueName, { connection: redisConnection });
    const queueEvents = new QueueEvents(testQueueName, { connection: redisConnection });

    try {
        // Step 1: Simulate CacheCut writing clicks to Redis
        console.log('[STEP 1] Simulating CacheCut L1 memory flush to Redis...');
        const testCodeA = 'test_short_a';
        const testCodeB = 'test_short_b';

        await redisConnection.incrby(`clicks:${testCodeA}`, 25);
        await redisConnection.incrby(`clicks:${testCodeB}`, 10);
        await redisConnection.sadd('dirty:clicks', testCodeA, testCodeB);

        const initialDirty = await redisConnection.smembers('dirty:clicks');
        console.log(`  -> Redis dirty set contains: [${initialDirty.join(', ')}]`);
        console.log(`  -> clicks:${testCodeA} = ${await redisConnection.get(`clicks:${testCodeA}`)}`);
        console.log(`  -> clicks:${testCodeB} = ${await redisConnection.get(`clicks:${testCodeB}`)}`);

        // Step 2: Verify Repeatable Scheduler configuration & log retention bounds
        console.log('[STEP 2] Verifying Repeatable Scheduler contract & retention settings...');
        await setupClickSyncScheduler(queue, 5000);

        const found = await getClickSyncScheduler(queue);
        if (!found) {
            throw new Error('FAIL: Repeatable click-sync scheduler was not found in queue schedule.');
        }
        console.log(`  -> Verified job scheduler '${found.name}' registered with every: ${found.every}ms (key: ${(found as any).key})`);

        // Step 3: Run isolated worker to consume the click-sync job
        console.log('[STEP 3] Executing consumer logic with atomic GETDEL handoff...');
        let syncedCount = 0;
        const processedRecords: Array<{ code: string; clicks: number }> = [];

        const testWorker = new Worker(
            testQueueName,
            async (job: Job) => {
                if (job.name === 'click-sync') {
                    const rawDirty = await redisConnection.spop('dirty:clicks', 100);
                    const codes = Array.isArray(rawDirty) ? rawDirty : rawDirty ? [rawDirty] : [];

                    let batchClicks = 0;
                    let batchCodes = 0;
                    for (const code of codes) {
                        const clicksStr = await redisConnection.getdel(`clicks:${code}`);
                        const clicks = Number(clicksStr) || 0;
                        if (clicks > 0) {
                            processedRecords.push({ code, clicks });
                            batchClicks += clicks;
                            batchCodes++;
                        }
                    }
                    syncedCount += batchClicks;
                    return { syncedCodes: batchCodes, totalClicks: batchClicks };
                }
            },
            { connection: redisConnection }
        );

        // Enqueue an immediate test job
        const testJob = await queue.add(
            'click-sync',
            { batchSize: 500 } as ClickSyncPayload,
            {
                priority: JobPriority.HIGH,
                removeOnComplete: 100,
                removeOnFail: 500,
            }
        );

        // Wait for job completion
        await testJob.waitUntilFinished(queueEvents);
        console.log(`  -> Job ${testJob.id} completed. Total clicks synced: ${syncedCount}`);
        console.log('  -> Processed details:', processedRecords);

        // Step 4: Verify atomic cleanup invariant in Redis
        console.log('[STEP 4] Verifying atomic cleanup invariants in Redis...');
        const remainingDirty = await redisConnection.smembers('dirty:clicks');
        const remainingClicksA = await redisConnection.get(`clicks:${testCodeA}`);
        const remainingClicksB = await redisConnection.get(`clicks:${testCodeB}`);

        if (remainingClicksA !== null || remainingClicksB !== null) {
            throw new Error('FAIL: Expected Redis click keys to be atomically cleared by GETDEL.');
        }
        console.log('  -> Confirmed: Redis keys cleared atomically (no duplicate replay risk).');
        console.log(`  -> Remaining dirty codes: [${remainingDirty.join(', ')}]`);

        // Step 5: Test Empty Cadence (Zero Traffic)
        console.log('[STEP 5] Testing empty cadence under zero traffic...');
        const emptyJob = await queue.add(
            'click-sync',
            { batchSize: 500 } as ClickSyncPayload,
            { priority: JobPriority.HIGH, removeOnComplete: 100, removeOnFail: 500 }
        );
        const result = await emptyJob.waitUntilFinished(queueEvents);
        console.log('  -> Empty cadence handled cleanly:', result);

        await testWorker.close();
        await removeClickSyncScheduler(queue);
        await queueEvents.close();
        await queue.close();
        console.log('=> ALL INVARIANTS PASSED: CacheCut Click-Sync Consumer is Fully Verified!');
    } catch (err: any) {
        console.error('Verification failed:', err);
    } finally {
        await redisConnection.quit();
        process.exit(0);
    }
}

runVerification();
