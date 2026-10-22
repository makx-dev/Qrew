import { Queue, Worker, Job } from 'bullmq';
import { redisConnection } from './redis-connection';
import { JobPriority, ImageProcessingPayload, EmailPayload, NotificationPayload } from './JobTypes';

interface TrackedRecord {
    id: string,
    name: string,
    priority: number,
    seq: number,
    processedAt: number | null,
}

async function runPriorityLoadTest() {
    console.log('##Starting Priority Load Test (500+ Jobs)##\n');
    const queueName = 'my-queue';
    const queue = new Queue(queueName, { connection: redisConnection });

    // Step 1: Clean & Pause Queue so jobs accumulate in Redis without premature consumption
    await queue.drain();
    await queue.pause();
    console.log('Queue drained and paused to allow batch accumulation.');
    const TOTAL_MEDIUM = 250;
    const TOTAL_HIGH = 200;
    const TOTAL_CRITICAL = 100;
    const TOTAL_JOBS = TOTAL_MEDIUM + TOTAL_HIGH + TOTAL_CRITICAL; // 550 jobs
    console.log(`\nEnqueuing ${TOTAL_JOBS} jobs in REVERSE priority order:`);
    console.log(`- ${TOTAL_MEDIUM} Medium (Priority 3)`);
    console.log(`- ${TOTAL_HIGH} High (Priority 2)`);
    console.log(`- ${TOTAL_CRITICAL} Critical (Priority 1)`);

    // Prepare bulk payloads
    const bulkJobs: Array<{
        name: string;
        data: any;
        opts: { priority: number; removeOnComplete: boolean; removeOnFail: boolean };
    }> = [];
    let seqCounter = 0;
    // 1. Medium Priority (P3) pushed FIRST
    for (let i = 1; i <= TOTAL_MEDIUM; i++) {
        bulkJobs.push({
            name: 'image-processing',
            data: { seq: ++seqCounter, imageUrl: `img-${i}.png`, dimensions: { width: 100, height: 100 }, format: 'png' },
            opts: { priority: JobPriority.MEDIUM, removeOnComplete: true, removeOnFail: true },
        });
    }
    // 2. High Priority (P2) pushed SECOND
    for (let i = 1; i <= TOTAL_HIGH; i++) {
        bulkJobs.push({
            name: 'email',
            data: { seq: ++seqCounter, to: `user-${i}@example.com`, subject: 'Invoice', body: '...' },
            opts: { priority: JobPriority.HIGH, removeOnComplete: true, removeOnFail: true },
        });
    }
    // 3. Critical Priority (P1) pushed LAST
    for (let i = 1; i <= TOTAL_CRITICAL; i++) {
        bulkJobs.push({
            name: 'notification',
            data: { seq: ++seqCounter, userId: `u-${i}`, message: 'OTP 1234', channel: 'sms' },
            opts: { priority: JobPriority.CRITICAL, removeOnComplete: true, removeOnFail: true },
        });
    }

    // Bulk enqueue in chunks of 100 to avoid packet size bottleneck
    const chunkSize = 100;
    for (let i = 0; i < bulkJobs.length; i += chunkSize) {
        await queue.addBulk(bulkJobs.slice(i, i + chunkSize));
        process.stdout.write(`\rEnqueued: ${Math.min(i + chunkSize, bulkJobs.length)}/${TOTAL_JOBS}`);
    }
    console.log('\nAll jobs enqueued into Redis sorted set.');

    //Initialize Worker with concurrency: 1 and ultra-fast simulation
    const processedOrder: TrackedRecord[] = [];
    const testWorker = new Worker(
        queueName,
        async (job: Job) => {
            // Simulated micro-delay for rapid execution across 550 jobs
            await new Promise((r) => setImmediate(r));
            processedOrder.push({
                id: String(job.id),
                name: job.name,
                priority: Number(job.opts.priority),
                seq: job.data.seq,
                processedAt: Date.now(),
            });
        },
        { connection: redisConnection, concurrency: 1 }
    );

    // Resume Queue & Start Processing
    console.log('\nResuming queue and processing...');
    const startTime = Date.now();
    await queue.resume();
    // Wait until all jobs are processed
    await new Promise<void>((resolve) => {
        testWorker.on('completed', () => {
            if (processedOrder.length % 50 === 0 || processedOrder.length === TOTAL_JOBS) {
                process.stdout.write(`\rProcessed: ${processedOrder.length}/${TOTAL_JOBS}`);
            }
            if (processedOrder.length === TOTAL_JOBS) {
                resolve();
            }
        });
    });
    const elapsedMs = Date.now() - startTime;
    console.log(`\n\nAll ${TOTAL_JOBS} jobs completed in ${elapsedMs}ms (${(TOTAL_JOBS / (elapsedMs / 1000)).toFixed(1)} jobs/s).\n`);

    //Verification & Reasoning Report
    verifyOrder(processedOrder, TOTAL_CRITICAL, TOTAL_HIGH, TOTAL_MEDIUM);
    // Teardown
    await testWorker.close();
    await queue.close();
    await redisConnection.quit();
    process.exit(0);
}
function verifyOrder(
    records: TrackedRecord[],
    expectedCritical: number,
    expectedHigh: number,
    expectedMedium: number
) {
    console.log('##VERIFICATION & ORDER ANALYSIS##');


    const criticalSlice = records.slice(0, expectedCritical);
    const highSlice = records.slice(expectedCritical, expectedCritical + expectedHigh);
    const mediumSlice = records.slice(expectedCritical + expectedHigh);
    let priorityOrderValid = true;
    let fifoValid = true;
    // Check Priority Invariant: strictly 1 -> 2 -> 3
    criticalSlice.forEach((r, idx) => {
        if (r.priority !== JobPriority.CRITICAL) {
            console.error(`[FAIL] Expected Priority 1 at index ${idx}, got ${r.priority} (Job ID ${r.id})`);
            priorityOrderValid = false;
        }
    });
    highSlice.forEach((r, idx) => {
        if (r.priority !== JobPriority.HIGH) {
            console.error(`[FAIL] Expected Priority 2 at index ${expectedCritical + idx}, got ${r.priority} (Job ID ${r.id})`);
            priorityOrderValid = false;
        }
    });
    mediumSlice.forEach((r, idx) => {
        if (r.priority !== JobPriority.MEDIUM) {
            console.error(`[FAIL] Expected Priority 3 at index ${expectedCritical + expectedHigh + idx}, got ${r.priority} (Job ID ${r.id})`);
            priorityOrderValid = false;
        }
    });
    // Check Intra-priority FIFO Invariant
    const checkFifo = (slice: TrackedRecord[], label: string) => {
        for (let i = 1; i < slice.length; i++) {
            if (slice[i].seq < slice[i - 1].seq) {
                console.error(`[FAIL] FIFO Violation in ${label}: job seq ${slice[i].seq} ran after ${slice[i - 1].seq}`);
                fifoValid = false;
                return false;
            }
        }
        return true;
    };
    const criticalFifo = checkFifo(criticalSlice, 'CRITICAL (P1)');
    const highFifo = checkFifo(highSlice, 'HIGH (P2)');
    const mediumFifo = checkFifo(mediumSlice, 'MEDIUM (P3)');
    console.log(`\nResults:`);
    console.log(`1. Global Priority Order (1 -> 2 -> 3): ${priorityOrderValid ? 'PASSED' : 'FAILED'}`);
    console.log(`2. Critical Jobs FIFO (1..${expectedCritical}): ${criticalFifo ? 'PASSED' : 'FAILED'}`);
    console.log(`3. High Jobs FIFO (1..${expectedHigh}): ${highFifo ? 'PASSED' : 'FAILED'}`);
    console.log(`4. Medium Jobs FIFO (1..${expectedMedium}): ${mediumFifo ? 'PASSED' : 'FAILED'}`);
    if (priorityOrderValid && fifoValid) {
        console.log('\nVERIFICATION SUCCESSFUL: BullMQ strictly respected priority ranking despite reverse enqueue order.');
    } else {
        console.error('\nVERIFICATION FAILED: Order voilation detected.');
        process.exitCode = 1;
    }
}
runPriorityLoadTest().catch((err) => {
    console.error('Fatal test error:', err);
    process.exit(1);
});

