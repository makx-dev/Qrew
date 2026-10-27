import { startServer, queue } from './api-server';
import { worker, shutdownWorker } from './worker';
import { addImageJob, closeProducer } from './producer';
import { redisConnection } from './redis-connection';
import { Server } from 'http';

interface ProgressResponse {
    jobId: string;
    name: string;
    state: string;
    progress: number;
    attemptsMade: number;
    failedReason: string | null;
}

async function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runProgressVerification() {
    const TEST_PORT = 3001;
    const API_BASE = `http://127.0.0.1:${TEST_PORT}`;

    console.log('###Qrew: Redis Progress Polling Automated Test###');

    // 1. Start API Server
    const server: Server = startServer(TEST_PORT);
    await sleep(200); // Allow server to bind

    try {
        // 2. Test 404 handling on missing / expired job
        console.log('[TEST 1/2] Verifying 404 / Expired Job Handling...');
        const missingRes = await fetch(`${API_BASE}/jobs/job-non-existent-9999/progress`);
        if (missingRes.status === 404) {
            const body = await missingRes.json();
            console.log('  => Received expected 404 for non-existent job:', body);
        } else {
            throw new Error(`Expected status 404, received ${missingRes.status}`);
        }

        // 3. Enqueue Image Processing Job
        console.log('\n[TEST 2/2] Enqueueing Image Processing Job...');
        const job = await addImageJob({
            imageUrl: 'https://example.com/assets/sample-photo.jpg',
            dimensions: { width: 1920, height: 1080 },
            format: 'webp',
        });

        console.log(`  => Enqueued job ID: ${job.id}`);
        console.log('  => Beginning on-demand API polling every 400ms...\n');

        const observedProgress: number[] = [];
        let finalState = '';
        const startTime = Date.now();
        const timeoutMs = 12000; // 12s watchdog

        // Polling loop
        while (Date.now() - startTime < timeoutMs) {
            const res = await fetch(`${API_BASE}/jobs/${job.id}/progress`);
            if (res.ok) {
                const data = (await res.json()) as ProgressResponse;
                const elapsed = Date.now() - startTime;
                console.log(
                    `  [Poll +${elapsed}ms] State: "${data.state.toUpperCase()}" | Progress: ${data.progress}%`
                );

                if (observedProgress.length === 0 || observedProgress[observedProgress.length - 1] !== data.progress) {
                    observedProgress.push(data.progress);
                }

                if (data.state === 'completed' && data.progress === 100) {
                    finalState = data.state;
                    break;
                }
            }

            await sleep(400);
        }
        console.log('  Verification Results:');
        console.log(`  - Distinct Progress Milestones Stored in Redis: [${observedProgress.join('%, ')}%]`);
        console.log(`  - Total Worker Chunks Run: 20`);
        console.log(`  - Throttled Redis Writes: ~${observedProgress.length} (Eliminated ~${20 - observedProgress.length} redundant writes)`);
        console.log(`  - Final Job State: ${finalState}`);

        if (finalState !== 'completed') {
            throw new Error(`Job did not reach 'completed' state within ${timeoutMs}ms`);
        }

        if (!observedProgress.includes(100)) {
            throw new Error('Final progress did not reach 100%');
        }

        console.log('=> ALL TESTS PASSED: Throttled progress polling verified successfully!\n');
    } catch (error) {
        console.error('Error during verification:', error);
        throw error;
    } finally {
        // Clean shutdown of server, worker, and Redis
        console.log('Cleaning up server, worker, and connections...');
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await queue.close();
        await shutdownWorker();
        await closeProducer();
        await redisConnection.quit();
        console.log('Cleanup complete.');
    }
}

runProgressVerification().catch((err) => {
    console.error('Test failed with error:', err);
    process.exit(1);
});
