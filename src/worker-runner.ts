import { Queue } from 'bullmq';
import { worker, shutdownWorker } from './worker';
import { redisConnection } from './redis-connection';
import { setupClickSyncScheduler } from './click-sync-scheduler';
import readline from 'readline';

console.log("Worker process is started waiting for jobs");

const queue = new Queue('my-queue', { connection: redisConnection });

// Register the repeatable click-sync consumer scheduler
setupClickSyncScheduler(queue, 5000).catch((err) => {
    console.error("Failed to register click-sync scheduler:", err);
});

let isShuttingDown = false;

const onSignal = async () => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log("Received shutdown signal");

    const forceExitTimer = setTimeout(() => {
        console.log("Force exiting after 10 seconds");
        process.exit(1);
    }, 10000);
    forceExitTimer.unref();

    try {
        await shutdownWorker();
        await queue.close();
        await redisConnection.quit(); // Close the Redis connection
        clearTimeout(forceExitTimer);
        console.log("Worker shut down successfully");
        process.exit(0);
    } catch (error) {
        console.log("Error shutting down worker", error);
        clearTimeout(forceExitTimer);
        process.exit(1);
    }
};

// Enable Ctrl+C (SIGINT) capture on Windows
if (process.platform === 'win32') {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });
    rl.on('SIGINT', () => {
        process.emit('SIGINT');
    });
}

// OS process signal hooks
process.on('SIGINT', () => onSignal()); // Ctrl+C in terminal
process.on('SIGTERM', () => onSignal()); // Docker / K8s stop signal
