// src/server.ts
import { startServer, queue, queueEvents } from './api-server';
import { worker, shutdownWorker } from './worker';
import { setupClickSyncScheduler } from './click-sync-scheduler';
import { redisConnection } from './redis-connection';

const PORT = Number(process.env.PORT || 3000);

// 1. Start REST API & Bull Board
const server = startServer(PORT);
console.log(`[Qrew] Server & Monitoring running on port ${PORT}`);

// 2. Start Background Worker in the same event loop
console.log('[Qrew] Background Worker initialized and listening for jobs');

// 3. Register repeatable scheduler
setupClickSyncScheduler(queue, 10000).catch((err) => {
    console.warn('[Qrew] Click-sync scheduler notice:', err.message);
});

// 4. Ensure queue is active on boot (resets any residual pause state in Redis)
queue.resume().catch((err) => {
    console.warn('[Qrew] Startup resume notice:', err.message);
});

// 5. Watchdog: Auto-resume queue every 60s if a demo visitor paused it
const watchdog = setInterval(async () => {
    try {
        if (await queue.isPaused()) {
            console.log('[WATCHDOG] Queue was paused. Auto-resuming for public demo availability...');
            await queue.resume();
        }
    } catch (err: any) {
        console.warn('[WATCHDOG] Check failed:', err.message);
    }
}, 60000);
watchdog.unref();

// Graceful shutdown handling
const handleShutdown = async () => {
    console.log('[Qrew] Graceful shutdown initiated...');
    clearInterval(watchdog);
    server.close();
    await shutdownWorker();
    await queueEvents.close();
    await queue.close();
    await redisConnection.quit();
    process.exit(0);
};

process.on('SIGTERM', handleShutdown);
process.on('SIGINT', handleShutdown);
