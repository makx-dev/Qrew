// src/server.ts
import { startServer } from './api-server';
import { worker, shutdownWorker } from './worker';
import { queue } from './api-server';
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

// Graceful shutdown handling
const handleShutdown = async () => {
    console.log('[Qrew] Graceful shutdown initiated...');
    server.close();
    await shutdownWorker();
    await queue.close();
    await redisConnection.quit();
    process.exit(0);
};

process.on('SIGTERM', handleShutdown);
process.on('SIGINT', handleShutdown);