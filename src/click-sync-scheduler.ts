import { Queue } from 'bullmq';
import { JobPriority, ClickSyncPayload } from './JobTypes';

export const CLICK_SYNC_SCHEDULER_ID = 'cachecut-click-sync-recurring';

/**
 * Registers a recurring click-sync job in BullMQ to flush CacheCut's
 * accumulated Redis click counts into PostgreSQL every 5 seconds.
 * 
 * Auto-cleans completed logs (removeOnComplete: 100) and failed logs (removeOnFail: 500)
 * to keep Redis memory bounded.
 */
export async function setupClickSyncScheduler(queue: Queue, intervalMs: number = 5000) {
    const jobPayload: ClickSyncPayload = { batchSize: 500 };

    const scheduler = await queue.upsertJobScheduler(
        CLICK_SYNC_SCHEDULER_ID,
        { every: intervalMs },
        {
            name: 'click-sync',
            data: jobPayload,
            opts: {
                priority: JobPriority.HIGH,
                removeOnComplete: 100, // Caps successful job records
                removeOnFail: 500,     // Retains failed jobs for DLQ inspection/replay
            },
        }
    );

    console.log(`[SCHEDULER] Click-sync repeatable job registered (every ${intervalMs}ms, removeOnComplete: 100, removeOnFail: 500)`);
    return scheduler;
}

export async function getClickSyncScheduler(queue: Queue) {
    const schedulers = await queue.getJobSchedulers();
    return schedulers.find(s => (s as any).key === CLICK_SYNC_SCHEDULER_ID || (s as any).id === CLICK_SYNC_SCHEDULER_ID);
}

export async function removeClickSyncScheduler(queue: Queue) {
    return await queue.removeJobScheduler(CLICK_SYNC_SCHEDULER_ID);
}
