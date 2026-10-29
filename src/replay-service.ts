import { Queue, Job } from 'bullmq';
import { redisConnection } from './redis-connection';
import { DistributedLock } from './redlock';
import { isPoisonPill, PoisonPillError } from './errors';

export interface ReplayOptions {
    maxReplays?: number;       // Max allowable replays for transient failures (default: 3)
    batchSize?: number;        // Number of failed jobs to inspect per run
    lockTtlMs?: number;        // Distributed lock TTL in milliseconds
}

export interface ReplayReport {
    inspected: number;
    replayed: number;
    poisonPillSkipped: number;
    maxReplayExceeded: number;
    details: Array<{
        jobId: string;
        name: string;
        action: 'REPLAYED' | 'SKIPPED_POISON_PILL' | 'ESCALATED_MAX_REPLAY_EXCEEDED';
        replayCount: number;
        reason?: string;
    }>;
}

const lock = new DistributedLock(redisConnection);

/**
 * Replays transiently failed jobs directly from BullMQ's native failed state via job.retry().
 * Protected by Redlock to prevent concurrent execution races.
 */
export async function inspectAndReplayFailedJobs(
    queue: Queue,
    options: ReplayOptions = {}
): Promise<ReplayReport> {
    const { maxReplays = 3, batchSize = 100, lockTtlMs = 30000 } = options;
    const resourceName = `replay:${queue.name}`;

    return await lock.withLock(resourceName, lockTtlMs, async () => {
        console.log(`[ReplayService] Acquired lock for "${queue.name}". Inspecting native failed jobs...`);

        // 1. Fetch failed jobs from BullMQ native failed state
        const failedJobs: Job[] = await queue.getFailed(0, batchSize - 1);

        const report: ReplayReport = {
            inspected: failedJobs.length,
            replayed: 0,
            poisonPillSkipped: 0,
            maxReplayExceeded: 0,
            details: [],
        };

        for (const job of failedJobs) {
            const currentData = (job.data || {}) as Record<string, any>;
            const currentReplayCount = currentData._replayCount ?? 0;

            // Check if job is a known poison pill (fatal validation or logic error)
            if (isPoisonPill(job)) {
                report.poisonPillSkipped++;
                report.details.push({
                    jobId: job.id ?? 'unknown',
                    name: job.name,
                    action: 'SKIPPED_POISON_PILL',
                    replayCount: currentReplayCount,
                    reason: job.failedReason ?? 'Identified as unrecoverable / poison pill',
                });
                console.warn(`[ReplayService] Quarantining Job ${job.id} (${job.name}): Poison pill detected. Reason: ${job.failedReason}`);
                continue;
            }

            // Case 4: Cap replay count at maxReplays (e.g. 3) to prevent infinite replay loops
            if (currentReplayCount >= maxReplays) {
                report.maxReplayExceeded++;
                // Escalate to permanent poison pill so future runs ignore it
                await job.updateData({
                    ...currentData,
                    _isPoisonPill: true,
                    _poisonPillReason: `Exceeded max replay threshold of ${maxReplays} cycles`,
                });

                report.details.push({
                    jobId: job.id ?? 'unknown',
                    name: job.name,
                    action: 'ESCALATED_MAX_REPLAY_EXCEEDED',
                    replayCount: currentReplayCount,
                    reason: `Exceeded max replays (${maxReplays}). Escalated to poison pill.`,
                });
                console.error(`[ReplayService] Escalating Job ${job.id} (${job.name}) to POISON_PILL: Failed after ${currentReplayCount} replay cycles.`);
                continue;
            }

            // Transient failure: Safe to retry!
            const newReplayCount = currentReplayCount + 1;
            await job.updateData({
                ...currentData,
                _replayCount: newReplayCount,
                _lastReplayedAt: new Date().toISOString(),
            });

            // BullMQ native retry: moves job back into the waiting queue atomically via Lua script
            await job.retry();
            report.replayed++;
            report.details.push({
                jobId: job.id ?? 'unknown',
                name: job.name,
                action: 'REPLAYED',
                replayCount: newReplayCount,
            });
            console.log(`[ReplayService] Replaying Job ${job.id} (${job.name}) - Replay attempt ${newReplayCount}/${maxReplays}`);
        }

        console.log(`[ReplayService] Finished. Inspected: ${report.inspected}, Replayed: ${report.replayed}, Poison Pills Skipped: ${report.poisonPillSkipped}, Max Replays Escalated: ${report.maxReplayExceeded}`);
        return report;
    });
}

/**
 * Trims failed jobs beyond maxAllowed count to avoid unbounded Redis memory growth.
 */
export async function trimFailedJobs(queue: Queue, maxAllowed: number = 1000): Promise<number> {
    const failedCount = await queue.getFailedCount();
    if (failedCount > maxAllowed) {
        const excess = failedCount - maxAllowed;
        console.log(`[ReplayService] Trimming ${excess} excess failed jobs from "${queue.name}" to prevent unbounded Redis memory growth...`);
        // queue.clean(gracePeriodMs, limit, type)
        const cleaned = await queue.clean(0, excess, 'failed');
        console.log(`[ReplayService] Cleaned ${cleaned.length} old failed jobs.`);
        return cleaned.length;
    }
    return 0;
}

