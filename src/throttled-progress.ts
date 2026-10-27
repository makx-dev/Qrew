import { Job } from 'bullmq';

export interface ThrottleOptions {
    /** Minimum time in ms between Redis writes (default: 1000ms) */
    minIntervalMs?: number;
    /** Minimum percentage change required before triggering a write (default: 5%) */
    minDelta?: number;
}

export class ThrottledProgress {
    private lastReportedTime = 0;
    private lastReportedValue = -1;
    private readonly minIntervalMs: number;
    private readonly minDelta: number;

    constructor(
        private readonly job: Job,
        options: ThrottleOptions = {}
    ) {
        this.minIntervalMs = options.minIntervalMs ?? 1000;
        this.minDelta = options.minDelta ?? 5;
    }

    /**
     * Reports progress subject to rate-limiting and threshold checks.
     * Skips Redis roundtrip if called too frequently or change is too small.
     */
    async update(progress: number): Promise<void> {
        const now = Date.now();
        const timeElapsed = now - this.lastReportedTime;
        const delta = Math.abs(progress - this.lastReportedValue);

        // Allow update only if both time gate and delta gate are satisfied
        if (timeElapsed >= this.minIntervalMs && delta >= this.minDelta) {
            await this.forceWrite(progress, now);
        }
    }

    /**
     * Bypasses all throttling logic.
     * MUST be called at the end of the job (or on completion/error) to guarantee 100%.
     */
    async flush(progress: number): Promise<void> {
        await this.forceWrite(progress, Date.now());
    }

    private async forceWrite(progress: number, timestamp: number): Promise<void> {
        this.lastReportedTime = timestamp;
        this.lastReportedValue = progress;
        // Native BullMQ call: writes to Redis job hash and emits progress event
        await this.job.updateProgress(progress);
    }
}


