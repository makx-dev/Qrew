import { UnrecoverableError, Job } from 'bullmq';

/**
 * Custom error indicating that a job payload or state is permanently invalid
 * and must not be retried by the queue.
 */
export class PoisonPillError extends UnrecoverableError {
    public readonly reason: string;

    constructor(message: string, reason: string = 'FATAL_VALIDATION_ERROR') {
        super(message);
        this.name = 'PoisonPillError';
        this.reason = reason;
    }
}

/**
 * Inspects a job and its error to determine if it is a poison pill (unrecoverable)
 * or a transient error.
 */
export function isPoisonPill(job: Job, err?: Error | null): boolean {
    // 1. Explicitly flagged in job data (e.g. after exceeding max replays)
    if (job.data?._isPoisonPill === true) {
        return true;
    }

    // 2. Error object check
    if (err) {
        if (err instanceof PoisonPillError || err.name === 'PoisonPillError' || err.name === 'UnrecoverableError') {
            return true;
        }
    }

    // 3. Stored failedReason check in BullMQ job state
    if (job.failedReason) {
        const lowerReason = job.failedReason.toLowerCase();
        if (
            lowerReason.includes('unrecoverable') ||
            lowerReason.includes('poisonpill') ||
            lowerReason.includes('invalid') ||
            lowerReason.includes('fatal')
        ) {
            return true;
        }
    }

    return false;
}

