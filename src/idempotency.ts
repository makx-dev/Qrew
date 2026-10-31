import crypto from 'crypto';
import IORedis from 'ioredis';

export interface CachedResponse {
    statusCode: number;
    body: any;
}

export interface IdempotencyRecord {
    status: 'PROCESSING' | 'COMPLETED';
    hash: string;
    response?: CachedResponse;
}

export class IdempotencyManager {
    constructor(
        private redis: IORedis,
        private prefix: string = 'idempotency:',
        private lockTtlSeconds: number = 60,
        private completedTtlSeconds: number = 86400 // 24 hours
    ) {}

    /**
     * Computes a deterministic SHA-256 fingerprint for the request payload.
     */
    public computeHash(body: any): string {
        const canonical = JSON.stringify(body ?? {}, Object.keys(body ?? {}).sort());
        return crypto.createHash('sha256').update(canonical).digest('hex');
    }

    /**
     * Attempts to acquire an idempotency lock or returns a previously cached response.
     */
    public async checkOrLock(key: string, bodyHash: string): Promise<
        | { state: 'ACQUIRED' }
        | { state: 'IN_FLIGHT' }
        | { state: 'PAYLOAD_MISMATCH' }
        | { state: 'CACHED'; response: CachedResponse }
    > {
        const redisKey = `${this.prefix}${key}`;
        const raw = await this.redis.get(redisKey);

        if (raw) {
            try {
                const record: IdempotencyRecord = JSON.parse(raw);
                if (record.hash !== bodyHash) {
                    return { state: 'PAYLOAD_MISMATCH' };
                }
                if (record.status === 'PROCESSING') {
                    return { state: 'IN_FLIGHT' };
                }
                if (record.status === 'COMPLETED' && record.response) {
                    return { state: 'CACHED', response: record.response };
                }
            } catch {
                // If malformed, treat as clean
            }
        }

        const initialRecord: IdempotencyRecord = {
            status: 'PROCESSING',
            hash: bodyHash,
        };

        const acquired = await this.redis.set(
            redisKey,
            JSON.stringify(initialRecord),
            'EX',
            this.lockTtlSeconds,
            'NX'
        );

        if (!acquired) {
            return { state: 'IN_FLIGHT' };
        }

        return { state: 'ACQUIRED' };
    }

    /**
     * Stores the final successful response for the idempotency key.
     */
    public async saveResponse(key: string, bodyHash: string, response: CachedResponse): Promise<void> {
        const redisKey = `${this.prefix}${key}`;
        const record: IdempotencyRecord = {
            status: 'COMPLETED',
            hash: bodyHash,
            response,
        };
        await this.redis.set(redisKey, JSON.stringify(record), 'EX', this.completedTtlSeconds);
    }

    /**
     * Releases the lock on error so the client can retry immediately.
     */
    public async release(key: string): Promise<void> {
        const redisKey = `${this.prefix}${key}`;
        await this.redis.del(redisKey);
    }
}

