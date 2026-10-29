import IORedis from 'ioredis';
import { randomUUID } from 'crypto';

/**
 * Distributed Lock (Redlock pattern for Redis)
 * Uses atomic SET resource token NX PX and Lua script release.
 */
export class DistributedLock {
    private client: IORedis;

    // Lua script to atomically release lock only if the token matches the owner
    private static UNLOCK_SCRIPT = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
    `;

    constructor(client: IORedis) {
        this.client = client;
    }

    /**
     * Attempts to acquire a distributed lock.
     * @param resource The resource name to lock
     * @param ttlMs Time to live for the lock in milliseconds
     * @returns lockToken if acquired successfully, or null if already locked
     */
    async acquire(resource: string, ttlMs: number = 30000): Promise<string | null> {
        const lockKey = `lock:${resource}`;
        const lockToken = randomUUID();

        // SET key token NX PX ttlMs
        const result = await this.client.set(lockKey, lockToken, 'PX', ttlMs, 'NX');
        if (result === 'OK') {
            return lockToken;
        }

        return null;
    }

    /**
     * Safely releases the lock using Lua script to prevent releasing another worker's lock
     */
    async release(resource: string, lockToken: string): Promise<boolean> {
        const lockKey = `lock:${resource}`;
        try {
            const result = await this.client.eval(
                DistributedLock.UNLOCK_SCRIPT,
                1,
                lockKey,
                lockToken
            );
            return result === 1;
        } catch (error) {
            console.error(`[Redlock] Failed to release lock on ${resource}:`, error);
            return false;
        }
    }

    /**
     * Executes an async operation protected by a distributed lock.
     */
    async withLock<T>(
        resource: string,
        ttlMs: number,
        operation: (token: string) => Promise<T>
    ): Promise<T> {
        const token = await this.acquire(resource, ttlMs);
        if (!token) {
            throw new Error(`[Redlock] Could not acquire lock for resource "${resource}". Replay already in progress.`);
        }

        try {
            return await operation(token);
        } finally {
            await this.release(resource, token);
        }
    }
}

