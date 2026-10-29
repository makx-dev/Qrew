import IORedis from 'ioredis';

/**
 * Ensures Redis is configured with maxmemory-policy 'noeviction'.
 * With 'noeviction', when Redis hits memory limits, write commands return an error
 * instead of silently deleting/evicting existing queue or job data.
 */
export async function ensureRedisNoEviction(redis: IORedis): Promise<void> {
    try {
        const policyConfig = (await redis.config('GET', 'maxmemory-policy')) as [string, string];
        const currentPolicy = policyConfig && policyConfig[1] ? policyConfig[1] : 'unknown';

        if (currentPolicy !== 'noeviction') {
            console.log(`[Redis Policy] Current maxmemory-policy is "${currentPolicy}". Updating to "noeviction"...`);
            await redis.config('SET', 'maxmemory-policy', 'noeviction');
            console.log('[Redis Policy] Successfully set maxmemory-policy to "noeviction". Redis will return errors on OOM instead of auto-deleting jobs.');
        } else {
            console.log('[Redis Policy] Verified: maxmemory-policy is set to "noeviction".');
        }
    } catch (err: any) {
        // Some managed Redis providers (like AWS ElastiCache / Redis Cloud) disallow CONFIG commands
        console.warn(`[Redis Policy] Note: Unable to check/set Redis maxmemory-policy via CONFIG command: ${err.message}. Ensure "maxmemory-policy noeviction" is set in your redis.conf / provider dashboard.`);
    }
}

