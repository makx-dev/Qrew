import IORedis from 'ioredis';
import { createClient } from 'redis';
import dotenv from 'dotenv';

dotenv.config();

export const redisClient = createClient({
    url: process.env.REDIS_URL,
});
redisClient.on('error', (err) => console.log('Redis Client Error', err));
export const redisConnection = new IORedis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: Number(process.env.REDIS_PORT ?? 6379),
    password: process.env.REDIS_PASSWORD,
    maxRetriesPerRequest: null,
    tls: process.env.REDIS_TLS === '1' ? {} : undefined,
    connectionName: 'Qrew-Main-Connection',
});
redisConnection.on('connect', () => {
    console.log('Connected to Redis on ' + process.env.REDIS_URL);
});
redisConnection.on('error', (err) => {
    console.log('Redis Connection Error', err);
});
