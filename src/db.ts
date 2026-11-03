import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

export const pgPool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:6732@localhost:5432/url_shortener',
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

pgPool.on('error', (err) => {
    console.error('[PostgreSQL Pool Error]', err.message);
});

