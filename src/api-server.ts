import express, { Request, Response } from 'express';
import { Queue } from 'bullmq';
import { redisConnection } from './redis-connection';
import { Server } from 'http';

export const app = express();
export const queue = new Queue('my-queue', { connection: redisConnection });

app.get('/jobs/:id/progress', async (req: Request, res: Response) => {
    try {
        const rawId = req.params.id;
        const jobId = Array.isArray(rawId) ? rawId[0] : rawId;

        if (!jobId) {
            return res.status(400).json({
                error: 'Bad Request',
                message: 'A valid Job ID is required.',
            });
        }

        // 1. Fetch job reference from Redis
        const job = await queue.getJob(jobId);

        // 2. Handle missing or expired jobs
        if (!job) {
            return res.status(404).json({
                error: 'Not Found',
                message: 'Job does not exist or has expired from cache.',
            });
        }

        // 3. Fetch state and progress in parallel
        const [state, progress] = await Promise.all([
            job.getState(), // 'active' | 'completed' | 'failed' | 'waiting' | 'delayed'
            job.progress ?? 0,
        ]);

        // 4. Return clean snapshot
        return res.json({
            jobId: job.id,
            name: job.name,
            state,
            progress,
            attemptsMade: job.attemptsMade,
            failedReason: job.failedReason ?? null,
        });
    } catch (error) {
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

export function startServer(port: number = 3000): Server {
    return app.listen(port, () => {
        console.log(`Progress API listening on http://localhost:${port}`);
    });
}

// Start standalone if executed directly
if (require.main === module) {
    startServer(3000);
}
