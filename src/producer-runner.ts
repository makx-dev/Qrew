import { addPaintJob, closeProducer } from './producer';
import { redisConnection } from './redis-connection';

async function main() {
    console.log('Enqueuing paint jobs');
    for (let i = 0; i < 20; i++) {
        await addPaintJob({ color: `color-${i}` });
    }
    console.log('Jobs enqueued');
    await closeProducer();
    await redisConnection.quit();
    process.exit(0)
}

main().catch(console.error);