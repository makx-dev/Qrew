import { startServer, queue } from './api-server';
import { redisConnection } from './redis-connection';
import { Server } from 'http';

async function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runApiVerification() {
    const TEST_PORT = 3005;
    const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

    console.log('##### Qrew REST API Layer Verification #####\n');

    const server: Server = startServer(TEST_PORT);
    await sleep(200);

    try {
        // 1. Health check
        console.log('[TEST 1/6] GET /health...');
        const healthRes = await fetch(`${BASE_URL}/health`);
        const healthBody = await healthRes.json();
        console.log('  => Status:', healthRes.status, healthBody);
        if (healthRes.status !== 200) throw new Error('Health check failed');

        // 2. Validation failure test
        console.log('\n[TEST 2/6] POST /api/v1/jobs with invalid payload (missing fields)...');
        const invalidRes = await fetch(`${BASE_URL}/api/v1/jobs`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                type: 'email',
                data: { to: 'test@example.com' }, // missing subject & body
            }),
        });
        const invalidBody = await invalidRes.json();
        console.log('  => Status:', invalidRes.status, invalidBody);
        if (invalidRes.status !== 400) throw new Error('Expected 400 Bad Request');

        // 3. Submit valid job with Idempotency-Key
        console.log('\n[TEST 3/6] POST /api/v1/jobs with Idempotency-Key...');
        const idempotencyKey = `test-key-${Date.now()}`;
        const validPayload = {
            type: 'email',
            data: {
                to: 'ops@company.com',
                subject: 'Daily Digest',
                body: 'Here is your report',
            },
            priority: 2,
        };

        const submitRes = await fetch(`${BASE_URL}/api/v1/jobs`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Idempotency-Key': idempotencyKey,
            },
            body: JSON.stringify(validPayload),
        });
        const submitBody = await submitRes.json();
        console.log('  => Status:', submitRes.status, submitBody);
        if (submitRes.status !== 202 || !submitBody.jobId) throw new Error('Expected 202 Accepted with jobId');
        const createdJobId = submitBody.jobId;

        // 4. Test Idempotency: exact same key & body must return cached response
        console.log('\n[TEST 4/6] Idempotency: Duplicate POST with same key & body...');
        const duplicateRes = await fetch(`${BASE_URL}/api/v1/jobs`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Idempotency-Key': idempotencyKey,
            },
            body: JSON.stringify(validPayload),
        });
        const duplicateBody = await duplicateRes.json();
        console.log('  => Status:', duplicateRes.status, duplicateBody);
        if (duplicateBody.jobId !== createdJobId) throw new Error('Idempotent request returned different job ID!');

        // 4b. Test Idempotency: same key with modified body must return 422
        console.log('\n[TEST 4b/6] Idempotency: Reuse key with altered payload...');
        const alteredRes = await fetch(`${BASE_URL}/api/v1/jobs`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Idempotency-Key': idempotencyKey,
            },
            body: JSON.stringify({ ...validPayload, priority: 1 }),
        });
        const alteredBody = await alteredRes.json();
        console.log('  => Status:', alteredRes.status, alteredBody);
        if (alteredRes.status !== 422) throw new Error('Expected 422 for payload mismatch');

        // 5. GET /api/v1/jobs/:id (Job status and metadata)
        console.log(`\n[TEST 5/6] GET /api/v1/jobs/${createdJobId}...`);
        const statusRes = await fetch(`${BASE_URL}/api/v1/jobs/${createdJobId}`);
        const statusBody = await statusRes.json();
        console.log('  => Status:', statusRes.status, statusBody);
        if (statusRes.status !== 200 || statusBody.jobId !== createdJobId) throw new Error('Failed to retrieve job status');

        // 6. DLQ Listing
        console.log('\n[TEST 6/6] GET /api/v1/dlq/jobs (DLQ list endpoint)...');
        const dlqRes = await fetch(`${BASE_URL}/api/v1/dlq/jobs?limit=5`);
        const dlqBody = await dlqRes.json();
        console.log('  => Status:', dlqRes.status, dlqBody);
        if (dlqRes.status !== 200 || !Array.isArray(dlqBody.data)) throw new Error('Failed to list DLQ jobs');

        console.log('\n=> ALL REST API ENDPOINT TESTS PASSED SUCCESSFULLY <=');
    } finally {
        server.close();
        await queue.close();
        await redisConnection.quit();
        process.exit(0);
    }
}

runApiVerification().catch((err) => {
    console.error('API Verification Failed:', err);
    process.exit(1);
});

