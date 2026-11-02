import { startServer, queue } from './api-server';
import { Server } from 'http';
import { redisConnection } from './redis-connection';

async function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeBasicAuthHeader(user: string, pass: string): string {
    return 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');
}

async function runMonitoringVerification() {
    const TEST_PORT = 3006;
    const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

    console.log('###Qrew Multi-Role Bull Board & Monitoring Verification ###');

    const server: Server = startServer(TEST_PORT);
    await sleep(300);

    let failures = 0;

    function assert(condition: boolean, testName: string, detail?: string) {
        if (condition) {
            console.log(`[PASS] ${testName}`);
        } else {
            console.error(`[FAIL] ${testName} ${detail ? `(${detail})` : ''}`);
            failures++;
        }
    }

    try {
        // -----------------------------------------------------------------
        // 1. Unauthenticated Access Protection
        // -----------------------------------------------------------------
        console.log('\n 1. Testing Unauthenticated Access');
        const unauthAdmin = await fetch(`${BASE_URL}/admin/queues/`);
        assert(unauthAdmin.status === 401, 'Unauthenticated access to /admin/queues is rejected with 401', `Status: ${unauthAdmin.status}`);

        const unauthUser = await fetch(`${BASE_URL}/user/queues/`);
        assert(unauthUser.status === 401, 'Unauthenticated access to /user/queues is rejected with 401', `Status: ${unauthUser.status}`);

        // -----------------------------------------------------------------
        // 2. Invalid Credentials Protection
        // -----------------------------------------------------------------
        console.log('\n 2. Testing Invalid Credentials');
        const badCreds = await fetch(`${BASE_URL}/user/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('viewer', 'wrong_password') },
        });
        assert(badCreds.status === 401, 'Wrong password rejected with 401', `Status: ${badCreds.status}`);

        // -----------------------------------------------------------------
        // 3. User with Missing/No Role (Strict Fail-Safe Verification)
        // -----------------------------------------------------------------
        console.log('\n 3. Testing Account with Missing Role (Strict Fail-Safe)');
        const noRoleAdmin = await fetch(`${BASE_URL}/admin/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('guest', 'guest_secret_pass') },
        });
        assert(
            noRoleAdmin.status === 403,
            'CRITICAL: Account without role is strictly DENIED admin access with 403',
            `Status: ${noRoleAdmin.status}`
        );

        const noRoleUser = await fetch(`${BASE_URL}/user/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('guest', 'guest_secret_pass') },
        });
        assert(
            noRoleUser.status === 403,
            'Account without role is denied user read-only access with 403',
            `Status: ${noRoleUser.status}`
        );

        // -----------------------------------------------------------------
        // 4. Standard User ("viewer") Privileges & Constraints
        // -----------------------------------------------------------------
        console.log('\n 4. Testing Standard User ("viewer") Role');
        const viewerAdminRes = await fetch(`${BASE_URL}/admin/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('viewer', 'viewer_secret_pass') },
        });
        assert(
            viewerAdminRes.status === 403,
            'Viewer cannot access /admin/queues (403 Forbidden)',
            `Status: ${viewerAdminRes.status}`
        );

        const viewerUserRes = await fetch(`${BASE_URL}/user/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('viewer', 'viewer_secret_pass') },
        });
        assert(
            viewerUserRes.status === 200,
            'Viewer can access /user/queues (200 OK)',
            `Status: ${viewerUserRes.status}`
        );

        // Mutating action on user dashboard must be hard-blocked by enforceReadOnlyMethods
        console.log('\n 5. Testing Mutating Method Blocker on /user/queues');
        const postAttempt = await fetch(`${BASE_URL}/user/queues/api/queues/my-queue/retry`, {
            method: 'POST',
            headers: { Authorization: makeBasicAuthHeader('viewer', 'viewer_secret_pass') },
        });
        assert(
            postAttempt.status === 403,
            'POST on /user/queues is hard-blocked with 403 (enforceReadOnlyMethods)',
            `Status: ${postAttempt.status}`
        );

        const putAttempt = await fetch(`${BASE_URL}/user/queues/api/queues/my-queue/clean`, {
            method: 'PUT',
            headers: { Authorization: makeBasicAuthHeader('viewer', 'viewer_secret_pass') },
        });
        assert(
            putAttempt.status === 403,
            'PUT on /user/queues is hard-blocked with 403 (enforceReadOnlyMethods)',
            `Status: ${putAttempt.status}`
        );

        const deleteAttempt = await fetch(`${BASE_URL}/user/queues/api/queues/my-queue/123`, {
            method: 'DELETE',
            headers: { Authorization: makeBasicAuthHeader('viewer', 'viewer_secret_pass') },
        });
        assert(
            deleteAttempt.status === 403,
            'DELETE on /user/queues is hard-blocked with 403 (enforceReadOnlyMethods)',
            `Status: ${deleteAttempt.status}`
        );

        // -----------------------------------------------------------------
        // 6. Admin User Full Access
        // -----------------------------------------------------------------
        console.log('\n 6. Testing Admin User ("admin") Role');
        const adminAccess = await fetch(`${BASE_URL}/admin/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('admin', 'admin_secret_pass') },
        });
        assert(
            adminAccess.status === 200,
            'Admin can access /admin/queues (200 OK)',
            `Status: ${adminAccess.status}`
        );

        const adminUserDashboardAccess = await fetch(`${BASE_URL}/user/queues/`, {
            headers: { Authorization: makeBasicAuthHeader('admin', 'admin_secret_pass') },
        });
        assert(
            adminUserDashboardAccess.status === 200,
            'Admin can also access /user/queues (200 OK)',
            `Status: ${adminUserDashboardAccess.status}`
        );

        // -----------------------------------------------------------------
        // 7. Programmatic Metrics API
        // -----------------------------------------------------------------
        console.log('\n 7. Testing /api/v1/metrics Endpoint');
        const metricsRes = await fetch(`${BASE_URL}/api/v1/metrics`);
        const metricsData = await metricsRes.json();
        if (redisConnection.status === 'ready') {
            assert(metricsRes.status === 200, 'GET /api/v1/metrics returns 200 when broker is ready', `Status: ${metricsRes.status}`);
            assert(typeof metricsData.queueDepth === 'number', 'Metrics returns numeric queueDepth');
            assert(typeof metricsData.metrics.active === 'number', 'Metrics returns numeric active count');
            assert(typeof metricsData.metrics.failed === 'number', 'Metrics returns numeric failed count');
            assert(typeof metricsData.metrics.completed === 'number', 'Metrics returns numeric completed count');
            console.log('  => Sample Telemetry Snapshot:', JSON.stringify(metricsData, null, 2));
        } else {
            assert(
                metricsRes.status === 503,
                'GET /api/v1/metrics gracefully handles offline broker with 503 Service Unavailable',
                `Status: ${metricsRes.status}`
            );
            console.log('  => Broker offline handler successfully verified (status 503).');
        }

    } catch (err: any) {
        console.error('Unexpected test error:', err);
        failures++;
    } finally {
        server.close();
        if (redisConnection.status === 'ready') {
            await queue.close();
            await redisConnection.quit();
        } else {
            redisConnection.disconnect();
        }
        if (failures === 0) {
            console.log('\n>>> ALL 12 MONITORING & SECURITY INVARIANTS PASSED SUCCESSFULLY! <<<');
        } else {
            console.error(`\n>>> VERIFICATION FAILED: ${failures} assertions failed. <<<`);
        }
    }
}

if (require.main === module) {
    runMonitoringVerification();
}

