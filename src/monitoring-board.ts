import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import { Queue } from 'bullmq';
import { Request, Response, NextFunction } from 'express';

export interface MultiRoleBoardOptions {
    queues: Queue[];
}

export function createMultiRoleBullBoards({ queues }: MultiRoleBoardOptions) {
    // ----------------------------------------------------
    // 1. ADMIN BOARD (Full Mutating Controls)
    // ----------------------------------------------------
    const adminAdapter = new ExpressAdapter();
    adminAdapter.setBasePath('/admin/queues');

    createBullBoard({
        queues: queues.map(
            (q) =>
                new BullMQAdapter(q, {
                    readOnlyMode: false,
                    description: `Admin control panel for queue "${q.name}" (Retry, Clean, and Inspect)`,
                })
        ),
        serverAdapter: adminAdapter,
        options: {
            uiConfig: {
                boardTitle: 'Qrew Admin Console (Full Mutating Access)',
            },
        },
    });

    // ----------------------------------------------------
    // 2. USER BOARD (Read-Only UI)
    // ----------------------------------------------------
    const userAdapter = new ExpressAdapter();
    userAdapter.setBasePath('/user/queues');

    createBullBoard({
        queues: queues.map(
            (q) =>
                new BullMQAdapter(q, {
                    readOnlyMode: true,
                    description: `Observer telemetry for queue "${q.name}" (Read-only mode)`,
                })
        ),
        serverAdapter: userAdapter,
        options: {
            uiConfig: {
                boardTitle: 'Qrew Queue Observer (Read-Only)',
            },
        },
    });

    return {
        adminRouter: adminAdapter.getRouter(),
        userRouter: userAdapter.getRouter(),
    };
}

/**
 * Hard security guard: Restricts standard users to idempotent HTTP read methods.
 * Prevents non-admin callers from invoking mutating endpoints via DevTools or curl.
 */
export function enforceReadOnlyMethods(req: Request, res: Response, next: NextFunction) {
    const allowedMethods = ['GET', 'HEAD', 'OPTIONS'];
    if (!allowedMethods.includes(req.method)) {
        return res.status(403).json({
            error: 'Forbidden',
            message: `Mutating HTTP method "${req.method}" is prohibited on the read-only dashboard. Contact an administrator to perform job retries or deletions.`,
        });
    }
    return next();
}

