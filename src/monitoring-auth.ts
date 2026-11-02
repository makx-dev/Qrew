import { Request, Response, NextFunction } from 'express';
import crypto from 'crypto';

export interface UserAccount {
    secret: string;
    roles?: string[]; // Optional or may be undefined/empty
}

/**
 * Registry of permitted accounts. In production, load from environment variables,
 * secret manager, or database.
 */
export const USER_REGISTRY: Record<string, UserAccount> = {
    // Observer account (read-only)
    viewer: {
        secret: process.env.USER_DASHBOARD_PASSWORD || 'viewer_secret_pass',
        roles: ['user'],
    },
    // Ops / Admin account (full mutating access)
    admin: {
        secret: process.env.ADMIN_DASHBOARD_PASSWORD || 'admin_secret_pass',
        roles: ['admin'],
    },
    // Demonstration account with no role assigned
    guest: {
        secret: process.env.GUEST_PASSWORD || 'guest_secret_pass',
        // roles deliberately omitted to test defense against missing roles
    },
};

/**
 * Timing-safe string comparison to protect against timing side-channel attacks
 */
export function timingSafeCompare(a: string, b: string): boolean {
    const bufA = Buffer.from(a);
    const bufB = Buffer.from(b);
    if (bufA.length !== bufB.length) {
        // Run comparison against self to equalize execution time before returning false
        crypto.timingSafeEqual(bufA, bufA);
        return false;
    }
    return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * Validates role membership with a strict fail-safe (deny by default):
 * If the role property is missing, undefined, null, or empty, it strictly
 * returns FALSE. Never grants admin or user access without explicit role.
 */
export function checkUserRole(account: UserAccount | undefined, requiredRole: 'user' | 'admin'): boolean {
    if (!account) {
        return false;
    }

    // STRICT FAIL-SAFE: If account.roles is undefined, null, or not a non-empty array, deny access
    if (!account.roles || !Array.isArray(account.roles) || account.roles.length === 0) {
        return false;
    }

    const isAdmin = account.roles.includes('admin');
    const isUser = account.roles.includes('user');

    if (requiredRole === 'admin') {
        // STRICT: Only explicit 'admin' role gets full admin permissions
        return isAdmin;
    }

    if (requiredRole === 'user') {
        // Read-only dashboard can be accessed by both 'user' and 'admin'
        return isUser || isAdmin;
    }

    return false;
}

/**
 * Express middleware factory enforcing HTTP Basic Auth and Role-Based Access Control (RBAC).
 */
export function createAuthGuard(requiredRole: 'user' | 'admin') {
    const realm = requiredRole === 'admin' ? 'Qrew Admin Console' : 'Qrew Read-Only Monitoring';

    return (req: Request, res: Response, next: NextFunction) => {
        const authHeader = req.headers.authorization;

        if (!authHeader || !authHeader.startsWith('Basic ')) {
            res.setHeader('WWW-Authenticate', `Basic realm="${realm}"`);
            return res.status(401).json({
                error: 'Unauthorized',
                message: 'Authentication required. Please provide valid HTTP Basic credentials.',
            });
        }

        try {
            const base64Credentials = authHeader.substring(6).trim();
            const decoded = Buffer.from(base64Credentials, 'base64').toString('utf8');
            const colonIndex = decoded.indexOf(':');

            if (colonIndex === -1) {
                res.setHeader('WWW-Authenticate', `Basic realm="${realm}"`);
                return res.status(401).json({
                    error: 'Unauthorized',
                    message: 'Malformed authorization credentials.',
                });
            }

            const username = decoded.substring(0, colonIndex);
            const password = decoded.substring(colonIndex + 1);

            const account = USER_REGISTRY[username];
            if (!account || !timingSafeCompare(password, account.secret)) {
                res.setHeader('WWW-Authenticate', `Basic realm="${realm}"`);
                return res.status(401).json({
                    error: 'Unauthorized',
                    message: 'Invalid username or password.',
                });
            }

            // Verify strict role assignment (fails safe if roles is missing)
            const hasRequiredRole = checkUserRole(account, requiredRole);
            if (!hasRequiredRole) {
                return res.status(403).json({
                    error: 'Forbidden',
                    message: `Access denied. Insufficient privileges: required role '${requiredRole}'.`,
                });
            }

            // Attach validated user context for downstream handlers/logging
            (req as any).user = {
                username,
                roles: account.roles || [],
            };

            return next();
        } catch (err: any) {
            return res.status(500).json({
                error: 'Internal Server Error',
                message: 'Failed to process authorization.',
            });
        }
    };
}

export const requireUserAuth = createAuthGuard('user');
export const requireAdminAuth = createAuthGuard('admin');

