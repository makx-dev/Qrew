// Priority Levels for Jobs
export enum JobPriority {
    CRITICAL = 1, //eg- OTP, Password reset
    HIGH = 2, //eg- User transaction email
    MEDIUM = 3, //eg- Image resizing
}

// Types of Jobs and Payloads
export interface NotificationPayload {
    userId: string;
    message: string;
    channel: 'sms' | 'push';
}

export interface EmailPayload {
    to: string;
    subject: string;
    body: string;
}

export interface ImageProcessingPayload {
    imageUrl: string;
    dimensions: { width: number; height: number };
    format: 'jpg' | 'png' | 'webp';
}

export interface ClickSyncPayload {
    batchSize?: number;
}

// Union type to tie names to payloads
export type AppJob =
    | { name: 'notification'; data: NotificationPayload; priority: JobPriority.CRITICAL }
    | { name: 'email'; data: EmailPayload; priority: JobPriority.HIGH }
    | { name: 'image-processing'; data: ImageProcessingPayload; priority: JobPriority.MEDIUM }
    | { name: 'click-sync'; data: ClickSyncPayload; priority: JobPriority.HIGH };
