import type { IndexDescription } from 'mongodb';

export interface Message<T = unknown> {
    id: string;
    ack: string;
    createdAt: Date;
    updatedAt: Date;
    payload: T;
    tries: number;
    occurrences: number;
}

export interface AddOptions<T = unknown> {
    hashKey?: keyof T;
    delay?: number;
}

export interface QueueOptions {
    /** Default visibility timeout in seconds (default: 30) */
    visibility?: number;
    /** TTL in seconds for soft-deleted messages (default: 86400 = 24h) */
    ttl?: number;
    /** Additional indexes to create on the queue collection */
    extraIndexes?: IndexDescription[];
}
