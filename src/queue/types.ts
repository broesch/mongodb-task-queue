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

/**
 * What a `hashKey` is deduplicated against:
 * - `'all'` (default): every message in the collection, including acknowledged ones kept until
 *   their TTL expires.
 * - `'active'`: only messages that are pending or in flight.
 */
export type DedupScope = 'all' | 'active';

export interface AddOptions<T = unknown> {
    hashKey?: keyof T;
    delay?: number;
    dedupScope?: DedupScope;
}

export interface QueueOptions {
    /** Default visibility timeout in seconds (default: 30) */
    visibility?: number;
    /** TTL in seconds for soft-deleted messages (default: 86400 = 24h) */
    ttl?: number;
    /** Additional indexes to create on the queue collection */
    extraIndexes?: IndexDescription[];
}
