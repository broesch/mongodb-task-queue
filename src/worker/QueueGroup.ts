import type { MongoQueue } from '../queue/MongoQueue.js';
import type { ResolvedQueue, GroupOptions } from './types.js';

export interface QueueEntry {
    definition: ResolvedQueue;
    queue: MongoQueue;
}

export class QueueGroup {
    readonly name: string;
    readonly concurrency: number;
    readonly pollingInterval: number;
    readonly useChangeStreams: boolean;
    private readonly entries: QueueEntry[];

    constructor(name: string, options: GroupOptions, entries: QueueEntry[]) {
        this.name = name;
        this.concurrency = options.concurrency ?? 1;
        this.pollingInterval = options.pollingInterval ?? 2000;
        this.useChangeStreams = options.useChangeStreams ?? true;
        // Sort by priority descending (higher priority first)
        this.entries = entries.sort((a, b) => b.definition.priority - a.definition.priority);
    }

    /** Get all queue entries in priority order */
    getQueues(): QueueEntry[] {
        return this.entries;
    }

    /** Collection names for change stream watching */
    getCollectionNames(): string[] {
        return this.entries.map(e => e.definition.name);
    }
}
