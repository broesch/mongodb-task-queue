import type { Db, ChangeStream } from 'mongodb';
import type { Logger } from './types.js';

export type WatchEvent = 'insert' | 'finish';

export class ChangeStreamWatcher {
    private _available: boolean | null = null;
    private _activeStream: ChangeStream | null = null;

    constructor(
        private readonly db: Db,
        private readonly logger: Logger
    ) {}

    get available(): boolean | null {
        return this._available;
    }

    /**
     * Wait for a change event on any of the given collections, or until `orUntil` Date.
     * Returns immediately if change streams are unavailable.
     */
    async waitForChange(
        collectionNames: string[],
        event: WatchEvent,
        orUntil?: Date | null,
        pollingInterval?: number
    ): Promise<void> {
        if (this._available === false) {
            return this.waitWithPolling(orUntil, pollingInterval ?? 2000);
        }

        try {
            await this.watchForChange(collectionNames, event, orUntil);
        } catch {
            // Change streams not available (not a replica set)
            this.logger.warn('Change streams unavailable, falling back to polling');
            this._available = false;
            return this.waitWithPolling(orUntil, pollingInterval ?? 2000);
        }
    }

    private async watchForChange(collectionNames: string[], event: WatchEvent, orUntil?: Date | null): Promise<void> {
        const operation =
            event === 'insert'
                ? { operationType: 'insert' }
                : {
                      $or: [
                          { 'updateDescription.updatedFields.deleted': { $exists: true } },
                          { 'updateDescription.updatedFields.requeued': { $exists: true } },
                      ],
                  };

        const pipeline = [
            {
                $match: {
                    $and: [{ 'ns.coll': { $in: collectionNames } }, operation],
                },
            },
        ];

        const stream = this.db.watch(pipeline);
        this._activeStream = stream;
        this._available = true;

        try {
            const changePromise = stream.next();

            if (orUntil && orUntil instanceof Date) {
                const sleepMs = Math.max(0, orUntil.getTime() - Date.now());
                await Promise.race([changePromise, sleep(sleepMs)]);
            } else {
                await changePromise;
            }
        } finally {
            this._activeStream = null;
            await stream.close().catch(() => {});
        }
    }

    private async waitWithPolling(orUntil?: Date | null, pollingInterval: number = 2000): Promise<void> {
        let waitMs = pollingInterval;

        if (orUntil && orUntil instanceof Date) {
            const untilMs = orUntil.getTime() - Date.now();
            if (untilMs > pollingInterval) {
                waitMs = Math.max(0, untilMs);
            }
        }

        await sleep(Math.max(0, waitMs));
    }

    async close(): Promise<void> {
        if (this._activeStream) {
            await this._activeStream.close().catch(() => {});
            this._activeStream = null;
        }
    }
}

function sleep(ms: number): Promise<void> {
    if (ms <= 0) return Promise.resolve();
    return new Promise(r => {
        const timer = setTimeout(r, ms);
        if (typeof timer === 'object' && 'unref' in timer) timer.unref();
    });
}
