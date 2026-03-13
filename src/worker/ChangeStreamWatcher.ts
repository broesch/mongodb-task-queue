import type { Db, ChangeStream } from 'mongodb';
import type { Logger } from './types.js';

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
     * Wait for an insert event on any of the given collections, or until `orUntil` Date.
     * Pass `useChangeStreams: false` to skip change streams and poll instead.
     */
    async waitForChange(
        collectionNames: string[],
        orUntil?: Date | null,
        pollingInterval?: number,
        useChangeStreams = true
    ): Promise<void> {
        if (!useChangeStreams || this._available === false) {
            return this.waitWithPolling(orUntil, pollingInterval ?? 2000);
        }

        try {
            await this.watchForChange(collectionNames, orUntil);
        } catch {
            // Change streams not available (not a replica set)
            this.logger.warn('Change streams unavailable, falling back to polling');
            this._available = false;
            return this.waitWithPolling(orUntil, pollingInterval ?? 2000);
        }
    }

    private async watchForChange(collectionNames: string[], orUntil?: Date | null): Promise<void> {
        const operation = { operationType: 'insert' };

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
