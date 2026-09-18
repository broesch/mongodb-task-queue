import type { Db, ChangeStream } from 'mongodb';
import type { Logger } from './types.js';

export interface WaitExtras {
    /** Resolves to end the wait early (an in-process wake-up). */
    wake?: Promise<void>;
    /** Upper bound of the wait in ms. */
    maxWaitMs?: number;
    /** Re-checked once the change stream is positioned; `true` ends the wait at once. */
    recheck?: () => Promise<boolean>;
}

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
        useChangeStreams = true,
        extras: WaitExtras = {}
    ): Promise<void> {
        if (!useChangeStreams || this._available === false) {
            return this.waitWithPolling(orUntil, pollingInterval ?? 2000, extras);
        }

        try {
            await this.watchForChange(collectionNames, orUntil, extras);
        } catch {
            // Change streams not available (not a replica set)
            this.logger.warn('Change streams unavailable, falling back to polling');
            this._available = false;
            return this.waitWithPolling(orUntil, pollingInterval ?? 2000, extras);
        }
    }

    private async watchForChange(collectionNames: string[], orUntil?: Date | null, extras: WaitExtras = {}): Promise<void> {
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

            const waitMs = boundedWait(Number.POSITIVE_INFINITY, orUntil, extras.maxWaitMs);
            const waits: Promise<unknown>[] = [changePromise, extras.wake ?? never()];
            if (Number.isFinite(waitMs)) waits.push(sleep(waitMs));
            await Promise.race(waits);
        } finally {
            this._activeStream = null;
            await stream.close().catch(() => {});
        }
    }

    private async waitWithPolling(
        orUntil: Date | null | undefined,
        pollingInterval: number,
        extras: WaitExtras
    ): Promise<void> {
        // Poll at the interval, or sooner when a task becomes visible before that. Never later:
        // `orUntil` is often the visibility deadline of a task that is IN FLIGHT, and sleeping
        // until it would stall every task enqueued meanwhile.
        const waitMs = boundedWait(pollingInterval, orUntil, extras.maxWaitMs);
        await Promise.race([sleep(waitMs), extras.wake ?? never()]);
    }

    async close(): Promise<void> {
        if (this._activeStream) {
            await this._activeStream.close().catch(() => {});
            this._activeStream = null;
        }
    }
}

/** The shortest of: the base wait, the time until `orUntil`, and the cap. */
function boundedWait(baseMs: number, orUntil: Date | null | undefined, maxWaitMs: number | undefined): number {
    let waitMs = baseMs;
    if (orUntil instanceof Date) waitMs = Math.min(waitMs, Math.max(0, orUntil.getTime() - Date.now()));
    if (typeof maxWaitMs === 'number' && maxWaitMs > 0) waitMs = Math.min(waitMs, maxWaitMs);
    return waitMs;
}

function never(): Promise<never> {
    return new Promise(() => {});
}

function sleep(ms: number): Promise<void> {
    if (ms <= 0) return Promise.resolve();
    return new Promise(r => {
        const timer = setTimeout(r, ms);
        if (typeof timer === 'object' && 'unref' in timer) timer.unref();
    });
}
