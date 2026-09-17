interface Waiter {
    weight: number;
    grant: (release: () => void) => void;
}

/**
 * A weighted FIFO concurrency limiter that several QueueWorkers can share.
 *
 * A worker acquires a slot BEFORE it claims a task, so a task that cannot run yet stays
 * visible and unclaimed in its queue instead of burning its visibility timeout.
 */
export class ConcurrencyLimiter {
    readonly limit: number;
    private used = 0;
    private readonly queue: Waiter[] = [];

    constructor(limit: number) {
        if (!(limit > 0)) throw new Error('ConcurrencyLimiter: limit must be greater than 0');
        this.limit = limit;
    }

    /** Sum of the weights currently acquired. */
    get inUse(): number {
        return this.used;
    }

    /** Number of acquirers waiting for a slot. */
    get waiting(): number {
        return this.queue.length;
    }

    /** Wait for `weight` capacity. Resolves with an idempotent release function. */
    acquire(weight = 1): Promise<() => void> {
        return new Promise(resolve => {
            this.queue.push({ weight, grant: resolve });
            this.drain();
        });
    }

    private drain(): void {
        // Strict FIFO: only ever look at the head, so a heavy waiter cannot be starved.
        while (this.queue.length > 0) {
            const head = this.queue[0];
            // A weight above the limit is admitted once nothing else runs — never a deadlock.
            const fits = this.used + head.weight <= this.limit || this.used === 0;
            if (!fits) return;

            this.queue.shift();
            this.used += head.weight;

            let released = false;
            head.grant(() => {
                if (released) return;
                released = true;
                this.used -= head.weight;
                this.drain();
            });
        }
    }
}
