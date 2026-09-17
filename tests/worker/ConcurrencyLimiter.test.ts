import { describe, it, expect } from 'vitest';
import { ConcurrencyLimiter } from '../../src/worker/ConcurrencyLimiter';

const tick = () => new Promise(resolve => setTimeout(resolve, 10));

describe('ConcurrencyLimiter', () => {
    it('rejects a non-positive limit', () => {
        expect(() => new ConcurrencyLimiter(0)).toThrow();
        expect(() => new ConcurrencyLimiter(-1)).toThrow();
    });

    it('grants slots immediately while under the limit', async () => {
        const limiter = new ConcurrencyLimiter(2);
        const r1 = await limiter.acquire();
        const r2 = await limiter.acquire();
        expect(limiter.inUse).toBe(2);
        r1();
        r2();
        expect(limiter.inUse).toBe(0);
    });

    it('queues an acquirer at the limit and grants it after a release', async () => {
        const limiter = new ConcurrencyLimiter(1);
        const r1 = await limiter.acquire();

        let granted = false;
        const pending = limiter.acquire().then(release => {
            granted = true;
            return release;
        });

        await tick();
        expect(granted).toBe(false);
        expect(limiter.waiting).toBe(1);

        r1();
        const r2 = await pending;
        expect(granted).toBe(true);
        expect(limiter.inUse).toBe(1);
        r2();
    });

    it('grants waiters in FIFO order', async () => {
        const limiter = new ConcurrencyLimiter(1);
        const first = await limiter.acquire();
        const order: string[] = [];

        const a = limiter.acquire().then(r => {
            order.push('a');
            return r;
        });
        const b = limiter.acquire().then(r => {
            order.push('b');
            return r;
        });

        first();
        (await a)();
        (await b)();
        expect(order).toEqual(['a', 'b']);
    });

    it('does not let a light waiter overtake a heavy one', async () => {
        const limiter = new ConcurrencyLimiter(2);
        const r1 = await limiter.acquire(1);
        const order: string[] = [];

        const heavy = limiter.acquire(2).then(r => {
            order.push('heavy');
            return r;
        });
        const light = limiter.acquire(1).then(r => {
            order.push('light');
            return r;
        });

        await tick();
        expect(order).toEqual([]); // light fits (1 free) but must wait behind heavy

        r1();
        (await heavy)();
        (await light)();
        expect(order).toEqual(['heavy', 'light']);
    });

    it('admits a weight above the limit when nothing is in use', async () => {
        const limiter = new ConcurrencyLimiter(1);
        const release = await limiter.acquire(5);
        expect(limiter.inUse).toBe(5);
        release();
        expect(limiter.inUse).toBe(0);
    });

    it('makes release idempotent', async () => {
        const limiter = new ConcurrencyLimiter(1);
        const release = await limiter.acquire();
        release();
        release();
        expect(limiter.inUse).toBe(0);
    });
});
