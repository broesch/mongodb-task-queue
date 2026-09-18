import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import type { Db } from 'mongodb';
import { QueueWorker } from '../../src/worker/QueueWorker';
import { ErrorAction } from '../../src/worker/types';
import type { TaskHandler } from '../../src/worker/types';
import { setup, teardown } from '../helpers/setup';

const silent = { debug: () => {}, log: () => {}, warn: () => {}, error: () => {} };
const uid = () => `${Date.now()}-${Math.random().toString(36).slice(2)}`;

function waitFor(condition: () => boolean, timeoutMs: number): Promise<void> {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        const check = () => {
            if (condition()) return resolve();
            if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
            setTimeout(check, 25);
        };
        check();
    });
}

describe('worker wake-ups — polling mode', () => {
    let db: Db;
    let worker: QueueWorker | undefined;

    beforeAll(async () => {
        db = await setup();
    });
    afterAll(async () => {
        await teardown();
    });
    afterEach(async () => {
        await worker?.stop(1000);
        worker = undefined;
    });

    const makeWorker = (queue: string, handler: TaskHandler, pollingInterval = 100) =>
        new QueueWorker({
            db,
            // A LONG visibility: the old code slept until it expired.
            queues: [{ name: queue, group: 'g', priority: 1, maxTaskAge: 60 }],
            groups: { g: { concurrency: 2, pollingInterval, useChangeStreams: false, maxIdleWait: 60_000 } },
            handler,
            logger: silent,
        });

    it('picks up a task enqueued while another one is running', async () => {
        const queue = `poll-busy-${uid()}`;
        const started: number[] = [];
        let releaseFirst: () => void = () => {};
        const handler: TaskHandler<{ n: number }> = {
            async *work(payload) {
                started.push(payload.n);
                if (payload.n === 1) await new Promise<void>(r => (releaseFirst = r));
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };
        worker = makeWorker(queue, handler as TaskHandler);
        await worker.init();
        await worker.add({ n: 1 }, queue);
        void worker.start('g');
        await waitFor(() => started.includes(1), 5000);
        // Let the loop park on the in-flight task's 60 s visibility deadline.
        await new Promise(r => setTimeout(r, 400));

        const enqueuedAt = Date.now();
        // A raw insert through another queue handle: no in-process shortcut can help.
        await new QueueWorker({
            db,
            queues: [{ name: queue, group: 'g', priority: 1 }],
            groups: { g: {} },
            handler: handler as TaskHandler,
            logger: silent,
        }).add({ n: 2 }, queue);

        await waitFor(() => started.includes(2), 5000);
        expect(Date.now() - enqueuedAt).toBeLessThan(2000);
        releaseFirst();
    });

    it('runs a delayed retry on time even though the loop parked before the failure', async () => {
        const queue = `poll-retry-${uid()}`;
        const attempts: number[] = [];
        const handler: TaskHandler = {
            async *work() {
                attempts.push(Date.now());
                if (attempts.length === 1) {
                    // Fail AFTER the loop has parked — like a model call that is throttled
                    // a second into the request.
                    await new Promise(r => setTimeout(r, 600));
                    throw new Error('throttled');
                }
                yield true;
            },
            onError: () => ({ action: ErrorAction.RETRY, delay: 1 }),
        };
        // A 60 s polling interval: only the wake signal can make this pass.
        worker = makeWorker(queue, handler, 60_000);
        await worker.init();
        await worker.add({ n: 1 }, queue);
        void worker.start('g');

        await waitFor(() => attempts.length >= 2, 8000);
        const gap = attempts[1] - attempts[0];
        expect(gap).toBeGreaterThanOrEqual(1500); // 600 ms until the failure + the 1 s delay
        expect(gap).toBeLessThan(4000);
    });

    it('looks again after maxIdleWait when nothing else wakes it', async () => {
        const queue = `poll-cap-${uid()}`;
        const seen: number[] = [];
        const handler: TaskHandler<{ n: number }> = {
            async *work(payload) {
                seen.push(payload.n);
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };
        worker = new QueueWorker({
            db,
            queues: [{ name: queue, group: 'g', priority: 1, maxTaskAge: 60 }],
            groups: { g: { concurrency: 1, pollingInterval: 60_000, useChangeStreams: false, maxIdleWait: 500 } },
            handler: handler as TaskHandler,
            logger: silent,
        });
        await worker.init();
        void worker.start('g');
        await new Promise(r => setTimeout(r, 300)); // parked on the 60 s polling interval

        // Bypass the worker entirely: a raw document, as another process would write it.
        await db.collection(queue).insertOne({
            createdAt: new Date(),
            visible: new Date(),
            payload: { n: 7 },
            tries: 0,
            occurrences: 1,
        });

        await waitFor(() => seen.includes(7), 5000);
    });
});
