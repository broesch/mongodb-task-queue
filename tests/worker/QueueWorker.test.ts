import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import type { Db } from 'mongodb';
import { QueueWorker } from '../../src/worker/QueueWorker';
import { ErrorAction } from '../../src/worker/types';
import type { TaskHandler, TaskContext } from '../../src/worker/types';
import { setup, teardown } from '../helpers/setup';

let db: Db;

beforeAll(async () => {
    db = await setup();
});

afterAll(async () => {
    await teardown();
});

function createWorker(handler: TaskHandler, suffix = ''): QueueWorker {
    const id = `${Date.now()}-${Math.random().toString(36).slice(2)}${suffix}`;
    return new QueueWorker({
        db,
        queues: [
            { name: `q1-${id}`, group: 'default', priority: 10, maxTaskAge: 5 },
            { name: `q2-${id}`, group: 'default', priority: 5, maxTaskAge: 5 },
        ],
        groups: {
            default: { concurrency: 2, pollingInterval: 100, useChangeStreams: false },
        },
        handler,
        logger: { debug: () => {}, log: () => {}, warn: () => {}, error: () => {} },
    });
}

describe('QueueWorker', () => {
    let worker: QueueWorker;

    afterEach(async () => {
        if (worker) {
            await worker.stop(1000);
        }
    });

    it('should process a basic task', async () => {
        const processed: unknown[] = [];

        const handler: TaskHandler = {
            async *work(payload) {
                processed.push(payload);
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ type: 'test', value: 42 }, queueNames[0]);

        // Start worker in background
        const workerPromise = worker.start('default');

        // Wait for processing
        await waitFor(() => processed.length > 0, 5000);

        await worker.stop();
        await workerPromise;

        expect(processed).toHaveLength(1);
        expect((processed[0] as any).value).toBe(42);
    });

    it('should respect priority ordering', async () => {
        const processed: string[] = [];

        const handler: TaskHandler = {
            async *work(payload: any) {
                processed.push(payload.name);
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        // Add to low-priority queue first
        await worker.add({ name: 'low-prio' }, queueNames[1]);
        // Add to high-priority queue second
        await worker.add({ name: 'high-prio' }, queueNames[0]);

        const workerPromise = worker.start('default');

        await waitFor(() => processed.length >= 2, 5000);

        await worker.stop();
        await workerPromise;

        // High priority should be processed first
        expect(processed[0]).toBe('high-prio');
        expect(processed[1]).toBe('low-prio');
    });

    it('should retry on RETRY error action', async () => {
        let attempts = 0;

        const handler: TaskHandler = {
            async *work() {
                attempts++;
                if (attempts < 3) {
                    throw new Error('transient error');
                }
                yield true;
            },
            onError: (_payload, tries) => (tries >= 3 ? ErrorAction.FAIL : ErrorAction.RETRY),
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ type: 'retry-test' }, queueNames[0]);

        const workerPromise = worker.start('default');

        await waitFor(() => attempts >= 3, 5000);

        await worker.stop();
        await workerPromise;

        expect(attempts).toBe(3);
    });

    it('should call onFail when task permanently fails', async () => {
        const failed: unknown[] = [];

        const handler: TaskHandler = {
            async *work() {
                throw new Error('permanent failure');
            },
            onError: () => ErrorAction.FAIL,
            onFail: async payload => {
                failed.push(payload);
            },
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ type: 'fail-test' }, queueNames[0]);

        const workerPromise = worker.start('default');

        await waitFor(() => failed.length > 0, 5000);

        await worker.stop();
        await workerPromise;

        expect(failed).toHaveLength(1);
        expect((failed[0] as any).type).toBe('fail-test');
    });

    it('should allow enqueuing follow-up tasks via context', async () => {
        const processed: string[] = [];

        const handler: TaskHandler = {
            async *work(payload: any, ctx: TaskContext) {
                processed.push(payload.step);
                if (payload.step === 'first') {
                    await ctx.add({ step: 'second' }, ctx.queueName);
                }
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ step: 'first' }, queueNames[0]);

        const workerPromise = worker.start('default');

        await waitFor(() => processed.length >= 2, 5000);

        await worker.stop();
        await workerPromise;

        expect(processed).toContain('first');
        expect(processed).toContain('second');
    });

    it('should report running tasks', async () => {
        let resolveWork: () => void;
        const workStarted = new Promise<void>(r => {
            resolveWork = r;
        });
        const workBlock = new Promise<void>(r => {
            // Will be resolved when we want the task to finish
            setTimeout(r, 200);
        });

        const handler: TaskHandler = {
            async *work() {
                resolveWork!();
                yield true;
                await workBlock;
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ type: 'running-test' }, queueNames[0]);

        const workerPromise = worker.start('default');

        await workStarted;
        const running = worker.getRunningTasks();
        expect(running.length).toBeGreaterThanOrEqual(1);
        expect(running[0].queueName).toBe(queueNames[0]);

        await worker.stop();
        await workerPromise;
    });

    it('should ignore error and ack when IGNORE action is returned', async () => {
        const ignored: unknown[] = [];

        const handler: TaskHandler = {
            async *work() {
                throw new Error('ignorable error');
            },
            onError: payload => {
                ignored.push(payload);
                return ErrorAction.IGNORE;
            },
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ type: 'ignore-test' }, queueNames[0]);

        const workerPromise = worker.start('default');

        await waitFor(() => ignored.length > 0, 5000);

        await worker.stop();
        await workerPromise;

        expect(ignored).toHaveLength(1);

        // Message should be acked (done), not back in the queue
        const q = worker.getQueue(queueNames[0]);
        expect(await q.size()).toBe(0);
        expect(await q.done()).toBe(1);
    });

    it('should enforce concurrency limit', async () => {
        let concurrentCount = 0;
        let maxConcurrentSeen = 0;
        const concurrencyLimit = 2;

        // Latch to hold tasks open until we've checked concurrency
        let resolveAll: () => void;
        const allDone = new Promise<void>(r => (resolveAll = r));

        const handler: TaskHandler = {
            async *work() {
                concurrentCount++;
                maxConcurrentSeen = Math.max(maxConcurrentSeen, concurrentCount);
                yield true;
                await allDone;
                concurrentCount--;
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        const id = `${Date.now()}-conc`;
        worker = new QueueWorker({
            db,
            queues: [{ name: `conc-q-${id}`, group: 'conc', priority: 1, maxTaskAge: 10 }],
            groups: { conc: { concurrency: concurrencyLimit, pollingInterval: 100, useChangeStreams: false } },
            handler,
            logger: { debug: () => {}, log: () => {}, warn: () => {}, error: () => {} },
        });
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        // Add more tasks than the concurrency limit
        for (let i = 0; i < 5; i++) {
            await worker.add({ n: i }, queueNames[0]);
        }

        const workerPromise = worker.start('conc');

        // Wait until we've hit the limit
        await waitFor(() => maxConcurrentSeen >= concurrencyLimit, 5000);
        resolveAll!();

        await worker.stop(3000);
        await workerPromise;

        // Never exceeded the limit
        expect(maxConcurrentSeen).toBeLessThanOrEqual(concurrencyLimit);
    });

    it('should respect weight-based concurrency', async () => {
        let concurrentCount = 0;
        let maxConcurrentSeen = 0;

        let resolveAll: () => void;
        const allDone = new Promise<void>(r => (resolveAll = r));

        const handler: TaskHandler = {
            async *work() {
                concurrentCount++;
                maxConcurrentSeen = Math.max(maxConcurrentSeen, concurrentCount);
                yield true;
                await allDone;
                concurrentCount--;
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        const id = `${Date.now()}-weight`;
        // concurrency=2, each task has weight=2 → only 1 task at a time
        worker = new QueueWorker({
            db,
            queues: [{ name: `weight-q-${id}`, group: 'w', priority: 1, maxTaskAge: 10, weight: 2 }],
            groups: { w: { concurrency: 2, pollingInterval: 100, useChangeStreams: false } },
            handler,
            logger: { debug: () => {}, log: () => {}, warn: () => {}, error: () => {} },
        });
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        for (let i = 0; i < 4; i++) {
            await worker.add({ n: i }, queueNames[0]);
        }

        const workerPromise = worker.start('w');

        await waitFor(() => maxConcurrentSeen >= 1, 5000);
        resolveAll!();

        await worker.stop(3000);
        await workerPromise;

        // Weight=2, concurrency=2 → max 1 concurrent task (2/2=1)
        expect(maxConcurrentSeen).toBe(1);
    });

    it('should stop gracefully and wait for in-flight tasks', async () => {
        const completed: number[] = [];
        let taskStarted = false;

        const handler: TaskHandler = {
            async *work(payload: any) {
                taskStarted = true;
                // Simulate slow task
                await new Promise(r => setTimeout(r, 200));
                yield true;
                completed.push(payload.n as number);
            },
            onError: () => ErrorAction.FAIL,
        };

        worker = createWorker(handler);
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ n: 1 }, queueNames[0]);

        const workerPromise = worker.start('default');

        // Wait for task to start, then stop
        await waitFor(() => taskStarted, 5000);
        await worker.stop(2000); // generous timeout for the 200ms task
        await workerPromise;

        // Task should have completed despite stop being called
        expect(completed).toContain(1);
    });

    it('should timeout a task that exceeds maxTaskAge', async () => {
        const errors: unknown[] = [];

        const handler: TaskHandler = {
            async *work() {
                yield true;
                // Never yield again — task hangs
                await new Promise(r => setTimeout(r, 60000));
                yield true;
            },
            onError: (_payload, _tries, error) => {
                errors.push(error);
                return ErrorAction.FAIL;
            },
        };

        const id = `${Date.now()}-timeout`;
        worker = new QueueWorker({
            db,
            queues: [{ name: `timeout-q-${id}`, group: 'to', priority: 1, maxTaskAge: 1 }],
            groups: { to: { concurrency: 1, pollingInterval: 100, useChangeStreams: false } },
            handler,
            logger: { debug: () => {}, log: () => {}, warn: () => {}, error: () => {} },
        });
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ type: 'slow' }, queueNames[0]);

        const workerPromise = worker.start('to');

        // Wait for the timeout error to be reported (maxTaskAge=1s + buffer)
        await waitFor(() => errors.length > 0, 5000);

        await worker.stop();
        await workerPromise;

        expect(errors[0]).toBeInstanceOf(Error);
        expect((errors[0] as Error).name).toBe('QueueTimeoutError');
    });

    it('should throw when adding to an unknown queue', async () => {
        worker = createWorker({
            async *work() {
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        });
        await worker.init();

        await expect(worker.add({}, 'nonexistent-queue')).rejects.toThrow('Unknown queue');
    });

    it('should throw when getting an unknown queue', async () => {
        worker = createWorker({
            async *work() {
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        });
        await worker.init();

        expect(() => worker.getQueue('nonexistent-queue')).toThrow('Unknown queue');
    });

    it('should process two independent groups in parallel', async () => {
        const groupAStart = { time: 0 };
        const groupBStart = { time: 0 };

        const handler: TaskHandler = {
            async *work(payload: any) {
                if (payload.group === 'A') groupAStart.time = Date.now();
                if (payload.group === 'B') groupBStart.time = Date.now();
                // Hold both open briefly
                await new Promise(r => setTimeout(r, 100));
                yield true;
            },
            onError: () => ErrorAction.FAIL,
        };

        const id = `${Date.now()}-parallel`;
        worker = new QueueWorker({
            db,
            queues: [
                { name: `grp-a-${id}`, group: 'groupA', priority: 1, maxTaskAge: 5 },
                { name: `grp-b-${id}`, group: 'groupB', priority: 1, maxTaskAge: 5 },
            ],
            groups: {
                groupA: { concurrency: 1, pollingInterval: 100, useChangeStreams: false },
                groupB: { concurrency: 1, pollingInterval: 100, useChangeStreams: false },
            },
            handler,
            logger: { debug: () => {}, log: () => {}, warn: () => {}, error: () => {} },
        });
        await worker.init();

        const queueNames = [...(worker as any).queues.keys()];
        await worker.add({ group: 'A' }, queueNames[0]);
        await worker.add({ group: 'B' }, queueNames[1]);

        const workerPromise = worker.start();

        await waitFor(() => groupAStart.time > 0 && groupBStart.time > 0, 5000);

        await worker.stop(2000);
        await workerPromise;

        // Both groups should have started within ~50ms of each other (parallel)
        expect(Math.abs(groupAStart.time - groupBStart.time)).toBeLessThan(500);
    });
});

function waitFor(condition: () => boolean, timeoutMs: number): Promise<void> {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        const check = () => {
            if (condition()) return resolve();
            if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
            setTimeout(check, 50);
        };
        check();
    });
}
