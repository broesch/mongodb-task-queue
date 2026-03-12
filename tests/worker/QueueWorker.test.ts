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
