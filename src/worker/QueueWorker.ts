import type { Db, Document, Filter } from 'mongodb';
import { ObjectId } from 'mongodb';

import { MongoQueue } from '../queue/MongoQueue.js';
import type { DedupScope, Message } from '../queue/types.js';
import { QueueTimeoutError, PingError, AckError, WrongAckIdError } from '../errors/index.js';
import { raceWithTimeout } from './HeartbeatRunner.js';
import { ChangeStreamWatcher } from './ChangeStreamWatcher.js';
import { QueueGroup, type QueueEntry } from './QueueGroup.js';
import type { ConcurrencyLimiter } from './ConcurrencyLimiter.js';
import type {
    QueueDefinition,
    GroupOptions,
    TaskHandler,
    TaskContext,
    TaskInfo,
    Logger,
    ResolvedQueue,
} from './types.js';
import { ErrorAction } from './types.js';

const defaultLogger: Logger = {
    debug: () => {},
    log: console.log.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console),
};

export interface QueueWorkerOptions {
    db: Db;
    queues: QueueDefinition[];
    groups: Record<string, GroupOptions>;
    handler: TaskHandler;
    logger?: Logger;
    /**
     * Optional limiter shared with other QueueWorkers. A slot is acquired BEFORE a task is
     * claimed, so tasks that cannot run yet stay visible and unclaimed in their queue.
     */
    limiter?: ConcurrencyLimiter;
}

interface RunningTask extends TaskInfo {
    promise: Promise<void>;
}

export class QueueWorker {
    private readonly db: Db;
    private readonly handler: TaskHandler;
    private readonly logger: Logger;
    private readonly queues = new Map<string, MongoQueue>();
    private readonly groups = new Map<string, QueueGroup>();
    private readonly runningTasks: RunningTask[] = [];
    private readonly watcher: ChangeStreamWatcher;
    private stopped = false;
    private readonly limiter?: ConcurrencyLimiter;
    private stopSignal!: Promise<null>;
    private resolveStop!: () => void;
    private wakeSignal!: Promise<void>;
    private resolveWake!: () => void;

    constructor(options: QueueWorkerOptions) {
        this.db = options.db;
        this.handler = options.handler;
        this.logger = options.logger ?? defaultLogger;
        this.limiter = options.limiter;
        this.armStopSignal();
        this.armWakeSignal();
        this.watcher = new ChangeStreamWatcher(this.db, this.logger);

        // Build queue instances and groups
        const groupEntries = new Map<string, QueueEntry[]>();

        for (const def of options.queues) {
            const groupOpts = options.groups[def.group] ?? {};
            const resolved: ResolvedQueue = {
                ...def,
                concurrency: groupOpts.concurrency ?? 1,
                pollingInterval: groupOpts.pollingInterval ?? 2000,
                useChangeStreams: groupOpts.useChangeStreams ?? true,
            };

            const queue = new MongoQueue(this.db, def.name, {
                visibility: def.maxTaskAge ?? 30,
            });
            this.queues.set(def.name, queue);

            const entries = groupEntries.get(def.group) ?? [];
            entries.push({ definition: resolved, queue });
            groupEntries.set(def.group, entries);
        }

        for (const [name, entries] of groupEntries) {
            const groupOpts = options.groups[name] ?? {};
            this.groups.set(name, new QueueGroup(name, groupOpts, entries));
        }
    }

    /** Initialize indexes for all queues */
    async init(): Promise<void> {
        for (const queue of this.queues.values()) {
            await queue.createIndexes();
        }
        this.logger.log('All queue indexes created');
    }

    /** Start processing tasks. Runs indefinitely until stop() is called. */
    async start(groupName?: string): Promise<void> {
        if (this.stopped) this.armStopSignal();
        this.stopped = false;

        const groupNames = groupName ? [groupName] : Array.from(this.groups.keys());
        const promises = groupNames.map(name => this.runGroup(name));
        await Promise.all(promises);
    }

    /** Graceful shutdown: stop accepting new tasks, wait for in-flight tasks to complete. */
    async stop(timeoutMs: number = 30000): Promise<void> {
        this.stopped = true;
        this.resolveStop();
        await this.watcher.close();

        if (this.runningTasks.length > 0) {
            this.logger.log(`Waiting for ${this.runningTasks.length} in-flight tasks to complete...`);
            const allDone = Promise.all(this.runningTasks.map(t => t.promise));
            const timeout = sleep(timeoutMs);
            await Promise.race([allDone, timeout]);
        }
    }

    /** Enqueue a task into a named queue */
    async add<U = unknown>(
        payload: U,
        queueName: string,
        options?: { hashKey?: keyof U; delay?: number; dedupScope?: DedupScope }
    ): Promise<string> {
        const queue = this.queues.get(queueName) as MongoQueue<U> | undefined;
        if (!queue) throw new Error(`Unknown queue: ${queueName}`);
        const id = await queue.add(payload, options);
        this.wake(); // local fast path; other processes are reached by the change stream
        return id;
    }

    /** Cancel matching tasks that no worker has claimed. Returns the number removed. */
    async cancel(filter: Filter<Document>, queueName: string): Promise<number> {
        return this.getQueue(queueName).cancel(filter);
    }

    /** Get direct access to a MongoQueue instance */
    getQueue(name: string): MongoQueue {
        const queue = this.queues.get(name);
        if (!queue) throw new Error(`Unknown queue: ${name}`);
        return queue;
    }

    /** Get info about currently running tasks */
    getRunningTasks(groupName?: string): TaskInfo[] {
        const tasks = groupName ? this.runningTasks.filter(t => t.group === groupName) : this.runningTasks;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        return tasks.map(({ promise, ...info }) => info);
    }

    // --- Internal ---

    private async runGroup(groupName: string): Promise<void> {
        const group = this.groups.get(groupName);
        if (!group) throw new Error(`Unknown group: ${groupName}`);

        this.logger.log(`Starting queue group: ${groupName}`);

        while (!this.stopped) {
            // Check if there's work available
            if (!(await this.hasWork(group))) {
                this.logger.debug(`No tasks in group ${groupName}, waiting...`);
                const nextVisible = await this.getNextVisibleTime(group);
                await this.watcher.waitForChange(
                    group.getCollectionNames(),
                    nextVisible,
                    group.pollingInterval,
                    group.useChangeStreams,
                    { wake: this.wakeSignal, maxWaitMs: group.maxIdleWait, recheck: () => this.hasWork(group) }
                );
                if (this.stopped) break;
                continue;
            }

            // Process available tasks
            await this.processGroup(group);
        }

        this.logger.log(`Queue group stopped: ${groupName}`);
    }

    private async processGroup(group: QueueGroup): Promise<void> {
        for (const entry of group.getQueues()) {
            if (this.stopped) return;

            // Shared limiter: take a slot BEFORE claiming, so a waiting task stays unclaimed.
            const release = await this.acquireSlot(entry.definition.weight ?? 1);
            if (release === null) return; // stopped while waiting

            // Try to get a message directly (fixes race condition: no separate size() check)
            let message: Message | undefined;
            try {
                message = await entry.queue.get();
            } catch (e) {
                release();
                throw e;
            }
            if (!message) {
                release();
                continue;
            }

            this.logger.log(`Got task ${message.id} from ${entry.definition.name}`);

            // Start processing in background
            const task = this.createRunningTask(message, entry);
            this.runningTasks.push(task);

            // Run task in background (no await)
            void task.promise.finally(() => {
                release();
                const idx = this.runningTasks.indexOf(task);
                if (idx !== -1) this.runningTasks.splice(idx, 1);
            });

            // Check concurrency limit
            const weightedCount = this.getWeightedCount(group.name);
            if (weightedCount >= group.concurrency) {
                this.logger.debug(`Concurrency limit reached for ${group.name}: ${weightedCount}/${group.concurrency}`);
                await this.waitForSlot(group);
            }
        }
    }

    private createRunningTask(message: Message, entry: QueueEntry): RunningTask {
        const info: RunningTask = {
            id: message.id,
            queueName: entry.definition.name,
            group: entry.definition.group,
            weight: entry.definition.weight ?? 1,
            startedAt: new Date(),
            promise: null!,
        };

        info.promise = this.executeTask(message, entry);
        return info;
    }

    private async executeTask(message: Message, entry: QueueEntry): Promise<void> {
        const { name: queueName, maxTaskAge = 30 } = entry.definition;
        const ctx: TaskContext = {
            add: <U>(payload: U, targetQueue: string) => this.add(payload, targetQueue),
            message,
            queueName,
        };

        try {
            const generator = this.handler.work(message.payload, ctx);

            for await (const heartbeat of raceWithTimeout(generator, maxTaskAge * 1000)) {
                if (heartbeat === false) {
                    throw new QueueTimeoutError(message.id, `maxTaskAge=${maxTaskAge}s`);
                }

                // Heartbeat: extend visibility
                this.logger.debug(`Ping ${message.id} (ack: ${message.ack})`);
                try {
                    await entry.queue.ping(message.ack);
                } catch {
                    throw new PingError(message.id, message.ack);
                }
            }

            // Success: ack the message
            try {
                const ackedId = await entry.queue.ack(message.ack);
                if (ackedId !== message.id) {
                    throw new WrongAckIdError(message.id, ackedId, message.ack);
                }
            } catch (e) {
                if (e instanceof WrongAckIdError) throw e;
                throw new AckError(message.id, message.ack);
            }

            this.logger.log(`Completed task ${message.id} from ${queueName}`);
        } catch (e) {
            await this.handleTaskError(message, entry, e);
        }
    }

    private async handleTaskError(message: Message, entry: QueueEntry, error: unknown): Promise<void> {
        const decision = this.handler.onError(message.payload, message.tries, error);
        const action = typeof decision === 'object' ? decision.action : decision;
        const retryDelay = typeof decision === 'object' ? Math.max(0, decision.delay) : 0;
        const { name: queueName } = entry.definition;

        switch (action) {
            case ErrorAction.RETRY:
                this.logger.log(`Retrying task ${message.id} from ${queueName}: ${String(error)}`);
                await this.requeueTask(message, entry, retryDelay);
                break;

            case ErrorAction.FAIL:
                this.logger.log(`Failing task ${message.id} from ${queueName}: ${String(error)}`);
                if (this.handler.onFail) {
                    await this.handler.onFail(message.payload, error).catch((e: unknown) => {
                        this.logger.error(`onFail handler threw for ${message.id}: ${String(e)}`);
                    });
                }
                // Ack to remove from queue
                await entry.queue.ack(message.ack).catch(() => {});
                break;

            case ErrorAction.IGNORE:
                this.logger.log(`Ignoring error for task ${message.id} from ${queueName}: ${String(error)}`);
                await entry.queue.ack(message.ack).catch(() => {});
                break;
        }
    }

    private async requeueTask(message: Message, entry: QueueEntry, delaySeconds = 0): Promise<void> {
        // Make the task visible again — immediately, or after `delaySeconds` for a backoff.
        const now = Date.now();
        await entry.queue.collection.updateOne(
            { _id: new ObjectId(message.id) },
            { $set: { visible: new Date(now + delaySeconds * 1000), requeued: new Date(now) }, $unset: { ack: '' } }
        );
        this.wake();
    }

    private getWeightedCount(groupName: string): number {
        return this.runningTasks.filter(t => t.group === groupName).reduce((sum, t) => sum + t.weight, 0);
    }

    private async hasWork(group: QueueGroup): Promise<boolean> {
        for (const entry of group.getQueues()) {
            if ((await entry.queue.size()) > 0) return true;
        }
        return false;
    }

    private async getNextVisibleTime(group: QueueGroup): Promise<Date | null> {
        const times: Date[] = [];

        for (const entry of group.getQueues()) {
            const doc = await entry.queue.collection.findOne<{ visible: Date }>(
                { deleted: null },
                { sort: { visible: 1 }, projection: { visible: 1 } }
            );
            if (doc?.visible instanceof Date) {
                times.push(doc.visible);
            }
        }

        if (times.length === 0) return null;
        return new Date(Math.min(...times.map(d => d.getTime())));
    }

    private async waitForSlot(group: QueueGroup): Promise<void> {
        // Wait for any task in this group to finish
        const groupTasks = this.runningTasks.filter(t => t.group === group.name);
        if (groupTasks.length === 0) return;

        await Promise.race(groupTasks.map(t => t.promise)).catch(() => {});
    }

    private armStopSignal(): void {
        this.stopSignal = new Promise<null>(resolve => {
            this.resolveStop = () => resolve(null);
        });
    }

    private armWakeSignal(): void {
        this.wakeSignal = new Promise<void>(resolve => {
            this.resolveWake = resolve;
        });
    }

    /**
     * End every idle wait of this worker now. A retry makes a task visible again through an
     * UPDATE, which the insert-only change stream never reports — and the loop may already be
     * waiting on a deadline that was computed before the failure.
     */
    private wake(): void {
        const resolve = this.resolveWake;
        this.armWakeSignal();
        resolve();
    }

    /**
     * Acquire a slot from the shared limiter. Resolves with a release function (a no-op when no
     * limiter is configured), or `null` when the worker was stopped while waiting.
     */
    private async acquireSlot(weight: number): Promise<(() => void) | null> {
        if (!this.limiter) return () => {};

        const pending = this.limiter.acquire(weight);
        const release = await Promise.race([pending, this.stopSignal]);
        if (release === null) {
            // Stopped first: hand the slot straight back whenever it is eventually granted.
            void pending.then(late => late());
            return null;
        }
        if (this.stopped) {
            release();
            return null;
        }
        return release;
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise(r => {
        const timer = setTimeout(r, ms);
        if (typeof timer === 'object' && 'unref' in timer) timer.unref();
    });
}
