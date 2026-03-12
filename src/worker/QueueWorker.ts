import type { Db } from 'mongodb';
import { ObjectId } from 'mongodb';

import { MongoQueue } from '../queue/MongoQueue.js';
import type { Message } from '../queue/types.js';
import { QueueTimeoutError, PingError, AckError, WrongAckIdError } from '../errors/index.js';
import { raceWithTimeout } from './HeartbeatRunner.js';
import { ChangeStreamWatcher } from './ChangeStreamWatcher.js';
import { QueueGroup, type QueueEntry } from './QueueGroup.js';
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

    constructor(options: QueueWorkerOptions) {
        this.db = options.db;
        this.handler = options.handler;
        this.logger = options.logger ?? defaultLogger;
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
        this.stopped = false;

        const groupNames = groupName ? [groupName] : Array.from(this.groups.keys());
        const promises = groupNames.map(name => this.runGroup(name));
        await Promise.all(promises);
    }

    /** Graceful shutdown: stop accepting new tasks, wait for in-flight tasks to complete. */
    async stop(timeoutMs: number = 30000): Promise<void> {
        this.stopped = true;
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
        options?: { hashKey?: keyof U; delay?: number }
    ): Promise<string> {
        const queue = this.queues.get(queueName) as MongoQueue<U> | undefined;
        if (!queue) throw new Error(`Unknown queue: ${queueName}`);
        return queue.add(payload, options);
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
                    'insert',
                    nextVisible,
                    group.pollingInterval
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

            // Try to get a message directly (fixes race condition: no separate size() check)
            const message = await entry.queue.get();
            if (!message) continue;

            this.logger.log(`Got task ${message.id} from ${entry.definition.name}`);

            // Start processing in background
            const task = this.createRunningTask(message, entry);
            this.runningTasks.push(task);

            // Run task in background (no await)
            void task.promise.finally(() => {
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
        const action = this.handler.onError(message.payload, message.tries, error);
        const { name: queueName } = entry.definition;

        switch (action) {
            case ErrorAction.RETRY:
                this.logger.log(`Retrying task ${message.id} from ${queueName}: ${String(error)}`);
                await this.requeueTask(message, entry);
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

    private async requeueTask(message: Message, entry: QueueEntry): Promise<void> {
        // Reset visibility so the task becomes immediately available again
        await entry.queue.collection.updateOne(
            { _id: new ObjectId(message.id) },
            { $set: { visible: new Date(), requeued: new Date() }, $unset: { ack: '' } }
        );
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
}

function sleep(ms: number): Promise<void> {
    return new Promise(r => {
        const timer = setTimeout(r, ms);
        if (typeof timer === 'object' && 'unref' in timer) timer.unref();
    });
}
