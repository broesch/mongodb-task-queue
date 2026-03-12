import type { Message } from '../queue/types.js';

export interface QueueDefinition {
    name: string;
    group: string;
    priority: number;
    /** Visibility timeout override in seconds */
    maxTaskAge?: number;
    /** Weight for concurrency math (default: 1). A task with weight 0.5 counts as half a slot. */
    weight?: number;
}

export interface GroupOptions {
    /** Max weighted concurrent tasks (default: 1) */
    concurrency?: number;
    /** Fallback polling interval in ms when change streams unavailable (default: 2000) */
    pollingInterval?: number;
    /** Whether to use MongoDB change streams (default: true, falls back to polling on error) */
    useChangeStreams?: boolean;
}

export enum ErrorAction {
    RETRY = 'RETRY',
    FAIL = 'FAIL',
    IGNORE = 'IGNORE',
}

export interface TaskContext {
    /** Enqueue a follow-up task into a named queue */
    add(payload: unknown, queueName: string): Promise<string>;
    /** The raw message from the queue */
    message: Message<unknown>;
    /** Name of the queue this task came from */
    queueName: string;
}

export interface TaskHandler<T = unknown> {
    /** Process a task. Yield `true` as a heartbeat to extend the visibility window. */
    work(payload: T, ctx: TaskContext): AsyncGenerator<true>;
    /** Decide what to do on error. Called with the number of attempts so far. */
    onError(payload: T, tries: number, error: unknown): ErrorAction;
    /** Called when a task is permanently failed (after onError returns FAIL). */
    onFail?(payload: T, error: unknown): Promise<void>;
}

export interface Logger {
    debug(message: string): void;
    log(message: string): void;
    warn(message: string): void;
    error(message: string): void;
}

export interface TaskInfo {
    id: string;
    queueName: string;
    group: string;
    weight: number;
    startedAt: Date;
}

/** Internal resolved config per queue */
export interface ResolvedQueue extends QueueDefinition {
    concurrency: number;
    pollingInterval: number;
    useChangeStreams: boolean;
}
