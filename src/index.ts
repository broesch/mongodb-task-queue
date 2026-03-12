// Layer 1: Low-level queue
export { MongoQueue } from './queue/MongoQueue.js';
export type { Message, AddOptions, QueueOptions } from './queue/types.js';

// Layer 2: Worker orchestration
export { QueueWorker } from './worker/QueueWorker.js';
export type { QueueWorkerOptions } from './worker/QueueWorker.js';
export { ErrorAction } from './worker/types.js';
export type { QueueDefinition, GroupOptions, TaskHandler, TaskContext, TaskInfo, Logger } from './worker/types.js';

// Errors
export { QueueTimeoutError, PingError, AckError, WrongAckIdError } from './errors/index.js';

// Utilities
export { raceWithTimeout } from './worker/HeartbeatRunner.js';
