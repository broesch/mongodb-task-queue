# mongodb-task-queue

A MongoDB-based task queue with worker orchestration, queue groups, concurrency control, change stream support, and heartbeat monitoring.

Use your existing MongoDB as a reliable task queue — no Redis or RabbitMQ needed.

## Features

- **Two layers**: Low-level `MongoQueue` for simple queue operations, and `QueueWorker` for full orchestration
- **Queue groups**: Organize queues into groups with shared concurrency limits
- **Priority ordering**: Higher-priority queues within a group are consumed first
- **Weighted concurrency**: Tasks can have fractional weight for fine-grained concurrency control
- **Heartbeat monitoring**: Async generator pattern — `yield true` to signal progress and extend visibility
- **Change streams**: Real-time task detection with automatic fallback to polling
- **Deduplication**: Built-in hashKey support to prevent duplicate tasks
- **Graceful shutdown**: Wait for in-flight tasks to complete before stopping
- **TypeScript-first**: Full type definitions included
- **Framework-agnostic**: Works with any Node.js project, with optional NestJS module

## Installation

```bash
npm install mongodb-task-queue
```

Requires `mongodb` as a peer dependency:
```bash
npm install mongodb
```

## Quick Start

### Low-level: MongoQueue

For simple produce/consume patterns without worker orchestration:

```typescript
import { MongoClient } from 'mongodb';
import { MongoQueue } from 'mongodb-task-queue';

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();
const db = client.db('myapp');

// Create a queue
const queue = new MongoQueue(db, 'emails', { visibility: 60 });
await queue.createIndexes();

// Add a task
await queue.add({ to: 'user@example.com', subject: 'Welcome!' });

// Consume
const msg = await queue.get();
if (msg) {
  await sendEmail(msg.payload);
  await queue.ack(msg.ack);
}
```

### Full orchestration: QueueWorker

For multi-queue processing with concurrency, priorities, and heartbeats:

```typescript
import { MongoClient } from 'mongodb';
import { QueueWorker, ErrorAction } from 'mongodb-task-queue';

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();
const db = client.db('myapp');

const worker = new QueueWorker({
  db,
  queues: [
    { name: 'high-priority', group: 'tasks', priority: 10, maxTaskAge: 120 },
    { name: 'low-priority', group: 'tasks', priority: 1, maxTaskAge: 300 },
    { name: 'emails', group: 'io', priority: 5, maxTaskAge: 60 },
  ],
  groups: {
    tasks: { concurrency: 3, useChangeStreams: true },
    io: { concurrency: 10, pollingInterval: 1000 },
  },
  handler: {
    async *work(payload, ctx) {
      // Yield true as a heartbeat to extend the visibility window.
      // This prevents the task from timing out during long operations.

      const result = await doExpensiveWork(payload);
      yield true; // heartbeat

      await saveResult(result);
      yield true; // heartbeat

      // Enqueue a follow-up task
      await ctx.add({ type: 'notify', resultId: result.id }, 'emails');
    },

    onError(payload, tries, error) {
      if (tries >= 5) return ErrorAction.FAIL;
      return ErrorAction.RETRY;
    },

    async onFail(payload, error) {
      console.error('Task permanently failed:', payload, error);
      // Clean up, notify, etc.
    },
  },
});

await worker.init();

// Add tasks
await worker.add({ type: 'process', data: '...' }, 'high-priority');

// Start processing (runs until stop() is called)
worker.start();

// Graceful shutdown
process.on('SIGTERM', () => worker.stop());
```

## API

### `MongoQueue<T>`

Low-level queue backed by a single MongoDB collection.

| Method | Description |
|--------|-------------|
| `constructor(db, name, options?)` | Create a queue. Options: `visibility` (seconds, default 30), `ttl` (seconds for deleted messages, default 86400), `extraIndexes` |
| `createIndexes()` | Create required MongoDB indexes |
| `add(payload, options?)` | Add a message. Options: `hashKey` (dedup field), `delay` (seconds) |
| `get(options?)` | Get next visible message. Options: `visibility` (override) |
| `ping(ack, options?)` | Extend visibility for an in-flight message |
| `ack(ack)` | Mark message as processed (soft delete) |
| `remove(filter)` | Batch remove messages matching a MongoDB filter |
| `size()` | Count of messages ready to process |
| `total()` | Total messages (including in-flight and done) |
| `inFlight()` | Count of messages currently being processed |
| `done()` | Count of completed messages |
| `collection` | Direct access to the underlying MongoDB collection |

### `QueueWorker`

Full orchestration engine.

| Method | Description |
|--------|-------------|
| `constructor(options)` | Configure queues, groups, handler, and logger |
| `init()` | Create indexes for all queues |
| `start(groupName?)` | Start processing (all groups or a specific one) |
| `stop(timeoutMs?)` | Graceful shutdown (default 30s timeout) |
| `add(payload, queueName, options?)` | Enqueue a task |
| `getQueue(name)` | Get direct access to a MongoQueue instance |
| `getRunningTasks(groupName?)` | List currently processing tasks |

### `TaskHandler`

Interface your application implements:

```typescript
interface TaskHandler<T = unknown> {
  work(payload: T, ctx: TaskContext): AsyncGenerator<true>;
  onError(payload: T, tries: number, error: unknown): ErrorAction;
  onFail?(payload: T, error: unknown): Promise<void>;
}
```

### `ErrorAction`

```typescript
enum ErrorAction {
  RETRY = 'RETRY',   // Put the task back in the queue
  FAIL = 'FAIL',     // Permanently fail (calls onFail, then acks)
  IGNORE = 'IGNORE', // Silently ack and discard
}
```

## NestJS Integration

```bash
import { MongoQueueModule } from 'mongodb-task-queue/nestjs';

@Module({
  imports: [
    MongoQueueModule.forRootAsync({
      useFactory: (configService: ConfigService) => ({
        db: getDb(configService),
        queues: [/* ... */],
        groups: { /* ... */ },
        handler: myHandler,
      }),
      inject: [ConfigService],
    }),
  ],
})
export class AppModule {}
```

The `MongoQueueService` is exported and automatically calls `init()` on module start and `stop()` on shutdown.

## How It Works

Each queue is a MongoDB collection. Messages have a `visible` timestamp — when you `get()` a message, its visibility is pushed forward so no other consumer can see it. When you `ack()`, the message is soft-deleted. If you don't ack before the visibility timeout, the message becomes visible again for retry.

The `QueueWorker` adds orchestration on top:
- **Groups** share a concurrency limit across multiple queues
- **Priority** determines which queue within a group is consumed first
- **Heartbeats** (`yield true`) call `ping()` under the hood to extend visibility for long-running tasks
- **Change streams** (on replica sets) provide instant notification of new tasks; falls back to polling on standalone instances

## License

MIT
