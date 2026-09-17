# Changelog

## 0.2.0

All changes are additive and backwards compatible.

- **Retry with delay:** `onError` may return `{ action: ErrorAction.RETRY, delay }` (seconds).
- **`ConcurrencyLimiter`:** a weighted FIFO limiter shared across several `QueueWorker`s via the new
  `limiter` option; a slot is acquired before a task is claimed.
- **`dedupScope: 'active'`:** deduplicate a `hashKey` only against pending and in-flight messages,
  backed by a sparse unique index on the new `dedup` field, atomic under concurrent adds.
  `createIndexes()` creates the index; run it (or `QueueWorker.init()`) after upgrading.
- **`cancel(filter)`** on `MongoQueue` and `QueueWorker`: removes only tasks nobody has claimed.

## 0.1.0

Initial release.
