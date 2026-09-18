# Changelog

## 0.2.1

Fixes to how an idle worker wakes up. No API change except the new `maxIdleWait` group option.

- **Polling mode** waited until the visibility deadline of an in-flight task instead of the polling
  interval, so tasks enqueued while another one ran could wait minutes.
- **Retries** (`ErrorAction.RETRY`, with or without a delay) did not wake a worker whose loop was
  already idle: a requeue is an update, and the change stream only reports inserts. The worker now
  wakes itself.
- **Change-stream mode** could miss a task inserted between the work check and the stream opening.
  The stream is now opened from a cluster time captured before a re-check.
- New **`maxIdleWait`** (default 30 s) bounds every idle wait in both modes.

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
