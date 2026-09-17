import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Db } from 'mongodb';
import { MongoQueue } from '../../src/queue/MongoQueue';
import { setup, teardown } from '../helpers/setup';

let db: Db;

beforeAll(async () => {
    db = await setup();
});

afterAll(async () => {
    await teardown();
});

describe('MongoQueue', () => {
    let queue: MongoQueue<{ id: string; data: string }>;

    beforeEach(async () => {
        const name = `test-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        queue = new MongoQueue(db, name);
        await queue.createIndexes();
    });

    describe('add and get', () => {
        it('should add and retrieve a message', async () => {
            const payload = { id: '1', data: 'hello' };
            const msgId = await queue.add(payload);

            expect(msgId).toBeDefined();
            expect(typeof msgId).toBe('string');

            const msg = await queue.get();
            expect(msg).toBeDefined();
            expect(msg!.payload).toEqual(payload);
            expect(msg!.tries).toBe(1);
            expect(msg!.id).toBe(msgId);
        });

        it('should return undefined when queue is empty', async () => {
            const msg = await queue.get();
            expect(msg).toBeUndefined();
        });

        it('should process messages in FIFO order', async () => {
            await queue.add({ id: '1', data: 'first' });
            await queue.add({ id: '2', data: 'second' });
            await queue.add({ id: '3', data: 'third' });

            const msg1 = await queue.get();
            expect(msg1!.payload.data).toBe('first');
            await queue.ack(msg1!.ack);

            const msg2 = await queue.get();
            expect(msg2!.payload.data).toBe('second');
            await queue.ack(msg2!.ack);

            const msg3 = await queue.get();
            expect(msg3!.payload.data).toBe('third');
        });
    });

    describe('visibility', () => {
        it('should hide messages during visibility timeout', async () => {
            const shortQueue = new MongoQueue(db, `vis-${Date.now()}`, { visibility: 1 });
            await shortQueue.createIndexes();

            await shortQueue.add({ id: '1', data: 'test' });
            const msg = await shortQueue.get();
            expect(msg).toBeDefined();

            // Message should be invisible
            const msg2 = await shortQueue.get();
            expect(msg2).toBeUndefined();

            // Wait for visibility to expire
            await new Promise(r => setTimeout(r, 1200));

            // Message should be visible again (not acked)
            const msg3 = await shortQueue.get();
            expect(msg3).toBeDefined();
            expect(msg3!.payload.data).toBe('test');
            expect(msg3!.tries).toBe(2);
        });
    });

    describe('ack', () => {
        it('should permanently remove a message on ack', async () => {
            await queue.add({ id: '1', data: 'test' });
            const msg = await queue.get();
            const ackedId = await queue.ack(msg!.ack);

            expect(ackedId).toBe(msg!.id);

            // Should not be retrievable
            const msg2 = await queue.get();
            expect(msg2).toBeUndefined();
        });

        it('should throw on invalid ack', async () => {
            await expect(queue.ack('nonexistent')).rejects.toThrow('unidentified ack');
        });
    });

    describe('ping', () => {
        it('should extend visibility timeout', async () => {
            const shortQueue = new MongoQueue(db, `ping-${Date.now()}`, { visibility: 1 });
            await shortQueue.createIndexes();

            await shortQueue.add({ id: '1', data: 'test' });
            const msg = await shortQueue.get();

            // Ping to extend visibility
            const pingedId = await shortQueue.ping(msg!.ack, { visibility: 5 });
            expect(pingedId).toBe(msg!.id);

            // Wait past original visibility
            await new Promise(r => setTimeout(r, 1200));

            // Should still be invisible (ping extended it)
            const msg2 = await shortQueue.get();
            expect(msg2).toBeUndefined();
        });

        it('should throw on invalid ack', async () => {
            await expect(queue.ping('nonexistent')).rejects.toThrow('unidentified ack');
        });
    });

    describe('deduplication with hashKey', () => {
        it('should deduplicate messages by hashKey', async () => {
            const id1 = await queue.add({ id: 'dup', data: 'first' }, { hashKey: 'id' });
            const id2 = await queue.add({ id: 'dup', data: 'second' }, { hashKey: 'id' });

            // Same message, should return same id
            expect(id1).toBe(id2);

            // Only one message in queue
            const size = await queue.size();
            expect(size).toBe(1);

            const msg = await queue.get();
            expect(msg!.payload.data).toBe('first'); // original payload preserved
            expect(msg!.occurrences).toBe(2);
        });

        it('should still deduplicate against acked messages by default (scope "all")', async () => {
            const id1 = await queue.add({ id: 'dup', data: 'first' }, { hashKey: 'id' });
            const msg = await queue.get();
            await queue.ack(msg!.ack);

            const id2 = await queue.add({ id: 'dup', data: 'second' }, { hashKey: 'id' });
            expect(id2).toBe(id1);
            expect(await queue.size()).toBe(0);
        });

        it('should deduplicate only against active messages with dedupScope "active"', async () => {
            const opts = { hashKey: 'id', dedupScope: 'active' } as const;

            const id1 = await queue.add({ id: 'dup', data: 'first' }, opts);
            const id2 = await queue.add({ id: 'dup', data: 'second' }, opts);
            expect(id2).toBe(id1); // pending → deduplicated
            expect(await queue.size()).toBe(1);

            const msg = await queue.get();
            const id3 = await queue.add({ id: 'dup', data: 'third' }, opts);
            expect(id3).toBe(id1); // in flight → still deduplicated

            await queue.ack(msg!.ack);

            const id4 = await queue.add({ id: 'dup', data: 'fourth' }, opts);
            expect(id4).not.toBe(id1); // acked → a new task is accepted
            expect(await queue.size()).toBe(1);
            expect((await queue.get())!.payload.data).toBe('fourth');
        });

        it('should insert exactly one message for concurrent active-scope adds', async () => {
            const opts = { hashKey: 'id', dedupScope: 'active' } as const;
            const ids = await Promise.all(
                Array.from({ length: 10 }, (_, i) => queue.add({ id: 'race', data: `n${i}` }, opts))
            );

            expect(new Set(ids).size).toBe(1);
            expect(await queue.total()).toBe(1);
        });

        it('should reject an active-scope add whose hashKey value is missing', async () => {
            await expect(
                queue.add({ data: 'no id' } as unknown as { id: string; data: string }, {
                    hashKey: 'id',
                    dedupScope: 'active',
                })
            ).rejects.toThrow(/hashKey/);
        });
    });

    describe('delay', () => {
        it('should delay message visibility', async () => {
            await queue.add({ id: '1', data: 'delayed' }, { delay: 1 });

            // Should not be visible yet
            const msg1 = await queue.get();
            expect(msg1).toBeUndefined();

            // Wait for delay
            await new Promise(r => setTimeout(r, 1100));

            const msg2 = await queue.get();
            expect(msg2).toBeDefined();
            expect(msg2!.payload.data).toBe('delayed');
        });
    });

    describe('stats', () => {
        it('should report correct stats', async () => {
            await queue.add({ id: '1', data: 'a' });
            await queue.add({ id: '2', data: 'b' });
            await queue.add({ id: '3', data: 'c' });

            expect(await queue.total()).toBe(3);
            expect(await queue.size()).toBe(3);
            expect(await queue.inFlight()).toBe(0);
            expect(await queue.done()).toBe(0);

            // Get one (in-flight)
            const msg = await queue.get();
            expect(await queue.size()).toBe(2);
            expect(await queue.inFlight()).toBe(1);

            // Ack it (done)
            await queue.ack(msg!.ack);
            expect(await queue.done()).toBe(1);
            expect(await queue.inFlight()).toBe(0);
        });
    });

    describe('remove', () => {
        it('should remove messages by filter', async () => {
            await queue.add({ id: '1', data: 'keep' });
            await queue.add({ id: '2', data: 'remove' });
            await queue.add({ id: '3', data: 'remove' });

            const deleted = await queue.remove({ 'payload.data': 'remove' });
            expect(deleted).toBe(2);
            expect(await queue.total()).toBe(1);
        });

        it('should return 0 when no messages match the filter', async () => {
            await queue.add({ id: '1', data: 'hello' });
            const deleted = await queue.remove({ 'payload.data': 'nonexistent' });
            expect(deleted).toBe(0);
            expect(await queue.total()).toBe(1);
        });
    });

    describe('cancel', () => {
        it('should cancel a pending message', async () => {
            await queue.add({ id: '1', data: 'x' });
            expect(await queue.cancel({ 'payload.id': '1' })).toBe(1);
            expect(await queue.total()).toBe(0);
        });

        it('should cancel a delayed message', async () => {
            await queue.add({ id: '1', data: 'x' }, { delay: 60 });
            expect(await queue.cancel({ 'payload.id': '1' })).toBe(1);
        });

        it('should NOT cancel a message a consumer has claimed', async () => {
            await queue.add({ id: '1', data: 'x' });
            const msg = await queue.get();

            expect(await queue.cancel({ 'payload.id': '1' })).toBe(0);
            await expect(queue.ack(msg!.ack)).resolves.toBe(msg!.id); // still ackable
        });

        it('should cancel a claimed message whose visibility has expired', async () => {
            const shortQueue = new MongoQueue<{ id: string; data: string }>(db, `cancel-${Date.now()}`, {
                visibility: 1,
            });
            await shortQueue.createIndexes();
            await shortQueue.add({ id: '1', data: 'x' });
            await shortQueue.get();
            await new Promise(resolve => setTimeout(resolve, 1100));

            expect(await shortQueue.cancel({ 'payload.id': '1' })).toBe(1);
        });

        it('should NOT remove acknowledged messages', async () => {
            await queue.add({ id: '1', data: 'x' });
            const msg = await queue.get();
            await queue.ack(msg!.ack);

            expect(await queue.cancel({ 'payload.id': '1' })).toBe(0);
            expect(await queue.done()).toBe(1);
        });

        it('should only touch messages matching the filter', async () => {
            await queue.add({ id: '1', data: 'x' });
            await queue.add({ id: '2', data: 'y' });
            expect(await queue.cancel({ 'payload.id': '2' })).toBe(1);
            expect(await queue.size()).toBe(1);
        });
    });

    describe('constructor validation', () => {
        it('should throw when no db is provided', () => {
            expect(() => new MongoQueue(null as any, 'test')).toThrow('Please provide a mongodb Db instance');
        });

        it('should throw when no name is provided', () => {
            expect(() => new MongoQueue(db, '')).toThrow('Please provide a queue name');
        });
    });

    describe('concurrent access', () => {
        it('should give each concurrent consumer a different message', async () => {
            await queue.add({ id: '1', data: 'a' });
            await queue.add({ id: '2', data: 'b' });

            const [msg1, msg2] = await Promise.all([queue.get(), queue.get()]);

            expect(msg1).toBeDefined();
            expect(msg2).toBeDefined();
            expect(msg1!.id).not.toBe(msg2!.id);
        });

        it('should not deliver the same message to two concurrent consumers', async () => {
            await queue.add({ id: '1', data: 'only-one' });

            const [msg1, msg2] = await Promise.all([queue.get(), queue.get()]);

            const received = [msg1, msg2].filter(Boolean);
            expect(received).toHaveLength(1);
        });
    });

    describe('ping edge cases', () => {
        it('should throw when pinging an expired visibility window', async () => {
            const shortQueue = new MongoQueue(db, `ping-exp-${Date.now()}`, { visibility: 1 });
            await shortQueue.createIndexes();

            await shortQueue.add({ id: '1', data: 'test' });
            const msg = await shortQueue.get();

            // Wait for visibility to expire
            await new Promise(r => setTimeout(r, 1200));

            await expect(shortQueue.ping(msg!.ack)).rejects.toThrow('unidentified ack');
        });
    });

    describe('ack edge cases', () => {
        it('should throw when acking an expired visibility window', async () => {
            const shortQueue = new MongoQueue(db, `ack-exp-${Date.now()}`, { visibility: 1 });
            await shortQueue.createIndexes();

            await shortQueue.add({ id: '1', data: 'test' });
            const msg = await shortQueue.get();

            await new Promise(r => setTimeout(r, 1200));

            await expect(shortQueue.ack(msg!.ack)).rejects.toThrow('unidentified ack');
        });
    });

    describe('extra indexes', () => {
        it('should create user-defined extra indexes without error', async () => {
            const indexedQueue = new MongoQueue(db, `extra-idx-${Date.now()}`, {
                extraIndexes: [{ key: { 'payload.id': 1 } }],
            });
            await expect(indexedQueue.createIndexes()).resolves.toBeUndefined();
        });
    });
});
