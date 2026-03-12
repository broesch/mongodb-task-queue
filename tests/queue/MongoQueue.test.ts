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
