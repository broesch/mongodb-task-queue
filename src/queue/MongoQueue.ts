import crypto from 'node:crypto';
import type { Collection, Db, Document, Filter } from 'mongodb';

import type { AddOptions, Message, QueueOptions } from './types.js';

function randomId(): string {
    return crypto.randomBytes(16).toString('hex');
}

export class MongoQueue<T = unknown> {
    private readonly _db: Db;
    private readonly _name: string;
    private readonly _visibility: number;
    private readonly _ttl: number;
    private readonly _options: QueueOptions;

    get collection(): Collection {
        return this._db.collection(this._name);
    }

    constructor(db: Db, name: string, options: QueueOptions = {}) {
        if (!db) throw new Error('Please provide a mongodb Db instance');
        if (!name) throw new Error('Please provide a queue name');

        this._db = db;
        this._name = name;
        this._visibility = options.visibility ?? 30;
        this._ttl = options.ttl ?? 86400;
        this._options = options;
    }

    async createIndexes(): Promise<void> {
        // Base indexes for queue operations
        await this.collection.createIndex({ deleted: 1, visible: 1 });
        await this.collection.createIndex({ ack: 1 }, { unique: true, sparse: true });

        // Active-scope dedup: `dedup` exists only while a message is pending or in flight.
        await this.collection.createIndex({ dedup: 1 }, { unique: true, sparse: true });

        // TTL index on soft-deleted messages
        if (this._ttl > 0) {
            await this.collection.createIndex({ deleted: 1 }, { expireAfterSeconds: this._ttl });
        }

        // User-provided extra indexes
        if (this._options.extraIndexes) {
            for (const index of this._options.extraIndexes) {
                await this.collection.createIndex(index.key, index);
            }
        }
    }

    async add(payload: T, options?: AddOptions<T>): Promise<string> {
        const hashKey = options?.hashKey;
        const delay = options?.delay ?? 0;
        const now = Date.now();

        const insertFields = {
            createdAt: new Date(now),
            visible: new Date(now + delay * 1000),
            payload,
            tries: 0,
        };

        if (hashKey === undefined) {
            const result = await this.collection.insertOne({
                ...insertFields,
                occurrences: 1,
            });
            return result.insertedId.toHexString();
        }

        if (options?.dedupScope === 'active') {
            return this.addActiveScoped(payload, hashKey, insertFields);
        }

        // Deduplication: upsert based on hashKey
        let filter: Document;
        if (typeof payload === 'object' && payload !== null) {
            filter = { [`payload.${String(hashKey)}`]: (payload as Record<string, unknown>)[hashKey as string] };
        } else {
            filter = { payload: { $eq: hashKey } };
        }

        const message = await this.collection.findOneAndUpdate(
            filter,
            {
                $inc: { occurrences: 1 },
                $set: { updatedAt: new Date() },
                $setOnInsert: insertFields,
            },
            { upsert: true, returnDocument: 'after', includeResultMetadata: true }
        );

        if (!message.value) {
            throw new Error('MongoQueue.add(): failed to add message');
        }

        return message.value._id.toHexString();
    }

    /**
     * Dedup against pending/in-flight messages only. The sparse unique index on `dedup` makes
     * concurrent adds atomic: the loser of an insert race gets E11000 and retries as an update.
     */
    private async addActiveScoped(payload: T, hashKey: keyof T, insertFields: Document): Promise<string> {
        const raw =
            typeof payload === 'object' && payload !== null
                ? (payload as Record<string, unknown>)[hashKey as string]
                : payload;
        if (raw === undefined || raw === null) {
            throw new Error(`MongoQueue.add(): payload has no value for hashKey "${String(hashKey)}"`);
        }
        const dedup = String(raw);

        for (let attempt = 0; ; attempt++) {
            try {
                const message = await this.collection.findOneAndUpdate(
                    { dedup },
                    {
                        $inc: { occurrences: 1 },
                        $set: { updatedAt: new Date() },
                        // `dedup` itself is seeded by the equality filter of the upsert.
                        $setOnInsert: insertFields,
                    },
                    { upsert: true, returnDocument: 'after', includeResultMetadata: true }
                );
                if (!message.value) {
                    throw new Error('MongoQueue.add(): failed to add message');
                }
                return message.value._id.toHexString();
            } catch (e) {
                const duplicateKey = typeof e === 'object' && e !== null && (e as { code?: number }).code === 11000;
                if (!duplicateKey || attempt >= 3) throw e;
            }
        }
    }

    async get(options: { visibility?: number } = {}): Promise<Message<T> | undefined> {
        const visibility = options.visibility ?? this._visibility;
        const now = Date.now();

        const query = {
            deleted: null,
            visible: { $lte: new Date(now) },
        };

        const update = {
            $inc: { tries: 1 },
            $set: {
                updatedAt: new Date(now),
                ack: randomId(),
                visible: new Date(now + visibility * 1000),
            },
        };

        const result = await this.collection.findOneAndUpdate(query, update, {
            sort: { _id: 1 },
            returnDocument: 'after',
            includeResultMetadata: true,
        });

        const message = result.value;
        if (!message) return undefined;

        if (!message.ack || !message.updatedAt) {
            throw new Error('MongoQueue.get(): failed to update message');
        }

        const doc = message as unknown as {
            _id: { toHexString(): string };
            ack: string;
            createdAt: Date;
            updatedAt: Date;
            payload: T;
            tries: number;
            occurrences?: number;
        };

        return {
            id: doc._id.toHexString(),
            ack: doc.ack,
            createdAt: doc.createdAt,
            updatedAt: doc.updatedAt,
            payload: doc.payload,
            tries: doc.tries,
            occurrences: doc.occurrences ?? 1,
        };
    }

    async ping(ack: string, options: { visibility?: number } = {}): Promise<string> {
        const visibility = options.visibility ?? this._visibility;
        const now = Date.now();

        const result = await this.collection.findOneAndUpdate(
            { ack, visible: { $gt: new Date(now) }, deleted: null },
            { $set: { visible: new Date(now + visibility * 1000) } },
            { returnDocument: 'after', includeResultMetadata: true }
        );

        if (!result.value) {
            throw new Error(`MongoQueue.ping(): unidentified ack: ${ack}`);
        }

        return result.value._id.toHexString();
    }

    async ack(ack: string): Promise<string> {
        const now = Date.now();

        const result = await this.collection.findOneAndUpdate(
            { ack, visible: { $gt: new Date(now) }, deleted: null },
            { $set: { deleted: new Date(now) }, $unset: { dedup: '' } },
            { returnDocument: 'after', includeResultMetadata: true }
        );

        if (!result.value) {
            throw new Error(`MongoQueue.ack(): unidentified ack: ${ack}`);
        }

        return result.value._id.toHexString();
    }

    async remove(filter: Filter<Document>): Promise<number> {
        const result = await this.collection.deleteMany(filter);
        return result.deletedCount;
    }

    /**
     * Remove matching messages that nobody is working on: not acknowledged, and either never
     * claimed or claimed with an expired visibility. Unlike `remove()`, this never pulls a message
     * out from under a consumer. Returns the number of messages removed.
     */
    async cancel(filter: Filter<Document>): Promise<number> {
        const result = await this.collection.deleteMany({
            $and: [
                filter,
                { deleted: null },
                { $or: [{ ack: { $exists: false } }, { visible: { $lte: new Date() } }] },
            ],
        });
        return result.deletedCount;
    }

    async size(): Promise<number> {
        return this.collection.countDocuments({
            deleted: null,
            visible: { $lte: new Date() },
        });
    }

    async total(): Promise<number> {
        return this.collection.countDocuments();
    }

    async inFlight(): Promise<number> {
        return this.collection.countDocuments({
            ack: { $exists: true },
            visible: { $gt: new Date() },
            deleted: null,
        });
    }

    async done(): Promise<number> {
        return this.collection.countDocuments({
            deleted: { $exists: true },
        });
    }
}
