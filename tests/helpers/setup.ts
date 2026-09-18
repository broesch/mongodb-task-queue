import { MongoMemoryServer, MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoClient, type Db } from 'mongodb';

let mongod: MongoMemoryServer;
let client: MongoClient;
let db: Db;

export async function setup(): Promise<Db> {
    mongod = await MongoMemoryServer.create();
    const uri = mongod.getUri();
    client = new MongoClient(uri);
    await client.connect();
    db = client.db('test-queue');
    return db;
}

export async function teardown(): Promise<void> {
    await client?.close();
    await mongod?.stop();
}

export function getDb(): Db {
    return db;
}

let replSet: MongoMemoryReplSet;
let replClient: MongoClient;

/** A single-node replica set — change streams need one. */
export async function setupReplSet(): Promise<Db> {
    replSet = await MongoMemoryReplSet.create({ replSet: { count: 1 } });
    replClient = new MongoClient(replSet.getUri());
    await replClient.connect();
    return replClient.db('test-queue-rs');
}

export async function teardownReplSet(): Promise<void> {
    await replClient?.close();
    await replSet?.stop();
}
