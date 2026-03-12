import { MongoMemoryServer } from 'mongodb-memory-server';
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
