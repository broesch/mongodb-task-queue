import type { DynamicModule, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { QueueWorker, type QueueWorkerOptions } from '../worker/QueueWorker.js';

const QUEUE_WORKER_TOKEN = 'MONGO_QUEUE_WORKER';

export class MongoQueueModule {
    static forRoot(options: QueueWorkerOptions): DynamicModule {
        return {
            module: MongoQueueModule,
            providers: [
                {
                    provide: QUEUE_WORKER_TOKEN,
                    useFactory: () => new QueueWorker(options),
                },
                {
                    provide: MongoQueueService,
                    useFactory: (worker: QueueWorker) => new MongoQueueService(worker),
                    inject: [QUEUE_WORKER_TOKEN],
                },
            ],
            exports: [MongoQueueService, QUEUE_WORKER_TOKEN],
        };
    }

    static forRootAsync(options: {
        useFactory: (...args: any[]) => QueueWorkerOptions | Promise<QueueWorkerOptions>;
        inject?: any[];
    }): DynamicModule {
        return {
            module: MongoQueueModule,
            providers: [
                {
                    provide: QUEUE_WORKER_TOKEN,
                    useFactory: async (...args: any[]) => {
                        const config = await options.useFactory(...args);
                        return new QueueWorker(config);
                    },
                    inject: options.inject ?? [],
                },
                {
                    provide: MongoQueueService,
                    useFactory: (worker: QueueWorker) => new MongoQueueService(worker),
                    inject: [QUEUE_WORKER_TOKEN],
                },
            ],
            exports: [MongoQueueService, QUEUE_WORKER_TOKEN],
        };
    }
}

export class MongoQueueService implements OnModuleInit, OnModuleDestroy {
    constructor(public readonly worker: QueueWorker) {}

    async onModuleInit(): Promise<void> {
        await this.worker.init();
    }

    async onModuleDestroy(): Promise<void> {
        await this.worker.stop();
    }
}

export { QUEUE_WORKER_TOKEN };
