export class QueueTimeoutError extends Error {
    constructor(id: string, detail?: string) {
        super(`Queue task timed out: ${id}${detail ? ` (${detail})` : ''}`);
        this.name = 'QueueTimeoutError';
    }
}

export class PingError extends Error {
    constructor(id: string, ack: string) {
        super(`Ping failed for message ${id} with ack ${ack}`);
        this.name = 'PingError';
    }
}

export class AckError extends Error {
    constructor(id: string, ack: string) {
        super(`Ack failed for message ${id} with ack ${ack}`);
        this.name = 'AckError';
    }
}

export class WrongAckIdError extends Error {
    constructor(expected: string, actual: string, ack: string) {
        super(`Ack returned id ${actual} but expected ${expected} (ack: ${ack})`);
        this.name = 'WrongAckIdError';
    }
}
