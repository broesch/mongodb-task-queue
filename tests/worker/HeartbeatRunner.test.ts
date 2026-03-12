import { describe, it, expect } from 'vitest';
import { raceWithTimeout } from '../../src/worker/HeartbeatRunner';

describe('raceWithTimeout', () => {
    it('should yield heartbeat values from the generator', async () => {
        async function* gen() {
            yield true;
            yield true;
        }

        const values: (true | false)[] = [];
        for await (const v of raceWithTimeout(gen(), 5000)) {
            values.push(v);
        }

        // Two heartbeats + the final return value (undefined, but we get the done signal)
        expect(values.filter(v => v === true).length).toBe(2);
    });

    it('should yield false on timeout', async () => {
        async function* slowGen() {
            await new Promise(r => setTimeout(r, 5000));
            yield true;
        }

        const values: (true | false)[] = [];
        for await (const v of raceWithTimeout(slowGen(), 50)) {
            values.push(v);
        }

        expect(values).toContain(false);
    });

    it('should not timeout if generator yields before deadline', async () => {
        async function* fastGen() {
            yield true;
            await new Promise(r => setTimeout(r, 10));
            yield true;
        }

        const values: (true | false)[] = [];
        for await (const v of raceWithTimeout(fastGen(), 1000)) {
            values.push(v);
        }

        expect(values.filter(v => v === false).length).toBe(0);
        expect(values.filter(v => v === true).length).toBe(2);
    });
});
