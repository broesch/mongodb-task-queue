/**
 * Races an async generator (the task's work function) against a timeout.
 *
 * Each time the generator yields (heartbeat), the timeout resets.
 * If the timeout fires before the next yield, `false` is yielded to signal timeout.
 * On normal completion, the generator's return value is yielded then the runner returns.
 */
export async function* raceWithTimeout<T>(generator: AsyncGenerator<T>, timeoutMs: number): AsyncGenerator<T | false> {
    while (true) {
        const timeout = new Promise<{ done: true; value: false }>(resolve => {
            const timer = setTimeout(() => resolve({ done: true, value: false }), timeoutMs);
            // Don't keep the process alive just for this timer
            if (typeof timer === 'object' && 'unref' in timer) timer.unref();
        });

        const result = await Promise.race([generator.next(), timeout]);

        if (result.done) {
            // Either the generator finished or the timeout fired
            yield result.value;
            return;
        }

        // Generator yielded a heartbeat value
        yield result.value;
    }
}
