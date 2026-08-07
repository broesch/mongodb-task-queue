import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const execFileAsync = promisify(execFile);

const repoRoot = resolve(__dirname, '..');
const distDir = join(repoRoot, 'dist');

let scratchDir: string;

beforeAll(async () => {
    // CI runs tests before build, so build dist/ ourselves if it is missing.
    if (!existsSync(join(distDir, 'index.cjs'))) {
        await execFileAsync('npx', ['tsup'], { cwd: repoRoot });
    }
    scratchDir = await mkdtemp(join(tmpdir(), 'mongo-queue-smoke-'));
}, 120000);

afterAll(async () => {
    await rm(scratchDir, { recursive: true, force: true });
});

describe('build artifacts', () => {
    // The root package.json sets "type": "module", so any CJS output accidentally
    // emitted as .js would be parsed as ESM by Node and crash on require() with
    // "ReferenceError: module is not defined in ES module scope". `node -e` decides
    // its module type independently of package.json and masks the bug, so we spawn
    // a real .cjs file for a genuine CommonJS consumer context.
    it.each(['index.cjs', 'nestjs.cjs'])('require() of dist/%s works from a real CJS file', async artifact => {
        const consumer = join(scratchDir, `require-${artifact}`);
        await writeFile(
            consumer,
            `const mod = require(${JSON.stringify(join(distDir, artifact))});\n` +
                `if (Object.keys(mod).length === 0) throw new Error('no exports');\n` +
                `console.log(JSON.stringify(Object.keys(mod)));\n`
        );
        const { stdout } = await execFileAsync(process.execPath, [consumer]);
        expect(JSON.parse(stdout).length).toBeGreaterThan(0);
    });

    it.each(['index.js', 'nestjs.js'])('dynamic import() of ESM dist/%s works', async artifact => {
        const mod = await import(join(distDir, artifact));
        expect(Object.keys(mod).length).toBeGreaterThan(0);
    });

    it('every exports-map target exists in dist/', async () => {
        const pkg = JSON.parse(await readFile(join(repoRoot, 'package.json'), 'utf8'));
        const targets: string[] = [];
        const collect = (value: unknown): void => {
            if (typeof value === 'string') targets.push(value);
            else if (value && typeof value === 'object') Object.values(value).forEach(collect);
        };
        collect(pkg.exports);
        expect(targets.length).toBeGreaterThan(0);
        for (const target of targets) {
            expect(existsSync(join(repoRoot, target)), `${target} missing`).toBe(true);
        }
    });
});

describe('package.json metadata', () => {
    it('peerDependenciesMeta entries are declared in peerDependencies', async () => {
        const pkg = JSON.parse(await readFile(join(repoRoot, 'package.json'), 'utf8'));
        for (const name of Object.keys(pkg.peerDependenciesMeta ?? {})) {
            expect(pkg.peerDependencies, `${name} marked optional but not a peer dependency`).toHaveProperty(name);
        }
    });
});
