// A separate native-IDB gate: no runtime, UI, workers or shared browser profile.
import { chromium } from '@playwright/test';
import { createServer } from 'node:http';
import { globSync } from 'node:fs';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { cpus, release } from 'node:os';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { fixture, seed, artifactHash } from './fixture.mjs';

const directory = dirname(fileURLToPath(import.meta.url)), root = resolve(directory, '../../../..');
// Use the bundler already installed by the workspace, without installing or
// launching another development build while samples are being collected.
const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
if (!bundler) throw new Error('Install the frozen workspace dependencies first');
const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
const temporary = await mkdtemp(join(root, '.sync-engine-bench-'));
let browser, server;
try {
  const bundle = join(temporary, 'benchmark.js');
  await build({ entryPoints: [join(directory, 'chromium-entry.mjs')], bundle: true, minify: true, platform: 'browser', format: 'esm', outfile: bundle });
  const javascript = await readFile(bundle);
  let payload;
  server = createServer((request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    if (request.url === '/benchmark.js') { response.setHeader('Content-Type', 'text/javascript'); response.end(javascript); }
    else if (request.url === '/fixture.json') { response.setHeader('Content-Type', 'application/json'); response.end(payload); }
    else { response.setHeader('Content-Type', 'text/html'); response.end('<!doctype html><script type="module" src="/benchmark.js"></script>'); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true });
  // Distinct BrowserContexts keep the legacy adapter's large serialization
  // allocations and garbage collection out of the candidate's measured heap.
  const pages = [];
  for (let variant = 0; variant < 2; variant++) {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    await page.waitForFunction(() => typeof window.prepareStorageCase === 'function');
    pages.push(page);
  }
  const results = { environment: { browser: await browser.version(), node: process.version, cpu: cpus()[0].model, os: release(),
    commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(), seed, artifactHash,
    adapter: 'native Chromium IndexedDB, separate isolated profiles/heaps, serial storage only', warmups: 5, iterations: 20 }, cases: [] };
  for (const count of [10_000, 50_000]) {
    const data = fixture(count); payload = JSON.stringify(data);
    console.log(`Native IndexedDB seeding ${count} rows`);
    for (const variant of [0, 1]) await pages[variant].evaluate(async variant => window.prepareStorageCase(await (await fetch('/fixture.json')).json(), variant), variant);
    const samples = [[], []];
    for (let index = 0; index < 25; index++) {
      for (const variant of index % 2 ? [1, 0] : [0, 1]) {
        const elapsed = await pages[variant].evaluate(index => window.applyStorageSample(index), index);
        if (index >= 5) samples[variant].push(elapsed);
      }
    }
    const summary = values => { const sorted = [...values].sort((a, b) => a - b); return { median: sorted[10], p95: sorted[18], samples: values }; };
    const result = { metric: `single-row-${count}`, encodedBytes: data.encodedBytes, legacy: summary(samples[0]), engine: summary(samples[1]) };
    results.cases.push(result);
    console.log(`${result.metric}: legacy p95 ${result.legacy.p95.toFixed(3)} ms; engine p95 ${result.engine.p95.toFixed(3)} ms`);
  }
  for (const page of pages) await page.evaluate(() => window.closeStorageCase());
  await writeFile(process.argv[2] ?? join(directory, 'chromium-results.json'), JSON.stringify(results, null, 2) + '\n');
} finally {
  await browser?.close();
  await new Promise(resolve => server ? server.close(resolve) : resolve());
  await rm(temporary, { recursive: true, force: true });
}
