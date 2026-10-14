import { chromium, firefox, webkit } from '@playwright/test';
import { createServer } from 'node:http';
import { globSync } from 'node:fs';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { cpus, release, totalmem, loadavg, tmpdir, getPriority } from 'node:os';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { fixture, seed, artifactHash, budget } from './fixture.mjs';
import { summary, comparison } from './statistics.mjs';
import { evaluate } from './gates.mjs';

const directory = dirname(fileURLToPath(import.meta.url)), root = resolve(directory, '../../../..');
const options = { browsers: 'chromium,firefox,webkit', cases: 'single-row,startup-working-set,delete-100-windows,outbox', samples: 20, warmups: 5, cap: 60, cv: .15, load: .5, retries: 10, output: join(directory, 'results.json') };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in options) || process.argv[i + 1] === undefined) throw new Error(`Unknown/missing option ${process.argv[i]}`);
  options[key] = typeof options[key] === 'number' ? Number(process.argv[++i]) : process.argv[++i];
}
for (const key of ['samples', 'warmups', 'cap', 'retries']) if (!Number.isInteger(options[key]) || options[key] < (key === 'samples' ? 5 : 0)) throw new Error(`Invalid ${key}`);
if (options.cap < options.samples || options.cap > 1000 || !Number.isFinite(options.cv) || !Number.isFinite(options.load) || options.cv <= 0 || options.load <= 0 || options.cap + options.warmups > 10000) throw new Error('Invalid cap/CV/load');
const engines = { chromium, firefox, webkit }, names = options.browsers.split(','), kinds = options.cases.split(',');
if (names.some(name => !engines[name]) || kinds.some(kind => !['single-row', 'startup-working-set', 'delete-100-windows', 'outbox'].includes(kind))) throw new Error('Unknown browser/case');
const machine = () => ({ loadavg: loadavg(), cpuCount: cpus().length, timestamp: new Date().toISOString() });
const quiet = reading => reading.loadavg[0] <= reading.cpuCount * options.load;
let interrupted = false;
const checkInterrupted = () => { if (interrupted) throw new Error('Benchmark interrupted'); };
async function waitForQuiet() {
  checkInterrupted();
  for (let retry = 0; retry < options.retries && !quiet(machine()); retry++) {
    console.warn(`BUSY: load ${loadavg()[0]} > ${cpus().length * options.load}; retry in 30s`);
    await new Promise(resolve => setTimeout(resolve, 30000));
    checkInterrupted();
  }
}
const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
if (!bundler) throw new Error('Install frozen workspace dependencies first');
const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
const temporary = await mkdtemp(join(tmpdir(), 'sync-engine-'));
const results = { kind: 'native-browser-storage-v1', complete: false, environment: { node: process.version, priority: getPriority(), os: release(), cpu: cpus()[0].model, ram: totalmem(),
  commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(), dirty: Boolean(execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim()),
  seed, artifactHash, budget, options, scope: 'storage only; no runtime, network, worker, DOM or subscribers; WebKit approximates Safari' }, browsers: [] };
let server, payload, contexts = [];
const interrupt = () => { interrupted = true; for (const context of contexts) void context?.close().catch(() => {}); };
process.on('SIGINT', interrupt); process.on('SIGTERM', interrupt);
try {
  await build({ entryPoints: [join(directory, 'chromium-entry.mjs')], bundle: true, minify: true, platform: 'browser', format: 'esm', outfile: join(temporary, 'benchmark.js') });
  const javascript = await readFile(join(temporary, 'benchmark.js'));
  server = createServer((request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    if (request.url === '/benchmark.js') { response.setHeader('Content-Type', 'text/javascript'); response.end(javascript); }
    else if (request.url === '/fixture.json') { response.setHeader('Content-Type', 'application/json'); response.end(payload); }
    else { response.setHeader('Content-Type', 'text/html'); response.end('<!doctype html><script type="module" src="/benchmark.js"></script>'); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  await writeFile(options.output, JSON.stringify(results, null, 2) + '\n');
  for (const name of names) {
    await waitForQuiet();
    checkInterrupted();
    const before = machine(), browser = { name, before, quiescent: quiet(before), cases: [] };
    results.browsers.push(browser);
    if (!browser.quiescent) console.warn(`NOT QUIESCENT: starting ${name} after ${options.retries} load retries`);
    const observe = () => {
      const reading = machine(); browser.loadReadings ??= []; browser.loadReadings.push(reading);
      if (!quiet(reading)) { browser.quiescent = false; console.warn(`NOT QUIESCENT: ${name} load ${reading.loadavg[0]}`); }
    };
    for (const kind of kinds) for (const count of kind === 'outbox' ? [0] : [10000, 50000]) {
      const profiles = await Promise.all([0, 1].map(() => mkdtemp(join(temporary, `${name}-`))));
      const pages = [];
      const launch = async variant => {
        checkInterrupted();
        contexts[variant] = await engines[name].launchPersistentContext(profiles[variant], { headless: true });
        const page = contexts[variant].pages()[0];
        await page.goto(url); await page.waitForFunction(() => typeof window.seedCase === 'function');
        browser.version ??= contexts[variant].browser()?.version();
        browser.userAgent ??= await page.evaluate(() => navigator.userAgent);
        pages[variant] = page;
      };
      try {
        if (count) {
          const data = fixture(count); payload = JSON.stringify(data);
          browser.fixtures ??= {}; browser.fixtures[count] = { encodedBytes: data.encodedBytes, relatedTables: 5, visibleIDs: 250, exactIDs: 1000 };
        }
        console.log(`${name} preparing ${kind}${count ? '-' + count : ''}`);
        for (const variant of [0, 1]) {
          await launch(variant);
          if (count) {
            try { await pages[variant].evaluate(args => window.seedCase(args), { variant, kind, count, budget, victims: Math.max(1000, options.cap + options.warmups) }); }
            catch (error) { throw new Error(`${name} ${kind}-${count} variant ${variant} seed failed: ${error}`, { cause: error }); }
          }
        }
        const subcases = kind === 'outbox' ? ['independent'] : [kind];
        for (const subcase of subcases) {
          const samples = [[], []];
          let target = options.samples, round = 0;
          results.inProgress = { browser: name, metric: kind === 'outbox' ? `outbox-500-${subcase}` : `${kind}-${count}`, samples };
          while (round < options.warmups + target) {
            checkInterrupted();
            for (const variant of round % 2 ? [1, 0] : [0, 1]) {
              if (kind === 'startup-working-set') {
                // Close the entire process, then reopen its populated profile.
                // OS page cache is not evicted; there is no live JS/adapter cache.
                await pages[variant].evaluate(() => window.closeCase()); await contexts[variant].close(); contexts[variant] = undefined;
                await launch(variant);
                await pages[variant].evaluate(args => window.openStartup(args), { variant, count, budget });
              } else if (kind === 'outbox') await pages[variant].evaluate(args => window.prepareOutbox(args), { index: round });
              else await pages[variant].evaluate(index => window.prepareSample(index), round);
              const elapsed = await pages[variant].evaluate(({ round, outbox }) => outbox ? window.runOutbox() : window.runSample(round), { round, outbox: kind === 'outbox' });
              if (round >= options.warmups) samples[variant].push(elapsed);
            }
            round++;
            if (round % 5 === 0) {
              observe();
              console.log(`${name} ${results.inProgress.metric} round ${round}/${options.warmups + target}`);
              await writeFile(options.output, JSON.stringify(results, null, 2) + '\n');
            }
            if (round === options.warmups + target && target < options.cap && samples.some(values => summary(values).cv > options.cv)) target = Math.min(options.cap, target + 10);
          }
          observe();
          const legacy = summary(samples[0]), engine = summary(samples[1]);
          const value = { metric: kind === 'outbox' ? `outbox-500-${subcase}` : `${kind}-${count}`, legacy, engine,
            ratio: comparison(samples[1], samples[0]), status: Math.max(legacy.cv, engine.cv) > options.cv ? 'NOISY' : 'OK' };
          if (kind === 'single-row') {
            value.engineRequestProfile = [];
            for (let i = 0; i < 5; i++) value.engineRequestProfile.push(await pages[1].evaluate(index => window.profileSample(index), round + i));
          }
          if (kind === 'startup-working-set') {
            value.startupProfiles = [];
            for (const variant of [0, 1]) {
              await pages[variant].evaluate(() => window.closeCase());
              await contexts[variant].close(); contexts[variant] = undefined;
              await launch(variant);
              await pages[variant].evaluate(args => window.openStartup(args), { variant, count, budget });
              const cdp = name === 'chromium' ? await contexts[variant].newCDPSession(pages[variant]) : undefined;
              if (cdp) { await cdp.send('Profiler.enable'); await cdp.send('HeapProfiler.enable'); await cdp.send('Profiler.start'); await cdp.send('HeapProfiler.startSampling', { samplingInterval: 32768 }); }
              value.startupProfiles.push(await pages[variant].evaluate(() => window.profileSample(0)));
              if (cdp) {
                const cpu = await cdp.send('Profiler.stop'), heap = await cdp.send('HeapProfiler.stopSampling');
                const nodes = new Map(cpu.profile.nodes.map(node => [node.id, node])), times = new Map();
                for (const [index, id] of cpu.profile.samples.entries()) times.set(id, (times.get(id) ?? 0) + cpu.profile.timeDeltas[index]);
                value.startupProfiles[variant].cpuTop = [...times].sort((a, b) => b[1] - a[1]).slice(0, 20).map(([id, us]) => {
                  const frame = nodes.get(id).callFrame;
                  return { function: frame.functionName, ms: us / 1000,
                    source: frame.url.endsWith('/benchmark.js') ? javascript.toString().split('\n')[frame.lineNumber]?.slice(frame.columnNumber, frame.columnNumber + 160) : undefined };
                });
                const allocated = node => node.selfSize + node.children.reduce((sum, child) => sum + allocated(child), 0);
                value.startupProfiles[variant].sampledLiveBytes = allocated(heap.profile.head);

                await writeFile(`${options.output}.${count}.${variant}.cpu.json`, JSON.stringify(cpu));
                await writeFile(`${options.output}.${count}.${variant}.heap.json`, JSON.stringify(heap));
                await cdp.detach();
              }
            }
          }
          browser.cases.push(value);
          delete results.inProgress;
          console.log(`${name} ${value.metric} ${value.status} n=${samples[0].length}: legacy p95=${legacy.p95.toFixed(3)} engine p95=${engine.p95.toFixed(3)}`);
          await writeFile(options.output, JSON.stringify(results, null, 2) + '\n');
        }
      } finally {
        for (const context of contexts) await context?.close(); contexts = [];
        for (const profile of profiles) await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
      }
    }
    browser.after = machine(); browser.quiescent &&= quiet(browser.after);
  }
  results.complete = true;
  results.gates = evaluate(results);
  for (const gate of results.gates) console.log(`${gate.verdict} ${gate.browser} ${gate.gate} CI=${gate.ci?.map(value => value.toFixed(3)).join('..')}`);
  await writeFile(options.output, JSON.stringify(results, null, 2) + '\n');
  if (results.gates.some(gate => gate.verdict === 'FAIL')) process.exitCode = 1;
} catch (error) {
  results.error = String(error);
  await writeFile(options.output, JSON.stringify(results, null, 2) + '\n').catch(() => {});
  throw error;
} finally {
  process.off('SIGINT', interrupt); process.off('SIGTERM', interrupt);
  for (const context of contexts) await context?.close();
  await new Promise(resolve => server ? server.close(resolve) : resolve());
  await rm(temporary, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
