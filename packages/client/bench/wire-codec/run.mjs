import { spawnSync } from 'node:child_process';
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { deflateRawSync } from 'node:zlib';
import { chromium } from '@playwright/test';
import assert from 'node:assert/strict';
import { fixture } from './fixture.mjs';
import { measure, stats, BinaryReplicaCodec } from './measure.mjs';

const directory = fileURLToPath(new URL('.', import.meta.url));
const root = fileURLToPath(new URL('../../../../', import.meta.url));
await mkdir(`${directory}output`, { recursive: true });
const groups = fixture();
const all = Object.values(groups).flat();
const executable = `${root}rust/target/release/examples/wire-codec`;
const rust = spawnSync(executable, [], { input: JSON.stringify(all), maxBuffer: 256 * 1024 * 1024, encoding: 'utf8' });
if (rust.status !== 0) throw new Error(rust.stderr);
const server = JSON.parse(rust.stdout);
const binary = server.frames.map(frame => Uint8Array.from(frame));
const decoder = new BinaryReplicaCodec();
binary.forEach((bytes, i) => assert.deepEqual(decoder.decode(bytes), all[i]));
const encoder = new BinaryReplicaCodec();
const edges = [{ type: 'replica.transaction', cursor: { epoch: 'é😀', revision: Number.MAX_SAFE_INTEGER }, originCommandId: 'origin', changes: [
  { entity: 'tasks', id: '1', operation: 'update', oldValue: null, newValue: { id: '1', nested: { empty: [], unicode: '你好', nullable: null } }, changedColumns: [] },
] }];
const tsFrames = [...all, ...edges].map(frame => Array.from(encoder.encode(frame)));
const inverse = spawnSync(executable, ['decode'], { input: JSON.stringify(tsFrames), maxBuffer: 256 * 1024 * 1024, encoding: 'utf8' });
if (inverse.status !== 0) throw new Error(inverse.stderr);
assert.deepEqual(JSON.parse(inverse.stdout), [...all, ...edges]);
const data = { json: all.map(frame => JSON.stringify(frame)), binary: binary.map(bytes => Buffer.from(bytes).toString('base64')), lengths: Object.fromEntries(Object.entries(groups).map(([k, v]) => [k, v.length])) };
await writeFile(`${directory}output/input.json`, JSON.stringify(data));
const browserServer = createServer(async (req, res) => {
  try {
    const path = req.url === '/bundle.js' ? `${directory}output/bundle.js` : req.url === '/input.json' ? `${directory}output/input.json` : undefined;
    res.setHeader('content-type', req.url === '/bundle.js' ? 'text/javascript' : req.url === '/input.json' ? 'application/json' : 'text/html');
    res.end(path ? await readFile(path) : '<script src="/bundle.js"></script>');
  } catch { res.statusCode = 500; res.end(); }
});
await new Promise(resolve => browserServer.listen(0, '127.0.0.1', resolve));
let browser;
try {
  browser = await chromium.launch({ headless: true, args: ['--enable-precise-memory-info', '--js-flags=--expose-gc'] });
  const page = await browser.newPage();
  await page.goto(`http://127.0.0.1:${browserServer.address().port}`);
  const chrome = await page.evaluate(async () => {
    const data = await (await fetch('/input.json')).json();
    const result = {};
    let offset = 0;
    for (const [name, count] of Object.entries(data.lengths)) {
      const frames = data.json.slice(offset, offset + count);
      const codec = new window.wire.BinaryReplicaCodec();
      const fresh = frames.map(text => codec.encode(JSON.parse(text)));
      result[name] = window.wire.measure(frames, fresh, () => performance.memory.usedJSHeapSize, window.gc);
      offset += count;
    }
    return result;
  });
  const node = {};
  const bytes = {};
  let offset = 0;
  for (const [name, frames] of Object.entries(groups)) {
    const json = frames.map(frame => JSON.stringify(frame));
    const codec = new BinaryReplicaCodec();
    const fresh = frames.map(frame => codec.encode(frame));
    node[name] = measure(json, fresh, () => process.memoryUsage().heapUsed, global.gc);
    bytes[name] = { json: json.reduce((sum, text) => sum + Buffer.byteLength(text), 0), binary: binary.slice(offset, offset + frames.length).reduce((sum, v) => sum + v.length, 0),
      hypotheticalDeflateJson: json.reduce((sum, text) => sum + deflateRawSync(text).length, 0), hypotheticalDeflateBinary: binary.slice(offset, offset + frames.length).reduce((sum, v) => sum + deflateRawSync(v).length, 0) };
    offset += frames.length;
  }
  const results = { date: new Date().toISOString(), nodeVersion: process.version, chromiumVersion: browser.version(), permessageDeflate: false,
    bytes, node, chromium: chrome, rustWholeFixture: { jsonMs: stats(server.jsonMs), binaryMs: stats(server.binaryMs), jsonPeakBytes: server.jsonPeakBytes, binaryPeakBytes: server.binaryPeakBytes },
    peakAllocationNote: 'Peak sampled live JS heap delta after whole decode, includes unreclaimed temporaries; not total allocations or OS RSS. Inputs already allocated. Rust peak tracked live allocation delta includes output buffers.', crossLanguageRoundTrips: `passed both directions, all ${all.length} frames` };
  await writeFile(`${directory}results.json`, JSON.stringify(results, null, 2) + '\n');
  console.log(JSON.stringify(results, null, 2));
} finally { await browser?.close(); await new Promise(resolve => browserServer.close(resolve)); }
