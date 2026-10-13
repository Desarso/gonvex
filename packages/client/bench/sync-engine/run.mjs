import 'fake-indexeddb/auto';
import { performance } from 'node:perf_hooks';
import { cpus, totalmem, release } from 'node:os';
import { execFileSync } from 'node:child_process';
import { writeFile } from 'node:fs/promises';
import { replicaAggregateDigest } from '../../dist/replica-integrity.js';
import { IndexedDBLocalReplicaStorage } from '../../dist/indexeddb-replica.js';
import { IndexedDBEngineReplicaStorage } from '../../dist/indexeddb-engine.js';
import { fixture, seed, artifactHash, budget, rowHash } from './fixture.mjs';

const warmups = 5, iterations = 20;
const output = process.argv[2] ?? 'bench/sync-engine/results.json';
const results = { environment: { node: process.version, os: release(), cpu: cpus()[0].model, ram: totalmem(),
  commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(), seed, artifactHash,
  adapter: 'fake-indexeddb, serial Node storage timings, no transport/worker/DOM/network', budget, warmups, iterations }, cases: [] };
const empty = { entities: {}, liveQueries: {} };
async function timed(run) { const start = performance.now(); await run(); return performance.now() - start; }
function summary(samples) { const sorted = [...samples].sort((a, b) => a - b); return { median: sorted[Math.floor(sorted.length / 2)], p95: sorted[Math.ceil(sorted.length * .95) - 1], samples }; }
async function measure(label, stores, prepare) {
  const samples = [[], []];
  for (let index = 0; index < warmups + iterations; index++) {
    const run = prepare(index);
    // Alternate order, one operation at a time, with a yield between pairs.
    for (const variant of index % 2 ? [1, 0] : [0, 1]) {
      const elapsed = await timed(() => run(stores[variant]));
      if (index >= warmups) samples[variant].push(elapsed);
    }
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  const value = { metric: label, legacy: summary(samples[0]), engine: summary(samples[1]) };
  results.cases.push(value);
  console.log(`${label}: legacy p95 ${value.legacy.p95.toFixed(3)} ms; engine p95 ${value.engine.p95.toFixed(3)} ms`);
}
for (const count of [10_000, 50_000]) {
  const data = fixture(count);
  console.log(`Seeding ${count} rows, ${data.encodedBytes} encoded bytes, ${data.encodedBytes / count} bytes/row`);
  const stores = [new IndexedDBLocalReplicaStorage(`baseline-${count}`), new IndexedDBEngineReplicaStorage(`candidate-${count}`)];
  try {
    for (const storage of stores) {
      await storage.replaceWindow(data.window, { ...empty, cursor: data.window.cursor }, 'default', data.rows);
      for (const { entity, rows } of data.related) await storage.replaceWindow({ ...data.window, signature: entity, entity, ids: rows.map(row => row.id), hashes: undefined }, empty, 'default', rows);
    }
    results.cases.push({ fixture: count, encodedBytes: data.encodedBytes, columns: 50, relatedTables: 5,
      visibleRows: data.visibleIDs.length, exactSubscriptionsFixture: data.exactIDs.length });
    await measure(`single-row-${count}`, stores, index => {
      const row = { ...data.rows[Math.floor(count / 2)], field1: `delta-${index}` };
      const cursor = { epoch: 'fixture', revision: index + 2 };
      const hashes = { ...data.window.hashes, [row.id]: rowHash(row) };
      const window = { ...data.window, cursor, hashes, aggregateDigest: replicaAggregateDigest(hashes) };
      return storage => storage.applyWindowDelta(window, { upserts: [row], deleted: [], memberships: [{ id: row.id }] }, { ...empty, cursor });
    });
    // Persisted process/store with empty decoded cache. This is storage startup,
    // not a browser navigation or cached paint measurement.
    await measure(`startup-working-set-${count}`, stores, () => storage => storage.loadWorkingSet('default', budget));
    // Many unrelated windows, exactly three owners of each victim. Changing
    // victims each iteration preserves owner count without restoring snapshots.
    for (const storage of stores) for (let index = 0; index < 100; index++) {
      const ids = index < 2 ? data.window.ids : [data.window.ids[count - 1]];
      await storage.replaceWindow({ ...data.window, signature: `window-${index}`, ids, hashes: undefined,
        cursor: { epoch: 'fixture', revision: 100 } }, empty, 'default', []);
    }
    await measure(`delete-100-windows-${count}`, stores, index => storage => storage.applyTransaction({ cursor: { epoch: 'fixture', revision: 101 + index },
      changes: [{ entity: 'tasks', id: data.rows[index].id, operation: 'delete' }] }, empty));
  } finally { stores.forEach(storage => storage.close()); }
}
await writeFile(output, JSON.stringify(results, null, 2) + '\n');
