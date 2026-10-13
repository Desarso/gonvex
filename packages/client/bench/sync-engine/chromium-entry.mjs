import { IndexedDBLocalReplicaStorage } from '../../dist/indexeddb-replica.js';
import { IndexedDBEngineReplicaStorage } from '../../dist/indexeddb-engine.js';
import { replicaRowsHashes } from '../../dist/replica-integrity.js';
import { DexieReducerOutbox } from '../../dist/outbox.js';

const empty = { entities: {}, liveQueries: {} };
let state;
const open = (variant, count) => variant === 0 ? new IndexedDBLocalReplicaStorage(`baseline-${count}`) : new IndexedDBEngineReplicaStorage(`engine-${count}`);
window.seedCase = async ({ variant, kind, count, budget }) => {
  const data = await (await fetch('/fixture.json')).json();
  const storage = open(variant, count);
  await storage.replaceWindow(data.window, { ...empty, cursor: data.window.cursor }, 'default', data.rows);
  for (const { entity, rows } of data.related) await storage.replaceWindow({ ...data.window, signature: entity, entity, ids: rows.map(row => row.id), hashes: undefined }, empty, 'default', rows);
  if (kind === 'delete-100-windows') {
    // Every victim has exactly 100 owners, including the original collection.
    const ids = data.rows.slice(0, 1000).map(row => row.id);
    for (let index = 1; index < 100; index++) await storage.replaceWindow({ ...data.window, signature: `owner-${index}`, ids, hashes: undefined }, empty, 'default', []);
  }
  state = { storage, kind, count, variant, budget, window: data.window, row: data.rows[Math.floor(count / 2)] };
};
window.openStartup = ({ variant, count, budget }) => { state = { storage: open(variant, count), budget }; };
window.prepareSample = async index => {
  if (state.kind !== 'single-row') return;
  const row = { ...state.row, field1: `delta-${index}` };
  const hashes = state.window.hashes, old = hashes[row.id];
  const hash = (await replicaRowsHashes([row], 'id'))[row.id];
  const bytes = Uint8Array.from({ length: 32 }, (_, i) => parseInt(state.window.aggregateDigest.slice(5 + i * 2, 7 + i * 2), 16));
  for (let i = 0; i < 32; i++) bytes[i] ^= parseInt(old.slice(i * 2, i * 2 + 2), 16) ^ parseInt(hash.slice(i * 2, i * 2 + 2), 16);
  hashes[row.id] = hash;
  const aggregateDigest = 'xor1:' + Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
  state.window = { ...state.window, hashes, aggregateDigest, cursor: { epoch: 'fixture', revision: index + 2 } };
  state.delta = row;
};
window.runSample = async index => {
  const start = performance.now();
  if (state.kind === 'single-row') await state.storage.applyWindowDelta(state.window,
    { upserts: [state.delta], deleted: [], memberships: [{ id: state.delta.id }] }, { ...empty, cursor: state.window.cursor });
  else if (state.kind === 'delete-100-windows') await state.storage.applyTransaction({ cursor: { epoch: 'fixture', revision: index + 2 },
    changes: [{ entity: 'tasks', id: `task-${String(index).padStart(6, '0')}`, operation: 'delete' }] }, empty);
  else {
    const loaded = await state.storage.loadWorkingSet('default', state.budget);
    if (!loaded) throw new Error('Missing persisted working set');
  }
  return performance.now() - start;
};
// Both replica modes use the same existing outbox. This measures durable state
// transitions only, never server replay or the network drain acceptance gate.
window.prepareOutbox = async ({ index, dependent }) => {
  state?.outbox?.dispose();
  const outbox = new DexieReducerOutbox({ databaseName: 'benchmark-outbox' });
  await outbox.clear('default');
  for (let i = 0; i < 500; i++) await outbox.enqueue({ scope: 'default', path: 'edit', args: { id: i },
    idempotencyKey: `sample-${index}-${i}`, entityKeys: [dependent ? 'tasks' : `task-${i}`] });
  if (outbox.memoryOnly) throw new Error('Outbox fell back to memory; no durable benchmark');
  state = { outbox };
};
window.runOutbox = async () => {
  const start = performance.now();
  for (let i = 0; i < 500; i++) {
    const entry = await state.outbox.nextReady('default', Date.now());
    if (!entry || !await state.outbox.markInflight(entry.id)) throw new Error('Outbox did not admit entry');
    await state.outbox.ack(entry.id);
  }
  const elapsed = performance.now() - start;
  if (state.outbox.memoryOnly) throw new Error('Outbox fell back to memory');
  if (await state.outbox.count('default') !== 0) throw new Error('Outbox did not drain');
  return elapsed;
};
window.closeCase = () => { state?.storage?.close(); state?.outbox?.dispose(); state = undefined; };
// Diagnostic request latencies are collected AFTER measured samples. They
// include native queue time and instrumentation, and are not performance samples.
window.profileSample = async index => {
  const requests = [], restores = [];
  for (const prototype of [IDBObjectStore.prototype, IDBIndex.prototype]) {
    for (const method of ['get', 'getAll', 'getAllKeys', 'count', 'put', 'delete', 'openCursor', 'openKeyCursor']) {
      const original = prototype[method];
      if (!original) continue;
      prototype[method] = function (...args) {
        const start = performance.now(), request = original.apply(this, args);
        const label = `${this.objectStore?.name ?? this.name}${this.objectStore ? '.' + this.name : ''}.${method}`;
        request.addEventListener('success', () => requests.push({ operation: label, ms: performance.now() - start }), { once: true });
        return request;
      };
      restores.push(() => { prototype[method] = original; });
    }
  }
  try {
    await window.prepareSample(index);
    const elapsed = await window.runSample(index);
    return { elapsed, requests };
  } finally { restores.forEach(restore => restore()); }
};
