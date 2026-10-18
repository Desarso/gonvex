import { fixtureWrites } from './fixture.mjs';
import { openOfficialSQLiteHost } from '../../src/sqlite-official-host.js';
import { SQLiteTypedReplicaEngine } from '../../src/sqlite-typed-engine.js';
let host: Awaited<ReturnType<typeof openOfficialSQLiteHost>>;
let engine: SQLiteTypedReplicaEngine;
const schema = { tasks: { key: '_id', fields: { _id: { kind: 'text' }, name: { kind: 'text' }, workspace: { kind: 'text' }, due: { kind: 'number' }, version: { kind: 'number' }, description: { kind: 'text' } }, indexes: [{ columns: ['workspace', 'due'] }, { columns: ['due'] }, { columns: ['name'] }], search: ['name', 'description'], counts: [{ columns: ['workspace'] }] } } as const;
const generated = { fixtureMs: 0, wireEncodeMs: 0 };
const handlers = {
  async profile() { const result = { ...engine.takeProfile(), ...generated, transactions: host.takeProfile(), wasmBytes: host.memoryBytes(), statementBytes: host.statementBytes() }; generated.fixtureMs = 0; generated.wireEncodeMs = 0; return result; },
  async open({ searchDetail = 'full' } = {}) {
    if (!['full', 'column', 'none'].includes(searchDetail)) throw new TypeError('Invalid benchmark FTS detail');
    const selectedSchema = searchDetail === 'full' ? schema : { tasks: { ...schema.tasks, searchDetail } };
    host = await openOfficialSQLiteHost(); engine = new SQLiteTypedReplicaEngine(host.database, selectedSchema); const epoch = await engine.open('browser-proof'); return { epoch, memoryBytes: host.memoryBytes() }; },
  async seed({ start = 0, count = 1000 }) {
    const begin = performance.now();
    for (let offset = start; offset < start + count; offset += 50) {
      const writes = Array.from({ length: Math.min(50, start + count - offset) }, (_, index) => { const id = offset + index;
        return { table: 'tasks', id: String(id), version: 1, row: { _id: String(id), name: `Task ${String(id).padStart(8, '0')} café 東京`, workspace: String(id % 10), due: id % 365, version: 1, description: 'Synthetic maintenance procedure. '.repeat(15) + (id % 997 === 0 ? 'rare-needle 🔧' : '') } }; });
      await engine.apply('test-scope', writes);
    }
    return { count, ms: performance.now() - begin, memoryBytes: host.memoryBytes() };
  },
  async bootstrapOpen({ token }) { return engine.beginBootstrap('test-scope', token); },
  async bootstrapSeed({ token, start, count }) {
    let state;
    // Worker replacement cadence is independent of the bounded commit credit.
    // Never enlarge a transaction just to keep a worker alive for more rows.
    for (let group = start; group < start + count; group += 1000) {
      const end = Math.min(group + 1000, start + count);
      state = await engine.appendBootstrapBatch('test-scope', token, (async function* () {
        for (let offset = group; offset < end; offset += 100) {
          let timed = performance.now(); const writes = fixtureWrites(offset, Math.min(100, end - offset));
          generated.fixtureMs += performance.now() - timed; timed = performance.now();
          const encoded = JSON.stringify(writes); generated.wireEncodeMs += performance.now() - timed;
          yield { page: offset / 100, encoded };
        }
      })());
    }
    return state;
  },
  async bootstrapFinish({ token, expected }) { return engine.finishBootstrap('test-scope',token,expected); },
  async seedAll({ count, encoded = false }) {
    const begin = performance.now();
    const result = await engine.importScope('test-scope', (async function* () {
      for (let offset = 0; offset < count; offset += 50) {
        const page = Array.from({ length: Math.min(50, count - offset) }, (_, index) => { const id = offset + index;
          return { table: 'tasks', id: String(id), version: 1, row: { _id: String(id), name: `Task ${String(id).padStart(8, '0')} café 東京`, workspace: String(id % 10), due: id % 365, version: 1, description: 'Synthetic maintenance procedure. '.repeat(15) + (id % 997 === 0 ? 'rare-needle 🔧' : '') } };
        });
        yield encoded ? JSON.stringify(page) : page;
      }
    })());
    return { ...result, ms: performance.now() - begin, memoryBytes: host.memoryBytes(), profile: { ...engine.takeProfile(), transactions: host.takeProfile() } };
  },
  async page(args) { const begin = performance.now(); const result = await engine.page('test-scope', { table: 'tasks', order: [{ field: 'due', direction: 'asc' }], limit: 50, select: ['name', 'due', 'workspace'], ...args }); return { ...result, ms: performance.now() - begin }; },
  async count() { return engine.count('test-scope', 'tasks', 0, ['3']); },
  async dirty() {
    await host.database.execAsync("CREATE TABLE IF NOT EXISTS crash_probe(id INTEGER PRIMARY KEY,value BLOB); BEGIN IMMEDIATE; INSERT INTO crash_probe VALUES(1,zeroblob(4194304));");
    return { dirty: true };
  },
  async check() { return { integrity: await host.database.getFirstAsync('PRAGMA integrity_check'), probe: await host.database.getFirstAsync('SELECT count(*) n FROM crash_probe'), count: await engine.count('test-scope', 'tasks', 0, ['3']) }; },
  async close() { host.close(); return true; },
};
let lane = Promise.resolve();
globalThis.onmessage = ({ data: { id, op, args } }) => { lane = lane.then(async () => {
  try { const result = await handlers[op](args ?? {}); globalThis.postMessage({ id, result }); }
  catch (error) { globalThis.postMessage({ id, error: { name: error.name, message: error.message, code: error.code } }); }
}); };
