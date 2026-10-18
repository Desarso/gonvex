import { afterEach, expect, it } from 'vitest';
import initialize, { type Database } from '@sqlite.org/sqlite-wasm';
import { officialSQLiteDatabase, openOfficialSQLiteHost } from './sqlite-official-host.js';
import { SQLiteTypedReplicaEngine } from './sqlite-typed-engine.js';

const sqlite3 = await initialize();
const databases: Database[] = [];
afterEach(() => { for (const db of databases.splice(0)) if (db.pointer) db.close(); });
function fixture(budget?: { maxRows: number; maxBytes: number }) {
  const db = new sqlite3.oo1.DB(':memory:'); databases.push(db);
  return { db, host: officialSQLiteDatabase(sqlite3, db, budget) };
}
it('executes typed rows, FTS and exactly-once receipts in the real pinned official WASM', async () => {
  const { host } = fixture();
  const engine = new SQLiteTypedReplicaEngine(host.database, { records: { key: '_id', fields: { _id: { kind: 'text' }, title: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }], search: ['title'], counts: [{ columns: ['rank'] }] } });
  await engine.open('owner');
  const writes = [{ table: 'records', id: 'r', version: 1, row: { _id: 'r', title: '東京 café 🔧', rank: 1 } }];
  const intent = { id: 'once', payload: { create: 'r' } };
  await engine.apply('s', writes, intent); await engine.apply('s', writes, intent);
  const result = await engine.page('s', { table: 'records', order: [{ field: 'rank', direction: 'asc' }], search: 'café', limit: 50 });
  expect(result.rows).toEqual(writes.map(write => write.row));
  expect(await engine.count('s', 'records', 0, [1])).toBe(1);
  expect((await host.database.getFirstAsync<{ integrity_check: string }>('PRAGMA integrity_check'))!.integrity_check).toBe('ok');
  expect(host.memoryBytes()).toBeGreaterThan(0);
});
it('rejects oversized text before copying it and leaves the connection usable', async () => {
  const { host } = fixture({ maxRows: 2, maxBytes: 256 });
  await expect(host.database.getAllAsync('SELECT hex(zeroblob(10000)) big')).rejects.toMatchObject({ code: 'sqlite-bridge-budget' });
  await expect(host.database.getAllAsync('SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3')).rejects.toMatchObject({ code: 'sqlite-bridge-budget', limit: 'rows' });
  expect(await host.database.getFirstAsync('SELECT 1 n')).toEqual({ n: 1 });
});
it('rolls back on error, rejects transaction interleaving and closes idempotently', async () => {
  const { host } = fixture(); await host.database.execAsync('CREATE TABLE t(id INTEGER PRIMARY KEY)');
  await expect(host.database.withTransactionAsync(async () => { await host.database.runAsync('INSERT INTO t VALUES(?)', 1); throw new Error('abort'); })).rejects.toThrow('abort');
  expect(await host.database.getFirstAsync('SELECT count(*) n FROM t')).toEqual({ n: 0 });
  await host.database.withTransactionAsync(async () => { await expect(host.database.withTransactionAsync(async () => {})).rejects.toThrow('Overlapping'); });
  host.close(); host.close(); await expect(host.database.getAllAsync('SELECT 1')).rejects.toThrow('closed');
});
it('refuses opening OPFS from a non-worker caller', async () => {
  await expect(openOfficialSQLiteHost()).rejects.toThrow('elected dedicated worker');
});
it('caps retained native write plans and drops bindings and failed statements', async () => {
  const { host } = fixture(); await host.database.execAsync('CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT)');
  for (let i = 0; i < 100; i++) {
    await host.database.runAsync(`INSERT INTO t VALUES(?,?) /* ${i} */`, i, 'x'.repeat(1000));
    expect(host.statementBytes()).toBeLessThanOrEqual(262144);
  }
  await expect(host.database.runAsync('INSERT INTO t VALUES(?,?)', 0, 'collision')).rejects.toThrow();
  await host.database.runAsync('INSERT INTO t VALUES(?,?)', 101, 'recovered');
  expect(await host.database.getFirstAsync('SELECT value FROM t WHERE id=101')).toEqual({ value: 'recovered' });
  host.close(); expect(host.statementBytes()).toBe(0);
});
it('imports encoded frames with deferred FTS using the pinned official WASM', async () => {
  const { host } = fixture();
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, title: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }], search: ['title'] } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('owner');
  await engine.importScope('fresh', (async function* () { yield JSON.stringify([{ table: 'records', id: 'r', version: 1, row: { _id: 'r', title: '東京 café', rank: 1 } }]); })());
  expect((await engine.page('fresh', { table: 'records', search: 'café', order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows).toHaveLength(1);
});
it('serves arbitrary native view positions under the official WASM result byte cap', async () => {
  const { host } = fixture({ maxRows: 4, maxBytes: 2048 });
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, rank: { kind: 'number' }, payload: { kind: 'text' } }, indexes: [{ columns: ['rank'] }] } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('owner');
  await engine.importScope('s', (async function* () {
    for (let i = 0; i < 100; i += 10) yield JSON.stringify(Array.from({ length: 10 }, (_, n) => ({ table: 'records', id: String(i + n), version: 1, row: { _id: String(i + n), rank: i + n, payload: 'x'.repeat(200) } })));
  })());
  const page = await engine.viewPage('s', 'grid', { table: 'records', order: [{ field: 'rank', direction: 'asc' }], limit: 32 }, 90);
  expect(page.total).toBe(100); expect(page.rows.length).toBeGreaterThan(0); expect(page.rows.length).toBeLessThanOrEqual(4);
  expect(page.rows[0]!.rank).toBe(90); expect(page.nextOffset).toBe(90 + page.rows.length);
  expect(await host.database.getFirstAsync('PRAGMA integrity_check')).toEqual({ integrity_check: 'ok' });
});

it('keeps staged memberships and certificate pages bounded in the official WASM', async () => {
  const { SQLiteWindowStore } = await import('./sqlite-window-store.js');
  const { host } = fixture({ maxRows: 4, maxBytes: 2048 });
  const store = new SQLiteWindowStore(host.database);
  const header = { signature: 'grid', entity: 'tasks', key: '_id', kind: 'replica', completeness: 'complete', source: 'server' } as const;
  await store.begin('s', header, 'one');
  const ids = Array.from({ length: 128 }, (_, n) => String(n).padStart(3, '0'));
  const state = await store.append('s', 'grid', 'one', 0, JSON.stringify({ ids, hashes: Object.fromEntries(ids.map(id => [id, 'h'.repeat(200)])) }));
  await store.publish('s', 'grid', state);
  expect((await store.header('s', 'grid'))?.uniqueRows).toBe(128);
  const page = await store.page('s', 'grid', 'one', 100, 128);
  expect(page.ids[0]).toBe('100'); expect(page.ids.length).toBeGreaterThan(0); expect(page.ids.length).toBeLessThanOrEqual(4);
  const certificates = await store.hashPage('s', 'grid', 'one');
  expect(certificates.length).toBeGreaterThan(0); expect(certificates.length).toBeLessThanOrEqual(4);
  expect(certificates[0]).toEqual({ id: '000', hash: 'h'.repeat(200) });
  expect((await store.hashPage('s', 'grid', 'one', certificates.at(-1)!.id))[0]!.id).toBe(String(certificates.length).padStart(3, '0'));
  expect(await host.database.getFirstAsync('PRAGMA integrity_check')).toEqual({ integrity_check: 'ok' });
});

it('resumes a deferred bootstrap in official WASM and builds searchable indexes on activation', async () => {
  const { host } = fixture();
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, title: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }], search: ['title'] } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('first');
  await engine.beginBootstrap('s', 'bulk');
  const state = await engine.appendBootstrap('s', 'bulk', 0, JSON.stringify(Array.from({ length: 100 }, (_, n) => ({ table: 'records', id: String(n), version: 1, row: { _id: String(n), title: '東京 café ' + n, rank: n } }))));
  const next = new SQLiteTypedReplicaEngine(host.database, schema); await next.open('second');
  await next.finishBootstrap('s', 'bulk', { pages: state.nextPage, rows: state.rows, root: state.root });
  const page = await next.page('s', { table: 'records', search: 'café', order: [{ field: 'rank', direction: 'asc' }], limit: 10 });
  expect(page.rows.map(row => row._id)).toEqual(Array.from({ length: 10 }, (_, n) => String(n)));
  expect(await next.integrity()).toBe('ok');
});
