import { afterEach, expect, it } from 'vitest';
import initialize, { type Database } from '@sqlite.org/sqlite-wasm';
import { officialSQLiteDatabase, openOfficialSQLiteHost } from './sqlite-official-host.js';
import { sqliteSearchName, sqliteTableName, sqliteIdentifier as q } from './sqlite-typed-schema.js';
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
  expect(engine.stats().binaryJSONFrames).toBe(1);
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

it('profiles native transaction boundaries using fixed numeric counters and resets them', async () => {
  const { host } = fixture();
  await host.database.withTransactionAsync(async () => { await host.database.execAsync('CREATE TABLE profile_probe(n)'); });
  await expect(host.database.withTransactionAsync(async () => { throw new Error('abort'); })).rejects.toThrow('abort');
  const result = host.takeProfile();
  expect(result).toMatchObject({ commits: 1, rollbacks: 1 });
  for (const value of Object.values(result)) expect(Number.isFinite(value) && value >= 0).toBe(true);
  expect(host.takeProfile()).toEqual({ prepareMs: 0, prepares: 0, bindMs: 0, boundTextUTF16Bytes: 0, beginMs: 0, callbackMs: 0, commitMs: 0, rollbackMs: 0, commits: 0, rollbacks: 0 });
  expect((await host.database.getFirstAsync<{ n: number }>('SELECT count(*) n FROM profile_probe'))!.n).toBe(0);
});

it.each([['full', false], ['column', false], ['none', false], ['full', true], ['column', true], ['none', true]] as const)('preserves literal substring queries and trigger maintenance with FTS detail=%s external=%s', async (searchDetail, external) => {
  const { host } = fixture();
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, title: { kind: 'text' }, description: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }], search: ['title', 'description'], searchDetail, ...(external ? { searchContent: 'external' as const } : {}) } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('owner');
  const rows = [
    { _id: 'a', title: 'Needle café 東京', description: 'Inspect *pump? [A] 100% back\\slash', rank: 1 },
    { _id: 'b', title: 'Other', description: 'A long phrase and NEEDLE 🔧', rank: 2 },
    { _id: 'c', title: '[A] needle', description: 'Unrelated text', rank: 3 },
  ];
  await engine.importScope('s', (async function* () { yield JSON.stringify(rows.map(row => ({ table: 'records', id: row._id, version: 1, row }))); })());
  expect(await host.database.getFirstAsync("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name GLOB '*_fts_content'")).toEqual({ n: external ? 0 : 1 });
  const terms = ['needle', 'NEEDLE', '東京', 'café', '🔧', '*pump?', '[A]', '100%', 'back\\slash', 'long phrase', 'absent'];
  for (const search of terms) {
    const expected = rows.filter(row => [row.title, row.description].some(value => value.toLowerCase().includes(search.toLowerCase()))).map(row => row._id);
    expect((await engine.page('s', { table: 'records', search, order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows.map(row => row._id), search).toEqual(expected);
  }
  await engine.apply('s', [
    { table: 'records', id: 'a', version: 2, row: { ...rows[0], title: 'Updated', description: 'Fresh phrase' } },
    { table: 'records', id: 'b', version: 2, row: null },
  ]);
  expect((await engine.page('s', { table: 'records', search: 'needle', order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows.map(row => row._id)).toEqual(['c']);
  expect((await engine.page('s', { table: 'records', search: 'Fresh phrase', order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows.map(row => row._id)).toEqual(['a']);
  expect(await engine.integrity()).toBe('ok');
});

it('opens and imports a schema without optional secondary indexes or FTS', async () => {
  const { host } = fixture();
  const engine = new SQLiteTypedReplicaEngine(host.database, { records: { key: '_id', fields: { _id: { kind: 'text' } }, indexes: [] } });
  await engine.open('owner');
  await engine.importScope('atomic', (async function* () { yield JSON.stringify([{ table: 'records', id: 'a', version: 1, row: { _id: 'a' } }]); })());
  await engine.beginBootstrap('restartable', 'token');
  const state = await engine.appendBootstrap('restartable', 'token', 0, JSON.stringify([{ table: 'records', id: 'b', version: 1, row: { _id: 'b' } }]));
  await engine.finishBootstrap('restartable', 'token', { pages: state.nextPage, rows: state.rows, root: state.root });
  const plan = { table: 'records', order: [{ field: '_id', direction: 'asc' }], textOrder: 'binary', limit: 2 } as const;
  expect((await engine.page('atomic', plan)).rows).toEqual([{ _id: 'a' }]);
  expect((await engine.page('restartable', plan)).rows).toEqual([{ _id: 'b' }]);
  expect(await host.database.getFirstAsync('SELECT count(*) n FROM gv_storage_flags')).toEqual({ n: 0 });
});

it('reconstructs JSON subtypes and missing fields from typed skeletons in the official WASM', async () => {
  const { host } = fixture();
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, flag: { kind: 'boolean' }, data: { kind: 'json' }, name: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }] } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('owner');
  const rows = [{ _id: 'a', flag: false, data: { values: [1, null, true], name: 'nested' }, name: 'body'.repeat(200), rank: 1, unknown: 'keep' }, { _id: 'b', flag: true, data: null, rank: 2 }];
  await engine.importScope('s', (async function* () { yield JSON.stringify(rows.map(row => ({ table: 'records', id: row._id, version: 1, row }))); })());
  expect(engine.takeProfile().nativeFrames).toBe(1);
  const plan = { table: 'records', order: [{ field: 'rank', direction: 'asc' }], limit: 2 } as const;
  expect((await engine.page('s', plan)).rows).toEqual(rows);
  expect((await engine.viewPage('s', 'all', plan)).rows).toEqual(rows);
});

it('checks external FTS content consistency in addition to SQLite page integrity', async () => {
  const { host } = fixture();
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, name: { kind: 'text' } }, indexes: [], search: ['name'], searchContent: 'external' } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('owner');
  await engine.apply('s', [{ table: 'records', id: 'a', version: 1, row: { _id: 'a', name: 'Needle' } }]);
  expect(await engine.integrity()).toBe('ok');
  const fn = q(sqliteSearchName('records'));
  await host.database.runAsync(`INSERT INTO ${fn}(${fn},rowid,s0) SELECT 'delete',rid,s0 FROM ${q(sqliteTableName('records'))}`);
  expect(await host.database.getFirstAsync('PRAGMA integrity_check')).toEqual({ integrity_check: 'ok' });
  await expect(engine.integrity()).rejects.toThrow();
});

it('reuses complex read plans under the shared cap while releasing inputs and live cursors', async () => {
  const { host } = fixture({ maxRows: 2, maxBytes: 256 });
  const sql = 'SELECT ? value /* ' + 'validation'.repeat(100) + ' */';
  expect(await host.database.getFirstAsync(sql, 'first')).toEqual({ value: 'first' });
  expect(host.statementBytes()).toBeGreaterThan(0);
  expect(await host.database.getFirstAsync(sql, 'second')).toEqual({ value: 'second' });
  expect(await host.database.getFirstAsync(sql)).toEqual({ value: null });
  await expect(host.database.getFirstAsync(sql, 'x'.repeat(1000))).rejects.toMatchObject({ code: 'sqlite-bridge-budget' });
  expect(await host.database.getFirstAsync(sql, 'recovered')).toEqual({ value: 'recovered' });
  for (let n = 0; n < 100; n++) {
    expect(await host.database.getFirstAsync(sql + ' /* '+n+' */', n)).toEqual({ value: n });
    expect(host.statementBytes()).toBeLessThanOrEqual(262144);
  }
  await host.database.withTransactionAsync(async () => { expect(await host.database.getFirstAsync(sql, 'inside')).toEqual({ value: 'inside' }); });
  host.close(); expect(host.statementBytes()).toBe(0);
});

it('binds each encoded frame once and releases its native scratch row before returning', async () => {
  const { host } = fixture();
  const engine = new SQLiteTypedReplicaEngine(host.database, { records: { key: '_id', fields: { _id: { kind: 'text' }, text: { kind: 'text' } }, indexes: [] } });
  await engine.open('owner'); host.takeProfile();
  const row = { _id: 'a', text: 'payload'.repeat(1000) };
  const encoded = JSON.stringify([{ table: 'records', id: 'a', version: 1, row }]);
  await engine.importScope('s', (async function* () { yield encoded; })());
  const profile = host.takeProfile();
  expect(profile.boundTextUTF16Bytes).toBeGreaterThanOrEqual(encoded.length * 2);
  expect(profile.boundTextUTF16Bytes).toBeLessThan(encoded.length * 2 + 2048);
  expect(await host.database.getFirstAsync('SELECT count(*) n FROM main.gv_input_frame')).toEqual({ n: 0 });
  expect(await host.database.getFirstAsync("SELECT count(*) n FROM sqlite_temp_master WHERE name='gv_input_frame'")).toEqual({ n: 0 });
  expect((await engine.page('s', { table: 'records', order: [{ field: '_id', direction: 'asc' }], textOrder: 'binary', limit: 2 })).rows).toEqual([row]);
});

it('folds ingress search text without projection frames and matches the portable host', async () => {
  const native = fixture().host, portable = fixture().host;
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, title: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }], search: ['title'], searchContent: 'external' } } as const;
  const rows = ['İSTANBUL', 'ΟΣ ΟΣΑ', 'CAFÉ 東京 🔧', 'ẞ Straße', '', null, undefined].map((title, rank) => ({ _id: String(rank), rank, ...(title === undefined ? {} : { title }) }));
  const folded = new SQLiteTypedReplicaEngine(native.database, schema);
  const { ingressUnicodeFold: _capability, ...fallback } = portable.database;
  const projected = new SQLiteTypedReplicaEngine(fallback, schema);
  for (const engine of [folded, projected]) {
    await engine.open('owner');
    await engine.importScope('s', (async function* () { for (const row of rows) yield JSON.stringify([{ table: 'records', id: row._id, version: 1, row }]); })());
    expect(await engine.integrity()).toBe('ok');
  }
  expect(folded.takeProfile()).toMatchObject({ nativeFrames: rows.length, searchProjectionMs: 0, normalizationMs: 0 });
  for (const search of ['i̇stan', 'ος', 'οσα', 'café', '東京', '🔧', 'ß', 'str']) {
    const plan = { table: 'records', order: [{ field: 'rank', direction: 'asc' as const }], limit: 50, search };
    const actual = (await folded.page('s', plan)).rows;
    expect(actual, search).toEqual((await projected.page('s', plan)).rows);
    expect(actual.map(row => row._id), search).toEqual(rows.filter(row => String(row.title ?? '').toLowerCase().includes(search)).map(row => row._id));
  }
});
