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
