import { afterEach, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteTypedReplicaEngine, sqlitePayloadBytes } from './sqlite-typed-engine.js';
import { compileSQLiteTypedPage, type SQLitePagePlan } from './sqlite-typed-query.js';
import { compileSQLiteTypedSchema, sqliteIdentifier, sqliteTableName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

const schema: SQLiteTypedSchema = {
  tasks: { key: '_id', fields: { _id: { kind: 'text' }, workspace: { kind: 'text' }, name: { kind: 'text' }, due: { kind: 'number' }, done: { kind: 'boolean' }, data: { kind: 'json' } },
    indexes: [{ columns: ['workspace', 'due'] }, { columns: ['due'] }], search: ['name'], counts: [{ columns: ['workspace', 'done'] }] },
  assignments: { key: '_id', fields: { _id: { kind: 'text' }, task: { kind: 'text' }, member: { kind: 'text' } }, indexes: [{ columns: ['member', 'task'] }] },
};
const connections: DatabaseSync[] = [];
afterEach(() => { for (const db of connections.splice(0)) db.close(); });
function fixture() {
  const db = new DatabaseSync(':memory:'); connections.push(db);
  const host: SQLiteReplicaDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...args) { return db.prepare(sql).run(...args as any[]); },
    async getFirstAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).get(...args as any[]) as T ?? null; },
    async getAllAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).all(...args as any[]) as T[]; },
    async withTransactionAsync(fn) { db.exec('BEGIN'); try { await fn(); db.exec('COMMIT'); } catch (error) { db.exec('ROLLBACK'); throw error; } },
  };
  return { db, host, engine: new SQLiteTypedReplicaEngine(host, schema) };
}
const plan: SQLitePagePlan = { table: 'tasks', order: [{ field: 'due', direction: 'asc' }], limit: 2 };
const write = (id: string, due: number | null, name = id, workspace = 'w') => ({ table: 'tasks', id, version: 1, row: { _id: id, due, name, workspace, done: false } });

it('uses real typed columns and covering keyset indexes with no temporary sort', async () => {
  const { db, engine } = fixture(); await engine.open('a');
  await engine.apply('scope', [write('a', 1), write('b', 2)]);
  const statement = compileSQLiteTypedPage(schema, 'scope', { ...plan, where: { field: 'workspace', op: 'eq', value: 'w' } });
  const explanation = db.prepare('EXPLAIN QUERY PLAN ' + statement.sql).all(...statement.bindings).map(row => row.detail).join('\n');
  expect(explanation).toContain('USING INDEX'); expect(explanation).not.toContain('TEMP B-TREE');
  expect(statement.sql).not.toMatch(/json_extract|gv_cmp|OFFSET|COLLATE/i);
  expect((await engine.page('scope', plan)).rows.map(row => row._id)).toEqual(['a', 'b']);
});

it.each(['asc', 'desc'] as const)('walks %s keysets exactly once across nulls, ties and updates', async direction => {
  const { engine } = fixture(); await engine.open('owner');
  await engine.apply('s', [write('a', 2), write('b', null), write('c', 1), write('d', 2), write('e', null)]);
  let cursor, ids: unknown[] = [];
  do { const page = await engine.page('s', { ...plan, order: [{ field: 'due', direction }], after: cursor });
    ids.push(...page.rows.map(row => row._id)); cursor = page.cursor ?? undefined; if (!page.rows.length) break;
  } while (cursor);
  expect(ids).toEqual(direction === 'asc' ? ['c', 'a', 'd', 'b', 'e'] : ['b', 'e', 'a', 'd', 'c']);
});

it('updates count buckets and trigram content atomically, preserving literal Unicode search', async () => {
  const { engine } = fixture(); await engine.open('a');
  await engine.apply('s', [write('a', 1, 'CAFÉ 東京 🔧 100%_ [*?]'), write('b', 2, 'café second')]);
  for (const term of ['café', '東京', '🔧', '100%_', '[*?]']) {
    const page = await engine.page('s', { ...plan, search: term });
    expect(page.rows.map(row => row._id)).toEqual(term === 'café' ? ['a', 'b'] : ['a']);
  }
  expect(await engine.count('s', 'tasks', 0, ['w', false])).toBe(2);
  await engine.apply('s', [{ ...write('a', 1, 'replaced'), version: 2 }]);
  expect((await engine.page('s', { ...plan, search: '東京' })).rows).toEqual([]);
  await expect(engine.apply('s', [{ ...write('a', 1, 'bad'), version: 3 }, { ...write('b', 1), row: { _id: 'wrong' } }])).rejects.toMatchObject({ code: 'INVALID_ROW' });
  expect((await engine.page('s', { ...plan, search: 'replaced' })).rows).toHaveLength(1);
  expect(await engine.count('s', 'tasks', 0, ['w', false])).toBe(2);
  await engine.apply('s', [{ table: 'tasks', id: 'a', version: 3, row: null }]);
  await engine.apply('s', [write('a', 0, 'stale')]);
  expect(await engine.count('s', 'tasks', 0, ['w', false])).toBe(1);
  expect((await engine.page('s', { ...plan, search: 'stale' })).rows).toEqual([]);
});

it('keeps relation queries and FTS results inside the selected storage scope', async () => {
  const { engine } = fixture(); await engine.open('a');
  await engine.apply('s', [write('a', 1, 'shared'), write('b', 2, 'shared'), { table: 'assignments', id: 'rel', version: 1, row: { _id: 'rel', task: 'b', member: 'm' } }]);
  await engine.apply('other', [write('c', 0, 'shared'), { table: 'assignments', id: 'rel', version: 1, row: { _id: 'rel', task: 'a', member: 'm' } }]);
  const page = await engine.page('s', { ...plan, search: 'shared', where: { op: 'relation', field: '_id', table: 'assignments', relatedField: 'task', where: { field: 'member', op: 'eq', value: 'm' } } });
  expect(page.rows.map(row => row._id)).toEqual(['b']);
});

it('deduplicates immutable intents after owner failover and fences the previous owner', async () => {
  const { engine, host, db } = fixture(); await engine.open('old');
  const writes = [write('a', 1)], intent = { id: 'once', payload: { action: 'create' } };
  const first = await engine.apply('s', writes, intent);
  const next = new SQLiteTypedReplicaEngine(host, schema); expect(await next.open('new')).toBe(2);
  expect(await next.apply('s', writes, intent)).toEqual(first);
  expect(await next.count('s', 'tasks', 0, ['w', false])).toBe(1);
  await expect(next.apply('s', [write('a', 2)], intent)).rejects.toMatchObject({ code: 'INTENT_CONFLICT' });
  await expect(engine.page('s', plan)).rejects.toMatchObject({ code: 'FENCED' });
  await expect(engine.apply('s', [write('b', 2)])).rejects.toMatchObject({ code: 'FENCED' });
  expect(db.prepare('SELECT count(*) n FROM gv_outbox').get()!.n).toBe(1);
  expect(db.prepare('PRAGMA integrity_check').get()!.integrity_check).toBe('ok');
});

it('does not let caller mutation change queued operations and releases queue accounting', async () => {
  const { engine } = fixture(); await engine.open('a');
  const writes = [write('a', 1)]; const pending = engine.apply('s', writes); writes[0]!.row.name = 'mutated';
  await pending; expect((await engine.page('s', plan)).rows[0]!.name).toBe('a');
  await Promise.resolve(); expect(engine.stats()).toMatchObject({ pendingOperations: 0, pendingBytes: 0 });
});

it('refuses unsafe input before reading getters and refuses incompatible schemas without resetting rows', async () => {
  let reads = 0; const value = { get bad() { reads++; return 'secret'; } };
  expect(() => sqlitePayloadBytes(value, 1000)).toThrow('accessor'); expect(reads).toBe(0);
  expect(() => sqlitePayloadBytes({ data: 'x'.repeat(1000) }, 100)).toThrow('budget');
  const { engine, host } = fixture(); await engine.open('a'); await engine.apply('s', [write('a', 1)]);
  const incompatible = new SQLiteTypedReplicaEngine(host, { ...schema, tasks: { ...schema.tasks!, search: undefined } });
  await expect(incompatible.open('b')).rejects.toMatchObject({ code: 'SCHEMA_MISMATCH' });
  expect((await engine.page('s', plan)).rows).toHaveLength(1);
});

it('encodes hostile schema identifiers and rejects implicit locale sorting', () => {
  const hostile = 'a"; DROP TABLE gv_storage_meta; --\u0000';
  const layout = compileSQLiteTypedSchema({ [hostile]: schema.tasks! });
  expect(layout.tables).not.toContain(hostile); expect(sqliteIdentifier('a"b')).toBe('"a""b"');
  expect(sqliteTableName('a')).not.toBe(sqliteTableName('A'));
  expect(() => compileSQLiteTypedPage(schema, 's', { ...plan, order: [{ field: 'name', direction: 'asc' }] })).toThrow('collation');
});

it('projects only visible fields while preserving JSON, booleans and missing versus null', async () => {
  const { engine } = fixture(); await engine.open('a');
  await engine.apply('s', [
    { ...write('a', 1), row: { _id: 'a', name: 'hidden'.repeat(8000), done: true, data: { nested: ['café', 2] } } },
    { ...write('b', 2), row: { _id: 'b', name: 'other', done: false, data: null } },
  ]);
  const result = await engine.page('s', { ...plan, select: ['done', 'data', 'workspace'] });
  expect(result.rows).toEqual([{ _id: 'a', done: true, data: { nested: ['café', 2] } }, { _id: 'b', done: false, data: null }]);
  expect(result.bytes).toBeLessThan(3000);
});

it('rejects huge sparse arrays before JSON.stringify can allocate their holes', () => {
  const sparse = new Array(1_000_000_000);
  expect(() => sqlitePayloadBytes({ sparse }, 262144)).toThrow('dense');
});

it('rejects scalar affinity coercion and arbitrary comparison operators', () => {
  expect(() => compileSQLiteTypedPage(schema, 's', { ...plan, where: { field: 'workspace', op: 'eq', value: 1 } })).toThrow('declared field type');
  expect(() => compileSQLiteTypedPage(schema, 's', { ...plan, where: { field: 'due', op: 'constructor', value: 1 } as any })).toThrow('comparison');
});
