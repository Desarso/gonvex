import { afterEach, expect, it, vi } from 'vitest';
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

it('invalidates a keyset after a durable write instead of skipping changing rows', async () => {
  const { engine } = fixture(); await engine.open('a'); await engine.apply('s', [write('a', 1), write('b', 2), write('c', 3)]);
  const first = await engine.page('s', plan);
  await engine.apply('s', [{ ...write('a', 4), version: 2 }]);
  await expect(engine.page('s', { ...plan, after: first.cursor! })).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
});

it('replays the outbox in durable causal order and keeps receipts after acknowledgment', async () => {
  const { engine } = fixture(); await engine.open('a');
  await engine.apply('s', [write('a', 1)], { id: 'z', payload: { create: 'a' } });
  await engine.apply('s', [write('b', 2)], { id: 'a', payload: { create: 'b' } });
  expect((await engine.outbox('s')).map(row => row.id)).toEqual(['z', 'a']);
  await engine.acknowledge('s', ['z']);
  await engine.apply('s', [write('a', 1)], { id: 'z', payload: { create: 'a' } });
  expect((await engine.outbox('s')).map(row => row.id)).toEqual(['a']);
  expect(await engine.outbox('other')).toEqual([]);
  expect(await engine.count('s', 'tasks', 0, ['w', false])).toBe(2);
});

it('streams bounded bootstrap pages in one durable transaction and rolls back a failed source', async () => {
  const { engine } = fixture(); await engine.open('owner');
  const source = async function* () { yield [write('a', 1)]; yield [write('b', 2)]; };
  expect(await engine.importScope('s', source())).toEqual({ rows: 2, sequence: 1 });
  expect((await engine.page('s', plan)).rows.map(row => row._id)).toEqual(['a', 'b']);
  await expect(engine.importScope('s', source())).rejects.toThrow('empty scope');
  await expect(engine.importScope('other', (async function* () { yield [write('a', 1)]; throw Error('source unavailable'); })())).rejects.toThrow('source unavailable');
  expect((await engine.page('other', plan)).rows).toEqual([]);
  expect(await engine.count('other', 'tasks', 0, ['w', false])).toBe(0);
  expect(await engine.outbox('other')).toEqual([]);
});
it('admits immutable encoded bootstrap frames without an object-graph clone and rejects invalid JSON numbers', async () => {
  const { engine } = fixture(); await engine.open('owner');
  expect(await engine.importScope('encoded', (async function* () { yield JSON.stringify([write('a', 1)]); })())).toMatchObject({ rows: 1 });
  expect((await engine.page('encoded', plan)).rows[0]!._id).toBe('a');
  await expect(engine.importScope('invalid', (async function* () { yield '[{"table":"tasks","id":"a","version":1,"row":{"_id":"a","due":1e999}}]'; })())).rejects.toThrow('Nonfinite');
  expect((await engine.page('invalid', plan)).rows).toEqual([]);
});
it('restores deferred indexes after bootstrap failure and maintains FTS on later writes', async () => {
  const { engine, db } = fixture(); await engine.open('owner');
  const before = db.prepare("SELECT name FROM sqlite_master WHERE type IN ('index','trigger') ORDER BY name").all();
  await expect(engine.importScope('fresh', (async function* () { yield [write('a', 1, 'needle')]; throw Error('broken frame'); })())).rejects.toThrow('broken frame');
  expect(db.prepare("SELECT name FROM sqlite_master WHERE type IN ('index','trigger') ORDER BY name").all()).toEqual(before);
  expect(db.prepare("SELECT count(*) n FROM gv_storage_flags").get()!.n).toBe(0);
  await engine.importScope('fresh', (async function* () { yield JSON.stringify([write('a', 1, 'needle')]); })());
  expect((await engine.page('fresh', { ...plan, search: 'needle' })).rows).toHaveLength(1);
  await engine.apply('fresh', [write('b', 2, 'needle two')]);
  expect((await engine.page('fresh', { ...plan, search: 'needle' })).rows).toHaveLength(2);
  expect(await engine.count('fresh', 'tasks', 0, ['w', false])).toBe(2);
});
it('resumes invisible bootstrap pages across owner changes, verifies the manifest and preserves exactly-once receipts', async () => {
  const { engine, host } = fixture(); await engine.open('owner');
  const token = 'bootstrap'; await engine.beginBootstrap('new', token);
  const encoded = JSON.stringify([write('a', 1, 'needle')]);
  const first = await engine.appendBootstrap('new', token, 0, encoded);
  expect((await engine.page('new', plan)).rows).toEqual([]); expect(await engine.count('new', 'tasks', 0, ['w', false])).toBe(0);
  const next = new SQLiteTypedReplicaEngine(host, schema); await next.open('replacement');
  expect(await next.beginBootstrap('new', token)).toEqual(first);
  expect(await next.appendBootstrap('new', token, 0, encoded)).toEqual(first);
  await expect(next.appendBootstrap('new', token, 0, JSON.stringify([write('a', 2)]))).rejects.toMatchObject({ code: 'INTENT_CONFLICT' });
  await expect(next.appendBootstrap('new', token, 2, JSON.stringify([write('b', 2)]))).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  await expect(next.appendBootstrap('new', token, 1, encoded)).rejects.toThrow('Duplicate');
  await expect(next.finishBootstrap('new', token, { pages: 1, rows: 1, root: 'wrong' })).rejects.toThrow('manifest');
  await next.finishBootstrap('new', token, { pages: 1, rows: 1, root: first.root });
  expect((await next.page('new', { ...plan, search: 'needle' })).rows[0]!._id).toBe('a');
  expect(await next.count('new', 'tasks', 0, ['w', false])).toBe(1);
  const state = await next.finishBootstrap('new', token, { pages: 1, rows: 1, root: first.root }); expect(state.complete).toBe(true);
  await next.apply('new', [write('b', 2)], { id: 'intent', payload: {} });
  expect((await next.page('new', plan)).rows.map(row => row._id)).toEqual(['a', 'b']);
  expect(await next.count('new', 'tasks', 0, ['w', false])).toBe(2);
  expect((await next.outbox('new'))[0]!.id).toBe('intent');
});
it('never activates bootstrap over a concurrent canonical intent', async () => {
  const { engine } = fixture(); await engine.open('owner'); await engine.beginBootstrap('s', 'new');
  const receipt = await engine.appendBootstrap('s', 'new', 0, JSON.stringify([write('a', 1)]));
  await engine.apply('s', [write('offline', 2)], { id: 'offline-intent', payload: {} });
  await expect(engine.finishBootstrap('s', 'new', { pages: 1, rows: 1, root: receipt.root })).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  expect((await engine.page('s', plan)).rows.map(row => row._id)).toEqual(['offline']);
  expect((await engine.outbox('s'))[0]!.id).toBe('offline-intent');
});
it('discards only unpublished bootstrap generations and permits a new source after cancellation', async () => {
  const { engine } = fixture(); await engine.open('owner'); await engine.beginBootstrap('s', 'first');
  await engine.appendBootstrap('s', 'first', 0, JSON.stringify([write('a', 1)]));
  await engine.abortBootstrap('s', 'first'); await engine.abortBootstrap('s', 'first');
  await engine.beginBootstrap('s', 'second');
  const state = await engine.appendBootstrap('s', 'second', 0, JSON.stringify([write('b', 2)]));
  await engine.finishBootstrap('s', 'second', { pages: 1, rows: 1, root: state.root });
  await expect(engine.abortBootstrap('s', 'second')).rejects.toThrow('published');
  expect((await engine.page('s', plan)).rows.map(row => row._id)).toEqual(['b']);
});
it('commits a bounded streamed credit group atomically, including all page receipts', async () => {
  const { engine } = fixture(); await engine.open('owner'); await engine.beginBootstrap('s', 'group');
  await expect(engine.appendBootstrapBatch('s', 'group', (async function* () {
    yield { page: 0, encoded: JSON.stringify([write('a', 1)]) };
    yield { page: 1, encoded: JSON.stringify([write('a', 2)]) };
  })())).rejects.toThrow('Duplicate');
  expect(await engine.beginBootstrap('s', 'group')).toMatchObject({ rows: 0, nextPage: 0 });
  const result = await engine.appendBootstrapBatch('s', 'group', (async function* () {
    yield { page: 0, encoded: JSON.stringify([write('a', 1)]) };
    yield { page: 1, encoded: JSON.stringify([write('b', 2)]) };
  })());
  expect(result).toMatchObject({ rows: 2, nextPage: 2 });
  await engine.finishBootstrap('s', 'group', { pages: 2, rows: 2, root: result.root });
  expect((await engine.page('s', plan)).rows.map(row => row._id)).toEqual(['a', 'b']);
});
it('rejects excessive bootstrap credit groups without retaining partial progress', async () => {
  const { engine } = fixture(); await engine.open('owner'); await engine.beginBootstrap('s', 'group');
  await expect(engine.appendBootstrapBatch('s', 'group', (async function* () { for (let page = 0; page < 17; page++) yield { page, encoded: '[]' }; })())).rejects.toMatchObject({ code: 'BUDGET' });
  expect(await engine.beginBootstrap('s', 'group')).toMatchObject({ rows: 0, nextPage: 0 });
});
it('shrinks wide row pages to their byte budget without losing keyset membership', async () => {
  const { engine } = fixture(); await engine.open('owner');
  for (let offset = 0; offset < 20; offset += 5) await engine.apply('s', Array.from({ length: 5 }, (_, i) => write(String(offset + i), offset + i, 'x'.repeat(10000))));
  let after, ids: unknown[] = [];
  do {
    const result = await engine.page('s', { ...plan, limit: 20, after });
    expect(result.bytes).toBeLessThanOrEqual(262144);
    ids.push(...result.rows.map(row => row._id)); after = result.cursor ?? undefined;
    if (!result.rows.length) break;
  } while (after);
  expect(ids).toEqual(Array.from({ length: 20 }, (_, i) => String(i)));
});
it('replays wide outbox entries in smaller causal pages instead of stalling on the default limit', async () => {
  const { engine } = fixture(); await engine.open('owner');
  for (let i = 0; i < 32; i++) await engine.apply('s', [], { id: 'intent-' + i, payload: { text: 'x'.repeat(10000) } });
  let after = 0; const ids: string[] = [];
  for (;;) {
    const page = await engine.outbox('s', after); if (!page.length) break;
    expect(page.length).toBeLessThan(32); ids.push(...page.map(row => row.id)); after = page.at(-1)!.sequence;
  }
  expect(ids).toEqual(Array.from({ length: 32 }, (_, i) => 'intent-' + i));
});
it('uses independently indexed FTS columns and deduplicates rows matching both', async () => {
  const { host } = fixture();
  const multiple: SQLiteTypedSchema = { ...schema, tasks: { ...schema.tasks!, fields: { ...schema.tasks!.fields, description: { kind: 'text' } }, search: ['name', 'description'] } };
  const engine = new SQLiteTypedReplicaEngine(host, multiple); await engine.open('owner');
  await engine.apply('s', [
    { ...write('a', 1, 'needle title'), row: { ...write('a', 1, 'needle title').row, description: 'needle detail' } },
    { ...write('b', 2, 'other'), row: { ...write('b', 2, 'other').row, description: 'needle detail' } },
  ]);
  const statement = compileSQLiteTypedPage(multiple, 's', { ...plan, search: 'needle' });
  expect(statement.sql).toContain(' UNION ');
  expect((await engine.page('s', { ...plan, search: 'needle' })).rows.map(row => row._id)).toEqual(['a', 'b']);
});

it('keeps deep view membership on disk and rebuilds atomically after canonical changes', async () => {
  const { engine, db, host } = fixture(); await engine.open('owner');
  await engine.importScope('s', (async function* () {
    for (let i = 0; i < 3000; i += 100) yield Array.from({ length: 100 }, (_, n) => write(String(i + n).padStart(5, '0'), i + n));
  })());
  const statements: string[] = [], read = host.getAllAsync.bind(host);
  host.getAllAsync = async <T>(sql: string, ...values: unknown[]) => { statements.push(sql); return read<T>(sql, ...values); };
  const view = { ...plan, select: ['due'], limit: 20 };
  const last = await engine.viewPage('s', 'grid', view, 2980);
  expect(last.total).toBe(3000); expect(last.nextOffset).toBeNull();
  expect(last.rows.map(row => row.due)).toEqual(Array.from({ length: 20 }, (_, n) => 2980 + n));
  expect(statements.every(sql => !/SELECT.*FROM gv_view_members(?! m JOIN)/.test(sql))).toBe(true);
  expect(statements.join(' ')).not.toMatch(/OFFSET/);
  expect(db.prepare('SELECT count(*) n FROM gv_view_members').get()!.n).toBe(3000);
  // The existing persisted header and order can be reused by the next owner.
  const next = new SQLiteTypedReplicaEngine(host, schema); await next.open('next');
  expect((await next.viewPage('s', 'grid', view, 1234)).rows[0]!.due).toBe(1234);
  await next.apply('s', [{ ...write('02999', -1), version: 2 }]);
  expect((await next.viewPage('s', 'grid', view, 0)).rows[0]!.due).toBe(-1);
  expect((await next.viewPage('other', 'grid', view)).total).toBe(0);
  expect(() => next.viewPage('s', 'grid', { ...view, order: [{ field: 'missing', direction: 'asc' }] })).toThrow();
  expect((await next.viewPage('s', 'grid', view)).total).toBe(3000);
});

it('bounds retained view plans and evicts their native membership together', async () => {
  const { engine, db } = fixture(); await engine.open('owner'); await engine.apply('s', [write('a', 1)]);
  for (let i = 0; i < 40; i++) await engine.viewPage('s', 'view-' + i, plan);
  expect(db.prepare('SELECT count(*) n FROM gv_views').get()!.n).toBe(32);
  expect(db.prepare('SELECT count(*) n FROM gv_view_members').get()!.n).toBe(32);
  expect(db.prepare('SELECT count(*) n FROM gv_view_members m LEFT JOIN gv_views v USING(scope,signature) WHERE v.signature IS NULL').get()!.n).toBe(0);
});

it('preserves JSON scalar types, duplicate-key semantics and Unicode in native bootstrap extraction', async () => {
  const { engine } = fixture(); await engine.open('owner');
  const encoded = '[{"table":"tasks","id":"a","version":1,"row":{"_id":"a","name":"ignored","name":"İ CAFÉ 東京","due":2,"done":true,"data":"plain"}},' +
    '{"table":"tasks","id":"b","version":1,"row":{"_id":"b","due":3,"data":[1,null,{"x":false}]}}]';
  await engine.importScope('s', (async function* () { yield encoded; })());
  expect(engine.takeProfile().nativeFrames).toBe(0);
  const result = await engine.page('s', { ...plan, select: ['name', 'data', 'done'] });
  expect(result.rows).toEqual([{ _id: 'a', name: 'İ CAFÉ 東京', data: 'plain', done: true }, { _id: 'b', data: [1, null, { x: false }] }]);
  expect((await engine.page('s', { ...plan, search: 'i\u0307 café' })).rows).toHaveLength(1);
});

it('serializes membership staging with canonical writes, snapshots arguments and fences every membership operation', async () => {
  const { engine, host } = fixture(); await engine.open('first');
  const header = { signature: 'grid', entity: 'tasks', key: '_id', kind: 'replica', completeness: 'complete', source: 'server' } as const;
  const pending = engine.windowBegin('s', header, 'one');
  (header as { entity: string }).entity = 'mutated';
  await pending;
  const state = await engine.windowAppend('s', 'grid', 'one', 0, '{"ids":["a"]}');
  await Promise.all([engine.apply('s', [write('a', 1)]), engine.windowPublish('s', 'grid', state)]);
  expect((await engine.windowHeader('s', 'grid'))?.header.entity).toBe('tasks');
  expect((await engine.windowPage('s', 'grid', 'one', 0)).ids).toEqual(['a']);
  const replacement = new SQLiteTypedReplicaEngine(host, schema); await replacement.open('second');
  await expect(engine.windowHeader('s', 'grid')).rejects.toMatchObject({ code: 'FENCED' });
  await expect(engine.windowAppend('s', 'grid', 'one', 1, '{"ids":[]}')).rejects.toMatchObject({ code: 'FENCED' });
  await expect(engine.windowCollect('s', 'grid', 'one')).rejects.toMatchObject({ code: 'FENCED' });
  expect((await replacement.windowPage('s', 'grid', 'one', 0)).ids).toEqual(['a']);
});

it('rejects a compact JSON graph before decoding it and still serves a bounded projection', async () => {
  const { engine } = fixture(); await engine.open('owner');
  await engine.apply('s', [{ ...write('wide', 1), row: { ...write('wide', 1).row, data: Array.from({ length: 5000 }, () => null) } }]);
  await expect(engine.page('s', { ...plan, limit: 1 })).rejects.toMatchObject({ code: 'BUDGET' });
  await expect(engine.viewPage('s', 'wide', { ...plan, limit: 1 }, 0)).rejects.toMatchObject({ code: 'BUDGET' });
  const projected = await engine.viewPage('s', 'small', { ...plan, select: ['name'], limit: 1 }, 0);
  expect(projected.rows).toEqual([{ _id: 'wide', name: 'wide' }]);
  expect(projected.bytes).toBeGreaterThanOrEqual(sqlitePayloadBytes(projected.rows, 262144));
  expect(await engine.integrity()).toBe('ok');
});
it('shrinks pages before decoding graphs whose aggregate expansion exceeds the page cap', async () => {
  const { engine } = fixture(); await engine.open('owner');
  for (let i = 0; i < 4; i++) await engine.apply('s', [{ ...write(String(i), i), row: { ...write(String(i), i).row, data: Array.from({ length: 1200 }, () => null) } }]);
  const page = await engine.page('s', { ...plan, limit: 4 });
  expect(page.rows.length).toBe(2);
  expect(page.bytes).toBeGreaterThanOrEqual(sqlitePayloadBytes(page.rows, 262144));
  const next = await engine.page('s', { ...plan, limit: 4, after: page.cursor! });
  expect(next.rows.map(row => row._id)).toEqual(['2', '3']);
});
it('resumes deferred bootstrap indexes across owners and builds FTS before activation', async () => {
  const { engine, host, db } = fixture(); await engine.open('first');
  await engine.beginBootstrap('s', 'bulk');
  const fts = sqliteTableName('tasks') + '_fts';
  expect(db.prepare('SELECT name FROM sqlite_master WHERE name=?').get(fts)).toBeUndefined();
  expect((await engine.page('s', { ...plan, search: 'café' })).rows).toEqual([]);
  const state = await engine.appendBootstrap('s', 'bulk', 0, JSON.stringify([write('a', 1, 'Café 東京')]));
  const next = new SQLiteTypedReplicaEngine(host, schema); await next.open('second');
  expect(db.prepare('SELECT name FROM sqlite_master WHERE name=?').get(fts)).toBeUndefined();
  await next.finishBootstrap('s', 'bulk', { pages: state.nextPage, rows: state.rows, root: state.root });
  expect(db.prepare('SELECT name FROM sqlite_master WHERE name=?').get(fts)).toBeDefined();
  expect((await next.page('s', { ...plan, search: 'café' })).rows.map(row => row._id)).toEqual(['a']);
  expect(await next.count('s', 'tasks', 0, ['w', false])).toBe(1);
  expect(db.prepare('SELECT count(*) n FROM gv_storage_flags').get()!.n).toBe(0);
  expect(await next.integrity()).toBe('ok');
});
it('restores deferred indexes before an interactive write and never hides that write in bootstrap activation', async () => {
  const { engine } = fixture(); await engine.open('owner'); await engine.beginBootstrap('s', 'bulk');
  const state = await engine.appendBootstrap('s', 'bulk', 0, JSON.stringify([write('staged', 1, 'Hidden needle')]));
  await engine.apply('s', [write('offline', 2, 'Visible needle')], { id: 'intent', payload: { create: 'offline' } });
  await expect(engine.finishBootstrap('s', 'bulk', { pages: state.nextPage, rows: state.rows, root: state.root })).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  expect((await engine.page('s', { ...plan, search: 'needle' })).rows.map(row => row._id)).toEqual(['offline']);
  await engine.abortBootstrap('s', 'bulk');
  expect((await engine.outbox('s')).map(entry => entry.id)).toEqual(['intent']);
  expect(await engine.integrity()).toBe('ok');
});
it.each([3, 4, 5])('upgrades format %i atomically without replacing canonical rows, pending intents or receipts', async format => {
  const { engine, host, db } = fixture(); await engine.open('first');
  const writes = [write('a', 1, 'Needle')], intent = { id: 'pending', payload: { create: 'a' } };
  const receipt = await engine.apply('s', writes, intent);
  db.prepare('UPDATE gv_storage_meta SET schema=? WHERE id=1').run(JSON.stringify({ format, tables: schema }));
  db.prepare(`UPDATE ${sqliteIdentifier(sqliteTableName('tasks'))} SET body=? WHERE id='a'`).run(JSON.stringify(writes[0]!.row));
  db.exec('DROP TABLE gv_storage_flags');
  const next = new SQLiteTypedReplicaEngine(host, schema); await next.open('second');
  expect(JSON.parse(String(db.prepare('SELECT schema FROM gv_storage_meta').get()!.schema)).format).toBe(6);
  expect(await next.apply('s', writes, intent)).toEqual(receipt);
  expect((await next.outbox('s')).map(entry => entry.id)).toEqual(['pending']);
  expect((await next.page('s', { ...plan, search: 'needle' })).rows.map(row => row._id)).toEqual(['a']);
  await expect(engine.apply('s', [write('b', 1)])).rejects.toMatchObject({ code: 'FENCED' });
});

it('rolls back an interrupted deferred FTS build and resumes the same verified manifest', async () => {
  const { host, db } = fixture();
  let failBuild = false;
  const injected = { ...host, async runAsync(sql: string, ...args: unknown[]) {
    if (failBuild && sql.startsWith('INSERT INTO') && sql.includes('_fts')) { failBuild = false; throw new Error('injected index build interruption'); }
    return host.runAsync(sql, ...args);
  } };
  const engine = new SQLiteTypedReplicaEngine(injected, schema); await engine.open('first'); await engine.beginBootstrap('s', 'bulk');
  const state = await engine.appendBootstrap('s', 'bulk', 0, JSON.stringify([write('a', 1, 'Needle café')]));
  const expected = { pages: state.nextPage, rows: state.rows, root: state.root }; failBuild = true;
  await expect(engine.finishBootstrap('s', 'bulk', expected)).rejects.toThrow('injected index build interruption');
  expect(db.prepare('SELECT count(*) n FROM gv_scope_alias').get()!.n).toBe(0);
  expect(db.prepare('SELECT count(*) n FROM gv_storage_flags').get()!.n).toBe(1);
  const next = new SQLiteTypedReplicaEngine(host, schema); await next.open('second');
  await next.finishBootstrap('s', 'bulk', expected);
  expect((await next.page('s', { ...plan, search: 'needle' })).rows.map(row => row._id)).toEqual(['a']);
  expect(await next.integrity()).toBe('ok');
});

it('rejects undeclared or invalid FTS detail policies before generating SQL', () => {
  const table = schema.tasks!;
  expect(() => compileSQLiteTypedSchema({ tasks: { ...table, searchDetail: 'invalid' as any } })).toThrow('search detail');
  expect(() => compileSQLiteTypedSchema({ tasks: { ...table, search: undefined, searchDetail: 'none' } })).toThrow('search detail');
});

it('reports rate-limited actual bootstrap progress without making telemetry a commit prerequisite', async () => {
  const { host } = fixture(); let clock = 0;
  const now = vi.spyOn(performance, 'now').mockImplementation(() => clock);
  const progress = vi.fn(() => { throw new Error('recipient closed'); });
  const engine = new SQLiteTypedReplicaEngine(host, schema, undefined, { onProgress: progress });
  try {
    await engine.open('owner'); clock = 1000;
    await engine.beginBootstrap('s', 'first'); expect(progress).toHaveBeenCalledTimes(1);
    await engine.appendBootstrap('s', 'first', 0, JSON.stringify([write('a', 1)]));
    expect(progress).toHaveBeenCalledTimes(1);
    clock = 1600;
    const state = await engine.appendBootstrap('s', 'first', 1, JSON.stringify([write('b', 2)]));
    expect(progress).toHaveBeenCalledTimes(2);
    clock = 2200;
    await engine.finishBootstrap('s', 'first', { pages: state.nextPage, rows: state.rows, root: state.root });
    expect(progress).toHaveBeenCalledTimes(3);
    expect((await engine.page('s', plan)).rows.map(row => row._id)).toEqual(['a', 'b']);
  } finally { now.mockRestore(); }
});

it('keeps generated field payloads only in typed columns and reconstructs exact bodies and projections', async () => {
  const { engine, db } = fixture(); await engine.open('owner');
  const rows = [
    { _id: 'a', name: 'payload'.repeat(200), due: null, done: false, data: { name: 'nested', values: [1, null, true] }, extra: { untouched: 'value' } },
    { _id: 'b', name: 'short', done: true, data: null },
  ];
  await engine.apply('s', rows.map(row => ({ table: 'tasks', id: row._id, version: 1, row })));
  const stored = String(db.prepare(`SELECT body FROM ${sqliteIdentifier(sqliteTableName('tasks'))} WHERE id='a'`).get()!.body);
  expect(JSON.parse(stored)).toEqual({ _id: null, name: null, due: null, done: null, data: null, extra: { untouched: 'value' } });
  expect(stored.length).toBeLessThan(150);
  const result = await engine.page('s', plan);
  expect(result.rows).toEqual(rows); expect(Object.keys(result.rows[0]!)).toEqual(Object.keys(rows[0]!));
  expect((await engine.page('s', { ...plan, select: ['name', 'due', 'done', 'data'] })).rows).toEqual(rows.map(({ extra, ...row }: any) => row));
  expect((await engine.viewPage('s', 'all', plan, 0)).rows).toEqual(rows);
});

it('ingests mixed-table encoded frames natively while decoding only search projections', async () => {
  const { engine, db } = fixture(); await engine.open('owner');
  const task = { _id: 'a', name: 'İ CAFÉ 東京', data: [1, null, { flag: false }], done: false, due: 1 };
  const assignment = { _id: 'x', task: 'a', member: 'm' };
  await engine.importScope('s', (async function* () { yield JSON.stringify([
    { table: 'assignments', id: 'x', version: 1, row: assignment }, { table: 'tasks', id: 'a', version: 1, row: task },
  ]); })());
  expect(engine.takeProfile()).toMatchObject({ nativeFrames: 1 });
  expect(db.prepare("SELECT count(*) n FROM main.gv_input_frame").get()!.n).toBe(0);
  expect((await engine.page('s', { ...plan, search: 'i\u0307 café' })).rows).toEqual([task]);
  expect((await engine.page('s', { table: 'assignments', order: [{ field: '_id', direction: 'asc' }], textOrder: 'binary', limit: 2 })).rows).toEqual([assignment]);
});
it('preserves invalid-frame rollback, finite-number and depth admission on the native wire path', async () => {
  const { engine, db } = fixture(); await engine.open('owner');
  await expect(engine.importScope('s', (async function* () {
    yield JSON.stringify([write('a', 1)]);
    yield JSON.stringify([{ ...write('b', 2), row: { _id: 'b', done: 'wrong' } }]);
  })())).rejects.toThrow('field type');
  expect((await engine.page('s', plan)).rows).toEqual([]);
  const nested = '['.repeat(40) + '0' + ']'.repeat(40);
  await expect(engine.importScope('deep', (async function* () { yield '[{"table":"tasks","id":"a","version":1,"row":{"_id":"a","data":' + nested + '}}]'; })())).rejects.toThrow('structure');
  await expect(engine.importScope('infinite', (async function* () { yield '[{"table":"tasks","id":"a","version":1,"row":{"_id":"a","extra":{"n":1e999}}}]'; })())).rejects.toThrow('Nonfinite');
  expect(db.prepare("SELECT count(*) n FROM main.gv_input_frame").get()!.n).toBe(0);
});

it('owns immutable schema metadata and bounds compiled import plans across entities', async () => {
  const { host } = fixture();
  const supplied = Object.fromEntries(Array.from({ length: 6 }, (_, index) => ['entity' + index, { key: '_id', fields: { _id: { kind: 'text' }, value: { kind: 'number' } }, indexes: [] }])) as SQLiteTypedSchema;
  const engine = new SQLiteTypedReplicaEngine(host, supplied);
  (supplied.entity0!.fields.value as any).kind = 'text';
  expect(Object.isFrozen(engine.schema.entity0!.fields)).toBe(true);
  expect(Reflect.set(engine.schema.entity0!.fields.value!, 'kind', 'text')).toBe(false);
  await engine.open('owner');
  await engine.importScope('s', (async function* () {
    for (let index = 0; index < 6; index++) {
      expect(engine.stats().planEntries).toBeLessThanOrEqual(4);
      expect(engine.stats().planBytes).toBeLessThanOrEqual(262144);
      yield JSON.stringify([{ table: 'entity' + index, id: 'a', version: 1, row: { _id: 'a', value: index } }]);
    }
  })());
  expect(engine.takeProfile().nativeFrames).toBe(6);
  expect(engine.stats().planEntries).toBeLessThanOrEqual(4);
  expect(engine.stats().planBytes).toBeGreaterThan(0);
  expect((await engine.page('s', { table: 'entity0', order: [{ field: 'value', direction: 'asc' }], limit: 2 })).rows).toEqual([{ _id: 'a', value: 0 }]);
});

it.each([false,true])('reuses one bounded private ingress backing store and clears settled work (rollback=%s)',async rollback=>{
 const {host}=fixture(),buffers:Uint8Array[]=[];
 const instrumented={...host,async runAsync(sql:string,...params:unknown[]){
  if(sql.startsWith('WITH source')&&params[0] instanceof Uint8Array)buffers.push(params[0]);
  return host.runAsync(sql,...params);
 }};
 const engine=new SQLiteTypedReplicaEngine(instrumented,schema);await engine.open('owner');
 const source=new TextEncoder().encode(JSON.stringify([write('a',1,'owned café 東京')])),before=source.slice();
 const second=new TextEncoder().encode(JSON.stringify([write('b',2)]));
 const operation=engine.importScope('s',(async function*(){yield source;yield second;if(rollback)yield new Uint8Array([128]);})());
 if(rollback)await expect(operation).rejects.toThrow('UTF-8');else await operation;
 expect(source).toEqual(before);expect(source.byteLength).toBe(before.byteLength);
 expect(buffers).toHaveLength(2);expect(buffers[0]!.buffer).toBe(buffers[1]!.buffer);
 for(const privateCopy of buffers)expect(privateCopy.every(byte=>byte===0)).toBe(true);
 expect(engine.takeProfile().copiedIngressBytes).toBeGreaterThanOrEqual(before.byteLength+second.byteLength);
 expect((await engine.page('s',plan)).rows.map(row=>row._id)).toEqual(rollback?[]:['a','b']);
 expect(engine.stats()).toMatchObject({pendingOperations:0,pendingBytes:0,ingressEntries:1,ingressBytes:131072,ingressMaxBytes:131072});
});

it('constrains indexed pages to an exact native membership and fences stale generations before reading rows',async()=>{
 const {engine}=fixture();await engine.open('owner');await engine.apply('s',[write('a',1),write('b',2),write('c',3),write('d',4)]);
 const header={signature:'selected',entity:'tasks',key:'_id',kind:'replica' as const,completeness:'complete' as const,source:'server' as const};
 await engine.windowBegin('s',header,'one');const progress=await engine.windowAppend('s','selected','one',0,JSON.stringify({ids:['b','d']}));await engine.windowPublish('s','selected',progress);
 const constrained={...plan,membership:{signature:'selected',generation:'one',requireComplete:true},limit:1};
 const first=await engine.page('s',constrained);expect(first.rows.map(row=>row._id)).toEqual(['b']);expect((await engine.page('s',{...constrained,after:first.cursor!})).rows.map(row=>row._id)).toEqual(['d']);
 expect((await engine.viewPage('s','filtered',constrained,1)).rows.map(row=>row._id)).toEqual(['d']);expect((await engine.viewPage('s','filtered',constrained,0)).total).toBe(2);
 await expect(engine.page('other',constrained)).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await expect(engine.page('s',{...constrained,table:'assignments',order:[{field:'_id',direction:'asc'}],textOrder:'binary'})).rejects.toMatchObject({code:'INVALID_ROW'});
 await engine.windowBegin('s',{...header,completeness:'partial'},'two');const next=await engine.windowAppend('s','selected','two',0,JSON.stringify({ids:['a']}));await engine.windowPublish('s','selected',next);
 await expect(engine.page('s',{...constrained,after:first.cursor!})).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await expect(engine.page('s',{...constrained,membership:{...constrained.membership,generation:'two'}})).rejects.toMatchObject({code:'PAGE_CHANGED'});
});

it('keeps logical membership constraints when rows live in an activated bootstrap alias',async()=>{
 const {engine}=fixture();await engine.open('owner');await engine.beginBootstrap('s','bulk');const state=await engine.appendBootstrap('s','bulk',0,JSON.stringify([write('a',1),write('b',2),write('c',3)]));await engine.finishBootstrap('s','bulk',{pages:state.nextPage,rows:state.rows,root:state.root});
 const header={signature:'member-set',entity:'tasks',key:'_id',kind:'replica' as const,completeness:'complete' as const,source:'server' as const};await engine.windowBegin('s',header,'one');const progress=await engine.windowAppend('s','member-set','one',0,JSON.stringify({ids:['b']}));await engine.windowPublish('s','member-set',progress);
 const memberPlan={...plan,membership:{signature:'member-set',generation:'one'}};expect((await engine.page('s',memberPlan)).rows.map(row=>row._id)).toEqual(['b']);expect((await engine.viewPage('s','aliased-members',memberPlan)).rows.map(row=>row._id)).toEqual(['b']);
});
