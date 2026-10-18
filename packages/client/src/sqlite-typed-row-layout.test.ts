import { afterEach, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteTypedReplicaEngine } from './sqlite-typed-engine.js';
import { sqliteIdentifier, sqliteTableName } from './sqlite-typed-schema.js';
import { sqliteTypedReplicaStorage } from './sqlite-typed-row-layout.js';
import { SQLiteLocalReplicaStorage, type SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { compileSQLiteTypedPage } from './sqlite-typed-query.js';
import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';
const schema: SQLiteTypedSchema = { tasks: { key: '_id', fields: { _id: { kind: 'text' }, name: { kind: 'text' }, due: { kind: 'number' }, done: { kind: 'boolean' } }, indexes: [{ columns: ['due'] }], search: ['name'], counts: [{ columns: ['done'] }] } };
const connections: DatabaseSync[] = [];
afterEach(() => { for (const db of connections.splice(0)) db.close(); });
function fixture() {
  const db = new DatabaseSync(':memory:'); connections.push(db);
  const host: SQLiteReplicaDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...params) { return db.prepare(sql).run(...params as any[]); },
    async getFirstAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).get(...params as any[]) as T ?? null; },
    async getAllAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).all(...params as any[]) as T[]; },
    async withTransactionAsync(run) { db.exec('BEGIN'); try { await run(); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; } },
  };
  return { db, host, storage: sqliteTypedReplicaStorage(host, schema) };
}
const empty = { entities: {}, liveQueries: {} };
const cursor = (revision: number, epoch = 'e') => ({ revision, epoch });
it('stores the canonical corpus only in typed tables and serves the shared adapter through a view', async () => {
  const { db, storage } = fixture();
  await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', name: 'Café 東京', due: 2, done: false }, b: { _id: 'b', name: 'Second', due: 1, done: true } } }, liveQueries: {}, cursor: cursor(1) }, 's');
  expect(db.prepare("SELECT type FROM sqlite_master WHERE name='_gonvex_replica_entities'").get()!.type).toBe('view');
  const plan = compileSQLiteTypedPage(schema, 's', { table: 'tasks', order: [{ field: 'due', direction: 'asc' }], search: 'café', limit: 50 });
  expect(db.prepare(plan.sql).all(...plan.bindings).map(row => row.id)).toEqual(['a']);
  expect((await storage.loadWorkingSet('s', { maxRows: 1, maxBytes: 4096 }))!.residentPartialTables).toEqual(['tasks']);
  expect((await storage.loadEntityRows('s', 'tasks', ['a']))[0]!.row.name).toBe('Café 東京');
  expect(await storage.loadEntityRows('other', 'tasks', ['a'])).toEqual([]);
});
it('preserves field-level authority, canonical tombstones, epoch fences and adjacent change receipts', async () => {
  const { storage, host } = fixture();
  const apply = (revision: number, fields: Record<string, any>, operation: 'insert' | 'update' | 'delete' = 'update', epoch = 'e') => storage.applyTransaction({ cursor: cursor(revision, epoch), changes: [{ entity: 'tasks', id: 'a', operation, ...(operation === 'delete' ? {} : { newValue: fields }) }] }, empty, 's');
  await apply(10, { _id: 'a', name: 'new' }, 'insert');
  await apply(5, { _id: 'a', name: 'old', due: 3 });
  expect((await storage.loadEntityRows('s', 'tasks', ['a']))[0]!.row).toEqual({ _id: 'a', name: 'new', due: 3 });
  const receipt = await apply(11, {}, 'delete');
  expect(receipt).toMatchObject({ previousSequence: 2, sequence: 3, changes: { entities: { tasks: { a: null } } } });
  await apply(9, { _id: 'a', name: 'stale' }, 'insert');
  expect(await storage.loadEntityRows('s', 'tasks', ['a'])).toEqual([]);
  await apply(1, { _id: 'a', name: 'next' }, 'insert', 'next');
  const reopened = sqliteTypedReplicaStorage(host, schema);
  await expect(reopened.applyTransaction({ cursor: cursor(12), changes: [] }, empty, 's')).rejects.toThrow('obsolete');
  expect((await reopened.loadEntityRows('s', 'tasks', ['a']))[0]!.row.name).toBe('next');
});
it('rolls back rows, FTS, counts and metadata on a bad row in the same commit', async () => {
  const { db, storage } = fixture();
  await storage.replaceSnapshot({ ...empty, entities: { tasks: { a: { _id: 'a', name: 'original', due: 1, done: false } } } }, 's');
  await expect(storage.applyTransaction({ cursor: cursor(2), changes: [
    { entity: 'tasks', id: 'a', operation: 'update', newValue: { name: 'bad replacement', done: true } },
    { entity: 'tasks', id: 'b', operation: 'insert', newValue: { _id: 'b', due: 'wrong type' } },
  ] }, empty, 's')).rejects.toThrow('type');
  expect((await storage.loadEntityRows('s', 'tasks', ['a']))[0]!.row.name).toBe('original');
  const query = compileSQLiteTypedPage(schema, 's', { table: 'tasks', order: [{ field: 'due', direction: 'asc' }], search: 'original', limit: 50 });
  expect(db.prepare(query.sql).all(...query.bindings)).toHaveLength(1);
  expect((await storage.readChanges('s', 1)).sequence).toBe(1);
  expect(db.prepare('PRAGMA integrity_check').get()!.integrity_check).toBe('ok');
});
it('serves portable reducer reads through the same typed canonical values', async () => {
  const { storage } = fixture();
  await storage.replaceSnapshot({ ...empty, entities: { tasks: { a: { _id: 'a', name: 'first', due: 1 }, b: { _id: 'b', name: 'second', due: 2 } } } }, 's');
  const result = await storage.withReadView('s', { tasks: { key: '_id', complete: true } }, view => view.select({ table: 'tasks', where: { column: 'due', op: 'eq', value: 2 } }));
  expect(result.rows).toEqual([{ _id: 'b', name: 'second', due: 2 }]);
  expect(result.complete).toBe(true);
});
it('rejects schema changes and generic SQLite databases without deleting pending data', async () => {
  const { storage, host } = fixture();
  await storage.replaceSnapshot({ ...empty, entities: { tasks: { a: { _id: 'a', name: 'keep' } } } }, 's');
  const incompatible = sqliteTypedReplicaStorage(host, { ...schema, tasks: { ...schema.tasks!, search: undefined } });
  await expect(incompatible.load('s')).rejects.toThrow('migration');
  expect((await storage.loadEntityRows('s', 'tasks', ['a']))[0]!.row.name).toBe('keep');
  const other = fixture(); const legacy = new SQLiteLocalReplicaStorage(other.host);
  await legacy.replaceSnapshot({ ...empty, entities: { tasks: { a: { _id: 'a', name: 'legacy' } } } }, 's');
  await expect(other.storage.load('s')).rejects.toThrow('verified migration');
  expect((await legacy.load('s'))!.entities.tasks!.a!.name).toBe('legacy');
});
it('clears only the selected scope, including tombstones, FTS and legacy lookup keys', async () => {
  const { storage, db } = fixture();
  for (const scope of ['a', 'b']) await storage.replaceSnapshot({ ...empty, entities: { tasks: { r: { _id: 'r', name: 'needle', due: 1 } } } }, scope);
  await storage.clear('a');
  expect(await storage.loadEntityRows('a', 'tasks', ['r'])).toEqual([]);
  expect(await storage.loadEntityRows('b', 'tasks', ['r'])).toHaveLength(1);
  expect(db.prepare("SELECT name FROM sqlite_master WHERE name='_gonvex_replica_lookup'").get()).toBeUndefined();
});

it('serves bounded native positions from canonical authority and invalidates on writes, resets and scope changes', async () => {
  const { storage, db } = fixture();
  const view = { table: 'tasks', order: [{ field: 'due', direction: 'asc' }], select: ['name', 'due'], limit: 2 } as const;
  const entities = { tasks: Object.fromEntries(Array.from({ length: 300 }, (_, n) => [String(n), { _id: String(n), name: 'Task ' + n, due: n }])) };
  await storage.replaceSnapshot({ ...empty, entities, cursor: cursor(1) }, 's');
  const page = await storage.readViewPage('s', 'grid', view, 290);
  expect(page).toMatchObject({ total: 300, offset: 290, nextOffset: 292 });
  expect(page.rows.map(row => row._id)).toEqual(['290', '291']);
  expect((await storage.readViewPage('other', 'grid', view, 0)).total).toBe(0);
  await storage.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: '290', operation: 'update', newValue: { due: -1 } }] }, empty, 's');
  expect((await storage.readViewPage('s', 'grid', view, 0)).rows[0]?._id).toBe('290');
  await storage.clear('s');
  expect((await storage.readViewPage('s', 'grid', view, 0)).total).toBe(0);
  await storage.replaceSnapshot({ ...empty, entities: { tasks: { fresh: { _id: 'fresh', name: 'Fresh', due: 1 } } }, cursor: cursor(1, 'next') }, 's');
  expect((await storage.readViewPage('s', 'grid', view, 0)).rows).toEqual([{ _id: 'fresh', name: 'Fresh', due: 1 }]);
  expect(db.prepare('PRAGMA integrity_check').get()!.integrity_check).toBe('ok');
});

it('upgrades a legacy canonical body view and fences its old decoder without losing durable intent receipts', async () => {
  const { db, host, storage } = fixture();
  const a = { _id: 'a', name: 'legacy', due: 1, done: false };
  await storage.replaceSnapshot({ ...empty, entities: { tasks: { a } } }, 's');
  const oldOwner = new SQLiteTypedReplicaEngine(host, schema); await oldOwner.open('old');
  const b = { _id: 'b', name: 'pending', due: 2, done: true };
  const writes = [{ table: 'tasks', id: 'b', version: 1, row: b }], intent = { id: 'durable', payload: { create: 'b' } };
  const receipt = await oldOwner.apply('s', writes, intent);
  for (const row of [a, b]) db.prepare(`UPDATE ${sqliteIdentifier(sqliteTableName('tasks'))} SET body=? WHERE id=?`).run(JSON.stringify(row), row._id);
  db.prepare('UPDATE gv_storage_meta SET schema=?').run(JSON.stringify({ format: 4, tables: schema }));
  db.prepare('UPDATE gv_canonical_layout SET signature=?').run(JSON.stringify({ format: 1, schema }));
  const upgraded = sqliteTypedReplicaStorage(host, schema);
  expect((await upgraded.loadEntityRows('s', 'tasks', ['a', 'b'])).map(value => value.row)).toEqual([a, b]);
  await expect(oldOwner.page('s', { table: 'tasks', order: [{ field: 'due', direction: 'asc' }], limit: 2 })).rejects.toMatchObject({ code: 'FENCED' });
  const next = new SQLiteTypedReplicaEngine(host, schema); await next.open('new');
  expect(await next.apply('s', writes, intent)).toEqual(receipt);
  expect((await next.outbox('s')).map(value => value.id)).toEqual(['durable']);
  await expect(upgraded.loadEntityRows('s','tasks',['a'])).rejects.toMatchObject({code:'FENCED'});
  const currentStorage = sqliteTypedReplicaStorage(host,schema);
  await currentStorage.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { name: 'updated' } }] }, empty, 's');
  expect((await currentStorage.loadEntityRows('s', 'tasks', ['a']))[0]!.row).toEqual({ ...a, name: 'updated' });
  expect(JSON.parse(String(db.prepare(`SELECT body FROM ${sqliteIdentifier(sqliteTableName('tasks'))} WHERE id='a'`).get()!.body))).toMatchObject({ name: null, due: null });
});

it('reads pre-existing typed imports natively without a scalar-copy index', async () => {
  const { db, host } = fixture();
  const engine = new SQLiteTypedReplicaEngine(host, schema); await engine.open('importer');
  await engine.importScope('s', (async function* () {
    for (let start=0;start<600;start+=50) yield JSON.stringify(Array.from({length:50},(_,n)=>{
      const id=String(start+n);return {table:'tasks',id,version:1,row:{_id:id,name:'Imported '+id,due:start+n,done:false}};
    }));
  })());
  const reads: number[] = [];
  const guarded: SQLiteReplicaDatabase = {...host, async getAllAsync<T>(sql: string,...params: unknown[]) {
    const result = await host.getAllAsync<T>(sql,...params); reads.push(result.length); return result;
  }};
  const storage = sqliteTypedReplicaStorage(guarded,schema);
  const result = await storage.withReadView('s',{tasks:{key:'_id',complete:true}},view=>view.select({table:'tasks',where:{column:'due',op:'eq',value:599}}));
  expect(result).toEqual({rows:[{_id:'599',name:'Imported 599',due:599,done:false}],complete:true});
  expect(Math.max(...reads)).toBeLessThanOrEqual(1);
  expect(db.prepare("SELECT name FROM sqlite_master WHERE name='_gonvex_replica_lookup'").get()).toBeUndefined();
  // Upgrade an older installation with a partial legacy scalar-copy index.
  db.prepare('UPDATE gv_canonical_layout SET signature=?').run(JSON.stringify({format:2,schema}));
  db.exec("CREATE TABLE _gonvex_replica_lookup(id TEXT PRIMARY KEY); INSERT INTO _gonvex_replica_lookup VALUES('stale')");
  const repaired = sqliteTypedReplicaStorage(host,schema);
  expect((await repaired.withReadView('s',{tasks:{key:'_id',complete:true}},view=>view.select({table:'tasks',where:{column:'due',op:'eq',value:599}}))).rows).toEqual(result.rows);
  expect(await engine.integrity()).toBe('ok');
});

it('invalidates typed continuations on canonical commits and fences a replaced canonical owner', async () => {
  const { host } = fixture();
  const engine = new SQLiteTypedReplicaEngine(host,schema); await engine.open('first');
  await engine.apply('s',[{table:'tasks',id:'a',version:1,row:{_id:'a',due:1}},{table:'tasks',id:'b',version:1,row:{_id:'b',due:2}}]);
  const storage = sqliteTypedReplicaStorage(host,schema);
  const plan = {table:'tasks',order:[{field:'due',direction:'asc' as const}],limit:1};
  const first = await engine.page('s',plan); expect(first.cursor).toBeDefined();
  await storage.applyTransaction({cursor:cursor(1),changes:[{entity:'tasks',id:'b',operation:'update',newValue:{due:0}}]},empty,'s');
  await expect(engine.page('s',{...plan,after:first.cursor!})).rejects.toMatchObject({code:'PAGE_CHANGED'});
  expect((await engine.page('s',plan)).rows[0]!._id).toBe('b');
  const next = new SQLiteTypedReplicaEngine(host,schema); await next.open('replacement');
  await expect(storage.applyTransaction({cursor:cursor(2),changes:[{entity:'tasks',id:'b',operation:'update',newValue:{due:99}}]},empty,'s')).rejects.toMatchObject({code:'FENCED'});
  await expect(storage.withReadView('s',{tasks:{key:'_id',complete:true}},view=>view.select({table:'tasks'}))).rejects.toMatchObject({code:'FENCED'});
  expect((await next.page('s',plan)).rows[0]!.due).toBe(0);
});

it('rolls canonical rows and both commit clocks back when the shared clock is exhausted', async () => {
  const { db, storage } = fixture();
  await storage.replaceSnapshot({...empty,entities:{tasks:{a:{_id:'a',due:1}}}},'s');
  const before = db.prepare("SELECT value FROM _gonvex_replica_meta WHERE scope='s' AND key='sequence'").get()!.value;
  db.prepare('UPDATE gv_storage_meta SET sequence=?').run(Number.MAX_SAFE_INTEGER);
  await expect(storage.applyTransaction({cursor:cursor(1),changes:[{entity:'tasks',id:'a',operation:'update',newValue:{due:99}}]},empty,'s')).rejects.toMatchObject({code:'FENCED'});
  expect((await storage.loadEntityRows('s','tasks',['a']))[0]!.row.due).toBe(1);
  expect(db.prepare("SELECT value FROM _gonvex_replica_meta WHERE scope='s' AND key='sequence'").get()!.value).toBe(before);
  expect(db.prepare('SELECT sequence FROM gv_storage_meta').get()!.sequence).toBe(Number.MAX_SAFE_INTEGER);
});

it.each([false,true])('shares a published physical bootstrap scope with the canonical adapter, installed first=%s', async installedFirst => {
  const { db, host } = fixture();
  const engine = new SQLiteTypedReplicaEngine(host,schema); await engine.open('owner');
  const storage = sqliteTypedReplicaStorage(host,schema);
  const plan = {table:'tasks',order:[{field:'due',direction:'asc' as const}],limit:10};
  if (installedFirst) expect((await storage.readViewPage('s','grid',plan)).rows).toEqual([]);
  await engine.beginBootstrap('s','generation');
  const state = await engine.appendBootstrap('s','generation',0,JSON.stringify([{table:'tasks',id:'a',version:1,row:{_id:'a',name:'Bootstrapped café',due:2,done:false}}]));
  await engine.finishBootstrap('s','generation',{pages:state.nextPage,rows:state.rows,root:state.root});
  const expected = {_id:'a',name:'Bootstrapped café',due:2,done:false};
  expect((await storage.loadEntityRows('s','tasks',['a']))[0]!.row).toEqual(expected);
  expect((await storage.withReadView('s',{tasks:{key:'_id',complete:true}},view=>view.select({table:'tasks',where:{column:'due',op:'eq',value:2}}))).rows).toEqual([expected]);
  expect((await storage.readViewPage('s','grid',plan)).rows).toEqual([expected]);
  expect((await storage.readViewPage('other','grid',plan)).rows).toEqual([]);
  await storage.applyTransaction({cursor:cursor(1),changes:[{entity:'tasks',id:'a',operation:'update',newValue:{due:1,name:'Updated café'}}]},empty,'s');
  expect((await engine.page('s',{...plan,search:'updated'})).rows[0]!.due).toBe(1);
  expect((await storage.withReadView('s',{tasks:{key:'_id',complete:true}},view=>view.select({table:'tasks',where:{column:'due',op:'eq',value:1}}))).rows[0]!.name).toBe('Updated café');
  const physical = db.prepare('SELECT physical FROM gv_scope_alias WHERE scope=?').get('s')!.physical;
  expect(db.prepare(`SELECT scope FROM ${sqliteIdentifier(sqliteTableName('tasks'))}`).all()).toEqual([{scope:physical}]);
  await storage.applyTransaction({cursor:cursor(2),changes:[{entity:'tasks',id:'a',operation:'delete'}]},empty,'s');
  expect((await engine.page('s',plan)).rows).toEqual([]);
  expect(await storage.loadEntityRows('s','tasks',['a'])).toEqual([]);
  expect(db.prepare("SELECT name FROM sqlite_master WHERE name='_gonvex_replica_lookup'").get()).toBeUndefined();
  expect(await engine.integrity()).toBe('ok');
});
