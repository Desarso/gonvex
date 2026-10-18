import { afterEach, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteWindowStore, type SQLiteWindowHeader } from './sqlite-window-store.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
const databases: DatabaseSync[] = [];
afterEach(() => { for (const db of databases.splice(0)) db.close(); });
function fixture() {
  const native = new DatabaseSync(':memory:'); databases.push(native);
  const database: SQLiteReplicaDatabase = {
    async execAsync(sql) { native.exec(sql); }, async runAsync(sql, ...values) { return native.prepare(sql).run(...values as any[]); },
    async getFirstAsync<T>(sql: string, ...values: unknown[]) { return native.prepare(sql).get(...values as any[]) as T ?? null; },
    async getAllAsync<T>(sql: string, ...values: unknown[]) { return native.prepare(sql).all(...values as any[]) as T[]; },
    async withTransactionAsync(run) { native.exec('BEGIN'); try { await run(); native.exec('COMMIT'); } catch (e) { native.exec('ROLLBACK'); throw e; } },
  };
  return { native, database, store: new SQLiteWindowStore(database) };
}
const header: SQLiteWindowHeader = { signature: 'tasks', entity: 'tasks', key: '_id', kind: 'replica', completeness: 'complete', source: 'server', cursor: { epoch: 'e', revision: 1 } };
it('stages memberships invisibly, preserves duplicate positions and certificate-only hashes, and resumes across owners', async () => {
  const { store, database } = fixture(); await store.begin('s', header, 'one');
  const frame = JSON.stringify({ ids: ['a', 'b', 'a'], hashes: { a: 'aaa', b: 'bbb', 'hash-only': 'hhh' } });
  const state = await store.append('s', 'tasks', 'one', 0, frame);
  expect(await store.header('s', 'tasks')).toBeUndefined();
  const next = new SQLiteWindowStore(database);
  expect(await next.begin('s', header, 'one')).toEqual(state);
  expect(await next.append('s', 'tasks', 'one', 0, frame)).toEqual(state);
  await expect(next.append('s', 'tasks', 'one', 0, JSON.stringify({ ids: ['changed'] }))).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  await next.publish('s', 'tasks', state);
  expect(await next.header('s', 'tasks')).toMatchObject({ rows: 3, uniqueRows: 2, header });
  expect(await next.page('s', 'tasks', 'one', 1, 2)).toEqual({ generation: 'one', total: 3, ids: ['b', 'a'], hashes: { a: 'aaa', b: 'bbb' }, nextOffset: null });
  expect(await next.hashPage('s', 'tasks', 'one')).toEqual([{ id: 'a', hash: 'aaa' }, { id: 'b', hash: 'bbb' }, { id: 'hash-only', hash: 'hhh' }]);
  expect(await next.header('other', 'tasks')).toBeUndefined();
});
it('refuses bad manifests, late frames and obsolete cursors without disturbing the published snapshot', async () => {
  const { store } = fixture(); await store.begin('s', header, 'one');
  const first = await store.append('s', 'tasks', 'one', 0, '{"ids":["a"]}'); await store.publish('s', 'tasks', first);
  await store.begin('s', { ...header, cursor: { epoch: 'e', revision: 2 } }, 'two');
  await expect(store.append('s', 'tasks', 'two', 1, '{"ids":["b"]}')).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  const second = await store.append('s', 'tasks', 'two', 0, '{"ids":["b"]}');
  await expect(store.publish('s', 'tasks', { ...second, root: 'wrong' })).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  expect((await store.page('s', 'tasks', 'one', 0)).ids).toEqual(['a']);
  await store.publish('s', 'tasks', second);
  await expect(store.page('s', 'tasks', 'one', 0)).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  await expect(store.publish('s', 'tasks', first)).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  await expect(store.append('s', 'tasks', 'two', 1, '{"ids":["c"]}')).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
});
it('reads deep positions with bounded pages and charges unique membership across duplicate-bearing frames', async () => {
  const { store, native } = fixture(); await store.begin('s', header, 'one');
  let state;
  for (let page = 0; page < 40; page++) state = await store.append('s', 'tasks', 'one', page, JSON.stringify({ ids: Array.from({ length: 128 }, (_, n) => String((page * 128 + n) % 4000)) }));
  await store.publish('s', 'tasks', state!);
  const metadata = await store.header('s', 'tasks'); expect(metadata).toMatchObject({ rows: 5120, uniqueRows: 4000 });
  expect(JSON.stringify(metadata).length).toBeLessThan(1024);
  expect((await store.page('s', 'tasks', 'one', 5000, 10)).ids).toEqual(Array.from({ length: 10 }, (_, n) => String(1000 + n)));
  const explanation = native.prepare('EXPLAIN QUERY PLAN SELECT id FROM gv_window_members WHERE scope=? AND signature=? AND generation=? AND position>=? AND position<? ORDER BY position').all('s', 'tasks', 'one', 5000, 5010).map(row => row.detail).join(' ');
  expect(explanation).toContain('PRIMARY KEY'); expect(explanation).not.toContain('TEMP');
});
it('collects obsolete generations in bounded chunks and never reactivates partly collected data', async () => {
  const { store, native } = fixture(); await store.begin('s', header, 'old');
  let old;
  for (let page = 0; page < 4; page++) old = await store.append('s', 'tasks', 'old', page, JSON.stringify({ ids: Array.from({ length: 128 }, (_, n) => String(page * 128 + n)) }));
  await store.publish('s', 'tasks', old!);
  await expect(store.collect('s', 'tasks', 'old')).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  const next = await store.begin('s', { ...header, cursor: { epoch: 'e', revision: 2 } }, 'new'); await store.publish('s', 'tasks', next);
  expect(await store.collect('s', 'tasks', 'old')).toEqual({ remaining: true });
  expect(native.prepare('SELECT count(*) n FROM gv_window_members WHERE generation=\'old\'').get()!.n).toBe(256);
  await expect(store.publish('s', 'tasks', old!)).rejects.toMatchObject({ code: 'PAGE_CHANGED' });
  expect(await store.collect('s', 'tasks', 'old')).toEqual({ remaining: false });
  expect((await store.header('s', 'tasks'))!.generation).toBe('new');
  expect(await store.collect('s', 'tasks', 'old')).toEqual({ remaining: false });
});
it('refuses object-sized headers and frames before they can become persistent window graphs', async () => {
  const { store } = fixture();
  await expect(store.begin('s', { ...header, ids: ['no'] } as any, 'bad')).rejects.toThrow('cannot contain');
  await store.begin('s', header, 'one');
  await expect(store.append('s', 'tasks', 'one', 0, JSON.stringify({ ids: new Array(257).fill('a') }))).rejects.toThrow('Invalid');
  await expect(store.append('s', 'tasks', 'one', 0, JSON.stringify({ ids: ['x'.repeat(40000)] }))).rejects.toThrow('budget');
  expect(await store.begin('s', header, 'one')).toMatchObject({ pages: 0, rows: 0 });
});

it('atomically mutates bounded hashes and memberships, preserves offset pages across holes, and deduplicates exact replay',async()=>{
 const {store,database,native}=fixture();const {replicaSumDigest}=await import('./replica-integrity.js');const {createHash}=await import('node:crypto');
 const sha=(value:string)=>createHash('sha256').update(value).digest('hex'),initial={a:sha('a'),b:sha('b'),c:sha('c')};
 await store.begin('s',{...header,aggregateDigest:replicaSumDigest(initial)},'one');const staged=await store.append('s','tasks','one',0,JSON.stringify({ids:['a','b','c'],hashes:initial}));await store.publish('s','tasks',staged);
 await database.execAsync('CREATE TABLE canonical_probe(id TEXT PRIMARY KEY,value INTEGER)');let calls=0;const final={a:initial.a,c:sha('updated c'),d:sha('d')};
 const input={scope:'s',signature:'tasks',generation:'one',cursor:{epoch:'e',revision:2},upserts:[{id:'c',hash:final.c},{id:'d',hash:final.d}],deleted:['b'],digest:replicaSumDigest(final)};
 const mutate=async()=>{calls++;await database.runAsync('INSERT INTO canonical_probe VALUES(?,?)','c',2);};
 expect(await store.applyDelta(input,mutate)).toEqual({changed:true,rows:3});expect(calls).toBe(1);expect((await store.page('s','tasks','one',1,1)).ids).toEqual(['c']);expect((await store.page('s','tasks','one',2,1)).ids).toEqual(['d']);expect((await store.hashPage('s','tasks','one')).map(row=>row.id)).toEqual(['a','c','d']);
 const reopened=new SQLiteWindowStore(database);expect(await reopened.applyDelta(input,mutate)).toEqual({changed:false,rows:3});expect(calls).toBe(1);expect((await reopened.header('s','tasks'))?.header.aggregateDigest).toBe(input.digest);
 await expect(reopened.applyDelta({...input,deleted:['a']},mutate)).rejects.toMatchObject({code:'PAGE_CHANGED'});expect(calls).toBe(1);
 const invalid={...input,cursor:{epoch:'e',revision:3},upserts:[],deleted:['a'],digest:replicaSumDigest({c:final.c,d:final.d})};
 await expect(reopened.applyDelta({...invalid,digest:'sum128-v1:00000000000000000000000000000000'})).rejects.toThrow('integrity');expect((await reopened.page('s','tasks','one',0,10)).ids).toEqual(['a','c','d']);
 await expect(reopened.applyDelta(invalid,async()=>{await database.runAsync('INSERT INTO canonical_probe VALUES(?,?)','late',1);throw Error('quota surrogate');})).rejects.toThrow('quota surrogate');expect(native.prepare('SELECT * FROM canonical_probe').all()).toEqual([{id:'c',value:2}]);expect((await reopened.page('s','tasks','one',0,10)).ids).toEqual(['a','c','d']);
 expect(await reopened.applyDelta(invalid)).toEqual({changed:true,rows:2});expect((await reopened.page('s','tasks','one',0,10)).ids).toEqual(['c','d']);
});
it('bounds legacy certificate reconstruction pages and refuses incomplete or ambiguous memberships',async()=>{
 const {store,database}=fixture();const {replicaSumDigest}=await import('./replica-integrity.js');const {createHash}=await import('node:crypto');
 const hashes=Object.fromEntries(Array.from({length:300},(_,n)=>['r'+String(n).padStart(3,'0'),createHash('sha256').update(String(n)).digest('hex')]));await store.begin('s',{...header,aggregateDigest:replicaSumDigest(hashes)},'bulk');let state;
 for(let n=0;n<3;n++){const ids=Object.keys(hashes).slice(n*100,n*100+100);state=await store.append('s','tasks','bulk',n,JSON.stringify({ids,hashes:Object.fromEntries(ids.map(id=>[id,hashes[id]]))}));}await store.publish('s','tasks',state!);
 const removed=Object.fromEntries(Object.entries(hashes).filter(([id])=>id!=='r000'));const input={scope:'s',signature:'tasks',generation:'bulk',cursor:{epoch:'e',revision:2},upserts:[],deleted:['r000'],digest:replicaSumDigest(removed)};
 const original=database.getAllAsync.bind(database),sizes:number[]=[];database.getAllAsync=async<T>(sql:string,...args:unknown[])=>{const result=await original<T>(sql,...args);if(sql.startsWith('SELECT id,hash FROM gv_window_hashes'))sizes.push(result.length);return result;};
 expect(await store.applyDelta(input)).toEqual({changed:true,rows:299});expect(Math.max(...sizes)).toBe(128);expect((await store.page('s','tasks','bulk',298,1)).ids).toEqual(['r299']);
 await expect(store.applyDelta({...input,cursor:{epoch:'other',revision:3}})).rejects.toMatchObject({code:'PAGE_CHANGED'});await expect(store.applyDelta({...input,generation:'old'})).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await expect(store.applyDelta({...input,deleted:Array.from({length:129},(_,n)=>String(n))})).rejects.toThrow('Invalid native');await expect(store.applyDelta({...input,upserts:[{id:'a',hash:'bad'}]})).rejects.toThrow('hash');
});
