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
  expect(host.takeProfile()).toEqual({ ingressFoldCalls:0, ingressFoldUTF16Bytes:0, prepareMs: 0, prepares: 0, bindMs: 0, boundTextUTF16Bytes: 0, beginMs: 0, callbackMs: 0, commitMs: 0, rollbackMs: 0, commits: 0, rollbacks: 0 });
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

it('imports owned UTF-8 frames and preserves canonical fallback and transaction rollback', async () => {
  const { host } = fixture();
  const schema = { records: { key: '_id', fields: { _id: { kind: 'text' }, title: { kind: 'text' }, rank: { kind: 'number' } }, indexes: [{ columns: ['rank'] }], search: ['title'] } } as const;
  const engine = new SQLiteTypedReplicaEngine(host.database, schema); await engine.open('owner');
  const encode = (text: string) => new TextEncoder().encode(text);
  await engine.importScope('s', (async function* () {
    yield encode('[{"table":"records","id":"a","version":1,"row":{"_id":"a","title":"İSTANBUL café 東京 🔧","rank":1}}]');
    yield encode('[{"table":"records","id":"b","version":1,"row":{"_id":"b","title":"old","title":"Canonical duplicate-key value","rank":2}}]');
  })());
  expect(engine.takeProfile().nativeFrames).toBe(1);
  expect((await engine.page('s', { table: 'records', search: 'canonical', order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows[0]!.title).toBe('Canonical duplicate-key value');
  expect((await engine.page('s', { table: 'records', search: 'i̇stan', order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows[0]!._id).toBe('a');
  await expect(engine.importScope('invalid', (async function* () {
    yield encode('[{"table":"records","id":"a","version":1,"row":{"_id":"a","rank":1}}]');
    yield Uint8Array.from([91,34,237,160,128,34,93]);
  })())).rejects.toMatchObject({ code: 'INVALID_ROW' });
  expect((await engine.page('invalid', { table: 'records', order: [{ field: 'rank', direction: 'asc' }], limit: 50 })).rows).toEqual([]);
  expect(await host.database.getFirstAsync('SELECT count(*) n FROM main.gv_input_frame')).toEqual({ n: 0 });
  expect(await engine.integrity()).toBe('ok');
});

it('resumes native UTF-8 bootstrap frames with exact raw receipts and atomic duplicate rejection', async () => {
  const {host}=fixture();
  const schema={records:{key:'_id',fields:{_id:{kind:'text'},title:{kind:'text'},rank:{kind:'number'}},indexes:[{columns:['rank']}],search:['title']}} as const;
  const engine=new SQLiteTypedReplicaEngine(host.database,schema);await engine.open('one');await engine.beginBootstrap('s','token');
  const frame=(...ids:string[])=>JSON.stringify(ids.map(id=>({table:'records',id,version:1,row:{_id:id,title:'CAFÉ 東京 '+id,rank:1}})));
  const text=frame('a'),input=new TextEncoder().encode(text);
  const pending=engine.appendBootstrap('s','token',0,input);input.fill(0);
  const first=await pending;
  expect(await engine.appendBootstrap('s','token',0,text)).toEqual(first);
  expect(engine.takeProfile()).toMatchObject({nativeFrames:1,parseMs:0,reencodeMs:0});
  await expect(engine.appendBootstrap('s','token',1,frame('b','b'))).rejects.toMatchObject({code:'INVALID_ROW'});
  await expect(engine.appendBootstrapBatch('s','token',(async function*(){yield {page:1,encoded:frame('b')};yield {page:2,encoded:frame('a')};})())).rejects.toMatchObject({code:'INVALID_ROW'});
  expect(await engine.beginBootstrap('s','token')).toEqual(first);
  await expect(engine.appendBootstrap('s','token',0,frame('different'))).rejects.toMatchObject({code:'INTENT_CONFLICT'});
  const next=new SQLiteTypedReplicaEngine(host.database,schema);await next.open('two');
  const second=await next.appendBootstrap('s','token',1,new TextEncoder().encode(frame('b')));
  await next.finishBootstrap('s','token',{pages:second.nextPage,rows:second.rows,root:second.root});
  expect((await next.page('s',{table:'records',search:'café',order:[{field:'rank',direction:'asc'}],limit:50})).rows.map(row=>row._id)).toEqual(['a','b']);
  expect(await next.integrity()).toBe('ok');
});

it('drives resumable duplicate probes from each bounded frame with a scope/id index lookup',async()=>{
  const {host}=fixture();const probes:string[]=[];
  const database={...host.database,async getFirstAsync<T>(sql:string,...params:unknown[]){
    if(sql.includes("json_extract(w.value,'$.id')")){
      const plan=await host.database.getAllAsync<{detail:string}>('EXPLAIN QUERY PLAN '+sql,...params);
      probes.push(plan.map(row=>row.detail).join('\n'));
    }
    return host.database.getFirstAsync<T>(sql,...params);
  }};
  const schema={records:{key:'_id',fields:{_id:{kind:'text'},rank:{kind:'number'}},indexes:[]}} as const;
  const engine=new SQLiteTypedReplicaEngine(database,schema);await engine.open('owner');await engine.beginBootstrap('s','token');
  for(let page=0;page<3;page++)await engine.appendBootstrap('s','token',page,JSON.stringify(Array.from({length:50},(_,n)=>{const id=String(page*50+n);return {table:'records',id,version:1,row:{_id:id,rank:n}};})));
  expect(probes).toHaveLength(3);
  for(const plan of probes){
    expect(plan).toContain('scope=? AND id=?');
    expect(plan.indexOf('SCAN w VIRTUAL TABLE')).toBeLessThan(plan.indexOf('SEARCH t USING'));
  }
});

it('atomically commits canonical rows with replay receipts on the composed owner host',async()=>{
  const {sqliteOwnerTransactions}=await import('./sqlite-owner-transactions.js');
  const {sqliteTypedReplicaStorage}=await import('./sqlite-typed-row-layout.js');
  const {host}=fixture(),database=sqliteOwnerTransactions(host.database);
  const schema={records:{key:'_id',fields:{_id:{kind:'text'},rank:{kind:'number'}},indexes:[{columns:['rank']}]}} as const;
  const engine=new SQLiteTypedReplicaEngine(database,schema);await engine.open('one');
  const storage=sqliteTypedReplicaStorage(database,schema);await storage.loadWorkingSet('s',{maxRows:1,maxBytes:1024});
  const input={cursor:{epoch:'e',revision:1},changes:[{entity:'records',id:'a',operation:'insert' as const,newValue:{_id:'a',rank:1}}]};
  let executions=0;
  const write=()=>{executions++;return storage.applyTransaction(input,{entities:{},liveQueries:{}},'s');};
  const first=await engine.executeStorageOnce('s','request',input,write);
  expect(await engine.executeStorageOnce('s','request',input,write)).toEqual(first);expect(executions).toBe(1);
  await expect(engine.executeStorageOnce('s','request',{changed:true},write)).rejects.toMatchObject({code:'INTENT_CONFLICT'});
  await expect(engine.executeStorageOnce('s','abort',{},async()=>{
    await storage.applyTransaction({...input,cursor:{epoch:'e',revision:2},changes:[{entity:'records',id:'a',operation:'update',newValue:{rank:99}}]},{entities:{},liveQueries:{}},'s');
    throw new Error('outer abort');
  })).rejects.toThrow('outer abort');
  expect((await engine.page('s',{table:'records',order:[{field:'rank',direction:'asc'}],limit:1})).rows[0]!.rank).toBe(1);
  expect(await host.database.getFirstAsync('SELECT count(*) n FROM gv_owner_receipts WHERE id=?','abort')).toEqual({n:0});
  let voidCalls=0;
  for(let n=0;n<2;n++)expect(await engine.executeStorageOnce('s','void',{},async()=>{voidCalls++;})).toBeUndefined();
  expect(voidCalls).toBe(1);
  const next=new SQLiteTypedReplicaEngine(database,schema);await next.open('two');
  expect(await next.executeStorageOnce('s','request',input,async()=>{throw Error('must not repeat');})).toEqual(first);
  await expect(engine.executeStorageOnce('s','new',{},write)).rejects.toMatchObject({code:'FENCED'});
});

it('folds ASCII natively and uses the exact Unicode ingress fallback without normalization frames',async()=>{
  const native=fixture().host,portable=fixture().host;
  const schema={records:{key:'_id',fields:{_id:{kind:'text'},title:{kind:'text'},description:{kind:'text'},rank:{kind:'number'}},indexes:[{columns:['rank']}],search:['title','description'],searchContent:'external'}} as const;
  const values=['ASCII TEXT','İSTANBUL','ΟΣ ΟΣΑ','CAFÉ 東京 🔧','ẞ Straße','A\u0000B','',null,undefined];
  const rows=values.map((title,rank)=>({_id:String(rank),rank,description:'Synthetic Maintenance PROCEDURE. '.repeat(15),...(title===undefined?{}:{title})}));
  const folded=new SQLiteTypedReplicaEngine(native.database,schema);
  const {ingressUnicodeFold:_capability,...fallback}=portable.database;
  const projected=new SQLiteTypedReplicaEngine(fallback,schema);
  for(const engine of [folded,projected]){await engine.open('owner');await engine.importScope('s',(async function*(){for(const row of rows)yield JSON.stringify([{table:'records',id:row._id,version:1,row}]);})());}
  expect(folded.takeProfile()).toMatchObject({nativeFrames:rows.length,searchProjectionMs:0,normalizationMs:0});
  expect(native.takeProfile().ingressFoldCalls).toBe(5);
  const table=sqliteTableName('records');
  const actual=await native.database.getAllAsync(`SELECT s0,s1 FROM ${q(table)} ORDER BY rid`);
  const expected=await portable.database.getAllAsync(`SELECT s0,s1 FROM ${q(table)} ORDER BY rid`);
  expect(actual).toEqual(expected);
  for(const search of ['ascii','i̇stan','ος','οσα','café','東京','🔧','ß','procedure']){
    const plan={table:'records',search,order:[{field:'rank',direction:'asc' as const}],limit:50};
    expect((await folded.page('s',plan)).rows,search).toEqual((await projected.page('s',plan)).rows);
  }
  expect(await folded.integrity()).toBe('ok');
});
