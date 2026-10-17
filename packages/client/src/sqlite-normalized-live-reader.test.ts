import { afterEach, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteLocalReplicaStorage, type SQLiteReplicaDatabase } from './sqlite-replica-storage';
import { sqliteLiveQueryFunctions, compileSQLiteLiveQuery } from './sqlite-query-expression';
import { readNormalizedSQLiteLiveWindow, readNormalizedSQLiteLiveSnapshot, type NormalizedLiveReadBudget } from './sqlite-normalized-live-reader';
import type { LiveQueryPlan } from './query-expression';
import type { OptimisticPatch } from './optimistic';
const databases:DatabaseSync[]=[];
afterEach(()=>{for(const db of databases.splice(0))db.close();});
const budget:NormalizedLiveReadBudget={maxRows:5,maxTextBytes:16000,maxPatches:10,maxOverlayBytes:16000};
const plan:LiveQueryPlan={table:'tasks',key:'_id',where:{operator:'eq',column:'workspaceId',value:{argument:'workspace'}},
 sort:{allowedColumns:['name'],defaultColumn:'name',defaultDirection:'asc'},window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:5,maxLimit:5,count:'exact'}};
async function fixture(normalizeKeys=false){
 const native=new DatabaseSync(':memory:');databases.push(native);
 for(const[name,fn]of Object.entries(sqliteLiveQueryFunctions))native.function(name,{varargs:true},fn as any);
 // Node's builtin has no custom collation API. These contract fixtures use
 // ASCII names/keys whose order agrees with BINARY. The separate real-WASM
 // differential harness tests the actual gv_json collation and generated plans.
 native.function('gv_nfc',(value:string|null)=>typeof value==='string'?value.normalize('NFC'):value);
 const sql=(text:string)=>(normalizeKeys?text.replace(/((?:page\.)?rk) COLLATE gv_json/g,'gv_nfc($1) COLLATE BINARY'):text).replaceAll('COLLATE gv_json','COLLATE BINARY');
 const bindings=(text:string,args:unknown[])=>/\?\d+/.test(text)?[Object.fromEntries(args.map((value,index)=>['?'+(index+1),value]))]:args;
 const db:SQLiteReplicaDatabase={
  async execAsync(text){native.exec(sql(text));},
  async runAsync(text,...args){return native.prepare(sql(text)).run(...bindings(text,args) as any[]);},
  async getAllAsync<T>(text,...args){return native.prepare(sql(text)).all(...bindings(text,args) as any[]) as T[];},
  async getFirstAsync<T>(text,...args){return native.prepare(sql(text)).get(...bindings(text,args) as any[]) as T??null;},
  async withTransactionAsync(run){native.exec('BEGIN');try{await run();native.exec('COMMIT');}catch(error){native.exec('ROLLBACK');throw error;}}
 };
 const store=new SQLiteLocalReplicaStorage(db);
 await store.replaceSnapshot({entities:{tasks:{a:{_id:'a',name:'Alpha',workspaceId:'w'},b:{_id:'b',name:'Beta',workspaceId:'w'}}},liveQueries:{}},'s');
 const read=(patches:OptimisticPatch[]=[],options:{complete?:boolean;budget?:NormalizedLiveReadBudget;signal?:AbortSignal}={})=>
  readNormalizedSQLiteLiveWindow(db,plan,{workspace:'w'},'s',{},options.budget??budget,async()=>({patches,coverage:async()=>options.complete===false?'partial':'complete'}),options.signal);
 return {db,store,native,read};
}
it('reads the existing normalized store and exposes a pending insert without persisting it',async()=>{
 const {read,store}=await fixture();
 const result=await read([{entity:'tasks',rowId:'c',op:'insert',fields:{_id:'c',name:'Aardvark',workspaceId:'w'}}]);
 expect(result.total).toBe(3);expect(result.rows.map(row=>row._id)).toEqual(['c','a','b']);expect(result.completeness).toBe('complete');
 expect(await store.loadEntityRows('s','tasks',['c'])).toEqual([]);
 expect((await read()).rows.map(row=>row._id)).toEqual(['a','b']);
});
it('merges patches, removes deletes and preserves canonical values after the read',async()=>{
 const {read,store}=await fixture();
 const result=await read([{entity:'tasks',rowId:'b',op:'patch',fields:{name:'Aardvark'}},{entity:'tasks',rowId:'a',op:'delete'}]);
 expect(result.rows).toEqual([{_id:'b',name:'Aardvark',workspaceId:'w'}]);expect(result.total).toBe(1);
 expect((await store.loadEntityRows('s','tasks',['a','b'])).map(r=>r.row.name)).toEqual(['Alpha','Beta']);
});
it('keeps an incomplete offline result partial while showing a newly predicted row',async()=>{
 const {read}=await fixture();
 const result=await read([{entity:'tasks',rowId:'c',op:'insert',fields:{_id:'c',name:'Gamma',workspaceId:'w'}}],{complete:false});
 expect(result.completeness).toBe('partial');expect(result.rows.some(row=>row._id==='c')).toBe(true);
});
it('does not carry temporary predictions to a second query or a different scope',async()=>{
 const {read,db}=await fixture();await read([{entity:'tasks',rowId:'b',op:'delete'}]);
 expect((await read()).total).toBe(2);
 const other=await readNormalizedSQLiteLiveWindow(db,plan,{workspace:'w'},'other',{},budget,async()=>({patches:[],coverage:async()=>'complete'}));
 expect(other.rows).toEqual([]);expect(other.total).toBe(0);
});
it('rolls back temporary state on overlay budget failures and remains usable',async()=>{
 const {read,store}=await fixture();
 await expect(read([{entity:'tasks',rowId:'a',op:'patch',fields:{note:'x'.repeat(10000)}}],{budget:{...budget,maxOverlayBytes:1000}})).rejects.toThrow('budget');
 expect((await read()).total).toBe(2);expect((await store.loadEntityRows('s','tasks',['a']))[0]?.row.note).toBeUndefined();
});
it('bounds overlay count rather than silently dropping pending intents',async()=>{
 const {read}=await fixture();
 await expect(read([{entity:'tasks',rowId:'a',op:'delete'},{entity:'tasks',rowId:'b',op:'delete'}],{budget:{...budget,maxPatches:1}})).rejects.toThrow('count budget');
 expect((await read()).total).toBe(2);
});
it('returns the explicit text-budget failure and clears its temporary overlay',async()=>{
 const {read}=await fixture();
 const result=await read([{entity:'tasks',rowId:'a',op:'patch',fields:{note:'large'}}],{budget:{...budget,maxTextBytes:1}});
 expect(result).toMatchObject({rows:[],supported:false,unsupportedOperator:'queryTextBudget'});
 expect((await read()).rows[0]?.note).toBeUndefined();
});
it('cancels between captured context and query publication without persisting predictions',async()=>{
 const {db,read}=await fixture();const controller=new AbortController(),error=Error('retired');
 await expect(readNormalizedSQLiteLiveWindow(db,plan,{workspace:'w'},'s',{},budget,async()=>{controller.abort(error);return {patches:[{entity:'tasks',rowId:'a',op:'delete'}],coverage:async()=>'complete'};},controller.signal)).rejects.toBe(error);
 expect((await read()).total).toBe(2);
});
it('validates an internal SQL row-source identifier and binds application values',()=>{
 expect(()=>compileSQLiteLiveQuery(plan,{},'s',{}, {rowsTable:'rows; DROP TABLE tasks'})).toThrow('row source');
 const compiled=compileSQLiteLiveQuery(plan,{workspace:'replica_rows'},'s',{}, {rowsTable:'_gonvex_live_rows'});
 expect(compiled.supported).toBe(true);if(!compiled.supported)throw Error('unsupported');
 expect(compiled.combined.sql).toContain('FROM _gonvex_live_rows');expect(compiled.combined.bindings).toContain(JSON.stringify('replica_rows'));
});

it('keeps pending delete/reinsert in its established order for canonically equivalent IDs',async()=>{
 // NFC+BINARY agrees with gv_json for this two-row fixture. The real-WASM
 // differential regression also runs the actual locale comparator.
 const {store,read}=await fixture(true);
 await store.replaceSnapshot({entities:{tasks:{'é':{_id:'é',name:'Same',workspaceId:'w'},'e\u0301':{_id:'e\u0301',name:'Same',workspaceId:'w'}}},liveQueries:{}},'s');
 const result=await read([{entity:'tasks',rowId:'é',op:'delete'},{entity:'tasks',rowId:'é',op:'insert',fields:{_id:'é',name:'Same',workspaceId:'w'}}]);
 expect(result.rows.map(row=>row._id)).toEqual(['é','e\u0301']);
});

it('returns canonical hydration separately from predicted rows and captures the same durable cursor',async()=>{
 const {db,store}=await fixture();
 await store.applyTransaction({cursor:{epoch:'e',revision:9},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{_id:'a',name:'Fresh'}}]},{entities:{},liveQueries:{}},'s');
 const snapshot=await readNormalizedSQLiteLiveSnapshot(db,plan,{workspace:'w'},'s',{},budget,async()=>({patches:[
  {entity:'tasks',rowId:'a',op:'patch',fields:{name:'Aardvark'}},
  {entity:'tasks',rowId:'c',op:'insert',fields:{_id:'c',name:'Gamma',workspaceId:'w'}}],coverage:async()=>'complete'}));
 expect(snapshot.view.rows.map(row=>row.name)).toEqual(['Aardvark','Beta','Gamma']);
 expect(snapshot.canonicalRows.map(entry=>[entry.id,entry.row.name])).toEqual([['a','Fresh'],['b','Beta']]);
 expect(snapshot.cursor).toEqual({epoch:'e',revision:9});
 expect(await store.loadEntityRows('s','tasks',['c'])).toEqual([]);
});
it('hydrates only selected canonical rows and does not apply a predicted delete to disk',async()=>{
 const {db,store}=await fixture();
 const snapshot=await readNormalizedSQLiteLiveSnapshot(db,plan,{workspace:'w'},'s',{},budget,async()=>({patches:[{entity:'tasks',rowId:'b',op:'delete'}],coverage:async()=>'complete'}));
 expect(snapshot.canonicalRows.map(entry=>entry.id)).toEqual(['a']);
 expect((await store.loadEntityRows('s','tasks',['b']))[0]?.row.name).toBe('Beta');
});
it('keeps canonical hydration scoped even when another tenant has the same selected ID',async()=>{
 const {db,store}=await fixture();await store.replaceSnapshot({entities:{tasks:{a:{_id:'a',name:'Secret',workspaceId:'w'}}},liveQueries:{}},'other');
 const snapshot=await readNormalizedSQLiteLiveSnapshot(db,plan,{workspace:'w'},'s',{},budget,async()=>({patches:[],coverage:async()=>'complete'}));
 expect(snapshot.canonicalRows.map(entry=>entry.row.name)).toEqual(['Alpha','Beta']);
});
it('rejects an oversized canonical base even when its predicted replacement is small',async()=>{
 const {db,store,read}=await fixture();
 await store.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{_id:'a',note:'x'.repeat(2000)}}]},{entities:{},liveQueries:{}},'s');
 await expect(readNormalizedSQLiteLiveSnapshot(db,plan,{workspace:'w'},'s',{}, {...budget,maxTextBytes:1000},async()=>({patches:[{entity:'tasks',rowId:'a',op:'insert',fields:{_id:'a',name:'Small',workspaceId:'w'}}],coverage:async()=>'complete'}))).rejects.toThrow('Canonical query text budget');
 expect((await read()).rows.find(row=>row._id==='a')?.name).toBe('Alpha');
});
it('requires a stable entity key before a predicted result can request canonical hydration',async()=>{
 const {db}=await fixture();
 await expect(readNormalizedSQLiteLiveSnapshot(db,plan,{workspace:'w'},'s',{},budget,async()=>({patches:[{entity:'tasks',rowId:'missing',op:'insert',fields:{name:'Small',workspaceId:'w'}}],coverage:async()=>'complete'}))).rejects.toThrow('stable entity key');
});
