import {afterEach,expect,it} from 'vitest';
import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import type {JsonValue} from '@gonvex/protocol';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import type {SnapshotPage} from './snapshot-pages.js';
import {replicaRowsHashes} from './replica-integrity.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
const connections:DatabaseSync[]=[];
afterEach(()=>{for(const db of connections.splice(0))db.close();});
function fixture(){
 const db=new DatabaseSync(':memory:');connections.push(db);
 const host:SQLiteReplicaDatabase={
  async execAsync(sql){db.exec(sql);},async runAsync(sql,...params){return db.prepare(sql).run(...params as any[]);},
  async getFirstAsync<T>(sql:string,...params:unknown[]){return db.prepare(sql).get(...params as any[]) as T??null;},
  async getAllAsync<T>(sql:string,...params:unknown[]){return db.prepare(sql).all(...params as any[]) as T[];},
  async withTransactionAsync(run){db.exec('BEGIN');try{await run();db.exec('COMMIT');}catch(error){db.exec('ROLLBACK');throw error;}}
 };
 let epoch=1;const fence=async()=>{if(epoch!==1)throw Object.assign(Error('replaced owner'),{code:'FENCED'});};
 return {db,host,fence,replaceOwner(){epoch++;},stager:new SQLiteSnapshotPages(host,fence)};
}
function page(rows:JsonValue[],sequence=0,last=false,digest?:string):SnapshotPage{return {
 type:'replica.snapshotPage',id:'collection',snapshotId:'snapshot',scope:'tenant',visibilityScope:'s',key:'id',
 cursor:{epoch:'e',revision:10},result:rows,sequence,last,...(digest?{digest}:{}),maxRows:10000,maxBytes:10000000,mode:'eager'
};}
async function certificate(rows:JsonValue[]){const state=new ReplicaSetDigest();for(const hash of Object.values(await replicaRowsHashes(rows,'id')))state.add(hash);return state;}
it('persists bounded isolated pages, survives reopen and verifies the whole set before exposing rows',async()=>{
 const {db,host,fence,stager}=fixture(),rows=Array.from({length:300},(_,id)=>({id:String(id),name:'café 東京 '+id}));
 const state=await certificate(rows);
 const first=page(rows.slice(0,100));
 expect(await stager.append('s',first)).toMatchObject({rows:100,nextPage:1,complete:false});
 await expect(stager.read('s','collection','snapshot')).rejects.toMatchObject({code:'PAGE_CHANGED'});
 const reopened=new SQLiteSnapshotPages(host,fence);
 await reopened.append('s',page(rows.slice(100,200),1));
 await expect(reopened.append('s',page(rows.slice(200),2,true,'sum128-v1:'+'0'.repeat(32)))).rejects.toThrow('integrity-mismatch');
 expect(db.prepare('SELECT count(*) n FROM gv_snapshot_rows').get()!.n).toBe(200);
 expect(await reopened.append('s',page(rows.slice(200),2,true,state.digest()))).toMatchObject({rows:300,nextPage:3,complete:true,digest:state.digest()});
 const replay=await reopened.append('s',first);expect(replay.complete).toBe(true);
 await expect(reopened.append('s',{...first,result:[{id:'changed'}]})).rejects.toMatchObject({code:'INTENT_CONFLICT'});
 let after=-1;const found:unknown[]=[];
 for(;;){const result=await reopened.read('s','collection','snapshot',after);expect(result.rows.length).toBeLessThanOrEqual(128);found.push(...result.rows.map(entry=>entry.row));if(result.nextPosition===null)break;after=result.nextPosition;}
 expect(found).toEqual(rows);
 expect(db.prepare('SELECT length(receipt) n FROM gv_snapshot_stages').get()!.n).toBe(4160);
 expect(db.prepare('PRAGMA integrity_check').get()!.integrity_check).toBe('ok');
});
it('rolls back duplicate, out-of-order and conflicting frames without mutating the durable certificate',async()=>{
 const {db,stager}=fixture();await stager.append('s',page([{id:'a'}]));
 const before=db.prepare('SELECT * FROM gv_snapshot_stages').get();
 await expect(stager.append('s',page([{id:'b'}],2))).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await expect(stager.append('s',page([{id:'a'}],1))).rejects.toThrow('duplicate-snapshot-key');
 await expect(stager.append('s',page([{id:'b'},{id:'b'}],1))).rejects.toThrow('duplicate key');
 expect(db.prepare('SELECT * FROM gv_snapshot_stages').get()).toEqual(before);
 await expect(stager.append('other',page([{id:'b'}]))).rejects.toThrow('invalid-snapshot-page');
});
it('snapshots caller ownership, supports legacy XOR, and fences discarded or replaced readers',async()=>{
 const {stager,replaceOwner}=fixture();const rows=Array.from({length:129},(_,i)=>({id:String(i),name:'original'}));
 const state=await certificate(rows),input=page(rows,0,true,state.legacyDigest());
 const pending=stager.append('s',input);rows[0]!.name='mutated';input.snapshotId='changed';await pending;
 expect((await stager.read('s','collection','snapshot')).rows[0]!.row.name).toBe('original');
 expect(await stager.discard('s','collection','snapshot')).toBe(false);
 await expect(stager.read('s','collection','snapshot')).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await expect(stager.append('s',page([],1,true,state.legacyDigest()))).rejects.toMatchObject({code:'PAGE_CHANGED'});
 expect(await stager.discard('s','collection','snapshot')).toBe(true);
 replaceOwner();await expect(stager.read('s','collection','snapshot')).rejects.toMatchObject({code:'FENCED'});
});
it('keeps the canonical first page available while an older collection stages and verifies',async()=>{
 const {host}=fixture(),database=sqliteOwnerTransactions(host);
 const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},due:{kind:'number'}},indexes:[],search:['name']}} as const;
 const engine=new SQLiteTypedReplicaEngine(database,schema);await engine.open('owner');
 const storage=sqliteTypedReplicaStorage(database,schema);await storage.ready();
 const current={id:'a',name:'newer first page'};
 await storage.applyTransaction({cursor:{epoch:'e',revision:12},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:current}]},{entities:{},liveQueries:{}},'s');
 const fence=async()=>{await storage.ready();};const stage=new SQLiteSnapshotPages(database,fence);
 const rows=[{id:'a',name:'older snapshot',due:5},{id:'b',name:'other',due:6}],state=await certificate(rows);
 await stage.append('s',page(rows.slice(0,1)));
 expect((await storage.loadEntityRows('s','tasks',['a']))[0]!.row).toEqual(current);
 await stage.append('s',page(rows.slice(1),1,true,state.digest()));
 expect((await storage.loadEntityRows('s','tasks',['a']))[0]!.row).toEqual(current);
 // The isolated certificate is not a canonical publication or reducer coverage.
 expect(await storage.loadEntityRows('s','tasks',['b'])).toEqual([]);
});

it('adapts verified read pages to decoded byte limits and rejects oversized ingress before storage',async()=>{
 const {stager,db}=fixture();const rows=Array.from({length:3},(_,id)=>({id:String(id),body:'x'.repeat(60000)}));
 const digest=(await certificate(rows)).digest();
 for(let i=0;i<rows.length;i++)await stager.append('s',page([rows[i]!],i,i===2,i===2?digest:undefined));
 const first=await stager.read('s','collection','snapshot');expect(first.rows.length).toBeGreaterThan(0);expect(first.rows.length).toBeLessThan(3);
 const oversized={...page([{id:'huge',body:'x'.repeat(140000)}]),id:'huge'};
 await expect(stager.append('s',oversized)).rejects.toMatchObject({code:'BUDGET'});
 expect(db.prepare("SELECT count(*) n FROM gv_snapshot_stages WHERE subscription='huge'").get()!.n).toBe(0);
 const bad={...page([{id:'only'}]),id:'budget',maxRows:1};await stager.append('s',bad);
 await expect(stager.append('s',{...bad,sequence:1,result:[{id:'second'}]})).rejects.toMatchObject({code:'BUDGET'});
 expect(db.prepare("SELECT rows FROM gv_snapshot_stages WHERE subscription='budget'").get()!.rows).toBe(1);
});

function nativeFixture(){
 const fixtureValue=fixture(),receipts:Array<Record<string,unknown>[]> = [];
 fixtureValue.db.function('gv_ingress_sha256',value=>value===null?null:createHash('sha256').update(typeof value==='string'?value:value as Uint8Array).digest('hex'));
 const host:SQLiteReplicaDatabase={...fixtureValue.host,ingressNativeHash:true,async getAllAsync<T>(sql:string,...params:unknown[]){
  const rows=await fixtureValue.host.getAllAsync<T>(sql,...params);
  receipts.push(rows as Record<string,unknown>[]);return rows;
 }};
 return {...fixtureValue,receipts,stager:new SQLiteSnapshotPages(host,fixtureValue.fence)};
}
function encodedPage(rows:JsonValue[],sequence=0,last=false,digest?:string){
 const {result,...metadata}=page(rows,sequence,last,digest);
 return new TextEncoder().encode(JSON.stringify({...metadata,type:'replica.snapshotEncodedPage',canonicalRows:rows.map(row=>JSON.stringify(row))}));
}
it('stages native canonical row strings without delivering row bodies through the bridge',async()=>{
 const {stager,receipts,db}=nativeFixture(),rows=Array.from({length:200},(_,n)=>({id:String(n),text:'café 東京 😀 '+n,config:{value:n}}));
 const state=await certificate(rows);
 // The wire contract requires stable key order, unlike ordinary JSON.stringify.
 const canonical=(value:any):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(canonical).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}';
 const frame=(start:number,end:number,sequence:number,last=false)=>{
  const {result,...metadata}=page([],sequence,last,last?state.digest():undefined);
  return new TextEncoder().encode(JSON.stringify({...metadata,type:'replica.snapshotEncodedPage',canonicalRows:rows.slice(start,end).map(canonical)}));
 };
 const input=frame(0,100,0),before=input.slice(),pending=stager.appendEncoded('s',input);input.fill(0);
 expect(await pending).toMatchObject({rows:100,nextPage:1,complete:false});
 expect(await stager.appendEncoded('s',frame(100,200,1,true))).toMatchObject({rows:200,nextPage:2,complete:true,digest:state.digest()});
 expect(await stager.appendEncoded('s',before)).toMatchObject({rows:200,complete:true});
 expect(await stager.appendEncodedReceipt('s',before)).toMatchObject({ack:{type:'replica.snapshotAck',id:'collection',snapshotId:'snapshot',sequence:0},progress:{rows:200,complete:true}});
 expect(receipts).toHaveLength(2);
 for(const batch of receipts){expect(batch).toHaveLength(100);for(const receipt of batch)expect(Object.keys(receipt).sort()).toEqual(['hash','id']);}
 expect(stager.stats()).toEqual({wireBytes:131072,wireMaxBytes:131072,wireEntries:1});
 expect(db.prepare('SELECT count(*) n FROM gv_snapshot_input').get()!.n).toBe(0);
 expect((await stager.read('s','collection','snapshot')).rows.map(row=>row.row)).toEqual(rows.slice(0,128));
});
it('rejects native duplicate rows and wrong certificates atomically and remains usable',async()=>{
 const {stager,db}=nativeFixture(),rows=[{id:'a'}],digest=(await certificate(rows)).digest();
 await stager.appendEncoded('s',encodedPage(rows));
 const before=db.prepare('SELECT * FROM gv_snapshot_stages').get();
 await expect(stager.appendEncoded('s',encodedPage([{id:'a'}],1,true,digest))).rejects.toThrow('duplicate-snapshot-key');
 await expect(stager.appendEncoded('s',encodedPage([{id:'b'},{id:'b'}],1))).rejects.toThrow('duplicate-snapshot-key');
 await expect(stager.appendEncoded('s',encodedPage([{id:'b'}],1,true,'sum128-v1:'+'0'.repeat(32)))).rejects.toThrow('integrity-mismatch');
 expect(db.prepare('SELECT * FROM gv_snapshot_stages').get()).toEqual(before);
 expect(db.prepare('SELECT count(*) n FROM gv_snapshot_input').get()!.n).toBe(0);
 expect(await stager.appendEncoded('s',encodedPage([],1,true,digest))).toMatchObject({complete:true,rows:1});
});
it('requires native hashing and strict scoped wire metadata before exposing any rows',async()=>{
 const legacy=fixture();await expect(legacy.stager.appendEncoded('s',encodedPage([]))).rejects.toMatchObject({code:'UNSUPPORTED'});
 const {stager,db}=nativeFixture();
 await expect(stager.appendEncoded('s',new Uint8Array([128]))).rejects.toThrow('UTF-8');
 await expect(stager.appendEncoded('other',encodedPage([{id:'a'}]))).rejects.toMatchObject({code:'INVALID_ROW'});
 const wire=JSON.parse(new TextDecoder().decode(encodedPage([{id:'a'}])));
 for(const invalid of [{...wire,result:[]},{...wire,canonicalRows:[{id:'a'}]},{...wire,canonicalRows:['not JSON']},{...wire,key:''},{...wire,last:1}])
  await expect(stager.appendEncoded('s',new TextEncoder().encode(JSON.stringify(invalid)))).rejects.toMatchObject({code:'INVALID_ROW'});
 expect(db.prepare('SELECT count(*) n FROM gv_snapshot_rows').get()!.n).toBe(0);
 expect(db.prepare('SELECT count(*) n FROM gv_snapshot_input').get()!.n).toBe(0);
});
