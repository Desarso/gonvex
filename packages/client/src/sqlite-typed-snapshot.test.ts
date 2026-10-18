import {afterEach,expect,it} from 'vitest';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
import {sqliteIdentifier as q,sqliteTableName} from './sqlite-typed-schema.js';
const stable=(value:unknown):unknown=>value===null||typeof value!=='object'?value:Array.isArray(value)?value.map(stable):Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable((value as Record<string,unknown>)[key])]));
const sqlite3=await initializePinnedSQLite(),connections:any[]=[];
afterEach(()=>{for(const db of connections.splice(0))db.close();});
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},detail:{kind:'text'},value:{kind:'number'},config:{kind:'json'}},indexes:[{columns:['name']}],counts:[{columns:['name']}],search:['name','detail'],searchContent:'external',searchDetail:'column'}} as const;
async function fixture(){
 const raw=new sqlite3.oo1.DB(':memory:');connections.push(raw);const host=officialSQLiteDatabase(sqlite3,raw),db=sqliteOwnerTransactions(host.database),engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('owner');
 const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();const fence=()=>engine.verifyOwner(),stager=new SQLiteSnapshotPages(db,fence,schema),activator=new SQLiteSnapshotActivator(db,canonical,fence,schema);await stager.ready();await activator.ready();
 const write=(revision:number,changes:any[])=>canonical.applyTransaction({cursor:{epoch:'e',revision},changes},{entities:{},liveQueries:{}},'s');
 async function stage(rows:Record<string,unknown>[],snapshot='one'){
  const digest=replicaSumDigest(await replicaRowsHashes(rows,'id'));await stager.prepareTyped({scope:'s',subscription:'collection',snapshot,entity:'tasks'});
  const page={type:'replica.snapshotEncodedPage',id:'collection',snapshotId:snapshot,scope:'tenant',visibilityScope:'s',key:'id',cursor:{epoch:'e',revision:10},sequence:0,last:true,canonicalRows:rows.map(row=>JSON.stringify(stable(row))),digest};
  const wire=new TextEncoder().encode(JSON.stringify(page));await stager.appendEncoded('s',wire);
  return {wire,input:{scope:'s',subscription:'collection',snapshot,header:{signature:'collection',entity:'tasks',key:'id',kind:'replica' as const,source:'server' as const,completeness:'complete' as const,cursor:page.cursor,aggregateDigest:digest}}};
 }
 return {db,engine,canonical,stager,activator,stage,write};
}
it('stages typed values once and atomically activates an entity while retaining newer authority and tombstones',async()=>{
 const f=await fixture();await f.write(20,[{entity:'tasks',id:'existing',operation:'insert',newValue:{id:'existing',name:'newer'}},{entity:'tasks',id:'deleted',operation:'delete'},{entity:'tasks',id:'retained',operation:'insert',newValue:{id:'retained',name:'another window'}}]);
 const rows=[{id:'fresh',name:'café 東京',config:{nested:[true,null,1]},value:3},{id:'existing',name:'old',detail:'missing projection'},{id:'deleted',name:'do not resurrect'}];
 const {wire,input}=await f.stage(rows);expect(await f.db.getAllAsync('SELECT body FROM gv_snapshot_rows')).toEqual(rows.map(()=>({body:'{}'})));
 expect((await f.stager.read('s','collection','one')).rows.map(entry=>entry.row)).toEqual(rows);
 expect(await f.canonical.loadEntityRows('s','tasks',['fresh'])).toEqual([]);expect((await f.stager.appendEncoded('s',wire)).rows).toBe(3);
 const receipt=await f.activator.activate(input);expect(receipt.changes.reset).toBe(true);await expect(f.stager.read('s','collection','one')).rejects.toMatchObject({code:'PAGE_CHANGED'});
 const values=await f.canonical.loadEntityRows('s','tasks',['fresh','existing','deleted','retained']);expect(values.map(row=>row.id).sort()).toEqual(['existing','fresh','retained']);expect(values.find(row=>row.id==='existing')!.row).toEqual({id:'existing',name:'newer',detail:'missing projection'});expect(values.find(row=>row.id==='fresh')!.row).toEqual(rows[0]);
 expect((await f.engine.page('s',{table:'tasks',search:'東京',order:[{field:'name',direction:'asc'}],textOrder:'binary',limit:10})).rows).toEqual([rows[0]]);expect(await f.engine.count('s','tasks',0,['café 東京'])).toBe(1);
 await f.write(30,[{entity:'tasks',id:'fresh',operation:'update',newValue:{name:'later'}}]);expect((await f.canonical.loadEntityRows('s','tasks',['fresh']))[0].row.name).toBe('later');expect(await f.activator.activate(input)).toEqual(receipt);expect(await f.engine.integrity()).toBe('ok');
});
it('rolls back an activation failure without losing the typed stage or exposing its rows',async()=>{
 const f=await fixture(),{input}=await f.stage([{id:'fresh',name:'stage'}]);
 await expect(f.db.withTransactionAsync(async()=>{await f.activator.activate(input);throw Error('power loss');})).rejects.toThrow('power loss');
 expect(await f.canonical.loadEntityRows('s','tasks',['fresh'])).toEqual([]);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_entity_alias')).toBeNull();expect(await f.db.getFirstAsync('SELECT count(*) n FROM '+q(sqliteTableName('tasks')))).toEqual({n:1});
 await f.activator.activate(input);expect((await f.canonical.loadEntityRows('s','tasks',['fresh']))[0].row.name).toBe('stage');
});
it('rejects invalid typed fields atomically and preserves the credited stage for retry',async()=>{
 const f=await fixture();await expect(f.stage([{id:'bad',name:12}])).rejects.toMatchObject({code:'INVALID_ROW'});expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_rows')).toEqual({n:0});expect(await f.db.getFirstAsync('SELECT count(*) n FROM '+q(sqliteTableName('tasks')))).toEqual({n:0});expect(await f.engine.integrity()).toBe('ok');
});

it('collects incomplete private partitions in bounded pages and keeps an activated partition',async()=>{
 const f=await fixture(),rows=Array.from({length:300},(_,i)=>({id:String(i),name:'stage '+i}));await f.stage(rows,'discard');
 let turns=0;while(!await f.stager.discard('s','collection','discard')){turns++;expect(turns).toBeLessThan(5);}expect(turns).toBe(2);
 expect(await f.db.getFirstAsync('SELECT count(*) n FROM '+q(sqliteTableName('tasks')))).toEqual({n:0});expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_typed')).toEqual({n:0});
 const {input}=await f.stage([{id:'active',name:'keep'}],'active');await f.activator.activate(input);expect(await f.stager.discard('s','collection','active')).toBe(true);expect((await f.canonical.loadEntityRows('s','tasks',['active']))[0].row.name).toBe('keep');expect(await f.engine.integrity()).toBe('ok');
});
it('collects retired physical rows without disturbing current counts, search or canonical writes',async()=>{
 const {collectSQLiteRetiredEntityRows}=await import('./sqlite-typed-snapshot.js'),f=await fixture();
 await f.write(20,Array.from({length:150},(_,i)=>({entity:'tasks',id:'old-'+i,operation:'insert',newValue:{id:'old-'+i,name:'retained '+i}})));
 const first=await f.stage([{id:'fresh',name:'café 東京'}]);await f.activator.activate(first.input);
 await f.db.withTransactionAsync(async()=>{expect(await collectSQLiteRetiredEntityRows(f.db,schema)).toBe(false);});
 await f.db.withTransactionAsync(async()=>{expect(await collectSQLiteRetiredEntityRows(f.db,schema)).toBe(true);});
 expect(await f.db.getFirstAsync('SELECT count(*) n FROM '+q(sqliteTableName('tasks')))).toEqual({n:151});expect((await f.canonical.loadEntityRows('s','tasks',['old-149','fresh'])).length).toBe(2);
 const second=await f.stage([{id:'fresh',name:'snapshot update'}],'second');await f.activator.activate(second.input);
 await expect(f.stager.read('s','collection','one')).rejects.toMatchObject({code:'PAGE_CHANGED'});
 let done=false;for(let i=0;i<3&&!done;i++)await f.db.withTransactionAsync(async()=>{done=await collectSQLiteRetiredEntityRows(f.db,schema);});expect(done).toBe(true);
 expect(await f.db.getFirstAsync('SELECT count(*) n FROM '+q(sqliteTableName('tasks')))).toEqual({n:151});
 expect(await f.engine.count('s','tasks',0,['snapshot update'])).toBe(1);expect((await f.engine.page('s',{table:'tasks',search:'snapshot update',order:[{field:'name',direction:'asc'}],textOrder:'binary',limit:1})).rows[0]?.id).toBe('fresh');
 await f.write(30,[{entity:'tasks',id:'fresh',operation:'update',newValue:{name:'later'}}]);expect((await f.canonical.loadEntityRows('s','tasks',['fresh']))[0].row.name).toBe('later');expect(await f.engine.integrity()).toBe('ok');
});
it('keeps first-page reads and search available after owner restart while bulk indexes remain deferred',async()=>{
 const f=await fixture();await f.write(20,[{entity:'tasks',id:'visible',operation:'insert',newValue:{id:'visible',name:'visible café'}}]);
 const {input}=await f.stage(Array.from({length:300},(_,i)=>({id:'staged-'+i,name:'stage '+i})));
 const index=sqliteTableName('tasks')+'_i_0';expect(await f.db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",index)).toBeNull();
 const next=new SQLiteTypedReplicaEngine(f.db,schema);await next.open('replacement');const canonical=sqliteTypedReplicaStorage(f.db,schema);await canonical.ready();
 expect(await f.db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",index)).toBeNull();
 expect((await next.page('s',{table:'tasks',search:'café',order:[{field:'name',direction:'asc'}],textOrder:'binary',limit:10})).rows.map(row=>row.id)).toEqual(['visible']);
 const activation=new SQLiteSnapshotActivator(f.db,canonical,()=>next.verifyOwner(),schema);await activation.ready();await activation.activate(input);
 expect(await f.db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",index)).toEqual({'1':1});expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_index_deferred')).toBeNull();
 expect((await next.page('s',{table:'tasks',search:'stage 299',order:[{field:'name',direction:'asc'}],textOrder:'binary',limit:10})).rows.map(row=>row.id)).toEqual(['staged-299']);expect(await next.integrity()).toBe('ok');
});
