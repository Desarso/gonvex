import {afterEach,expect,it,vi} from 'vitest';
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
it('reuses certified memberships and hashes after staging cleanup, including predicted pages and shared-source collection',async()=>{
 const {SQLiteWindowStore}=await import('./sqlite-window-store.js'),{SQLitePredictions}=await import('./sqlite-predictions.js'),f=await fixture(),windows=new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner());
 const rows=Array.from({length:300},(_,i)=>({id:String(i).padStart(3,'0'),name:'Stage '+String(i).padStart(3,'0')})),{input}=await f.stage(rows);await f.activator.activate(input);
 expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_window_members')).toEqual({n:0});expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_window_hashes')).toEqual({n:0});
 const page=await windows.page('s','collection','one',297,3);expect(page.ids).toEqual(['297','298','299']);expect(Object.keys(page.hashes)).toEqual(page.ids);expect((await windows.hashPage('s','collection','one','297',2)).map(row=>row.id)).toEqual(['298','299']);
 const plan={table:'tasks',membership:{signature:'collection',generation:'one',requireComplete:true},order:[{field:'name',direction:'asc' as const}],textOrder:'binary' as const,limit:3};expect((await f.engine.page('s',plan)).rows.map(row=>row.id)).toEqual(['000','001','002']);expect((await f.engine.viewPage('s','aliased-deep',plan,299)).rows.map(row=>row.id)).toEqual(['299']);
 const predictions=new SQLitePredictions(f.db,f.canonical,schema,()=>f.engine.verifyOwner());await predictions.ready();await predictions.apply('s',1,[{entity:'tasks',rowId:'000',op:'delete'},{entity:'tasks',rowId:'local',op:'insert',fields:{id:'local',name:'Stage -1'}}]);expect((await predictions.page('s',plan,true)).rows.map(row=>row.id)).toEqual(['local','001','002']);await predictions.reset('s');
 await windows.publishSnapshot('s',{...input.header,signature:'retained'},'collection','one',true);
 expect(await f.stager.discard('s','collection','one')).toBe(true);expect((await windows.page('s','collection','one',297,3)).ids).toEqual(page.ids);expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_rows')).toEqual({n:300});
 const next=await f.stage(rows.slice(0,50),'two');await f.activator.activate(next.input);expect((await windows.page('s','collection','two',49,1)).ids).toEqual(['049']);
 expect(await windows.collect('s','collection','one')).toEqual({remaining:false});expect((await windows.page('s','retained','one',297,3)).ids).toEqual(page.ids);
 const empty=await windows.begin('s',{...input.header,signature:'retained'},'empty');await windows.publish('s','retained',empty);
 let calls=0;while((await windows.collect('s','retained','one')).remaining){expect(++calls).toBeLessThan(4);}expect(calls).toBe(2);expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_rows')).toEqual({n:50});expect((await f.engine.page('s',{...plan,membership:{...plan.membership,generation:'two'}})).rows).toHaveLength(3);expect(await f.engine.integrity()).toBe('ok');
});
it('allocates compact physical scopes durably without reusing a retired scope and fails closed on clock exhaustion',async()=>{
 const f=await fixture(),one=await f.stage([{id:'one',name:'one'}]);await f.activator.activate(one.input);
 const physical=async(snapshot:string)=>JSON.parse((await f.db.getFirstAsync<{physical:string}>('SELECT json_quote(physical) physical FROM gv_snapshot_typed WHERE snapshot=?',snapshot))!.physical) as string;
 const first=await physical('one');expect(first).toBe('\u0000s:1');
 await expect(f.db.withTransactionAsync(async()=>{await f.stager.prepareTyped({scope:'s',subscription:'collection',snapshot:'aborted',entity:'tasks'});throw Error('rollback');})).rejects.toThrow('rollback');
 expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_typed WHERE snapshot=?','aborted')).toBeNull();
 const two=await f.stage([{id:'two',name:'two'}],'two');expect(await physical('two')).toBe('\u0000s:2');await f.activator.activate(two.input);
 await f.stager.discard('s','collection','one');expect(await f.stager.prepareTyped({scope:'s',subscription:'collection',snapshot:'three',entity:'tasks'})).toBe('\u0000s:3');
 await f.db.runAsync('UPDATE gv_snapshot_scope_clock SET value=?',Number.MAX_SAFE_INTEGER);await expect(f.stager.prepareTyped({scope:'s',subscription:'collection',snapshot:'exhausted',entity:'tasks'})).rejects.toMatchObject({code:'FENCED'});
 expect((await f.canonical.loadEntityRows('s','tasks',['two']))[0].row.name).toBe('two');expect(await f.engine.integrity()).toBe('ok');
});

it('stores native delta overlays without copying shared source certificates or overwriting newer canonical values',async()=>{
 const {SQLiteWindowStore}=await import('./sqlite-window-store.js');const f=await fixture(),rows=Array.from({length:300},(_,n)=>({id:String(n).padStart(3,'0'),name:'Source '+n}));
 const {input}=await f.stage(rows);await f.activator.activate(input);const windows=new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner());await windows.publishSnapshot('s',{...input.header,signature:'other'},'collection','one',true);await f.stager.discard('s','collection','one');
 const sourceHashes=await replicaRowsHashes(rows,'id'),updated={id:'001',name:'Delta source'},inserted={id:'new',name:'New delta row'},newHashes=await replicaRowsHashes([updated,inserted],'id');const next={...sourceHashes,...newHashes};delete next['000'];
 await f.write(30,[{entity:'tasks',id:'001',operation:'update',newValue:{id:'001',name:'Newer authoritative value'}}]);
 const delta={scope:'s',signature:'collection',generation:'one',cursor:{epoch:'e',revision:20},upserts:Object.entries(newHashes).map(([id,hash])=>({id,hash})),deleted:['000'],digest:replicaSumDigest(next)};
 let mutations=0;const before=await f.engine.verifyOwner();
 const apply=()=>windows.applyDelta(delta,async()=>{mutations++;await f.write(delta.cursor.revision,[{entity:'tasks',id:updated.id,operation:'update',newValue:updated},{entity:'tasks',id:inserted.id,operation:'insert',newValue:inserted}]);});
 expect(await apply()).toEqual({changed:true,rows:300});expect(await apply()).toEqual({changed:false,rows:300});expect(mutations).toBe(1);expect((await f.engine.verifyOwner()).sequence).toBe(before.sequence+1);
 expect((await windows.page('s','collection','one',0,2)).ids).toEqual(['001','002']);expect((await windows.page('s','other','one',0,2)).ids).toEqual(['000','001']);expect((await windows.hashPage('s','other','one','000',1))[0]?.hash).toBe(sourceHashes['001']);expect((await windows.hashPage('s','collection','one','000',1))[0]?.hash).toBe(newHashes['001']);
 expect((await f.canonical.loadEntityRows('s','tasks',['001']))[0].row.name).toBe('Newer authoritative value');expect((await windows.header('s','collection'))?.header.cursor?.revision).toBe(20);expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_rows')).toEqual({n:300});
 const empty=await windows.begin('s',{...input.header,signature:'other'},'empty');await windows.publish('s','other',empty);let collections=0;while((await windows.collect('s','other','one')).remaining){expect(++collections).toBeLessThan(4);}expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_rows')).toEqual({n:300});expect((await windows.page('s','collection','one',299,1)).ids).toEqual(['new']);expect(await f.db.getFirstAsync("SELECT count(*) n FROM gv_window_members WHERE signature='collection'")).toEqual({n:0});expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_window_mutations')).toEqual({n:3});
 const plan={table:'tasks',membership:{signature:'collection',generation:'one',requireComplete:true},order:[{field:'id',direction:'asc' as const}],textOrder:'binary' as const,limit:3};expect((await f.engine.page('s',plan)).rows.map(row=>row.id)).toEqual(['001','002','003']);
 const without={...next};delete without['001'];await windows.applyDelta({...delta,cursor:{epoch:'e',revision:21},upserts:[],deleted:['001'],digest:replicaSumDigest(without)});expect((await f.engine.page('s',plan)).rows.map(row=>row.id)).toEqual(['002','003','004']);
 await windows.applyDelta({...delta,cursor:{epoch:'e',revision:22},upserts:[{id:'001',hash:newHashes['001']}],deleted:[],digest:replicaSumDigest(next)});expect((await windows.page('s','collection','one',299,1)).ids).toEqual(['001']);expect((await f.engine.page('s',plan)).rows.map(row=>row.id)).toEqual(['001','002','003']);
 const final=await windows.begin('s',{...input.header,cursor:{epoch:'e',revision:31}},'empty');await windows.publish('s','collection',final);collections=0;while((await windows.collect('s','collection','one')).remaining){expect(++collections).toBeLessThan(4);}expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_snapshot_rows')).toEqual({n:0});expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_window_mutations')).toEqual({n:0});expect(await f.engine.integrity()).toBe('ok');
});

it('reads a committed first-sync page before the final certificate without granting complete coverage',async()=>{
 const f=await fixture(),rows=[{id:'first',name:'First received page café 東京'},{id:'second',name:'Second received page'},{id:'last',name:'Last frame'}],digest=replicaSumDigest(await replicaRowsHashes(rows,'id'));
 await f.stager.prepareTyped({scope:'s',subscription:'collection',snapshot:'progressive',entity:'tasks'});
 const metadata={type:'replica.snapshotEncodedPage',id:'collection',snapshotId:'progressive',scope:'tenant',visibilityScope:'s',key:'id',cursor:{epoch:'e',revision:10}};
 const frame=(part:typeof rows,sequence:number,last=false)=>new TextEncoder().encode(JSON.stringify({...metadata,sequence,last,canonicalRows:part.map(row=>JSON.stringify(stable(row))),...(last?{digest}:{})}));
 await f.stager.appendEncoded('s',frame(rows.slice(0,2),0));
 const pending=await f.stager.readPending('s','collection','progressive',-1,1);expect(pending).toMatchObject({complete:false,receivedRows:2,nextPage:1,nextPosition:0});expect(pending.rows.map(entry=>entry.row)).toEqual([rows[0]]);expect((await f.canonical.loadEntityRows('s','tasks',['first']))).toEqual([]);await expect(f.stager.read('s','collection','progressive')).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await f.stager.appendEncoded('s',frame(rows.slice(2),1,true));expect(await f.stager.readPending('s','collection','progressive',1,1)).toMatchObject({complete:true,receivedRows:3,nextPage:2,nextPosition:null});
 await f.activator.activate({scope:'s',subscription:'collection',snapshot:'progressive',header:{signature:'collection',entity:'tasks',key:'id',kind:'replica',source:'server',completeness:'complete',cursor:metadata.cursor,aggregateDigest:digest}});
 await expect(f.stager.readPending('s','collection','progressive')).rejects.toMatchObject({code:'PAGE_CHANGED'});expect((await f.canonical.loadEntityRows('s','tasks',['first']))[0].row).toEqual(rows[0]);
});

it('keeps indexes installed for an empty source instead of dropping and rebuilding the application schema',async()=>{
 const f=await fixture(),commands:string[]=[];
 const execute=f.db.execAsync.bind(f.db);vi.spyOn(f.db,'execAsync').mockImplementation(async sql=>{commands.push(sql);return execute(sql);});
 const {input}=await f.stage([],'empty');
 const index=sqliteTableName('tasks')+'_i_0';
 expect(await f.db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",index)).toEqual({'1':1});
 expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_index_deferred')).toBeNull();
 await f.activator.activate(input);
 expect(commands.join('\n')).not.toMatch(/DROP INDEX|CREATE INDEX/);
 expect(await f.engine.integrity()).toBe('ok');
});
