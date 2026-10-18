import {afterEach,expect,it} from 'vitest';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteFeedGroups} from './sqlite-feed-groups.js';
import {SQLiteOwnerMaintenance} from './sqlite-owner-maintenance.js';
import {sqliteIdentifier,sqliteTableName,sqliteSearchName} from './sqlite-typed-schema.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
const sqlite=await initializePinnedSQLite(),cleanup:Array<()=>void>=[];afterEach(()=>{for(const close of cleanup.splice(0))close();});
const schema={records:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},n:{kind:'number'}},indexes:[{columns:['name']}],search:['name'],searchContent:'external',searchDetail:'column'}} as const;
const stable=(row:Record<string,unknown>)=>JSON.stringify(Object.fromEntries(Object.keys(row).sort().map(key=>[key,row[key]])));
async function fixture(){
 const raw=new sqlite.oo1.DB(':memory:'),host=officialSQLiteDatabase(sqlite,raw),db=sqliteOwnerTransactions(host.database);cleanup.push(()=>raw.close());
 async function owner(token:string){const engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open(token);const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();const fence=()=>engine.verifyOwner(),snapshots=new SQLiteSnapshotPages(db,fence,schema),activation=new SQLiteSnapshotActivator(db,canonical,fence,schema);await snapshots.ready();await activation.ready();let lane=Promise.resolve();return {database:db,engine,canonical,snapshots,activation,runOwner<T>(_bytes:number,run:()=>Promise<T>){const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;}} as SQLiteOwnerProgramContext;}
 const first=await owner('first');
 async function stage(current:SQLiteOwnerProgramContext,subscription:string,snapshot:string,rows:Array<{id:string;name:string;n:number}>,revision=10,last=true){await current.snapshots.prepareTyped({scope:'s',entity:'records',subscription,snapshot});const digest=replicaSumDigest(await replicaRowsHashes(rows,'id'));await current.snapshots.appendEncoded('s',new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:subscription,snapshotId:snapshot,scope:'tenant',visibilityScope:'s',cursor:{epoch:'e',revision},key:'id',sequence:0,last,canonicalRows:rows.map(stable),...(last?{digest}:{})})));return {scope:'s',subscription,snapshot,header:{signature:subscription,entity:'records',key:'id',kind:'replica' as const,source:'server' as const,completeness:'complete' as const,cursor:{epoch:'e',revision},aggregateDigest:digest}};}
 return {db,first,owner,stage};
}
it('reclaims abandoned and retired disk pages in bounded steps after owner recovery while retaining the active canonical source',async()=>{
 const f=await fixture(),rows=Array.from({length:300},(_,n)=>({id:String(n).padStart(4,'0'),name:'Task '+n,n}));const old=await f.stage(f.first,'active','old',rows);await f.first.activation.activate(old);
 const physical=JSON.parse((await f.db.getFirstAsync<{physical:string}>('SELECT json_quote(physical) physical FROM gv_entity_alias WHERE scope=? AND entity=?','s','records'))!.physical) as string;
 const current=await f.stage(f.first,'active','current',[...rows,{id:'kept',name:'Kept café 東京',n:300}],20);await f.first.activation.activate(current);
 await f.first.snapshots.prepareTyped({scope:'s',entity:'records',subscription:'never-received',snapshot:'empty-orphan'});await f.stage(f.first,'interrupted','partial',Array.from({length:400},(_,n)=>({id:'partial-'+n,name:'Partial '+n,n})),20,false);
 const next=await f.owner('next'),failures:unknown[]=[],maintenance=new SQLiteOwnerMaintenance(next,error=>failures.push(error));cleanup.unshift(()=>maintenance.close());await maintenance.ready();let finished=false,steps=0,previous=300;
 for(;steps<50;steps++){const result=await maintenance.pulse(),remaining=(await f.db.getFirstAsync<{n:number}>(`SELECT count(*) n FROM ${sqliteIdentifier(sqliteTableName('records'))} WHERE scope=?`,physical))!.n;expect(previous-remaining).toBeLessThanOrEqual(128);previous=remaining;expect((await next.canonical.loadEntityRows('s','records',['kept']))[0].row.name).toBe('Kept café 東京');if(!result.more){finished=true;break;}}
 expect(finished).toBe(true);expect(steps).toBeGreaterThan(3);expect(failures).toEqual([]);expect((await f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_typed'))?.n).toBe(1);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_entity_retired')).toBeNull();expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_cleanup_queue')).toBeNull();expect(await f.db.getFirstAsync("SELECT 1 FROM gv_snapshot_stages WHERE subscription IN ('never-received','interrupted')")).toBeNull();expect((await f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_rows WHERE subscription=? AND snapshot=?','active','current'))?.n).toBe(301);expect(await next.engine.integrity()).toBe('ok');const fts=sqliteIdentifier(sqliteSearchName('records'));await f.db.runAsync(`INSERT INTO ${fts}(${fts},rank) VALUES('integrity-check',1)`);
});
it('prepares more than64 on-disk sources without materializing their rows and retires current-owner orphan bindings',async()=>{
 const f=await fixture(),maintenance=new SQLiteOwnerMaintenance(f.first,()=>{});cleanup.unshift(()=>maintenance.close());await maintenance.ready();const ids=Array.from({length:70},(_,n)=>'source-'+n);
 for(const id of ids)await f.first.snapshots.prepareTyped({scope:'s',entity:'records',subscription:id,snapshot:'never-started'});expect((await f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_typed'))?.n).toBe(70);await maintenance.retire('s',ids);maintenance.close();
 // A recovered owner resumes the same cleanup without a JS list of old stages.
 const owner=await f.owner('recovered'),next=new SQLiteOwnerMaintenance(owner,()=>{});cleanup.unshift(()=>next.close());await next.ready();for(let n=0;n<80;n++)if(!(await next.pulse()).more)break;
 expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_typed')).toBeNull();expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_cleanup_queue')).toBeNull();expect(await owner.engine.integrity()).toBe('ok');
});

it('reclaims unfinished feed incarnations in128-record steps without deleting the new owner stream',async()=>{
 const f=await fixture(),groups=new SQLiteFeedGroups(f.db,()=>f.first.engine.verifyOwner());await groups.ready();
 const frame={type:'replica.transactionEncoded' as const,cursor:{epoch:'e',revision:12},canonicalChanges:[]};
 for(let n=0;n<270;n++)await groups.append('abandoned',frame);
 const owner=await f.owner('recovered'),maintenance=new SQLiteOwnerMaintenance(owner,()=>{});cleanup.unshift(()=>maintenance.close());await maintenance.ready();const current=new SQLiteFeedGroups(f.db,()=>owner.engine.verifyOwner());await current.ready();await current.append('current',frame);
 let previous=270,finished=false;for(let n=0;n<20;n++){const result=await maintenance.pulse(),remaining=(await f.db.getFirstAsync<{n:number}>("SELECT count(*) n FROM gv_feed_group_frames WHERE stream='abandoned'"))!.n;expect(previous-remaining).toBeLessThanOrEqual(128);previous=remaining;expect(await current.pending('current')).toBe(true);if(!result.more){finished=true;break;}}
 expect(finished).toBe(true);expect(previous).toBe(0);expect((await f.db.getFirstAsync<{frames:number}>('SELECT frames FROM gv_feed_group_usage'))?.frames).toBe(1);await current.retire('current');for(let n=0;n<10;n++)if(!(await maintenance.pulse()).more)break;expect((await f.db.getFirstAsync<{bytes:number;frames:number}>('SELECT bytes,frames FROM gv_feed_group_usage'))).toEqual({bytes:0,frames:0});expect(await owner.engine.integrity()).toBe('ok');
});
