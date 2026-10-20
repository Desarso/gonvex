import {ByteLedger,BoundedByteCache} from './byte-budget.js';
import type {ReplicaRow} from './local-replica.js';
import type {SQLiteLocalReplicaStorage,SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSnapshotPages,SQLiteSnapshotMetadata} from './sqlite-snapshot-pages.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {SQLitePredictions} from './sqlite-predictions.js';
import {mergeReplicaRecord,type ReplicaRecordVersion} from './replica-record.js';
import {sqlitePhysicalEntityScope} from './sqlite-entity-scope.js';
import {sqliteIdentifier as q,sqliteTableName} from './sqlite-typed-schema.js';
import {decodedJSONBytes,fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';

export type SQLitePendingSnapshotGeneration={epoch:number;sequence:number;predictionSequence:number;snapshot:string;receivedPages:number};
export type SQLitePendingSnapshotRead={scope:string;entity:string;subscription:string;snapshot:string;after?:number;limit?:number;expected?:SQLitePendingSnapshotGeneration};
export type SQLitePendingSnapshotPage={source:'pending';complete:false;serverVerified:boolean;generation:SQLitePendingSnapshotGeneration;receivedRows:number;nextPosition:number|null;rows:Array<{id:string;position:number;value:ReplicaRow|null}>};
type Options={database:SQLiteReplicaDatabase;canonical:SQLiteLocalReplicaStorage;snapshots:SQLiteSnapshotPages;schema:SQLiteTypedSchema;fence():Promise<{epoch:number;sequence:number}>;predictions?:Pick<SQLitePredictions,'ready'|'overlayRows'>};
/** Owner-only display of an admitted in-progress snapshot. Source values merge
 * with per-field canonical authority and intent-owned fields in one read TX.
 * This neither publishes canonical rows nor grants complete reducer coverage.
 * The generated program derives scope/entity/subscription from tab admission. */
export class SQLitePendingSnapshotReader {
 private closed=false;
 private readonly ledger=new ByteLedger(262144);
 private readonly cache=new BoundedByteCache<string,{rows:SQLitePendingSnapshotPage['rows'];receivedRows:number;fixedByBudget:boolean}>('pending-snapshot-pages',262144,2,this.ledger);
 budgets(){return this.ledger.snapshot();}
 clear(){this.cache.shrink(0);}
 close(){this.closed=true;this.cache.close();}
 private predictions?:Options['predictions'];
 constructor(private readonly options:Options){this.predictions=options.predictions;}
 usePredictions(predictions:NonNullable<Options['predictions']>){if(this.predictions&&this.predictions!==predictions)throw Error('Pending reader already has a prediction store');this.predictions=predictions;this.clear();}
 async ready(){await this.options.snapshots.ready();await this.options.canonical.ready();await this.predictions?.ready();}
 async page(input:SQLitePendingSnapshotRead,isCurrent:()=>boolean=()=>true):Promise<SQLitePendingSnapshotPage>{
  if(this.closed)fail('FENCED','Pending snapshot reader is closed');
  sqlitePayloadBytes(input,16384);const table=Object.hasOwn(this.options.schema,input.entity)&&this.options.schema[input.entity],after=input.after??-1,limit=input.limit??50;
  if(!table||!input.scope||input.scope.length>512||input.scope.includes('\u0000')||!input.subscription||input.subscription.length>256||!input.snapshot||input.snapshot.length>128||!Number.isSafeInteger(after)||after< -1||!Number.isSafeInteger(limit)||limit<1||limit>100||after>=0&&!input.expected)throw new TypeError('Invalid pending snapshot page');
  const owned=JSON.parse(JSON.stringify(input)) as SQLitePendingSnapshotRead;await this.ready();let output!:SQLitePendingSnapshotPage;
  const {database:db}=this.options;
  // Nested readers can see ingest rows which have not survived FULL commit.
  // Their display lifetime belongs to that transaction, never this durable-
  // prefix cache. Even a later page rollback can reuse the same generation.
  const cacheable=!db.ownerTransactionDepth;
  await db.withTransactionAsync(async()=>{
   const owner=await this.options.fence();if(this.closed||!isCurrent())fail('FENCED','Pending page admission changed');
   const stage=await db.getFirstAsync<{metadata:string;rows:number;next_page:number;complete:number;discarded:number}>('SELECT metadata,rows,next_page,complete,discarded FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=?',owned.scope,owned.subscription,owned.snapshot);
   if(!stage||stage.discarded)fail('PAGE_CHANGED','Pending snapshot is unavailable');decodedJSONBytes(stage.metadata,16384);const metadata=JSON.parse(stage.metadata) as SQLiteSnapshotMetadata;
   if(!await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gv_snapshot_typed'"))fail('PAGE_CHANGED','Typed pending source is unavailable');
   const typed=await db.getFirstAsync<{entity:string}>('SELECT entity FROM gv_snapshot_typed WHERE scope=? AND subscription=? AND snapshot=?',owned.scope,owned.subscription,owned.snapshot);
   if(typed?.entity!==owned.entity||metadata.visibilityScope!==owned.scope||metadata.key!==table.key||metadata.id!==owned.subscription||metadata.snapshotId!==owned.snapshot)fail('FENCED','Pending snapshot differs from admitted entity');
   const state=await db.getAllAsync<{key:string;value:string}>("SELECT key,value FROM _gonvex_replica_meta WHERE scope=? AND key IN ('currentEpoch','sequence')",owned.scope),epoch=state.find(row=>row.key==='currentEpoch'),canonicalSequence=Number(state.find(row=>row.key==='sequence')?.value??0),physical=await sqlitePhysicalEntityScope(db,owned.scope,owned.entity);
   if(!Number.isSafeInteger(canonicalSequence)||canonicalSequence<0)fail('FENCED','Invalid canonical page generation');
   const predictionSequence=this.predictions?(await db.getFirstAsync<{n:number}>('SELECT n FROM gv_prediction_versions WHERE scope=?',owned.scope))?.n??0:0;
   const expected=owned.expected;
   if(expected&&(!Number.isSafeInteger(expected.receivedPages)||expected.receivedPages<0||expected.epoch!==owner.epoch||expected.sequence!==canonicalSequence||expected.predictionSequence!==predictionSequence||expected.snapshot!==owned.snapshot))fail('PAGE_CHANGED','Pending page authority changed');
   const cacheKey=JSON.stringify([owned.scope,owned.entity,owned.subscription,owned.snapshot,after,limit,owner.epoch,canonicalSequence,predictionSequence,metadata.cursor.epoch]);
   const cached=cacheable?this.cache.get(cacheKey):undefined;
   if(cached&&(cached.rows.length===limit||cached.fixedByBudget||cached.receivedRows===stage.rows)){
    if(expected&&stage.next_page<expected.receivedPages)fail('PAGE_CHANGED','Pending page generation was replaced');
    const last=cached.rows.at(-1)?.position??after;
    output={source:'pending',complete:false,serverVerified:!!stage.complete,generation:{epoch:owner.epoch,sequence:canonicalSequence,predictionSequence,snapshot:owned.snapshot,receivedPages:stage.next_page},receivedRows:stage.rows,nextPosition:last+1<stage.rows?last:null,rows:structuredClone(cached.rows)};
    if(this.closed||!isCurrent())fail('FENCED','Pending page admission changed');await this.options.fence();return;
   }
   let take=limit,readRows=limit;
   for(;;){try{
    const page=await this.options.snapshots.readPending(owned.scope,owned.subscription,owned.snapshot,after,take);
    readRows=page.rows.length;
    if(expected&&page.nextPage<expected.receivedPages)fail('PAGE_CHANGED','Pending page generation was replaced');
    const ids=page.rows.map(row=>row.id),current=epoch?.value===metadata.cursor.epoch?await this.options.canonical.loadBoundedEntityRows(owned.scope,owned.entity,ids):[],byID=new Map(current.map(row=>[row.id,row.row]));
    const authorities=ids.length&&epoch?.value===metadata.cursor.epoch?await db.getAllAsync<{key:string;value:string}>('SELECT key,value FROM _gonvex_replica_meta WHERE scope=? AND key IN (SELECT value FROM json_each(?))',owned.scope,JSON.stringify(ids.map(id=>'row:'+JSON.stringify([owned.entity,id])))):[];
    let bytes=0;const versions=new Map<string,ReplicaRecordVersion>();for(const record of authorities){bytes+=decodedJSONBytes(record.value,131072-bytes);versions.set(record.key,JSON.parse(record.value));}
    const bases=ids.length&&epoch?.value===metadata.cursor.epoch?await db.getAllAsync<{id:string;version:number}>(`SELECT id,version FROM ${q(sqliteTableName(owned.entity))} WHERE scope=? AND id IN (SELECT value FROM json_each(?))`,physical,JSON.stringify(ids)):[],byVersion=new Map(bases.map(row=>[row.id,row.version]));
    const merged=page.rows.map(row=>({id:row.id,row:mergeReplicaRecord({row:byID.get(row.id)??null,authority:versions.get('row:'+JSON.stringify([owned.entity,row.id]))??{epoch:metadata.cursor.epoch,base:byVersion.get(row.id)??0,fields:{}}},row.row,metadata.cursor).row}));
    const predicted=this.predictions?await this.predictions.overlayRows(owned.scope,owned.entity,merged):merged,values=new Map(predicted.map(row=>[row.id,row.row]));
    output={source:'pending',complete:false,serverVerified:page.complete,generation:{epoch:owner.epoch,sequence:canonicalSequence,predictionSequence,snapshot:owned.snapshot,receivedPages:page.nextPage},receivedRows:page.receivedRows,nextPosition:page.nextPosition,rows:page.rows.map(row=>({id:row.id,position:row.position,value:values.get(row.id)??null}))};sqlitePayloadBytes(output,131072);if(this.closed||!isCurrent())fail('FENCED','Pending page admission changed');await this.options.fence();
    if(cacheable){const cachedRows=structuredClone(output.rows),cacheBytes=sqlitePayloadBytes({key:cacheKey,rows:cachedRows},262144);this.cache.put(cacheKey,{rows:cachedRows,receivedRows:page.receivedRows,fixedByBudget:take<limit||page.rows.length<take&&page.nextPosition!==null},cacheBytes+128);}break;
   }catch(error){if(take===1||!pageBudgetError(error))throw error;take=Math.max(1,Math.min(Math.floor(take/2),Math.floor(readRows/2)));}}
  });return output;
 }
}
