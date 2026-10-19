import {sqlitePhysicalEntityScope,sqliteQueryEntityScopes} from './sqlite-entity-scope.js';
import {readSQLiteOptimisticPage} from './sqlite-optimistic-query.js';
import {readSQLiteTypedView} from './sqlite-native-view.js';
import {compileSQLiteTypedPage,type SQLitePagePlan} from './sqlite-typed-query.js';
import type {LocalPatch} from '@gonvex/local-runtime';
import type {ReadCoverage} from '@gonvex/local-runtime/read-view';
import type {ReducerReadView} from '@gonvex/local-runtime/portable';
import type {SQLiteReplicaDatabase,SQLiteLocalReplicaStorage} from './sqlite-replica-storage.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import {sqliteIdentifier as q,sqliteTableName} from './sqlite-typed-schema.js';
import {sqliteTypedRowLayout} from './sqlite-typed-row-layout.js';
import {pagedReplicaReadView} from './paged-read-view.js';
import {readSQLitePortablePage} from './sqlite-portable-page.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
import type {ReplicaRow} from './local-replica.js';
const schema=`CREATE TABLE IF NOT EXISTS gv_prediction_versions(scope TEXT PRIMARY KEY,n INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_prediction_members(scope TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,deleted INTEGER NOT NULL,intent_id INTEGER NOT NULL,PRIMARY KEY(scope,entity,id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_prediction_fields(scope TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,field TEXT NOT NULL,PRIMARY KEY(scope,entity,id,field)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_prediction_intents ON gv_prediction_members(scope,intent_id,entity,id);`;
const physical=(scope:string)=>'\u0000prediction:'+scope;
function scope(value:string){if(typeof value!=='string'||!value||value.length>512||value.includes('\u0000'))throw new TypeError('Invalid prediction scope');}

/** Pending row values use generated typed columns under a private disk scope.
 * Canonical values and authority never change. The marker relation is the
 * only replacement/deletion set; no pending-intent corpus is loaded into JS.
 * The owner must compose changes with its durable intent and retry receipt. */
export class SQLitePredictions {
 private initialized?:Promise<void>;
 private readonly layout;
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly canonical:SQLiteLocalReplicaStorage,private readonly tables:SQLiteTypedSchema,private readonly fence:()=>Promise<unknown>){this.layout=sqliteTypedRowLayout(tables);}
 ready(){
  if(!this.initialized){if(this.db.ownerTransactionDepth)throw new Error('Prepare predictions before an owner transaction');this.initialized=(async()=>{await this.canonical.ready();await this.db.withTransactionAsync(async()=>{await this.fence();await this.db.execAsync(schema);});})().catch(error=>{this.initialized=undefined;throw error;});}
  return this.initialized;
 }
 private async touch(ownerScope:string){const row=await this.db.getFirstAsync<{n:number}>('INSERT INTO gv_prediction_versions VALUES(?,1) ON CONFLICT(scope) DO UPDATE SET n=n+1 WHERE n<9007199254740991 RETURNING n',ownerScope);if(!row)fail('FENCED','Prediction generation exhausted');}
 private async generation(ownerScope:string){
  const fence=await this.fence() as {sequence?:number};if(!fence||!Number.isSafeInteger(fence.sequence)||fence.sequence!<0)fail('FENCED','Invalid optimistic query generation');
  const version=await this.db.getFirstAsync<{n:number}>('SELECT n FROM gv_prediction_versions WHERE scope=?',ownerScope);
  return {sequence:fence.sequence!,predictionSequence:version?.n??0};
 }
 /** The owner supplies a complete generated collection predicate before
  * allowing local inserts outside its last server membership. */
 async page(ownerScope:string,plan:SQLitePagePlan,allowLocalInserts=false){
  scope(ownerScope);sqlitePayloadBytes({plan,allowLocalInserts},16384);if(plan.limit>128||typeof allowLocalInserts!=='boolean')throw new RangeError('Optimistic pages are bounded to 128 rows');const owned=structuredClone(plan);compileSQLiteTypedPage(this.tables,ownerScope,owned);await this.ready();
  let result!:Awaited<ReturnType<typeof readSQLiteOptimisticPage>>;await this.db.withTransactionAsync(async()=>{
   const generation=await this.generation(ownerScope),resolved=await sqliteQueryEntityScopes(this.db,ownerScope,owned),physicalScope=resolved.physical;
   const pending=await this.db.getFirstAsync('SELECT 1 FROM gv_prediction_members WHERE scope=? LIMIT 1',ownerScope);
   result=await readSQLiteOptimisticPage(this.db,this.tables,ownerScope,physicalScope,owned,generation.sequence,generation.predictionSequence,pending?{scope:physical(ownerScope),allowLocalInserts}:undefined,resolved.sources);
  });return result;
 }
 async viewPage(ownerScope:string,signature:string,plan:SQLitePagePlan,offset=0,allowLocalInserts=false){
  scope(ownerScope);sqlitePayloadBytes({signature,plan,offset,allowLocalInserts},16384);if(typeof signature!=='string'||!signature||signature.length>4096||!Number.isSafeInteger(offset)||offset<0||plan.limit>128||typeof allowLocalInserts!=='boolean')throw new TypeError('Invalid optimistic view page');const owned=structuredClone(plan);compileSQLiteTypedPage(this.tables,ownerScope,owned);await this.ready();
  let result!:Awaited<ReturnType<typeof readSQLiteTypedView>>;await this.db.withTransactionAsync(async()=>{
   const generation=await this.generation(ownerScope),resolved=await sqliteQueryEntityScopes(this.db,ownerScope,owned),physicalScope=resolved.physical;
   const pending=await this.db.getFirstAsync('SELECT 1 FROM gv_prediction_members WHERE scope=? LIMIT 1',ownerScope);
   result=await readSQLiteTypedView(this.db,this.tables,ownerScope,physicalScope,signature,owned,offset,generation.sequence,131072,`${generation.sequence}:${generation.predictionSequence}`,pending?{scope:physical(ownerScope),allowLocalInserts}:undefined,resolved.sources);
  });return result;
 }
 async entities(ownerScope:string,entity:string,ids:readonly string[],columns?:readonly string[]):Promise<Array<{id:string;row:ReplicaRow}>>{
  scope(ownerScope);sqlitePayloadBytes({entity,ids},32768);if(!Object.hasOwn(this.tables,entity)||!Array.isArray(ids)||ids.length>128||ids.some(id=>typeof id!=='string'||!id))throw new TypeError('Invalid prediction entity page');await this.ready();let result!:Array<{id:string;row:ReplicaRow}>;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const markers=await this.db.getAllAsync<{id:string;deleted:number}>('SELECT id,deleted FROM gv_prediction_members WHERE scope=? AND entity=? AND id IN (SELECT value FROM json_each(?))',ownerScope,entity,JSON.stringify(ids)),byID=new Map(markers.map(marker=>[marker.id,marker]));
   const base=await this.canonical.loadBoundedEntityRows(ownerScope,entity,ids.filter(id=>!byID.has(id)),131072,columns);
   const predicted=await this.canonical.loadBoundedEntityRows(physical(ownerScope),entity,ids.filter(id=>byID.get(id)?.deleted===0),131072,columns);
   const values=new Map([...base,...predicted].map(entry=>[entry.id,entry]));result=ids.flatMap(id=>{const entry=values.get(id);return entry?[entry]:[];});sqlitePayloadBytes(result,131072);
  });return result;
 }
 /** Merge only intent-touched fields over a freshly received canonical page.
  * Full predicted rows contain the old base too; copying that base would hide
  * newer untouched values arriving during first sync. Legacy predictions must
  * be rebased from their durable intents before this path may publish them. */
 async overlayRows(ownerScope:string,entity:string,rows:readonly {id:string;row:ReplicaRow|null}[]):Promise<Array<{id:string;row:ReplicaRow|null}>>{
  scope(ownerScope);sqlitePayloadBytes({entity,rows},131072);if(!Object.hasOwn(this.tables,entity)||rows.length>128)throw new TypeError('Invalid predicted overlay page');const owned=JSON.parse(JSON.stringify(rows)) as typeof rows;await this.ready();let result!:Array<{id:string;row:ReplicaRow|null}>;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const ids=JSON.stringify(owned.map(row=>row.id));
   const markers=await this.db.getAllAsync<{id:string;deleted:number}>('SELECT id,deleted FROM gv_prediction_members WHERE scope=? AND entity=? AND id IN (SELECT value FROM json_each(?))',ownerScope,entity,ids);
   const predicted=await this.canonical.loadBoundedEntityRows(physical(ownerScope),entity,markers.filter(row=>!row.deleted).map(row=>row.id)),values=new Map(predicted.map(row=>[row.id,row.row])),byID=new Map(markers.map(row=>[row.id,row]));
   const fields=await this.db.getAllAsync<{id:string;fields:string}>("SELECT id,json_group_array(field) fields FROM gv_prediction_fields WHERE scope=? AND entity=? AND id IN (SELECT value FROM json_each(?)) GROUP BY id",ownerScope,entity,ids),touched=new Map(fields.map(row=>[row.id,JSON.parse(row.fields) as string[]]));
   result=owned.map(entry=>{
    const marker=byID.get(entry.id);if(!marker)return entry;if(marker.deleted)return {id:entry.id,row:null};
    const pending=values.get(entry.id),keys=touched.get(entry.id);if(!pending||!keys?.includes('0'))fail('PAGE_CHANGED','Rebase legacy predictions before reading pending source pages');
    return {id:entry.id,row:{...(entry.row??pending),...Object.fromEntries(keys.filter(key=>key.startsWith('1')).map(key=>key.slice(1)).filter(key=>Object.hasOwn(pending,key)).map(key=>[key,pending[key]!]))}};
   });sqlitePayloadBytes(result,131072);
  });return result;
 }
 /** Recovery may have its durable envelope before canonical rows return.
  * Test the whole prediction without decoding row bodies or publishing a
  * partial transaction. Unknown outcomes wait for their missing bases. */
 async canApply(ownerScope:string,patches:readonly LocalPatch[]):Promise<boolean>{
  scope(ownerScope);sqlitePayloadBytes(patches,262144);if(patches.length>512)throw new RangeError('Invalid prediction batch');await this.ready();await this.fence();const present=new Map<string,boolean>();
  for(const patch of patches){if(!Object.hasOwn(this.tables,patch.entity))throw new TypeError('Unknown prediction entity');const key=JSON.stringify([patch.entity,patch.rowId]);let exists=present.get(key);if(exists===undefined){const marker=await this.db.getFirstAsync<{deleted:number}>('SELECT deleted FROM gv_prediction_members WHERE scope=? AND entity=? AND id=?',ownerScope,patch.entity,patch.rowId);exists=marker?marker.deleted===0:!!await this.db.getFirstAsync(`SELECT 1 FROM ${q(sqliteTableName(patch.entity))} WHERE scope=? AND id=? LIMIT 1`,await sqlitePhysicalEntityScope(this.db,ownerScope,patch.entity),patch.rowId);}if(patch.op==='patch'&&!exists)return false;present.set(key,patch.op!=='delete');}return true;
 }
 async apply(ownerScope:string,intent:number,patches:readonly LocalPatch[]):Promise<void>{
  scope(ownerScope);sqlitePayloadBytes(patches,262144);if(!Number.isSafeInteger(intent)||intent<1||!Array.isArray(patches)||patches.length>512)throw new TypeError('Invalid prediction batch');
  // Snapshot every admitted input before the actor awaits initialization.
  const owned=JSON.parse(JSON.stringify(patches)) as LocalPatch[];await this.ready();
  await this.db.withTransactionAsync(async()=>{
   await this.fence();
   for(const patch of owned){
    const table=Object.hasOwn(this.tables,patch.entity)&&this.tables[patch.entity];if(!table||typeof patch.rowId!=='string'||!patch.rowId||patch.rowId.length>1024||!['insert','patch','delete'].includes(patch.op))throw new TypeError('Invalid prediction patch');
    let row:ReplicaRow|null=null;
    if(patch.op!=='delete'){
     if(!patch.fields||Array.isArray(patch.fields)||typeof patch.fields!=='object')throw new TypeError('Invalid prediction fields');
     const previous=patch.op==='patch'?(await this.entities(ownerScope,patch.entity,[patch.rowId]))[0]?.row:undefined;
     if(patch.op==='patch'&&!previous)fail('INVALID_ROW','Predicted patch requires an existing row');
     row={...previous,...patch.fields};sqlitePayloadBytes(row,131072);
     if(String(row[table.key])!==patch.rowId)throw new TypeError('Prediction changes its row identity');
    }
    await this.layout.upsert(this.db,physical(ownerScope),patch.entity,[{id:patch.rowId,row}]);
    if(patch.op!=='patch')await this.db.runAsync('DELETE FROM gv_prediction_fields WHERE scope=? AND entity=? AND id=?',ownerScope,patch.entity,patch.rowId);
    // '0' marks a tracked (possibly empty) patch; '1'+field encodes every
    // application key without reserving or colliding with a real field name.
    if(patch.op!=='delete')await this.db.runAsync("INSERT OR IGNORE INTO gv_prediction_fields SELECT ?,?,?,field FROM (SELECT '0' field UNION ALL SELECT '1'||value FROM json_each(?))",ownerScope,patch.entity,patch.rowId,JSON.stringify(Object.keys(patch.fields!)));
    await this.db.runAsync('INSERT INTO gv_prediction_members VALUES(?,?,?,?,?) ON CONFLICT(scope,entity,id) DO UPDATE SET deleted=excluded.deleted,intent_id=excluded.intent_id',ownerScope,patch.entity,patch.rowId,Number(row===null),intent);
   }
   if(owned.length)await this.touch(ownerScope);
  });
 }
 /** Coverage is established by the owner's authorized collection plans. Row
  * presence alone never grants a complete scan or missing-row conclusion. */
 async withReadView<T>(ownerScope:string,coverage:ReadCoverage,run:(view:ReducerReadView)=>Promise<T>):Promise<T>{
  scope(ownerScope);sqlitePayloadBytes(coverage,524288);await this.ready();let result!:T;
  await this.db.withTransactionAsync(async()=>{await this.fence();result=await run(pagedReplicaReadView(coverage,(read,after,excluded)=>readSQLitePortablePage(this.db,this.tables,ownerScope,read,after,excluded,coverage[read.table]?.columns,physical(ownerScope))));});return result;
 }
 /** Rebase clears only derived predictions. Intent bodies/receipts remain
  * durable and are replayed in bounded pages within the same owner operation. */
 async reset(ownerScope:string):Promise<void>{
  scope(ownerScope);await this.ready();await this.db.withTransactionAsync(async()=>{
   await this.fence();let after:string|undefined,changed=false;
   // apply() atomically records a marker with every derived row, including
   // deletions. Walk only those entity names, never every application table.
   for(;;){
    const entities=await this.db.getAllAsync<{entity:string}>('SELECT DISTINCT entity FROM gv_prediction_members WHERE scope=?'+(after===undefined?'':' AND entity>?')+' ORDER BY entity LIMIT 128',ownerScope,...(after===undefined?[]:[after]));
    if(!entities.length)break;
    for(const {entity} of entities){if(!Object.hasOwn(this.tables,entity))fail('INTENT_CORRUPT','Prediction marker has no generated entity');await this.db.runAsync(`DELETE FROM ${q(sqliteTableName(entity))} WHERE scope=?`,physical(ownerScope));changed=true;}
    after=entities.at(-1)!.entity;
   }
   if(!changed)return;
   await this.db.runAsync('DELETE FROM gv_canonical_tombstones WHERE scope=?',physical(ownerScope));await this.db.runAsync('DELETE FROM gv_prediction_fields WHERE scope=?',ownerScope);await this.db.runAsync('DELETE FROM gv_prediction_members WHERE scope=?',ownerScope);await this.touch(ownerScope);
  });
 }
}
