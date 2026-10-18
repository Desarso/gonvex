import {sqliteQueryEntityScopes} from './sqlite-entity-scope.js';
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
 async entities(ownerScope:string,entity:string,ids:readonly string[]):Promise<Array<{id:string;row:ReplicaRow}>>{
  scope(ownerScope);sqlitePayloadBytes({entity,ids},32768);if(!Object.hasOwn(this.tables,entity)||!Array.isArray(ids)||ids.length>128||ids.some(id=>typeof id!=='string'||!id))throw new TypeError('Invalid prediction entity page');await this.ready();let result!:Array<{id:string;row:ReplicaRow}>;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const markers=await this.db.getAllAsync<{id:string;deleted:number}>('SELECT id,deleted FROM gv_prediction_members WHERE scope=? AND entity=? AND id IN (SELECT value FROM json_each(?))',ownerScope,entity,JSON.stringify(ids)),byID=new Map(markers.map(marker=>[marker.id,marker]));
   const base=await this.canonical.loadBoundedEntityRows(ownerScope,entity,ids.filter(id=>!byID.has(id)));
   const predicted=await this.canonical.loadBoundedEntityRows(physical(ownerScope),entity,ids.filter(id=>byID.get(id)?.deleted===0));
   const values=new Map([...base,...predicted].map(entry=>[entry.id,entry]));result=ids.flatMap(id=>{const entry=values.get(id);return entry?[entry]:[];});sqlitePayloadBytes(result,131072);
  });return result;
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
    await this.db.runAsync('INSERT INTO gv_prediction_members VALUES(?,?,?,?,?) ON CONFLICT(scope,entity,id) DO UPDATE SET deleted=excluded.deleted,intent_id=excluded.intent_id',ownerScope,patch.entity,patch.rowId,Number(row===null),intent);
   }
   if(owned.length)await this.touch(ownerScope);
  });
 }
 /** Coverage is established by the owner's authorized collection plans. Row
  * presence alone never grants a complete scan or missing-row conclusion. */
 async withReadView<T>(ownerScope:string,coverage:ReadCoverage,run:(view:ReducerReadView)=>Promise<T>):Promise<T>{
  scope(ownerScope);sqlitePayloadBytes(coverage,65536);await this.ready();let result!:T;
  await this.db.withTransactionAsync(async()=>{await this.fence();result=await run(pagedReplicaReadView(coverage,(read,after,excluded)=>readSQLitePortablePage(this.db,this.tables,ownerScope,read,after,excluded,coverage[read.table]?.columns,physical(ownerScope))));});return result;
 }
 /** Rebase clears only derived predictions. Intent bodies/receipts remain
  * durable and are replayed in bounded pages within the same owner operation. */
 async reset(ownerScope:string):Promise<void>{
  scope(ownerScope);await this.ready();await this.db.withTransactionAsync(async()=>{
   await this.fence();for(const entity of Object.keys(this.tables))await this.db.runAsync(`DELETE FROM ${q(sqliteTableName(entity))} WHERE scope=?`,physical(ownerScope));
   await this.db.runAsync('DELETE FROM gv_canonical_tombstones WHERE scope=?',physical(ownerScope));await this.db.runAsync('DELETE FROM gv_prediction_members WHERE scope=?',ownerScope);await this.touch(ownerScope);
  });
 }
}
