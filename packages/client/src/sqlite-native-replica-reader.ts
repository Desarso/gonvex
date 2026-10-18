import type {SQLitePredictions} from './sqlite-predictions.js';
import type {ReplicaRow} from './local-replica.js';
import type {SQLiteCanonicalGeneration} from './sqlite-canonical-dispatch.js';
import type {SQLiteLocalReplicaStorage,SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {SQLiteWindowStore,type SQLiteWindowHeader} from './sqlite-window-store.js';
import {fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';
export type SQLiteEntitySlot={id:string;value:ReplicaRow|null};
export type SQLiteNativeWindowRead={scope:string;signature:string;generation?:string;offset?:number;limit?:number;expected?:SQLiteCanonicalGeneration};
export type SQLiteNativeWindowResult={generation:SQLiteCanonicalGeneration;window:{generation:string;header:SQLiteWindowHeader;total:number};offset:number;nextOffset:number|null;rows:SQLiteEntitySlot[]};
export type SQLiteNativeEntityRead={scope:string;entity:string;ids:readonly string[];expected?:SQLiteCanonicalGeneration};
export type SQLiteNativeEntityResult={generation:SQLiteCanonicalGeneration;rows:SQLiteEntitySlot[]};

/** The owner reads membership and authoritative values under one transaction.
 * Only this bounded page crosses the broker. Tombstones retain their positions
 * as null slots, so paging cannot skip or duplicate a following member. */
export class SQLiteNativeReplicaReader {
 private readonly windows:SQLiteWindowStore;
 private predictions?:Pick<SQLitePredictions,'entities'|'ready'>;
 /** Installed by the generated owner program before admitting UI reads. */
 usePredictions(predictions:Pick<SQLitePredictions,'entities'|'ready'>){if(this.predictions&&this.predictions!==predictions)throw Error('Native reader already has its prediction store');this.predictions=predictions;}
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly canonical:SQLiteLocalReplicaStorage,private readonly engine:SQLiteTypedReplicaEngine){
  this.windows=new SQLiteWindowStore(db,()=>engine.verifyOwner());
 }
 async ready(){await this.canonical.ready();await this.windows.ready();await this.predictions?.ready();}
 private scope(scope:unknown):asserts scope is string{if(typeof scope!=='string'||!scope||scope.length>512)throw new TypeError('Invalid native replica scope');}
 private async generation(expected?:SQLiteCanonicalGeneration){
  if(expected!==undefined&&(!expected||!Number.isSafeInteger(expected.epoch)||expected.epoch<0||!Number.isSafeInteger(expected.sequence)||expected.sequence<0))throw new TypeError('Invalid native replica generation');
  const current=await this.engine.verifyOwner();
  if(expected&&(expected.epoch!==current.epoch||expected.sequence!==current.sequence))fail('PAGE_CHANGED','Native replica generation changed');
  return current;
 }
 private async values(scope:string,entity:string,ids:readonly string[],columns?:readonly string[]):Promise<SQLiteEntitySlot[]>{
  const values=await (this.predictions?this.predictions.entities(scope,entity,ids,columns):this.canonical.loadBoundedEntityRows(scope,entity,ids,131072,columns)),byID=new Map(values.map(entry=>[entry.id,entry.row]));
  return ids.map(id=>({id,value:byID.get(id)??null}));
 }
 async entities(input:SQLiteNativeEntityRead):Promise<SQLiteNativeEntityResult>{
  sqlitePayloadBytes(input,32768);this.scope(input.scope);
  if(!Array.isArray(input.ids)||input.ids.length>128||input.ids.some(id=>typeof id!=='string'||!id||id.length>1024)||typeof input.entity!=='string'||!Object.hasOwn(this.engine.schema,input.entity))throw new TypeError('Invalid bounded entity request');
  await this.ready();let result!:SQLiteNativeEntityResult;
  await this.db.withTransactionAsync(async()=>{
   const generation=await this.generation(input.expected);
   result={generation,rows:await this.values(input.scope,input.entity,input.ids)};sqlitePayloadBytes(result,131072);
  });return result;
 }
 async window(input:SQLiteNativeWindowRead):Promise<SQLiteNativeWindowResult|undefined>{
  sqlitePayloadBytes(input,32768);this.scope(input.scope);
  const offset=input.offset??0,limit=input.limit??50;
  if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128||offset>0&&!input.generation)throw new TypeError('Native continuation requires a bounded page and window generation');
  await this.ready();let result:SQLiteNativeWindowResult|undefined;
  await this.db.withTransactionAsync(async()=>{
   const generation=await this.generation(input.expected),window=await this.windows.header(input.scope,input.signature);
   if(!window){if(input.generation)fail('PAGE_CHANGED','Native window was removed');return;}
   if(input.generation&&input.generation!==window.generation)fail('PAGE_CHANGED','Native window generation changed');
   let take=limit;
   for(;;){
    try{
     const members=await this.windows.page(input.scope,input.signature,window.generation,offset,take);
     result={generation,window:{generation:window.generation,header:window.header,total:members.total},offset,nextOffset:members.nextOffset,rows:await this.values(input.scope,window.header.entity,members.ids,window.header.columns)};
     sqlitePayloadBytes(result,131072);break;
    }catch(error){if(take===1||!pageBudgetError(error))throw error;take=Math.max(1,Math.floor(take/2));}
   }
  });return result;
 }
}
