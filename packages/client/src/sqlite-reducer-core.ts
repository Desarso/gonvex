import type {LocalExecution,LocalTransactionResult,LocalPatch} from '@gonvex/local-runtime';
import type {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import type {ReadCoverage} from '@gonvex/local-runtime/read-view';
import type {JsonValue} from '@gonvex/protocol';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import type {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import type {SQLitePredictions} from './sqlite-predictions.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes} from './sqlite-admission.js';
export type SQLiteReducerInvocation={scope:string;path:string;commandId:string;args:JsonValue;now:number;intentEntropy?:string};
export class SQLiteReducerPreparationRequiredError extends Error {
 readonly code='MODULE_REQUIRED';
 constructor(readonly path:string){super('Prepare the reducer module outside the SQLite owner queue');this.name='SQLiteReducerPreparationRequiredError';}
}
export type SQLiteReducerReceipt={intentId:number;commandId:string;result:JsonValue;originalGeneration:{epoch:number;sequence:number}};
type Options={
 database:SQLiteReplicaDatabase;engine:SQLiteTypedReplicaEngine;journal:SQLiteIntentJournal;predictions:SQLitePredictions;
 runtime:Pick<PortableReducerRuntime,'execute'|'prepare'|'isPrepared'>;artifactHash:string;
 /** Captured by the worker from its authenticated session and admitted plans.
  * Application arguments never select an identity or grant replica coverage. */
 context(scope:string):Promise<{identity:LocalExecution['identity'];coverage:ReadCoverage}>;
};
const schema=`CREATE TABLE IF NOT EXISTS gv_reducer_invocations(scope TEXT NOT NULL,command_id TEXT NOT NULL,hash TEXT NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(scope,command_id)) WITHOUT ROWID;`;
async function digest(text:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text))),byte=>byte.toString(16).padStart(2,'0')).join('');}

/** The owner executes the generated portable body against a fenced disk read
 * view, then commits predictions, durable intent and original reply together.
 * Same-command replay returns before executing the body against its own prior
 * prediction. Only the owner FIFO may invoke this component. */
export class SQLiteReducerCore {
 private initialized?:Promise<void>;
 constructor(private readonly options:Options){}
 ready(){
  if(!this.initialized){if(this.options.database.ownerTransactionDepth)throw new Error('Prepare reducer core before an owner transaction');this.initialized=(async()=>{
   await this.options.journal.ready();await this.options.predictions.ready();await this.options.database.withTransactionAsync(async()=>{await this.options.engine.verifyOwner();await this.options.database.execAsync(schema);});
  })().catch(error=>{this.initialized=undefined;throw error;});}return this.initialized;
 }
 private async stored(scope:string,command:string,hash:string):Promise<SQLiteReducerReceipt|undefined>{
  const old=await this.options.database.getFirstAsync<{hash:string;receipt:string}>('SELECT hash,receipt FROM gv_reducer_invocations WHERE scope=? AND command_id=?',scope,command);
  if(!old)return;if(old.hash!==hash)fail('INTENT_CONFLICT','Reducer command reused with different invocation');decodedJSONBytes(old.receipt,131072);return JSON.parse(old.receipt);
 }
 /** This may load a chunk; call it outside context.runOwner. */
 prepare(path:string){if(typeof path!=='string'||!path||path.length>1024)throw new TypeError('Invalid reducer path');return this.options.runtime.prepare(path);}
 async invoke(input:SQLiteReducerInvocation):Promise<SQLiteReducerReceipt>{
  sqlitePayloadBytes(input,131072);
  if(typeof input.scope!=='string'||!input.scope||input.scope.length>512||input.scope.includes('\u0000')||typeof input.path!=='string'||!input.path||input.path.length>1024||typeof input.commandId!=='string'||!input.commandId||input.commandId.length>128||!Number.isSafeInteger(input.now)||input.now<0||input.intentEntropy!==undefined&&(typeof input.intentEntropy!=='string'||input.intentEntropy.length>256))throw new TypeError('Invalid local reducer invocation');
  // Retain only admitted known envelope fields. Arbitrary extra fields cannot
  // become identity, execution metadata or a different retry fingerprint.
  const encoded=JSON.stringify({scope:input.scope,path:input.path,commandId:input.commandId,args:input.args,now:input.now,...(input.intentEntropy===undefined?{}:{intentEntropy:input.intentEntropy})}),owned=JSON.parse(encoded) as SQLiteReducerInvocation,hash=await digest(encoded);
  await this.ready();let receipt:SQLiteReducerReceipt|undefined;
  // A durable same-command reply remains available offline even when the
  // corresponding lazy reducer module has not been cached in this worker.
  await this.options.database.withTransactionAsync(async()=>{await this.options.engine.verifyOwner();await this.options.context(owned.scope);receipt=await this.stored(owned.scope,owned.commandId,hash);});
  if(receipt)return receipt;if(!this.options.runtime.isPrepared(owned.path))throw new SQLiteReducerPreparationRequiredError(owned.path);
  await this.options.database.withTransactionAsync(async()=>{
   const generation=await this.options.engine.verifyOwner(),captured=await this.options.context(owned.scope);sqlitePayloadBytes(captured,131072);
   const old=await this.stored(owned.scope,owned.commandId,hash);if(old){receipt=old;return;}
   const execution:LocalExecution={scope:owned.scope,commandId:owned.commandId,now:owned.now,artifactHash:this.options.artifactHash,identity:structuredClone(captured.identity),...(owned.intentEntropy===undefined?{}:{intentEntropy:owned.intentEntropy})};
   const transaction:LocalTransactionResult=await this.options.predictions.withReadView(owned.scope,captured.coverage,view=>this.options.runtime.execute(owned.path,owned.args,view,execution));sqlitePayloadBytes(transaction,262144);
   const dependencies={reads:transaction.readRows??[],writes:transaction.patches.map(patch=>({entity:patch.entity,rowId:patch.rowId})),scans:transaction.scanTables??transaction.readTables};
   const intent=await this.options.journal.append({scope:owned.scope,path:owned.path,args:owned.args,idempotencyKey:owned.commandId,entityKeys:[],dependencies,patches:transaction.patches,localExecution:execution,createdAt:owned.now,attempts:0,nextAttemptAt:owned.now,state:'pending',...(transaction.coalesceKey?{coalesceKey:transaction.coalesceKey}:{})});
   await this.options.predictions.apply(owned.scope,intent.id,transaction.patches);
   if(generation.sequence>=Number.MAX_SAFE_INTEGER)fail('FENCED','Storage sequence exhausted');
   receipt={intentId:intent.id,commandId:owned.commandId,result:transaction.result,originalGeneration:{epoch:generation.epoch,sequence:generation.sequence+1}};
   sqlitePayloadBytes(receipt,131072);const stored=JSON.stringify(receipt);decodedJSONBytes(stored,131072);if(new TextEncoder().encode(stored).byteLength>130000)fail('BUDGET','Reducer receipt exceeds native wire budget');
   await this.options.database.runAsync('INSERT INTO gv_reducer_invocations VALUES(?,?,?,?)',owned.scope,owned.commandId,hash,stored);
   await this.options.database.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1',receipt.originalGeneration.sequence);
  });return receipt!;
 }
 /** Rebuild derived predictions over a new authoritative base, atomically with
  * the trusted owner's canonical commit/ack callback. Only a bounded intent
  * page is resident. A missing module rolls back everything and asks the host
  * to preload outside the SQL queue before retrying. Original replies and
  * immutable intent receipts are never replaced by prediction replay. */
 async rebase(scope:string,authority?:()=>Promise<void>):Promise<{replayed:number;retained:number;unavailable:number;generation:{epoch:number;sequence:number}}>{
  if(typeof scope!=='string'||!scope||scope.length>512||scope.includes('\u0000'))throw new TypeError('Invalid reducer scope');
  await this.ready();let result!:{replayed:number;retained:number;unavailable:number;generation:{epoch:number;sequence:number}};
  await this.options.database.withTransactionAsync(async()=>{
   await this.options.engine.verifyOwner();await this.options.context(scope);await authority?.();
   const captured=await this.options.context(scope);sqlitePayloadBytes(captured,131072);
   await this.options.predictions.reset(scope);let after=0,replayed=0,retained=0,unavailable=0;
   for(;;){
    const page=await this.options.journal.page(scope,after,8);
    for(const entry of page.entries){
     if(entry.state==='rejected')continue;
     let patches=entry.patches??[];
     if(entry.localExecution&&entry.state!=='inflight'&&entry.state!=='committed'){
      if(!this.options.runtime.isPrepared(entry.path))throw new SQLiteReducerPreparationRequiredError(entry.path);
      const execution:LocalExecution={...entry.localExecution,scope,identity:structuredClone(captured.identity),artifactHash:this.options.artifactHash};
      let readFailure:{error:unknown}|undefined;
      const outcome=await this.options.predictions.withReadView(scope,captured.coverage,async view=>{
       try{return {transaction:await this.options.runtime.execute(entry.path,entry.args as JsonValue,{
        async select(read){try{return await view.select(read);}catch(error){readFailure={error};throw error;}}
       },execution)};}
       catch(error){if(readFailure)throw readFailure.error;return {unavailable:true as const};}
      });
      const transaction=outcome.transaction;
      if(transaction)sqlitePayloadBytes(transaction,262144);
      patches=transaction?.patches??[];
      const dependencies=transaction?{reads:transaction.readRows??[],writes:transaction.patches.map(patch=>({entity:patch.entity,rowId:patch.rowId})),scans:transaction.scanTables??transaction.readTables}:entry.dependencies;
      const updated=await this.options.journal.update(scope,entry.id,[entry.state],before=>({...before,patches,localExecution:execution,...(dependencies?{dependencies}:{})}));
      if(!updated)fail('FENCED','Intent changed during prediction replay');
      if(transaction)replayed++;else unavailable++;
     }else retained++;
     const local:LocalPatch[]=patches.map(patch=>{
      const entity=patch.entity??patch.collection;if(!entity)throw new TypeError('Intent prediction has no entity');
      return patch.op==='delete'?{entity,rowId:patch.rowId,op:'delete'}:{entity,rowId:patch.rowId,op:patch.op==='upsert'?'insert':patch.op,fields:patch.fields as Record<string,JsonValue>};
     });
     await this.options.predictions.apply(scope,entry.id,local);
    }
    if(page.nextId===null)break;after=page.nextId;
   }
   const generation=await this.options.engine.verifyOwner();if(generation.sequence>=Number.MAX_SAFE_INTEGER)fail('FENCED','Storage sequence exhausted');
   generation.sequence++;await this.options.database.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1',generation.sequence);
   result={replayed,retained,unavailable,generation};
  });return result;
 }

}
