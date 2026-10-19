import type {ReducerOutboxEntry} from './outbox.js';
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
export type SQLiteReducerReceipt={intentId:number;commandId:string;result:JsonValue;queuedOnly?:true;originalGeneration:{epoch:number;sequence:number}};
export type SQLitePreparedReducer={intent:ReducerOutboxEntry;receipt:SQLiteReducerReceipt};
export type SQLiteReducerRecoveryContext=Pick<SQLiteReducerContext,'identity'|'intentScope'>;
export type SQLiteReducerContext={identity:LocalExecution['identity'];coverage:ReadCoverage;intentScope:string;predictionBlocked?:boolean};
type Options={
 database:SQLiteReplicaDatabase;engine:SQLiteTypedReplicaEngine;journal:SQLiteIntentJournal;predictions:SQLitePredictions;
 runtime:Pick<PortableReducerRuntime,'execute'|'prepare'|'isPrepared'>;artifactHash:string;
 /** Captured by the worker from its authenticated session and admitted plans.
  * Application arguments never select an identity or grant replica coverage. */
 context(scope:string,includeCoverage?:boolean):Promise<SQLiteReducerContext>;
 settledReply?(input:SQLiteReducerInvocation,context:SQLiteReducerRecoveryContext):Promise<SQLiteReducerReceipt|undefined>;
};
const schema=`CREATE TABLE IF NOT EXISTS gv_reducer_invocations(scope TEXT NOT NULL,command_id TEXT NOT NULL,hash TEXT NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(scope,command_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_reducer_reply_context(scope TEXT NOT NULL,command_id TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(scope,command_id)) WITHOUT ROWID;`;
const canonicalContext=(_scope:string,artifactHash:string,identity:LocalExecution['identity'])=>JSON.stringify({artifactHash,identity},(_key,value)=>value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.keys(value).filter(key=>value[key]!==undefined).sort().map(key=>[key,value[key]])):value);
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
 private async context(scope:string,includeCoverage=true){const captured=await this.options.context(scope,includeCoverage);sqlitePayloadBytes(captured,524288);if(typeof captured.intentScope!=='string'||!captured.intentScope||captured.intentScope.length>512)throw new TypeError('Invalid durable identity scope');return captured;}
 private async stored(scope:string,command:string,hash:string,admission:SQLiteReducerContext,visibilityScope:string):Promise<SQLiteReducerReceipt|undefined>{
  const old=await this.options.database.getFirstAsync<{hash:string;receipt:string;context_hash?:string}>('SELECT i.hash,i.receipt,c.hash context_hash FROM gv_reducer_invocations i LEFT JOIN gv_reducer_reply_context c USING(scope,command_id) WHERE i.scope=? AND i.command_id=?',scope,command);
  if(!old)return;if(old.hash!==hash)fail('INTENT_CONFLICT','Reducer command reused with different invocation');decodedJSONBytes(old.receipt,131072);const receipt=JSON.parse(old.receipt) as SQLiteReducerReceipt;return old.context_hash===await digest(canonicalContext(visibilityScope,this.options.artifactHash,admission.identity))?receipt:{...receipt,result:null,queuedOnly:true};
 }
 /** This may load a chunk; call it outside context.runOwner. */
 prepare(path:string){if(typeof path!=='string'||!path||path.length>1024)throw new TypeError('Invalid reducer path');return this.options.runtime.prepare(path);}
 async invoke(input:SQLiteReducerInvocation,persist?: (input:Readonly<SQLiteReducerInvocation>,context:SQLiteReducerRecoveryContext,prepared:SQLitePreparedReducer)=>Promise<void>):Promise<SQLiteReducerReceipt>{
  sqlitePayloadBytes(input,131072);
  if(typeof input.scope!=='string'||!input.scope||input.scope.length>512||input.scope.includes('\u0000')||typeof input.path!=='string'||!input.path||input.path.length>1024||typeof input.commandId!=='string'||!input.commandId||input.commandId.length>256||!Number.isSafeInteger(input.now)||input.now<0||input.intentEntropy!==undefined&&(typeof input.intentEntropy!=='string'||input.intentEntropy.length>256))throw new TypeError('Invalid local reducer invocation');
  // Retain only admitted known envelope fields. Arbitrary extra fields cannot
  // become identity, execution metadata or a different retry fingerprint.
  const encoded=JSON.stringify({scope:input.scope,path:input.path,commandId:input.commandId,args:input.args,now:input.now,...(input.intentEntropy===undefined?{}:{intentEntropy:input.intentEntropy})}),owned=JSON.parse(encoded) as SQLiteReducerInvocation;
  await this.ready();let receipt:SQLiteReducerReceipt|undefined,hash='',intentScope='',predictionBlocked=false;
  // A durable same-command reply remains available offline even when the
  // corresponding lazy reducer module has not been cached in this worker.
  await this.options.database.withTransactionAsync(async()=>{await this.options.engine.verifyOwner();const captured=await this.context(owned.scope);intentScope=captured.intentScope;predictionBlocked=!!captured.predictionBlocked;hash=await digest(JSON.stringify({...owned,scope:intentScope}));receipt=await this.stored(intentScope,owned.commandId,hash,captured,owned.scope)??await this.options.settledReply?.(owned,captured);await this.options.engine.verifyOwner();});
  if(receipt)return receipt;if(!predictionBlocked&&!this.options.runtime.isPrepared(owned.path))throw new SQLiteReducerPreparationRequiredError(owned.path);
  await this.options.database.withTransactionAsync(async()=>{
   const generation=await this.options.engine.verifyOwner(),captured=await this.context(owned.scope);if(captured.intentScope!==intentScope)fail('FENCED','Reducer durable identity changed');
   const old=await this.stored(intentScope,owned.commandId,hash,captured,owned.scope);if(old){receipt=old;return;}
   const execution:LocalExecution={scope:owned.scope,commandId:owned.commandId,now:owned.now,artifactHash:this.options.artifactHash,identity:structuredClone(captured.identity),...(owned.intentEntropy===undefined?{}:{intentEntropy:owned.intentEntropy})};
   let transaction:LocalTransactionResult|undefined;
   try{if(!captured.predictionBlocked)transaction=await this.options.predictions.withReadView(owned.scope,captured.coverage,view=>this.options.runtime.execute(owned.path,owned.args,view,execution));}
   catch(error){if(!(error instanceof Error&&(error.name==='IncompleteReplicaError'||'code' in error&&error.code==='replica-read-budget')))throw error;}
   if(transaction)sqlitePayloadBytes(transaction,262144);
   // Missing coverage is an explicit queued-only command, never an invented
   // result or partial prediction. Its barrier preserves causal delivery.
   const patches=transaction?.patches??[],dependencies=transaction?{reads:transaction.readRows??[],writes:patches.map(patch=>({entity:patch.entity,rowId:patch.rowId})),scans:transaction.scanTables??transaction.readTables}:undefined;
   const draft:Omit<ReducerOutboxEntry,'id'>={scope:intentScope,path:owned.path,args:owned.args,idempotencyKey:owned.commandId,entityKeys:transaction?[]:['__gonvex_local_intents'],...(dependencies?{dependencies}:{}),patches,localExecution:execution,createdAt:owned.now,attempts:0,nextAttemptAt:owned.now,state:'pending',...(transaction?.coalesceKey?{coalesceKey:transaction.coalesceKey}:{})};
   const intent=await this.options.journal.append(draft);
   await this.options.predictions.apply(owned.scope,intent.id,patches);
   if(generation.sequence>=Number.MAX_SAFE_INTEGER)fail('FENCED','Storage sequence exhausted');
   receipt={intentId:intent.id,commandId:owned.commandId,result:transaction?.result??null,...(!transaction?{queuedOnly:true as const}:{}),originalGeneration:{epoch:generation.epoch,sequence:generation.sequence+1}};
   sqlitePayloadBytes(receipt,131072);const stored=JSON.stringify(receipt);decodedJSONBytes(stored,131072);if(new TextEncoder().encode(stored).byteLength>130000)fail('BUDGET','Reducer receipt exceeds native wire budget');
   // The local body and all row/intent/reply admission have succeeded. Make
   // the independent recovery copy durable before this SQL transaction can
   // commit. A crash or later SQL failure leaves that command recoverable.
   await persist?.(structuredClone(owned),structuredClone({identity:captured.identity,intentScope:captured.intentScope}),{intent:{...draft,id:intent.id},receipt:structuredClone(receipt)});
   await this.options.engine.verifyOwner();
   await this.options.database.runAsync('INSERT INTO gv_reducer_invocations VALUES(?,?,?,?)',intentScope,owned.commandId,hash,stored);await this.options.database.runAsync('INSERT INTO gv_reducer_reply_context VALUES(?,?,?)',intentScope,owned.commandId,await digest(canonicalContext(owned.scope,this.options.artifactHash,captured.identity)));
   await this.options.database.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1',receipt.originalGeneration.sequence);
  });return receipt!;
 }
 /** Restore an independently verified prepared envelope before new work.
  * Within its original admission the exact reply remains immutable. A changed
  * artifact, permissions or visibility keeps the command queued for the server
  * and suppresses stale predictions/results. The safety copy stays intact. */
 recover(input:SQLiteReducerInvocation,prepared:SQLitePreparedReducer){return this.restore(input,prepared,false);}
 /** Old SDK journals never stored local replies. Preserve their stable command
  * and row IDs, without inventing a local result during adapter migration. */
 async recoverLegacy(input:SQLiteReducerInvocation,legacy:ReducerOutboxEntry){
  await this.ready();const generation=await this.options.engine.verifyOwner();return this.restore(input,{intent:legacy,receipt:{intentId:legacy.id,commandId:input.commandId,result:null,queuedOnly:true,originalGeneration:generation}},true);
 }
 private async restore(input:SQLiteReducerInvocation,prepared:SQLitePreparedReducer,legacy:boolean):Promise<SQLiteReducerReceipt>{
  sqlitePayloadBytes({input,prepared},262144);await this.ready();let restored!:SQLiteReducerReceipt;
  await this.options.database.withTransactionAsync(async()=>{
   const generation=await this.options.engine.verifyOwner(),captured=await this.context(input.scope),entry=prepared.intent,execution=entry.localExecution;
   if(entry.scope!==captured.intentScope||entry.idempotencyKey!==input.commandId||entry.path!==input.path||JSON.stringify(entry.args)!==JSON.stringify(input.args)||(legacy?execution?.now??entry.createdAt:entry.createdAt)!==input.now||prepared.receipt.commandId!==input.commandId||!legacy&&!execution||execution&&(execution.commandId!==input.commandId||execution.identity.auth.account?.id!==captured.identity.auth.account?.id||execution.identity.tenant?.id!==captured.identity.tenant?.id))fail('FENCED','Prepared recovery identity changed');
   const hash=await digest(JSON.stringify({scope:captured.intentScope,path:input.path,commandId:input.commandId,args:input.args,now:input.now,...(input.intentEntropy===undefined?{}:{intentEntropy:input.intentEntropy})})),old=await this.stored(captured.intentScope,input.commandId,hash,captured,input.scope);
   const replyCompatible=!!execution&&canonicalContext(execution.scope,execution.artifactHash,execution.identity)===canonicalContext(input.scope,this.options.artifactHash,captured.identity),compatible=replyCompatible&&execution!.scope===input.scope;
   const existing=await this.options.journal.find(captured.intentScope,input.commandId);if(old&&(compatible||!existing)){if(existing&&(existing.sentAt===undefined||existing.state==='inflight'||existing.state==='committed'))await this.options.journal.update(captured.intentScope,existing.id,[existing.state],before=>({...before,state:before.state==='inflight'||before.state==='committed'?'pending':before.state,sentAt:before.sentAt??input.now}));restored=old;return;}
   if(existing&&(existing.path!==entry.path||existing.createdAt!==entry.createdAt||JSON.stringify(existing.args)!==JSON.stringify(entry.args)))fail('INTENT_CONFLICT','Recovered command differs from its existing intent');
   const prior=existing??entry,{id:_id,...draft}=prior,state=prior.state==='inflight'||prior.state==='committed'?'pending':prior.state;
   const admitted={...draft,state,sentAt:prior.sentAt??input.now,patches:compatible?prior.patches??[]:[],...(execution?{localExecution:{...execution,scope:input.scope}}:{})};
   const intent=existing?await this.options.journal.update(captured.intentScope,existing.id,[existing.state],()=>({...admitted,id:existing.id})):await this.options.journal.append(admitted);
   if(!intent)fail('FENCED','Recovered intent lost its delivery state');
   const patches:LocalPatch[]=(admitted.patches??[]).map(patch=>{const entity=patch.entity??patch.collection;if(!entity)fail('INTENT_CORRUPT','Prepared patch has no entity');return patch.op==='delete'?{entity,rowId:patch.rowId,op:'delete'}:{entity,rowId:patch.rowId,op:patch.op==='upsert'?'insert':patch.op,fields:patch.fields as Record<string,JsonValue>};});
   if(state!=='rejected'&&await this.options.predictions.canApply(input.scope,patches))await this.options.predictions.apply(input.scope,intent.id,patches);
   const receipt=replyCompatible?prepared.receipt:{...prepared.receipt,result:null,queuedOnly:true as const};
   if(!old){const encoded=JSON.stringify(prepared.receipt);decodedJSONBytes(encoded,131072);await this.options.database.runAsync('INSERT INTO gv_reducer_invocations VALUES(?,?,?,?)',captured.intentScope,input.commandId,hash,encoded);await this.options.database.runAsync('INSERT INTO gv_reducer_reply_context VALUES(?,?,?)',captured.intentScope,input.commandId,await digest(canonicalContext(execution?.scope??input.scope,execution?.artifactHash??this.options.artifactHash,execution?.identity??captured.identity)));}
   if(generation.sequence>=Number.MAX_SAFE_INTEGER)fail('FENCED','Storage sequence exhausted');await this.options.database.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1',generation.sequence+1);restored=structuredClone(receipt);
  });return restored;
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
   await this.options.engine.verifyOwner();const before=await this.context(scope,false);await authority?.();
   // Snapshot publication still verifies admission on both sides of authority.
   // Coverage is only consumed when a durable intent might need replay. During
   // ordinary first sync there is no reason to aggregate every window column.
   const pending=await this.options.journal.summary(before.intentScope),captured=await this.context(scope,pending.count>0);if(captured.intentScope!==before.intentScope)fail('FENCED','Rebase durable identity changed');
   const hasPredictions=await this.options.database.getFirstAsync('SELECT 1 FROM gv_prediction_members WHERE scope=? LIMIT 1',scope);
   if(pending.count||hasPredictions)await this.options.predictions.reset(scope);let after=0,replayed=0,retained=0,unavailable=0;
   for(;pending.count;){
    const page=await this.options.journal.page(captured.intentScope,after,8);
    for(const entry of page.entries){
     if(entry.state==='rejected')continue;
     let patches=entry.patches??[];
     // A recovered pending delivery may already have committed on the server.
     // Its first-send fence survives recovery/backoff; executing its body again
     // over that canonical commit would predict the same intent twice.
     if(!captured.predictionBlocked&&entry.localExecution&&!entry.entityKeys.includes('__gonvex_local_intents')&&entry.sentAt===undefined&&entry.state!=='inflight'&&entry.state!=='committed'){
      if(!this.options.runtime.isPrepared(entry.path))throw new SQLiteReducerPreparationRequiredError(entry.path);
      const execution:LocalExecution={...entry.localExecution,scope,identity:structuredClone(captured.identity),artifactHash:this.options.artifactHash};
      let readFailure:{error:unknown}|undefined;
      const outcome=await this.options.predictions.withReadView(scope,captured.coverage,async view=>{
       try{return {transaction:await this.options.runtime.execute(entry.path,entry.args as JsonValue,{
        async select(read){try{return await view.select(read);}catch(error){readFailure={error};throw error;}}
       },execution)};}
       catch(error){
        // Missing certified coverage can be detected before view.select runs.
        // Keep the prior durable prediction while other sources catch up.
        if(error instanceof Error&&error.name==='IncompleteReplicaError')return {unavailable:true as const,preserve:true as const};
        if(readFailure)throw readFailure.error;return {unavailable:true as const};
       }
      });
      const transaction=outcome.transaction;
      if(transaction)sqlitePayloadBytes(transaction,262144);
      patches=transaction?.patches??(outcome.preserve?patches:[]);
      const dependencies=transaction?{reads:transaction.readRows??[],writes:transaction.patches.map(patch=>({entity:patch.entity,rowId:patch.rowId})),scans:transaction.scanTables??transaction.readTables}:entry.dependencies;
      const updated=await this.options.journal.update(captured.intentScope,entry.id,[entry.state],before=>({...before,patches,localExecution:execution,...(dependencies?{dependencies}:{})}));
      if(!updated)fail('FENCED','Intent changed during prediction replay');
      if(transaction)replayed++;else unavailable++;
     }else retained++;
     const local:LocalPatch[]=patches.map(patch=>{
      const entity=patch.entity??patch.collection;if(!entity)throw new TypeError('Intent prediction has no entity');
      return patch.op==='delete'?{entity,rowId:patch.rowId,op:'delete'}:{entity,rowId:patch.rowId,op:patch.op==='upsert'?'insert':patch.op,fields:patch.fields as Record<string,JsonValue>};
     });
     if(await this.options.predictions.canApply(scope,local))await this.options.predictions.apply(scope,entry.id,local);else unavailable++;
    }
    if(page.nextId===null)break;after=page.nextId;
   }
   const generation=await this.options.engine.verifyOwner();if(generation.sequence>=Number.MAX_SAFE_INTEGER)fail('FENCED','Storage sequence exhausted');
   generation.sequence++;await this.options.database.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1',generation.sequence);
   result={replayed,retained,unavailable,generation};
  });return result;
 }

}
