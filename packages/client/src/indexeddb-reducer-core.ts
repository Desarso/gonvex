import {SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
import type {LocalExecution,LocalTransactionResult} from '@gonvex/local-runtime';
import type {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import type {SQLiteReducerInvocation,SQLiteReducerReceipt,SQLitePreparedReducer,SQLiteReducerContext} from './sqlite-reducer-core.js';
import type {IndexedDBReplicaEngine} from './indexeddb-replica-engine.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
const stable=(value:unknown):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable((value as Record<string,unknown>)[key])).join(',')+'}';
type Context=SQLiteReducerContext&{generation:string};
type Options={engine:IndexedDBReplicaEngine;artifactHash:string;runtime:Pick<PortableReducerRuntime,'execute'|'prepare'|'isPrepared'>;context(scope:string,transaction?:IDBTransaction):Promise<Context>;assertCurrent(scope:string,generation:string):void;settledReply?(input:SQLiteReducerInvocation,context:Pick<Context,'identity'|'intentScope'>):Promise<SQLiteReducerReceipt|undefined>;/** Writes only to the independent recovery journal, never this engine's DB. */persist?(input:SQLiteReducerInvocation,context:Pick<Context,'identity'|'intentScope'>,prepared:SQLitePreparedReducer):Promise<void>};
/** The fallback executes the same generated portable reducer against fenced
 * IDB pages. Publication validates the read generation in its strict intent
 * transaction. An independent prepared receipt is accepted while that write
 * transaction remains alive, before the original reply can become visible. */
export class IndexedDBReducerCore {
 constructor(private readonly options:Options){if(!options.artifactHash||options.artifactHash.length>256)throw new TypeError('Invalid fallback artifact identity');}
 private async context(scope:string){const context=await this.options.context(scope);sqlitePayloadBytes(context,524288);if(!context.intentScope||context.intentScope.length>512||!context.generation||context.generation.length>128||!context.identity?.auth?.account?.id)fail('FENCED','Fallback intent lacks its admitted identity');return structuredClone(context);}
 private async current(scope:string,expected:Context){const current=await this.context(scope);if(current.generation!==expected.generation||current.intentScope!==expected.intentScope)fail('FENCED','Fallback reducer admission changed');return current;}
 prepare(path:string){if(!path||path.length>1024)throw new TypeError('Invalid fallback reducer path');return this.options.runtime.prepare(path);}
 async invoke(input:SQLiteReducerInvocation):Promise<SQLiteReducerReceipt>{
  sqlitePayloadBytes(input,131072);if(input.intentEntropy!==undefined&&(typeof input.intentEntropy!=='string'||input.intentEntropy.length>256))throw new TypeError('Invalid fallback intent entropy');const owned:SQLiteReducerInvocation={scope:input.scope,path:input.path,commandId:input.commandId,args:structuredClone(input.args),now:input.now,...(input.intentEntropy===undefined?{}:{intentEntropy:input.intentEntropy})},admitted=await this.context(owned.scope),old=await this.options.engine.reply(owned,admitted.intentScope,{identity:admitted.identity,artifactHash:this.options.artifactHash});await this.current(owned.scope,admitted);if(old)return old;const settled=await this.options.settledReply?.(owned,admitted);await this.current(owned.scope,admitted);if(settled)return settled;
  if(!admitted.predictionBlocked)await this.prepare(owned.path);await this.current(owned.scope,admitted);const generation=await this.options.engine.generation();if(generation.sequence>=Number.MAX_SAFE_INTEGER)fail('FENCED','Fallback intent sequence exhausted');
  const execution:LocalExecution={scope:owned.scope,commandId:owned.commandId,now:owned.now,artifactHash:this.options.artifactHash,identity:admitted.identity,...(owned.intentEntropy===undefined?{}:{intentEntropy:owned.intentEntropy})};let transaction:LocalTransactionResult|undefined;
  try{if(!admitted.predictionBlocked)transaction=await this.options.engine.withReadView(owned.scope,admitted.coverage,view=>this.options.runtime.execute(owned.path,owned.args,view,execution),generation);}catch(error){if(!(error instanceof Error&&(error.name==='IncompleteReplicaError'||'code'in error&&error.code==='replica-read-budget')))throw error;}
  if(transaction)sqlitePayloadBytes(transaction,262144);await this.current(owned.scope,admitted);const patches=transaction?.patches??[],id=generation.sequence+1,receipt:SQLiteReducerReceipt={intentId:id,commandId:owned.commandId,result:transaction?.result??null,...(!transaction?{queuedOnly:true as const}:{}),originalGeneration:{epoch:generation.epoch,sequence:id}},prepared:SQLitePreparedReducer={receipt,intent:{id,scope:admitted.intentScope,path:owned.path,args:owned.args,idempotencyKey:owned.commandId,createdAt:owned.now,attempts:0,nextAttemptAt:owned.now,state:'pending',entityKeys:transaction?[]:['__gonvex_local_intents'],patches,localExecution:execution,...(transaction?{dependencies:{reads:transaction.readRows??[],writes:patches.map(patch=>({entity:patch.entity,rowId:patch.rowId})),scans:transaction.scanTables??transaction.readTables},...(transaction.coalesceKey?{coalesceKey:transaction.coalesceKey}:{})}:{})}};
  return this.options.engine.commit(owned,prepared,admitted.intentScope,generation,{persist:async(captured,value)=>{this.options.assertCurrent(owned.scope,admitted.generation);await this.options.persist?.(captured,{identity:admitted.identity,intentScope:admitted.intentScope},value);this.options.assertCurrent(owned.scope,admitted.generation);}});
 }
 /** Authority, accepted/rejected settlement and all derived predictions swap
  * in one strict transaction. Original envelopes/results remain immutable.
  * Modules load outside that transaction; only eight pending bodies are live. */
 async rebase(scope:string,authority?:(transaction:IDBTransaction)=>Promise<void>){
  const admitted=await this.context(scope),loaded=new Set<string>();
  const prepare=async(path:string)=>{if(loaded.has(path)||loaded.size>=128)fail('BUDGET','Fallback rebase module allowance exceeded');loaded.add(path);await this.prepare(path);await this.current(scope,admitted);};
  // Bound preloading to the prefix already durable when this rebase started.
  // An arriving command with a new module is caught by the atomic retry below.
  const through=(await this.options.engine.generation()).sequence;let after=0;
  for(;;){const page=await this.options.engine.deliveryPage(admitted.intentScope,after);for(const record of page.records){if(record.sequence>through)break;const entry={...record.prepared.intent,...record.delivery};if(!admitted.predictionBlocked&&entry.state!=='rejected'&&entry.sentAt===undefined&&entry.localExecution?.artifactHash===this.options.artifactHash&&!entry.entityKeys.includes('__gonvex_local_intents')&&!this.options.runtime.isPrepared(entry.path))await prepare(entry.path);}if(page.nextSequence===null||page.records.some(record=>record.sequence>=through))break;after=page.nextSequence;}
  for(;;){const generation=await this.options.engine.generation();await this.current(scope,admitted);try{
   return await this.options.engine.atomic(generation,async tx=>{
    this.options.assertCurrent(scope,admitted.generation);await authority?.(tx);const captured=await this.options.context(scope,tx);sqlitePayloadBytes(captured,524288);if(captured.generation!==admitted.generation||captured.intentScope!==admitted.intentScope)fail('FENCED','Fallback rebase identity changed');await this.options.engine.resetPredictionsInTransaction(tx,scope);let cursor=0,replayed=0,retained=0,unavailable=0;const sharedView=this.options.engine.readViewInTransaction(tx,scope,captured.coverage);
    for(;;){const page=await this.options.engine.pendingInTransaction(tx,captured.intentScope,cursor);for(const record of page.records){const entry={...record.prepared.intent,...record.delivery};if(entry.state==='rejected')continue;const execution=entry.localExecution,compatible=execution?.scope===scope&&execution.artifactHash===this.options.artifactHash&&stable(execution.identity)===stable(captured.identity);let patches=compatible?entry.patches??[]:[];
      if(compatible&&!captured.predictionBlocked&&!entry.entityKeys.includes('__gonvex_local_intents')&&entry.sentAt===undefined&&entry.state!=='inflight'&&entry.state!=='committed'){
       if(!this.options.runtime.isPrepared(entry.path))throw new SQLiteReducerPreparationRequiredError(entry.path);
       let readFailure:{error:unknown}|undefined;const view=sharedView;try{const value=await this.options.runtime.execute(entry.path,entry.args as import('@gonvex/protocol').JsonValue,{async select(read){try{return await view.select(read);}catch(error){readFailure={error};throw error;}}},{...execution,scope,identity:captured.identity});sqlitePayloadBytes(value,262144);patches=value.patches;replayed++;}catch(error){if(readFailure&&(readFailure.error as {code?:string})?.code!=='replica-read-budget')throw readFailure.error;patches=[];unavailable++;}
      }else retained++;
      await this.options.engine.predictionsInTransaction(tx,scope,entry.idempotencyKey,patches);this.options.assertCurrent(scope,admitted.generation);
     }if(page.nextSequence===null)break;cursor=page.nextSequence;}
    this.options.assertCurrent(scope,admitted.generation);return {replayed,retained,unavailable};
   });
  }catch(error){if(!(error instanceof SQLiteReducerPreparationRequiredError))throw error;await prepare(error.path);}}
 }
 /** The independently accepted result wins after a crash or late IDB failure.
  * Never execute that command again over its own original prediction. */
 async recover(input:SQLiteReducerInvocation,prepared:SQLitePreparedReducer):Promise<SQLiteReducerReceipt>{
  sqlitePayloadBytes({input,prepared},262144);const owned=structuredClone(input),captured=structuredClone(prepared),admitted=await this.context(owned.scope),execution=captured.intent.localExecution;if(!execution||captured.intent.scope!==admitted.intentScope||execution.identity.auth.account?.id!==admitted.identity.auth.account?.id||execution.identity.tenant?.id!==admitted.identity.tenant?.id)fail('FENCED','Fallback prepared command belongs to another identity');
  const compatible=execution.scope===owned.scope&&execution.artifactHash===this.options.artifactHash&&stable(execution.identity)===stable(admitted.identity),generation=await this.options.engine.generation();await this.current(owned.scope,admitted);await this.options.engine.commit({...owned,scope:execution.scope},captured,admitted.intentScope,generation,{predict:compatible,recovered:true});await this.current(owned.scope,admitted);const receipt=await this.options.engine.reply(owned,admitted.intentScope,{identity:admitted.identity,artifactHash:this.options.artifactHash});if(!receipt)fail('FENCED','Fallback recovery lost its receipt');return receipt;
 }
}
