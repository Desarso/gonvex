import type {JsonValue} from '@gonvex/protocol';
import type {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import type {SQLiteOwnerCaller,SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteReducerCore,SQLiteReducerPreparationRequiredError,type SQLiteReducerReceipt,type SQLiteReducerInvocation,type SQLiteReducerContext,type SQLitePreparedReducer,type SQLiteReducerRecoveryContext} from './sqlite-reducer-core.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import {SQLitePredictions} from './sqlite-predictions.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
export type SQLiteTabAdmission={scope:string;generation:string};
export type SQLiteReducerOwnerOptions={
 runtime:Pick<PortableReducerRuntime,'prepare'|'execute'|'isPrepared'>;artifactHash:string;
 /** Owned by the authenticated worker session registry, never by call args. */
 resolveTab(tabID:string):SQLiteTabAdmission;
 context(scope:string):Promise<SQLiteReducerContext>;
 settledReply?(input:SQLiteReducerInvocation,context:SQLiteReducerRecoveryContext):Promise<SQLiteReducerReceipt|undefined>;
 /** Independent local recovery durability, before the SQLite intent commits. */
 persistIntent?(input:Readonly<SQLiteReducerInvocation>,context:SQLiteReducerRecoveryContext,prepared:SQLitePreparedReducer):Promise<void>;
};
export type SQLiteOwnerReducerCall={path:string;args:JsonValue;now:number;intentEntropy?:string};
/** The native client program composes this endpoint with its authenticated
 * transport. Cold module downloads happen outside the shared SQL FIFO. The
 * caller's stable request ID is the durable command ID across ownership loss. */
export class SQLiteReducerOwner {
 readonly journal:SQLiteIntentJournal;
 readonly predictions:SQLitePredictions;
 readonly core:SQLiteReducerCore;
 private readonly initialized:Promise<void>;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly options:SQLiteReducerOwnerOptions){
  const fence=()=>owner.engine.verifyOwner();
  this.journal=new SQLiteIntentJournal(owner.database,fence);
  this.predictions=new SQLitePredictions(owner.database,owner.canonical,owner.engine.schema,fence);
  owner.reader.usePredictions(this.predictions);owner.pendingReader.usePredictions(this.predictions);
  this.core=new SQLiteReducerCore({database:owner.database,engine:owner.engine,journal:this.journal,predictions:this.predictions,runtime:options.runtime,artifactHash:options.artifactHash,context:options.context,settledReply:options.settledReply});
  this.initialized=owner.runOwner(0,()=>this.core.ready());
  // The first call observes the same initialization error without a detached
  // rejected promise if a generated program has not received a call yet.
  void this.initialized.catch(()=>{});
 }
 private admission(tab:string){
  const value=this.options.resolveTab(tab);
  if(!value||typeof value.scope!=='string'||!value.scope||value.scope.length>512||value.scope.includes('\u0000')||typeof value.generation!=='string'||!value.generation||value.generation.length>128)throw new TypeError('Invalid authenticated tab admission');
  return {scope:value.scope,generation:value.generation};
 }
 private async run<T>(bytes:number,scope:string,operation:()=>Promise<T>):Promise<T>{
  const loaded=new Set<string>();let preparationBytes=0;
  for(;;){
   try{return await this.owner.runOwner(bytes,async()=>{await this.initialized;return operation();},[scope]);}
   catch(error){
    if(!(error instanceof SQLiteReducerPreparationRequiredError))throw error;
    if(loaded.has(error.path))fail('FENCED','Prepared reducer module is still unavailable');
    preparationBytes+=error.path.length*2+128;
    if(loaded.size>=128||preparationBytes>131072)fail('BUDGET','Reducer preparation exceeds bounded operation allowance');
    loaded.add(error.path);await this.core.prepare(error.path);
   }
  }
 }
 async invoke(input:SQLiteOwnerReducerCall,caller:SQLiteOwnerCaller):Promise<SQLiteReducerReceipt>{
  const bytes=sqlitePayloadBytes(input,131072);
  if(!caller||typeof caller.tabID!=='string'||!caller.tabID||caller.tabID.length>128||typeof caller.requestID!=='string'||!caller.requestID||caller.requestID.length>256)throw new TypeError('Invalid reducer caller');
  const owned=JSON.parse(JSON.stringify(input)) as SQLiteOwnerReducerCall,admitted=this.admission(caller.tabID);
  return this.run(bytes,admitted.scope,async()=>{let receipt!:SQLiteReducerReceipt;await this.owner.database.withTransactionAsync(async()=>{
   if(caller.signal.aborted)fail('FENCED','Reducer caller is detached');
   const current=this.admission(caller.tabID);
   if(current.scope!==admitted.scope||current.generation!==admitted.generation)fail('FENCED','Reducer caller session changed');
   const check=()=>{if(caller.signal.aborted)fail('FENCED','Reducer caller is detached');const current=this.admission(caller.tabID);if(current.scope!==admitted.scope||current.generation!==admitted.generation)fail('FENCED','Reducer caller session changed');};
   receipt=await this.core.invoke({scope:admitted.scope,path:owned.path,args:owned.args,now:owned.now,...(owned.intentEntropy===undefined?{}:{intentEntropy:owned.intentEntropy}),commandId:caller.requestID},async(input,context,prepared)=>{check();await this.options.persistIntent?.(input,context,prepared);check();});
   check();});return receipt;
  });
 }
 /** Internal server reconciliation only. This callback is never deserialized
  * from an application RPC or accepted as an application function argument. */
 rebase(scope:string,authority?:()=>Promise<void>){return this.run(0,scope,()=>this.core.rebase(scope,authority));}
}
