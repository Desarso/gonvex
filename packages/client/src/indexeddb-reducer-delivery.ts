import {indexedDBEffectiveIntent} from './indexeddb-intent-ledger.js';
import type {JsonValue} from '@gonvex/protocol';
import type {ReducerOutboxEntry,OutboxErrorClass} from './outbox.js';
import type {ReducerRejectionEvent} from './index.js';
import type {IndexedDBReducerCore} from './indexeddb-reducer-core.js';
import type {IndexedDBReplicaEngine} from './indexeddb-replica-engine.js';
import type {IndexedDBDeliveryCandidate} from './indexeddb-intent-ledger.js';
import type {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {outboxBackoffMs} from './outbox.js';
import {deliveryErrorClass,DEFAULT_OUTBOX_MAX_ATTEMPTS,DEFAULT_OUTBOX_RETRY_MAX_BACKOFF_MS} from './client-error.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
type Admission={scope:string;intentScope:string;generation:string;online:boolean};
type Options={engine:IndexedDBReplicaEngine;core:IndexedDBReducerCore;safety:Pick<SQLiteSafetyJournal,'settle'>;admission():Admission|undefined;
 runOwner<T>(bytes:number,run:()=>Promise<T>,scopes?:readonly string[]):Promise<T>;
 send(entry:ReducerOutboxEntry):Promise<JsonValue>;rejected?(event:ReducerRejectionEvent):void;updateRequired?(message:string):void;now?():number;maxAttempts?:number;maxBackoffMs?:number;
};
type Outcome={record:IndexedDBDeliveryCandidate;entry:ReducerOutboxEntry;accepted:true}|{record:IndexedDBDeliveryCandidate;entry:ReducerOutboxEntry;accepted:false;error:string;classification:OutboxErrorClass};
/** Eight immutable envelopes at a time. Claim commits before sending; unknown
 * outcomes preserve their first-send fence. Authority arrives through the native
 * feed, then settlement and prediction removal commit in one atomic rebase. */
export class IndexedDBReducerDelivery {
 private active?:Promise<{sent:number;nextDelay:number|null}>;private recovered?:string;private closed=false;private updateBlocked=false;private retainedBytes=0;private entries=0;
 private readonly attempts:number;private readonly backoff:number;
 constructor(private readonly options:Options){this.attempts=options.maxAttempts??DEFAULT_OUTBOX_MAX_ATTEMPTS;this.backoff=options.maxBackoffMs??DEFAULT_OUTBOX_RETRY_MAX_BACKOFF_MS;if(!(this.attempts===Infinity||Number.isSafeInteger(this.attempts)&&this.attempts>0)||!Number.isSafeInteger(this.backoff)||this.backoff<1)throw new TypeError('Invalid fallback retry policy');}
 private admission(){const value=this.options.admission();if(!value||!value.online||this.closed||this.updateBlocked)return;sqlitePayloadBytes(value,4096);if(!value.scope||value.scope.length>512||!value.intentScope||value.intentScope.length>512||!value.generation||value.generation.length>128)throw new TypeError('Invalid fallback delivery admission');return {...value};}
 private check(expected:Admission){const current=this.admission();if(!current||current.scope!==expected.scope||current.intentScope!==expected.intentScope||current.generation!==expected.generation)fail('FENCED','Fallback delivery admission changed');}
 private now(){const value=(this.options.now??Date.now)();if(!Number.isSafeInteger(value)||value<0)throw new TypeError('Invalid fallback delivery clock');return value;}
 private run<T>(expected:Admission,run:()=>Promise<T>){return this.options.runOwner(262144,async()=>{this.check(expected);const value=await run();this.check(expected);return value;});}
 private async flushSafety(expected:Admission){const rows=await this.run(expected,()=>this.options.engine.safetySettlementPage(expected.intentScope));for(const row of rows){this.check(expected);await this.options.safety.settle(expected.intentScope,row.command,{status:row.status,...(row.error?{error:row.error}:{})});await this.run(expected,()=>this.options.engine.markSafetySettled(expected.intentScope,row.command,row.status));}return rows.length;}
 private async recover(expected:Admission){
  const key=JSON.stringify([expected.intentScope,expected.generation]);if(key===this.recovered)return;let after=0;for(;;){const page=await this.run(expected,()=>this.options.engine.deliveryPage(expected.intentScope,after)),inflight=page.records.filter(record=>['inflight','committed'].includes(record.delivery?.state??record.prepared.intent.state));if(inflight.length)await this.run(expected,()=>this.options.engine.metadataAtomic(undefined,async tx=>{for(const record of inflight){this.check(expected);await this.options.engine.updateDeliveryInTransaction(tx,expected.intentScope,record.command,record.hash,['inflight','committed'],{state:'pending',sentAt:record.delivery?.sentAt??record.prepared.intent.sentAt??this.now()});}}));if(page.nextSequence===null)break;after=page.nextSequence;}this.check(expected);this.recovered=key;
 }
 private async claim(expected:Admission){
  for(let attempt=0;attempt<3;attempt++){
   const generation=await this.run(expected,()=>this.options.engine.generation()),records:IndexedDBDeliveryCandidate[]=[];let after=0,next=Infinity,bytes=64,limited=false;const now=this.now();
   for(;;){const page=await this.run(expected,()=>this.options.engine.deliveryPage(expected.intentScope,after));for(const record of page.records){const entry=indexedDBEffectiveIntent(record);if(entry.state!=='pending')continue;if(entry.nextAttemptAt>now){next=Math.min(next,entry.nextAttemptAt);continue;}if(!await this.run(expected,()=>this.options.engine.canDeliver(record,generation)))continue;const size=sqlitePayloadBytes(entry,262144)+512;if(bytes+size>262144){if(!records.length)fail('BUDGET','Fallback delivery envelope exceeds allowance');limited=true;break;}records.push({scope:record.scope,command:record.command,sequence:record.sequence,hash:record.hash,entry});bytes+=size;if(records.length===8)break;}if(records.length||limited||page.nextSequence===null)break;after=page.nextSequence;}
   if(!records.length)return {records,entries:[] as ReducerOutboxEntry[],nextDelay:next===Infinity?null:Math.max(0,next-now)};
   try{const entries=await this.run(expected,()=>this.options.engine.metadataAtomic(generation,async tx=>{const entries:ReducerOutboxEntry[]=[];for(const record of records){this.check(expected);if(!await this.options.engine.canDeliverInTransaction(tx,record))throw Object.assign(Error('Fallback causal delivery changed'),{code:'BUSY'});entries.push(await this.options.engine.updateDeliveryInTransaction(tx,expected.intentScope,record.command,record.hash,['pending'],{state:'inflight',sentAt:record.entry.sentAt??now}));}return entries;}));return {records,entries,nextDelay:0};}catch(error){if(!['PAGE_CHANGED','FENCED','BUSY'].includes(String((error as {code?:string})?.code)))throw error;this.check(expected);}
  }return {records:[] as IndexedDBDeliveryCandidate[],entries:[] as ReducerOutboxEntry[],nextDelay:10};
 }
 drainOnce(){if(this.active)return this.active;const operation=this.drain();this.active=operation;void operation.then(()=>{this.active=undefined;this.retainedBytes=0;this.entries=0;},()=>{this.active=undefined;this.recovered=undefined;this.retainedBytes=0;this.entries=0;});return operation;}
 private async drain():Promise<{sent:number;nextDelay:number|null}>{
  const expected=this.admission();if(!expected)return {sent:0,nextDelay:null};await this.recover(expected);await this.flushSafety(expected);const claim=await this.claim(expected);this.retainedBytes=sqlitePayloadBytes(claim.entries,262144)+sqlitePayloadBytes(claim.records,262144);this.entries=claim.entries.length;
  const outcomes:Outcome[]=await Promise.all(claim.entries.map(async(entry,index)=>{const record=claim.records[index]!;try{this.check(expected);sqlitePayloadBytes(await this.options.send(entry),131072);return {record,entry,accepted:true as const};}catch(error){return {record,entry,accepted:false as const,classification:deliveryErrorClass(error),error:String(error instanceof Error?error.message:error).slice(0,512)};}}));
  if(outcomes.length)await this.options.runOwner(262144,()=>this.options.core.rebase(expected.scope,async tx=>{this.check(expected);const now=this.now();for(const outcome of outcomes){if(outcome.accepted||outcome.classification==='rejected')await this.options.engine.settleInTransaction(tx,expected.intentScope,outcome.entry.idempotencyKey,outcome.accepted?'accepted':'rejected',outcome.accepted?undefined:outcome.error);else{const counted=outcome.classification==='transient',attempts=outcome.entry.attempts+Number(counted),failed=counted&&attempts>=this.attempts;await this.options.engine.updateDeliveryInTransaction(tx,expected.intentScope,outcome.record.command,outcome.record.hash,['inflight'],{state:failed?'failed':'pending',attempts,lastError:outcome.error,lastErrorClass:outcome.classification,nextAttemptAt:now+(outcome.classification==='update_required'?0:counted?outboxBackoffMs(attempts,this.backoff):1000),...(failed?{settledAt:now}:{})});}this.check(expected);}}),[expected.scope]);
  for(const outcome of outcomes)if(!outcome.accepted&&outcome.classification==='rejected')try{this.options.rejected?.({reducerId:outcome.entry.idempotencyKey,path:outcome.entry.path,error:outcome.error,errorClass:'rejected'});}catch{/* Observer only. */}
  const unsettled=await this.flushSafety(expected),update=outcomes.find(outcome=>!outcome.accepted&&outcome.classification==='update_required');if(update&&!update.accepted){this.updateBlocked=true;this.options.updateRequired?.(update.error);return {sent:outcomes.length,nextDelay:null};}return {sent:outcomes.length,nextDelay:outcomes.length||unsettled?0:claim.nextDelay};
 }
 stats(){return {bytes:this.retainedBytes+this.entries*2048+(this.recovered?.length??0)*2+1024,maxBytes:1310720,entries:this.entries,maxEntries:8,active:!!this.active};}
 close(){this.closed=true;}
}
