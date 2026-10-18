import {waitForSQLiteOwnerAdmission} from './sqlite-owner-transactions.js';
import type {JsonValue} from '@gonvex/protocol';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import type {SQLiteReducerOwner} from './sqlite-reducer-owner.js';
import type {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import type {ReducerRejectionEvent} from './index.js';
import type {ReducerOutboxEntry,OutboxErrorClass} from './outbox.js';
import {outboxBackoffMs} from './outbox.js';
import {deliveryErrorClass,DEFAULT_OUTBOX_MAX_ATTEMPTS,DEFAULT_OUTBOX_RETRY_MAX_BACKOFF_MS} from './client-error.js';
import {ByteLedger} from './byte-budget.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Admission={scope:string;intentScope:string;generation:string;online:boolean};
type Outcome={entry:ReducerOutboxEntry;accepted:true}|{entry:ReducerOutboxEntry;accepted:false;error:string;classification:OutboxErrorClass};
type Options={owner:SQLiteOwnerProgramContext;reducer:SQLiteReducerOwner;safety:Pick<SQLiteSafetyJournal,'settle'>;
 admission():Admission|undefined;
 /** Resolves only after GClient's durable native terminal-watermark fence. */
 send(entry:ReducerOutboxEntry):Promise<JsonValue>;
 rejected?(event:ReducerRejectionEvent):void;
 updateRequired?(message:string):void;
 now?():number;maxAttempts?:number;maxBackoffMs?:number;
};
const schema=`CREATE TABLE IF NOT EXISTS gv_reducer_delivery_settlements(scope TEXT NOT NULL,command TEXT NOT NULL,intent_id INTEGER NOT NULL,status TEXT NOT NULL,safety_settled INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,command)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_reducer_delivery_safety ON gv_reducer_delivery_settlements(scope,safety_settled,command);`;
/** Owner-only delivery of durable commands. At most eight bounded intents wait
 * on the network outside the SQL FIFO. Accepted/rejected settlement and derived
 * prediction rebase commit together; independent safety retirement follows that
 * commit and can resume after a crash or a failed IndexedDB transaction. */
export class SQLiteReducerDelivery {
 private readonly initialized:Promise<void>;private active?:Promise<{sent:number;nextDelay:number|null}>;
 private readonly ledger=new ByteLedger(327680);private readonly unregister:()=>void;
 private recovered?:string;private closed=false;private updateBlocked=false;private retainedBytes=0;private entries=0;
 private readonly maxAttempts:number;private readonly maxBackoff:number;
 constructor(private readonly options:Options){
  this.unregister=this.ledger.register('native-intent-delivery',()=>({name:'native-intent-delivery',...this.stats(),evictions:0,refusals:0}));
  this.maxAttempts=options.maxAttempts??DEFAULT_OUTBOX_MAX_ATTEMPTS;this.maxBackoff=options.maxBackoffMs??DEFAULT_OUTBOX_RETRY_MAX_BACKOFF_MS;
  if(!(this.maxAttempts===Infinity||Number.isSafeInteger(this.maxAttempts)&&this.maxAttempts>0)||!Number.isSafeInteger(this.maxBackoff)||this.maxBackoff<1)throw new TypeError('Invalid native intent retry policy');
  this.initialized=options.owner.runOwner(0,async()=>{await options.reducer.journal.ready();await options.owner.database.execAsync(schema);});void this.initialized.catch(()=>{});
 }
 private admission(){const admission=this.options.admission();if(!admission||!admission.online||this.closed||this.updateBlocked)return;sqlitePayloadBytes(admission,4096);if(!admission.scope||admission.scope.length>512||!admission.intentScope||admission.intentScope.length>512||!admission.generation||admission.generation.length>128)throw new TypeError('Invalid delivery admission');return {...admission};}
 private check(expected:Admission){const current=this.admission();if(!current||current.scope!==expected.scope||current.intentScope!==expected.intentScope||current.generation!==expected.generation)fail('FENCED','Intent delivery admission changed');}
 private now(){const now=(this.options.now??Date.now)();if(!Number.isSafeInteger(now)||now<0)throw new TypeError('Invalid intent delivery clock');return now;}
 private run<T>(expected:Admission,run:()=>Promise<T>){return waitForSQLiteOwnerAdmission(()=>this.options.owner.runOwner(262144,async()=>{await this.initialized;this.check(expected);return run();}),()=>this.check(expected));}
 private async flushSafety(expected:Admission){
  const rows=await this.run(expected,()=>this.options.owner.database.getAllAsync<{command:string}>('SELECT command FROM gv_reducer_delivery_settlements WHERE scope=? AND safety_settled=0 ORDER BY command LIMIT 8',expected.intentScope));
  for(const row of rows){this.check(expected);await this.options.safety.settle(expected.intentScope,row.command);await this.run(expected,()=>this.options.owner.database.runAsync('UPDATE gv_reducer_delivery_settlements SET safety_settled=1 WHERE scope=? AND command=?',expected.intentScope,row.command));}
 }
 drainOnce():Promise<{sent:number;nextDelay:number|null}>{
  if(this.active)return this.active;
  const operation=this.drain();this.active=operation;void operation.then(()=>{this.active=undefined;this.retainedBytes=0;this.entries=0;},()=>{this.active=undefined;this.recovered=undefined;this.retainedBytes=0;this.entries=0;});return operation;
 }
 private async drain(){
  const expected=this.admission();if(!expected)return {sent:0,nextDelay:null};
  const recovery=JSON.stringify([expected.intentScope,expected.generation]);
  if(this.recovered!==recovery){await this.run(expected,()=>this.options.reducer.journal.recoverInflight(expected.intentScope));this.recovered=recovery;}
  await this.flushSafety(expected);
  const entries=await this.run(expected,()=>this.options.reducer.journal.claim(expected.intentScope,this.now(),8));this.retainedBytes=sqlitePayloadBytes(entries,262144);this.entries=entries.length;
  const outcomes:Outcome[]=await Promise.all(entries.map(async entry=>{
   try{this.check(expected);const result=await this.options.send(entry);sqlitePayloadBytes(result,131072);return {entry,accepted:true as const};}
   catch(error){return {entry,accepted:false as const,classification:deliveryErrorClass(error),error:String(error instanceof Error?error.message:error).slice(0,512)};}
  }));
  // All network promises have settled before entering the shared SQL FIFO.
  // One rebase handles the batch; no command can disappear on a partial failure.
  if(entries.length)await this.options.reducer.rebase(expected.scope,async()=>{
   this.check(expected);const accepted:number[]=[],now=this.now();
   for(const outcome of outcomes){
    const entry=outcome.entry;
    if(outcome.accepted||outcome.classification==='rejected'){
     const status=outcome.accepted?'accepted':'rejected';
     const old=await this.options.owner.database.getFirstAsync<{intent_id:number;status:string}>('SELECT intent_id,status FROM gv_reducer_delivery_settlements WHERE scope=? AND command=?',expected.intentScope,entry.idempotencyKey);
     if(old&&(old.intent_id!==entry.id||old.status!==status))fail('INTENT_CONFLICT','Authoritative intent settlement changed');
     await this.options.owner.database.runAsync('INSERT OR IGNORE INTO gv_reducer_delivery_settlements(scope,command,intent_id,status) VALUES(?,?,?,?)',expected.intentScope,entry.idempotencyKey,entry.id,status);
     if(outcome.accepted)accepted.push(entry.id);
     else if(!await this.options.reducer.journal.update(expected.intentScope,entry.id,['inflight'],before=>({...before,state:'rejected',lastError:outcome.error,lastErrorClass:'rejected',settledAt:now})))fail('FENCED','Rejected intent lost its delivery fence');
    }else{
     const classification=outcome.classification,counted=classification==='transient',attempts=entry.attempts+Number(counted),parked=counted&&attempts>=this.maxAttempts;
     if(!await this.options.reducer.journal.update(expected.intentScope,entry.id,['inflight'],before=>({...before,state:parked?'failed':'pending',attempts,lastError:outcome.error,lastErrorClass:classification,nextAttemptAt:now+(classification==='update_required'?0:counted?outboxBackoffMs(attempts,this.maxBackoff):1000),...(parked?{settledAt:now}:{})})))fail('FENCED','Retry intent lost its delivery fence');
    }
   }
   await this.options.reducer.journal.acknowledge(expected.intentScope,accepted);this.check(expected);
  });
  for(const outcome of outcomes)if(!outcome.accepted&&outcome.classification==='rejected')try{this.options.rejected?.({reducerId:outcome.entry.idempotencyKey,path:outcome.entry.path,error:outcome.error,errorClass:'rejected'});}catch{/* Observers cannot roll back an authoritative settlement. */}
  await this.flushSafety(expected);
  const update=outcomes.find(outcome=>!outcome.accepted&&outcome.classification==='update_required');
  if(update&&!update.accepted){this.updateBlocked=true;this.options.updateRequired?.(update.error);return {sent:entries.length,nextDelay:null};}
  const next=await this.run(expected,()=>this.options.owner.database.getFirstAsync<{next:number|null}>("SELECT min(next_attempt) next FROM gv_reducer_journal WHERE scope=? AND state='pending'",expected.intentScope));
  const safety=await this.run(expected,()=>this.options.owner.database.getFirstAsync('SELECT 1 FROM gv_reducer_delivery_settlements WHERE scope=? AND safety_settled=0 LIMIT 1',expected.intentScope));
  return {sent:entries.length,nextDelay:safety?0:next?.next==null?null:Math.max(0,next.next-this.now())};
 }
 stats(){return {entries:this.entries,maxEntries:8,bytes:this.retainedBytes+this.entries*2048+(this.recovered?.length??0)*2+1024,maxBytes:this.ledger.maxBytes,active:!!this.active};}
 close(){this.closed=true;this.unregister();}
}
