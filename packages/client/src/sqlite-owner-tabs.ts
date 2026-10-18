import {ByteLedger} from './byte-budget.js';
import {fail} from './sqlite-admission.js';
type Generation={epoch:number;sequence:number};
export type SQLiteOwnerChange=Generation&{scope:string;reset?:true;notification:number};
type Tab={seen:number;sentAt?:number;controller:AbortController;inFlight?:SQLiteOwnerChange;pending?:Omit<SQLiteOwnerChange,'notification'>;acked?:Generation};
const compare=(a:Generation,b:Generation)=>a.epoch-b.epoch||a.sequence-b.sequence;
/** One unacknowledged message and one coalesced descriptor per tab. A frozen
 * renderer cannot accumulate a message per commit. A reset covers every scope
 * when intermediate changes span several scopes. No rows or intent bodies live
 * here; expiry releases the generated program's tab subscriptions. */
export class SQLiteOwnerTabs {
 private readonly tabs=new Map<string,Tab>();
 private readonly ledger=new ByteLedger(262144);
 private readonly unregister:()=>void;
 private latest?:Generation;
 private serial=0;
 private sent=0;
 private refusals=0;
 private closed=false;
 constructor(private readonly send:(tab:string,change:SQLiteOwnerChange)=>void,private readonly detached:(tab:string)=>void=()=>{}){this.unregister=this.ledger.register('owner-change-delivery',()=>this.stats());}
 stats(){let bytes=0,inFlight=0,pending=0;for(const [id,tab]of this.tabs){bytes+=384+id.length*2;for(const value of [tab.inFlight,tab.pending])if(value)bytes+=256+value.scope.length*2;inFlight+=Number(!!tab.inFlight);pending+=Number(!!tab.pending);}return {name:'owner-change-delivery',bytes,maxBytes:262144,entries:this.tabs.size,maxEntries:64,evictions:0,refusals:this.refusals,inFlight,pending,sent:this.sent};}
 setGeneration(generation:Generation){if(!this.latest||compare(generation,this.latest)>=0)this.latest={...generation};}
 register(id:string,now=Date.now()):AbortSignal{
  if(this.closed)fail('FENCED','Owner tab registry is closed');if(typeof id!=='string'||!id||id.length>128)throw new TypeError('Invalid owner tab');
  let tab=this.tabs.get(id);if(!tab){if(this.tabs.size>=64){this.refusals++;fail('BUDGET','Owner tab allowance exhausted');}tab={seen:now,controller:new AbortController()};this.tabs.set(id,tab);}tab.seen=now;
  if(tab.inFlight&&now-(tab.sentAt??0)>=1000){tab.sentAt=now;this.sent++;this.send(id,tab.inFlight);}
  if(this.latest&&!tab.inFlight&&(!tab.acked||compare(this.latest,tab.acked)>0))this.deliver(id,tab,{...this.latest,scope:'*',reset:true});
  return tab.controller.signal;
 }
 private deliver(id:string,tab:Tab,value:Omit<SQLiteOwnerChange,'notification'>){
  if(this.serial>=Number.MAX_SAFE_INTEGER)fail('FENCED','Owner notification sequence exhausted');
  tab.inFlight={...value,notification:++this.serial};tab.sentAt=Date.now();this.sent++;this.send(id,tab.inFlight);
 }
 /** Control hints share the same one-inflight/one-pending credit as data. */
 notify(id:string,generation:Generation){if(!this.tabs.has(id))fail('FENCED','Control recipient is detached');this.publish('@control',generation,id);}
 publish(scope:string,generation:Generation,target?:string){
  if(this.closed)return;if(!scope||scope.length>512||!Number.isSafeInteger(generation.epoch)||generation.epoch<0||!Number.isSafeInteger(generation.sequence)||generation.sequence<0)throw new TypeError('Invalid committed owner change');
  if(this.latest&&compare(generation,this.latest)<0)return;this.setGeneration(generation);
  for(const [id,tab]of this.tabs){
   if(target!==undefined&&id!==target)continue;
   const previous=tab.pending??tab.inFlight;
   if(previous&&(compare(generation,previous)<0||compare(generation,previous)===0&&(previous.reset||previous.scope===scope)&&!(target!==undefined&&scope==='@control')))continue;
   if(!tab.inFlight){this.deliver(id,tab,{scope,...generation});continue;}
   const reset=!!tab.pending&&(tab.pending.reset||tab.pending.scope!==scope);
   tab.pending={...generation,scope:reset?'*':scope,...(reset?{reset:true as const}:{})};
  }
 }
 acknowledge(id:string,notification:number,generation:Generation){
  const tab=this.tabs.get(id),inFlight=tab?.inFlight;
  if(!tab||!inFlight||inFlight.notification!==notification||compare(inFlight,generation)!==0)return;
  tab.acked={epoch:inFlight.epoch,sequence:inFlight.sequence};tab.inFlight=undefined;
  const pending=tab.pending;tab.pending=undefined;if(pending)this.deliver(id,tab,pending);
 }
 drop(id:string){const tab=this.tabs.get(id);if(!tab)return;this.tabs.delete(id);tab.controller.abort();this.detached(id);}
 expire(now=Date.now()){for(const [id,tab]of this.tabs)if(now-tab.seen>30000)this.drop(id);}
 close(){if(this.closed)return;this.closed=true;for(const id of this.tabs.keys())this.drop(id);this.unregister();}
}
