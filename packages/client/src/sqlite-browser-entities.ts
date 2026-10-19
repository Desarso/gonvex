import type {ReplicaRow} from './local-replica.js';
import type {SQLiteCanonicalGeneration} from './sqlite-canonical-dispatch.js';
import type {SQLiteNativeEntityResult} from './sqlite-native-replica-reader.js';
import type {ByteLedger} from './byte-budget.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
type Entry={entity:string;id:string;key:string;refs:number;subscribers:Set<()=>void>;row?:ReplicaRow;encoded?:string;version:number;bytes:number;valueBytes:number};
/** The synchronous React entity view retains only explicitly mounted IDs.
 * Values come from fenced owner RPCs, never an application write or hydration
 * of a whole table. One owner notification coalesces into four read lanes. */
export class SQLiteBrowserEntities {
 private readonly entries=new Map<string,Entry>();private readonly entityVersions=new Map<string,number>();private bytes=0;private version=0;private stagingBytes=0;private revision=0;private closed=false;private refreshing?:Promise<void>;private again=false;private refreshAll=false;
 constructor(private readonly read:(entity:string,ids:string[],expected?:SQLiteCanonicalGeneration)=>Promise<SQLiteNativeEntityResult>,ledger:ByteLedger,private readonly failed:(error:unknown)=>void){ledger.register('browser-entities',()=>({name:'browser-entities',bytes:this.bytes,entries:this.entries.size,maxBytes:2359296,maxEntries:512,evictions:0,refusals:0}));ledger.register('browser-entity-staging',()=>({name:'browser-entity-staging',bytes:this.stagingBytes,entries:Number(this.stagingBytes>0),maxBytes:2359296,maxEntries:512,evictions:0,refusals:0}));}
 private entry(entity:string,id:string){const key=JSON.stringify([entity,id]);let entry=this.entries.get(key);if(entry)return entry;sqlitePayloadBytes({entity,id},4096);if(!entity||entity.length>256||!id||id.length>1024)throw new TypeError('Invalid retained entity identity');const bytes=key.length*2+512;if(this.closed)fail('FENCED','Entity view is closed');if(this.entries.size>=512||this.bytes+bytes>2359296)fail('BUDGET','Mounted entity allowance exceeded');entry={entity,id,key,refs:0,subscribers:new Set(),version:0,bytes,valueBytes:0};this.entries.set(key,entry);this.bytes+=bytes;return entry;}
 private release(entry:Entry){if(entry.refs||entry.subscribers.size||this.entries.get(entry.key)!==entry)return;this.entries.delete(entry.key);this.bytes-=entry.bytes+entry.valueBytes;entry.row=undefined;entry.encoded=undefined;if(![...this.entries.values()].some(value=>value.entity===entry.entity))this.entityVersions.delete(entry.entity);}
 private select(entity:string,ids:readonly string[]){if(!Array.isArray(ids)||ids.length>512)throw new RangeError('Retained entity lists are bounded');const selected:Entry[]=[];try{for(const id of new Set(ids))selected.push(this.entry(entity,id));return selected;}catch(error){for(const entry of selected)this.release(entry);throw error;}}
 retain(entity:string,ids:readonly string[]){const selected=this.select(entity,ids);for(const entry of selected)entry.refs++;this.refresh();let closed=false;return ()=>{if(closed)return;closed=true;if(this.closed)return;for(const entry of selected){entry.refs--;this.release(entry);}};}
 subscribe(listener:(touched?:ReadonlySet<string>)=>void,interest:{entity:string;ids?:readonly string[]}){
  if(!interest.ids)throw new TypeError('SQLite entity subscriptions require explicit bounded IDs');const selected=this.select(interest.entity,interest.ids),wrapped=()=>listener();
  if(selected.some(entry=>entry.subscribers.size>=32)||this.bytes+selected.length*128>2359296){for(const entry of selected)this.release(entry);fail('BUDGET','Entity subscriber allowance exceeded');}for(const entry of selected)entry.subscribers.add(wrapped);this.bytes+=selected.length*128;this.refresh();let closed=false;
  return ()=>{if(closed)return;closed=true;if(this.closed)return;for(const entry of selected){entry.subscribers.delete(wrapped);this.bytes-=128;this.release(entry);}};
 }
 entity<T extends ReplicaRow>(entity:string,id:string):T|undefined{return this.entries.get(JSON.stringify([entity,id]))?.row as T|undefined;}
 counters(){let rows=0,listeners=0;for(const entry of this.entries.values()){rows+=Number(!!entry.row);listeners+=entry.subscribers.size;}return {replicaRows:rows,replicaBytes:this.bytes,replicaMetadataBytes:this.entries.size*512,sdkListeners:listeners};}
 entityVersion(entity:string,id?:string){return id===undefined?this.entityVersions.get(entity)??0:this.entries.get(JSON.stringify([entity,id]))?.version??0;}
 private publish(entry:Entry){entry.version=++this.version;this.entityVersions.set(entry.entity,this.version);for(const listener of entry.subscribers)try{listener();}catch{/* Observer only. */}}
 refresh(all=true){if(this.closed)return;this.refreshAll||=all;if(this.refreshing){this.again=true;return;}const operation=this.drain();this.refreshing=operation;const finished=()=>{if(this.refreshing!==operation)return;this.refreshing=undefined;
  // An empty drain can settle synchronously. A mount in the same task must
  // survive the gap before this promise's completion callback runs.
  if(!this.closed&&(this.again||this.refreshAll))this.refresh(this.refreshAll);
 };void operation.then(finished,finished);}
 private async drain(){do{this.again=false;const all=this.refreshAll;this.refreshAll=false;const revision=this.revision,groups=new Map<string,string[][]>();for(const entry of this.entries.values()){if(!all&&entry.encoded!==undefined)continue;let pages=groups.get(entry.entity);if(!pages)groups.set(entry.entity,pages=[[]]);let page=pages.at(-1)!;if(page.length>=128||sqlitePayloadBytes([...page,entry.id],262144)>16384)pages.push(page=[]);page.push(entry.id);}
  const work=[...groups].flatMap(([entity,pages])=>pages.map(ids=>({entity,ids})));if(!work.length)return;
  let generation:SQLiteCanonicalGeneration|undefined,next=0,failed=false,stagedBytes=0;
  const staged:Array<{entity:string;result:SQLiteNativeEntityResult}>=[];
  const load=async(task:{entity:string;ids:string[]})=>{const result=await this.read(task.entity,task.ids,generation);if(this.closed||revision!==this.revision)return;if(generation&&(result.generation.epoch!==generation.epoch||result.generation.sequence!==generation.sequence))fail('PAGE_CHANGED','Mounted entity snapshot changed');generation??=result.generation;stagedBytes+=sqlitePayloadBytes(result,131072);this.stagingBytes=stagedBytes;if(stagedBytes>2359296)fail('BUDGET','Mounted entity staging exceeds its byte budget');staged.push({entity:task.entity,result});};
  try{
   // One read establishes the SQL generation. Every remaining bounded read
   // uses that fence, so a commit cannot produce a mixed mounted entity set.
   await load(work[next++]!);const outcomes=await Promise.allSettled(Array.from({length:Math.min(4,work.length-1)},async()=>{while(next<work.length&&!failed){try{await load(work[next++]!);}catch(error){failed=true;throw error;}}}));const failure=outcomes.find(value=>value.status==='rejected');if(failure?.status==='rejected')throw failure.reason;
   if(this.closed||revision!==this.revision)continue;
   const updates=staged.flatMap(({entity,result})=>result.rows.flatMap(slot=>{const entry=this.entries.get(JSON.stringify([entity,slot.id]));if(!entry)return [];const encoded=JSON.stringify(slot.value);if(encoded===entry.encoded)return [];const bytes=sqlitePayloadBytes(slot.value,131072)+encoded.length*2;return [{entry,row:slot.value??undefined,encoded,bytes}];}));
   const delta=updates.reduce((sum,value)=>sum+value.bytes-value.entry.valueBytes,0);if(this.bytes+delta>2359296)fail('BUDGET','Mounted entity rows exceed their byte budget');this.bytes+=delta;
   for(const update of updates){update.entry.row=update.row;update.entry.encoded=update.encoded;update.entry.valueBytes=update.bytes;}for(const update of updates)this.publish(update.entry);
  }catch(error){if(!this.closed&&revision===this.revision)this.failed(error);}finally{this.stagingBytes=0;}
 }while(this.again&&!this.closed);}
 invalidate(clear:boolean){this.revision++;if(clear)for(const entry of this.entries.values()){this.bytes-=entry.valueBytes;entry.valueBytes=0;entry.row=undefined;entry.encoded=undefined;this.publish(entry);}this.refresh();}
 close(){if(this.closed)return;this.closed=true;this.revision++;for(const entry of this.entries.values()){entry.row=undefined;entry.encoded=undefined;entry.subscribers.clear();}this.entries.clear();this.entityVersions.clear();this.bytes=0;this.stagingBytes=0;}
}
