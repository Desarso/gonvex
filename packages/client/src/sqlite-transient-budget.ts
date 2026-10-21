import {fail} from './sqlite-admission.js';
/** Shared owner admission. Independent connections cannot each consume their
 * local queue maximum at the same time. Reserve before cloning or copying. */
export class SQLiteTransientBudget {
 private bytes=0;private entries=0;private refusals=0;private peakBytes=0;private peakEntries=0;
 private peak(){this.peakBytes=Math.max(this.peakBytes,this.bytes+this.waitingBytes+this.heldBytes);this.peakEntries=Math.max(this.peakEntries,this.entries+this.waiting.length+this.heldEntries);}
 private heldBytes=0;private heldEntries=0;readonly maxWaitingEntries=256;
 private waiting:Array<{bytes:number;resolve:(release:()=>void)=>void;reject:(error:Error)=>void}>=[];private waitingBytes=0;private closed=false;
 constructor(readonly maxBytes=2097152,readonly maxEntries=128){if(!Number.isSafeInteger(maxBytes)||maxBytes<1||!Number.isSafeInteger(maxEntries)||maxEntries<1)throw new RangeError('Invalid native transient budget');}
 reserve(bytes:number):()=>void{
  if(!Number.isSafeInteger(bytes)||bytes<1)throw new RangeError('Invalid native transient reservation');
  if(this.bytes+this.waitingBytes+this.heldBytes+bytes>this.maxBytes||this.entries>=this.maxEntries){this.refusals++;fail('BUDGET',`Shared native ingress budget exhausted (active=${this.entries}/${this.maxEntries}, bytes=${this.bytes}, waiting=${this.waiting.length}/${this.waitingBytes}, held=${this.heldEntries}/${this.heldBytes}, requested=${bytes}, cap=${this.maxBytes})`);}
  this.bytes+=bytes;this.entries++;this.peak();let released=false;
  return ()=>{if(released)return;released=true;this.bytes-=bytes;this.entries--;this.pump();};
 }
 /** Queued background metadata shares the byte/wait allowance without taking
  * an active RPC slot away from foreground reads or user intents. */
 hold(bytes:number):()=>void{
  if(this.closed)fail('FENCED','Native ingress is closed');
  if(!Number.isSafeInteger(bytes)||bytes<1||bytes>this.maxBytes)fail('BUDGET','Invalid native transient reservation');
  if(this.waiting.length+this.heldEntries>=this.maxWaitingEntries||this.bytes+this.waitingBytes+this.heldBytes+bytes>this.maxBytes){this.refusals++;fail('BUDGET','Native background metadata budget exhausted');}
  this.heldBytes+=bytes;this.heldEntries++;this.peak();let released=false;
  return ()=>{if(released)return;released=true;this.heldBytes-=bytes;this.heldEntries--;this.pump();};
 }
 acquire(bytes:number):Promise<()=>void>{
  if(this.closed)return Promise.reject(Object.assign(Error('Native ingress is closed'),{code:'FENCED'}));
  if(!Number.isSafeInteger(bytes)||bytes<1||bytes>this.maxBytes)return Promise.reject(Object.assign(Error('Invalid native transient reservation'),{code:'BUDGET'}));
  if(!this.waiting.length&&this.entries<this.maxEntries&&this.bytes+this.heldBytes+bytes<=this.maxBytes)return Promise.resolve(this.reserve(bytes));
  if(this.waiting.length+this.heldEntries>=this.maxWaitingEntries||this.bytes+this.waitingBytes+this.heldBytes+bytes>this.maxBytes){this.refusals++;return Promise.reject(Object.assign(Error('Native ingress wait budget exhausted'),{code:'BUDGET'}));}
  this.waitingBytes+=bytes;return new Promise((resolve,reject)=>{this.waiting.push({bytes,resolve,reject});this.peak();});
 }
 private pump(){while(!this.closed&&this.waiting.length){const next=this.waiting[0]!;if(this.entries>=this.maxEntries||this.bytes+this.heldBytes+next.bytes>this.maxBytes)return;this.waiting.shift();this.waitingBytes-=next.bytes;next.resolve(this.reserve(next.bytes));}}
 close(){if(this.closed)return;this.closed=true;for(const item of this.waiting)item.reject(Object.assign(Error('Native ingress is closed'),{code:'FENCED'}));this.waiting.length=0;this.waitingBytes=0;}
 stats(){return {name:'native-transient',peakBytes:this.peakBytes,peakEntries:this.peakEntries,bytes:this.bytes+this.waitingBytes+this.heldBytes,entries:this.entries+this.waiting.length+this.heldEntries,maxBytes:this.maxBytes,maxEntries:this.maxEntries+this.maxWaitingEntries,evictions:0,refusals:this.refusals};}
}
