import {fail} from './sqlite-admission.js';
/** Shared owner admission. Independent connections cannot each consume their
 * local queue maximum at the same time. Reserve before cloning or copying. */
export class SQLiteTransientBudget {
 private bytes=0;private entries=0;private refusals=0;
 private waiting:Array<{bytes:number;resolve:(release:()=>void)=>void;reject:(error:Error)=>void}>=[];private waitingBytes=0;private closed=false;
 constructor(readonly maxBytes=2097152,readonly maxEntries=128){if(!Number.isSafeInteger(maxBytes)||maxBytes<1||!Number.isSafeInteger(maxEntries)||maxEntries<1)throw new RangeError('Invalid native transient budget');}
 reserve(bytes:number):()=>void{
  if(!Number.isSafeInteger(bytes)||bytes<1)throw new RangeError('Invalid native transient reservation');
  if(this.bytes+bytes>this.maxBytes||this.entries>=this.maxEntries){this.refusals++;fail('BUDGET','Shared native ingress budget exhausted');}
  this.bytes+=bytes;this.entries++;let released=false;
  return ()=>{if(released)return;released=true;this.bytes-=bytes;this.entries--;this.pump();};
 }
 acquire(bytes:number):Promise<()=>void>{
  if(this.closed)return Promise.reject(Object.assign(Error('Native ingress is closed'),{code:'FENCED'}));
  if(!Number.isSafeInteger(bytes)||bytes<1||bytes>this.maxBytes)return Promise.reject(Object.assign(Error('Invalid native transient reservation'),{code:'BUDGET'}));
  if(!this.waiting.length&&this.entries<this.maxEntries&&this.bytes+bytes<=this.maxBytes)return Promise.resolve(this.reserve(bytes));
  if(this.waiting.length>=256||this.bytes+this.waitingBytes+bytes>this.maxBytes){this.refusals++;return Promise.reject(Object.assign(Error('Native ingress wait budget exhausted'),{code:'BUDGET'}));}
  this.waitingBytes+=bytes;return new Promise((resolve,reject)=>{this.waiting.push({bytes,resolve,reject});});
 }
 private pump(){while(!this.closed&&this.waiting.length){const next=this.waiting[0]!;if(this.entries>=this.maxEntries||this.bytes+next.bytes>this.maxBytes)return;this.waiting.shift();this.waitingBytes-=next.bytes;next.resolve(this.reserve(next.bytes));}}
 close(){if(this.closed)return;this.closed=true;for(const item of this.waiting)item.reject(Object.assign(Error('Native ingress is closed'),{code:'FENCED'}));this.waiting.length=0;this.waitingBytes=0;}
 stats(){return {name:'native-transient',bytes:this.bytes+this.waitingBytes,entries:this.entries+this.waiting.length,maxBytes:this.maxBytes,maxEntries:this.maxEntries+256,evictions:0,refusals:this.refusals};}
}
