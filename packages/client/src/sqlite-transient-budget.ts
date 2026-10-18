import {fail} from './sqlite-admission.js';
/** Shared owner admission. Independent connections cannot each consume their
 * local queue maximum at the same time. Reserve before cloning or copying. */
export class SQLiteTransientBudget {
 private bytes=0;private entries=0;private refusals=0;
 constructor(readonly maxBytes=2097152,readonly maxEntries=128){if(!Number.isSafeInteger(maxBytes)||maxBytes<1||!Number.isSafeInteger(maxEntries)||maxEntries<1)throw new RangeError('Invalid native transient budget');}
 reserve(bytes:number):()=>void{
  if(!Number.isSafeInteger(bytes)||bytes<1)throw new RangeError('Invalid native transient reservation');
  if(this.bytes+bytes>this.maxBytes||this.entries>=this.maxEntries){this.refusals++;fail('BUDGET','Shared native ingress budget exhausted');}
  this.bytes+=bytes;this.entries++;let released=false;
  return ()=>{if(released)return;released=true;this.bytes-=bytes;this.entries--;};
 }
 stats(){return {name:'native-transient',bytes:this.bytes,entries:this.entries,maxBytes:this.maxBytes,maxEntries:this.maxEntries,evictions:0,refusals:this.refusals};}
}
