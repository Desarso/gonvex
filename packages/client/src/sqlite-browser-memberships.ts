import type {LiveQueryResult} from './local-replica.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
export type SQLiteBrowserMembership=Omit<LiveQueryResult,'rows'> & {entity:string};
type Entry={refs:number;listeners:Set<()=>void>;value?:SQLiteBrowserMembership;bytes:number};
/** Main-thread ordered IDs only. Values remain in bounded normalized entity
 * projections of SQLite; observing a membership never opens a network query. */
export class SQLiteBrowserMemberships {
 private entries=new Map<string,Entry>();private bytes=0;private closed=false;
 private entry(key:string){
  if(this.closed)fail('FENCED','Native memberships are closed');
  let entry=this.entries.get(key);if(entry)return entry;
  const bytes=sqlitePayloadBytes(key,32768)+256;
  if(this.entries.size>=64||this.bytes+bytes>524288)fail('BUDGET','Native membership metadata exceeded budget');
  this.bytes+=bytes;entry={refs:0,listeners:new Set(),bytes};this.entries.set(key,entry);return entry;
 }
 private collect(key:string,entry:Entry){if(this.entries.get(key)!==entry||entry.refs||entry.listeners.size)return;this.entries.delete(key);this.bytes-=entry.bytes;}
 retain(key:string){const entry=this.entry(key);if(entry.refs>=128)fail('BUDGET','Native membership lease budget exceeded');entry.refs++;let released=false;return ()=>{if(released)return;released=true;entry.refs--;this.collect(key,entry);};}
 subscribe(key:string,listener:()=>void){const entry=this.entry(key);if(entry.listeners.size>=64)fail('BUDGET','Native membership listeners exceeded budget');entry.listeners.add(listener);return ()=>{entry.listeners.delete(listener);this.collect(key,entry);};}
 read(key:string){return this.entries.get(key)?.value;}
 publish(key:string,value:SQLiteBrowserMembership){
  const entry=this.entries.get(key);if(!entry||this.closed)return;
  if((value.ids?.length??0)>256)fail('BUDGET','Native membership row budget exceeded');
  const bytes=sqlitePayloadBytes({key,value},262144)+256;if(this.bytes+bytes-entry.bytes>524288)fail('BUDGET','Native membership byte budget exceeded');
  if(entry.value&&JSON.stringify(entry.value)===JSON.stringify(value))return;
  this.bytes+=bytes-entry.bytes;entry.bytes=bytes;entry.value=structuredClone(value);
  for(const listener of entry.listeners)try{listener();}catch{/* Observer only. */}
 }
 invalidate(){for(const entry of this.entries.values()){if(!entry.value)continue;entry.value=undefined;for(const listener of entry.listeners)try{listener();}catch{/* Observer only. */}}}
 stats(){return {name:'browser-live-memberships',bytes:this.bytes,entries:this.entries.size,maxBytes:524288,maxEntries:64,evictions:0,refusals:0};}
 close(){this.closed=true;this.entries.clear();this.bytes=0;}
}
