import {sqlitePayloadBytes} from './sqlite-admission.js';
const storageKey='gonvex:sqlite:last-visible:v1';
const maxBytes=2097152,maxEntries=192;
export type SQLiteDisplayFence={url:string;artifact:string;project:string;tenant:string;identity:unknown;credential:string};
type Entry={key:string;encoded:string};
function hex(bytes:ArrayBuffer){return Array.from(new Uint8Array(bytes),n=>n.toString(16).padStart(2,'0')).join('');}
function certified(page:any){return !!page&&Array.isArray(page.rows)&&page.rows.length<=128&&(
 page.window?.header?.completeness==='complete'&&!page.window.header.truncated&&page.offset===0&&page.nextOffset===null||
 page.supported===true&&page.complete===true&&page.cursor===null);
}
/** SDK-owned stale display pages, never a session, coverage or intent store.
 * Executable admission stays in the SQLite owner. Only sealed, exact-context
 * pages can be read before that owner finishes booting. */
export class SQLiteLastVisiblePages{
 private revision=0;private scope='';private fingerprint='';private signing?:CryptoKey;private pending?:Promise<number>;
 private admission='';private entries=new Map<string,Entry>();private eligible=new Set<string>();private served=new Set<string>();private restoredPages=new WeakSet<object>();private bytes=0;private closed=false;
 constructor(private readonly storage:Pick<Storage,'getItem'|'setItem'|'removeItem'>|undefined){}
 private reset(){this.entries.clear();this.eligible.clear();this.served.clear();this.bytes=0;this.admission='';this.fingerprint='';this.signing=undefined;}
 private remove(){try{this.storage?.removeItem(storageKey);}catch{/* Display cache is optional. */}}
 revoke(){this.revision++;this.scope='';this.pending=undefined;this.reset();this.remove();}
 close(){this.closed=true;this.revision++;this.scope='';this.pending=undefined;this.reset();}
 async open(input:SQLiteDisplayFence):Promise<number>{
  if(this.closed)return 0;
  if(!input.url||!input.artifact||!input.project||!input.tenant||!input.identity||!input.credential){this.revoke();return 0;}
  sqlitePayloadBytes(input,65536);
  const scope=JSON.stringify([input.url,input.artifact,input.project,input.tenant,input.identity,input.credential]);
  if(scope===this.scope)return this.pending??this.eligible.size;
  if(this.scope)this.remove();this.reset();this.scope=scope;const revision=++this.revision;
  const current=()=>!this.closed&&this.revision===revision;
  const operation=(async()=>{
   const material=new TextEncoder().encode(scope);
   const [digest,key]=await Promise.all([crypto.subtle.digest('SHA-256',material),crypto.subtle.importKey('raw',material,{name:'HMAC',hash:'SHA-256'},false,['sign','verify'])]);
   if(!current())return 0;this.fingerprint=hex(digest);this.signing=key;
   let raw:string|null;try{raw=this.storage?.getItem(storageKey)??null;}catch{return 0;}
   if(!raw||raw.length*2>maxBytes)return 0;
   try{
    const envelope=JSON.parse(raw);
    if(typeof envelope.payload!=='string'||typeof envelope.mac!=='string'||! /^[0-9a-f]{64}$/.test(envelope.mac)||envelope.payload.length*2>maxBytes)return 0;
    const mac=Uint8Array.from(envelope.mac.match(/../g) as string[],part=>parseInt(part,16));
    if(!await crypto.subtle.verify('HMAC',key,mac,new TextEncoder().encode(envelope.payload))||!current())return 0;
    const saved=JSON.parse(envelope.payload);
    if(saved.v!==1||saved.fence!==this.fingerprint||typeof saved.admission!=='string'||!saved.admission||saved.admission.length>128||!Array.isArray(saved.entries)||saved.entries.length>maxEntries)return 0;
    this.admission=saved.admission;
    for(const entry of saved.entries)if(this.put(entry)){this.eligible.add(entry.key);}
    return this.eligible.size;
   }catch{return 0;}
  })();this.pending=operation;return operation;
 }
 private put(entry:Entry):boolean{
  if(typeof entry?.key!=='string'||entry.key.length>32768||typeof entry.encoded!=='string'||entry.encoded.length*2>262144)return false;
  const bytes=(entry.key.length+entry.encoded.length)*2+128;if(bytes>maxBytes)return false;
  const old=this.entries.get(entry.key);if(old){this.bytes-=(old.key.length+old.encoded.length)*2+128;this.entries.delete(entry.key);}
  while(this.entries.size>=maxEntries||this.bytes+bytes>maxBytes){const key=this.entries.keys().next().value!;const value=this.entries.get(key)!;this.bytes-=(value.key.length+value.encoded.length)*2+128;this.entries.delete(key);this.eligible.delete(key);this.served.delete(key);}
  this.entries.set(entry.key,{key:entry.key,encoded:entry.encoded});this.bytes+=bytes;return true;
 }
 admit(admission:string|null){
  if(admission===null||this.admission&&this.admission!==admission){this.revoke();return false;}
  if(!admission||admission.length>128)return false;this.admission=admission;return true;
 }
 remember(key:string,page:unknown,admission:string){
  if(this.closed||!this.signing||!this.admit(admission))return;
  this.eligible.delete(key);this.served.delete(key);
  const old=this.entries.get(key);if(old){this.bytes-=(old.key.length+old.encoded.length)*2+128;this.entries.delete(key);}
  if(!certified(page))return;
  try{this.put({key,encoded:JSON.stringify(page)});}catch{/* Invalid/unbounded pages never replace display data. */}
 }
 read<T>(key:string):T|undefined{
  if(this.closed||!this.signing||!this.eligible.has(key)||this.served.has(key))return;
  const entry=this.entries.get(key);if(!entry)return;
  try{const page=JSON.parse(entry.encoded);if(!certified(page))return;this.served.add(key);this.restoredPages.add(page);return page as T;}catch{return;}
 }
 restored(page:unknown){return !!page&&typeof page==='object'&&this.restoredPages.has(page);}
 available(){return !this.closed&&this.eligible.size>0;}
 async persist(){
  if(this.closed||!this.signing||!this.admission)return;
  if(!this.entries.size){this.remove();return;}
  const revision=this.revision,key=this.signing;
  // Work on strings, with no second retained decoded row graph. The outer
  // envelope also counts against the same 2 MiB storage allowance.
  let entries=[...this.entries.values()],payload='',mac='';
  while(entries.length){payload=JSON.stringify({v:1,fence:this.fingerprint,admission:this.admission,entries});if((JSON.stringify({payload,mac:'0'.repeat(64)}).length)*2<=maxBytes)break;entries.shift();}
  if(!entries.length)return;
  mac=hex(await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(payload)));
  if(this.closed||revision!==this.revision)return;
  try{this.storage?.setItem(storageKey,JSON.stringify({payload,mac}));}catch{/* Preserve normal SQLite startup on quota/permission failure. */}
 }
 stats(){return {name:'browser-last-visible-pages',bytes:this.bytes,entries:this.entries.size,maxBytes,maxEntries,evictions:0,refusals:0};}
}
