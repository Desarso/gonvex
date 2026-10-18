import type {ReplicaRow} from './local-replica.js';
import type {SQLiteNativeWindowRead,SQLiteNativeWindowResult} from './sqlite-native-replica-reader.js';
import type {SQLiteWindowHeader} from './sqlite-window-store.js';
import {IndexedDBReplicaEngine} from './indexeddb-replica-engine.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
type Identity={scope:string;source:string;snapshot:string};
type Stage=Identity&{header:SQLiteWindowHeader;sequence:number;rows:number;bytes:number;receipt:Uint8Array;complete:boolean;maxRows:number;maxBytes:number;published?:boolean};
type StageRow=Identity&{position:number;id:string;row:ReplicaRow;hash:string};
type StageMember=Omit<StageRow,'row'>;
type Window={scope:string;signature:string;stage:Identity;header:SQLiteWindowHeader;total:number;generation:string};
const request=<T>(value:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{value.onsuccess=()=>resolve(value.result);value.onerror=()=>reject(value.error??Error('Fallback window request failed'));});
const hash=async(value:string)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');
/** Fallback snapshots retain rows, membership, hashes and frame receipts on
 * disk. Unverified pages are display-only. Publication merges canonical values
 * and switches the ordered window inside the reducer's atomic rebase. */
export class IndexedDBReplicaWindows extends IndexedDBReplicaEngine {
 private key(input:Identity){sqlitePayloadBytes(input,4096);if(!input.scope||input.scope.length>512||!input.source||input.source.length>256||!input.snapshot||input.snapshot.length>128)throw new TypeError('Invalid fallback snapshot identity');return [input.scope,input.source,input.snapshot];}
 async beginSnapshot(input:Identity,header:SQLiteWindowHeader,budget:{maxRows:number;maxBytes:number}){
  const key=this.key(input);sqlitePayloadBytes(header,32768);if(!header.signature||header.signature.length>4096||!header.cursor||!header.cursor.epoch||!Number.isSafeInteger(header.cursor.revision)||header.cursor.revision<0||!Object.hasOwn(this.schema,header.entity)||this.schema[header.entity]!.key!==header.key||!header.columns?.includes(header.key)||header.columns.some(column=>!Object.hasOwn(this.schema[header.entity]!.fields,column))||!Number.isSafeInteger(budget.maxRows)||budget.maxRows<0||!Number.isSafeInteger(budget.maxBytes)||budget.maxBytes<0)throw new TypeError('Invalid generated fallback snapshot');
  const stage:Stage={...structuredClone(input),header:structuredClone(header),...budget,sequence:0,rows:0,bytes:0,receipt:new ReplicaSetDigest().encode(),complete:false};
  return this.transaction(['meta','snapshots'],'readwrite',async tx=>{await this.fence(tx);const store=tx.objectStore('snapshots'),old=await request(store.get(key)) as Stage|undefined;if(old){if(JSON.stringify(old.header)!==JSON.stringify(stage.header)||old.maxRows!==stage.maxRows||old.maxBytes!==stage.maxBytes)fail('INTENT_CONFLICT','Fallback snapshot identity reused');return;}await request(store.add(stage));});
 }
 async appendSnapshot(input:Identity,sequence:number,canonicalRows:readonly string[],last:boolean,digest?:string){
  const owned=structuredClone(input),key=this.key(owned);sqlitePayloadBytes({sequence,canonicalRows,last,digest},131072);if(!Number.isSafeInteger(sequence)||sequence<0||typeof last!=='boolean'||canonicalRows.length>8192)throw new TypeError('Invalid fallback snapshot frame');
  const rows=canonicalRows.map(value=>{const row=JSON.parse(value);if(!row||typeof row!=='object'||Array.isArray(row))throw new TypeError('Invalid fallback snapshot row');return {row:row as ReplicaRow,encoded:value};}),fingerprint=await hash(JSON.stringify({sequence,canonicalRows,last,digest})),hashes=await Promise.all(rows.map(row=>hash(row.encoded)));
  return this.transaction(['meta','snapshots','snapshotRows','snapshotMembers','snapshotFrames'],'readwrite',async tx=>{
   await this.fence(tx);const stages=tx.objectStore('snapshots'),stage=await request(stages.get(key)) as Stage|undefined;if(!stage)fail('FENCED','Fallback snapshot stage disappeared');const frames=tx.objectStore('snapshotFrames'),old=await request(frames.get([...key,sequence])) as {hash:string}|undefined;if(old){if(old.hash!==fingerprint)fail('INTENT_CONFLICT','Fallback snapshot frame changed');return {sequence:stage.sequence,rows:stage.rows,complete:stage.complete};}
   if(stage.complete||sequence!==stage.sequence)fail('PAGE_CHANGED','Fallback snapshot sequence changed');const receipt=ReplicaSetDigest.decode(stage.receipt),store=tx.objectStore('snapshotRows');let bytes=stage.bytes;
   for(let index=0;index<rows.length;index++){
    const {row,encoded}=rows[index]!,id=row[stage.header.key];if(typeof id!=='string'||!id||id.length>1024||Object.keys(row).some(column=>!stage.header.columns!.includes(column)))fail('INVALID_ROW','Fallback snapshot exceeds generated projection');bytes+=new TextEncoder().encode(encoded).byteLength;receipt.add(hashes[index]!);if(receipt.count>stage.maxRows||bytes>stage.maxBytes)fail('BUDGET','Fallback snapshot exceeds declared collection budget');const member={...owned,position:stage.rows+index,id,hash:hashes[index]!};await request(store.add({...member,row} satisfies StageRow));await request(tx.objectStore('snapshotMembers').add(member));
   }
   const actual=digest?.startsWith('xor1:')?receipt.legacyDigest():receipt.digest();if(last&&(digest!==actual||digest!==stage.header.aggregateDigest))fail('INVALID_ROW','Fallback snapshot integrity mismatch');
   const updated={...stage,sequence:sequence+1,rows:receipt.count,bytes,receipt:receipt.encode(),complete:last};await request(stages.put(updated));await request(frames.add({...owned,sequence,hash:fingerprint}));return {sequence:updated.sequence,rows:updated.rows,complete:updated.complete};
  });
 }
 private rows<T extends StageMember=StageRow>(tx:IDBTransaction,input:Identity,after:number,limit:number,storeName='snapshotRows'):Promise<T[]>{return new Promise((resolve,reject)=>{
  const rows:T[]=[];let bytes=64;const key=this.key(input),cursor=tx.objectStore(storeName).openCursor(this.ranges.bound([...key,after],[...key,Number.MAX_SAFE_INTEGER],false,false));cursor.onerror=()=>reject(cursor.error);cursor.onsuccess=()=>{const entry=cursor.result;if(!entry){resolve(rows);return;}try{const row=entry.value as T,size=sqlitePayloadBytes(row,131072);if(rows.length>=limit||bytes+size>131072){if(!rows.length)fail('BUDGET','Fallback row exceeds its page budget');resolve(rows);return;}rows.push(row);bytes+=size;entry.continue();}catch(error){reject(error);}};
 });}
 async pendingSnapshot(input:Identity,offset=0,limit=50){
  const owned=structuredClone(input),key=this.key(owned);if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128)throw new TypeError('Invalid fallback snapshot page');return this.transaction(['meta','snapshots','snapshotRows'],'readonly',async tx=>{const generation=await this.fence(tx),stage=await request(tx.objectStore('snapshots').get(key)) as Stage|undefined;if(!stage||stage.published)return null;const rows=await this.rows(tx,owned,offset,limit);return {generation:{epoch:generation.epoch,sequence:generation.sequence},snapshot:owned.snapshot,rows:rows.map(row=>({id:row.id,value:row.row})),nextOffset:offset+rows.length<stage.rows?offset+rows.length:null,complete:false as const};});
 }
 /** Caller supplies this to IndexedDBReducerCore.rebase's authority phase. */
 async publishSnapshotInTransaction(tx:IDBTransaction,input:Identity){
  const owned=structuredClone(input),key=this.key(owned);return this.transactionTask(tx,32768,async()=>{
   const stages=tx.objectStore('snapshots'),stage=await request(stages.get(key)) as Stage|undefined;if(!stage?.complete)fail('PAGE_CHANGED','Fallback snapshot is not verified');if(stage.published)return;const windows=tx.objectStore('windows'),old=await request(windows.get([owned.scope,stage.header.signature])) as Window|undefined;if(old?.header.cursor?.epoch===stage.header.cursor!.epoch&&old.header.cursor.revision>stage.header.cursor!.revision)fail('PAGE_CHANGED','Fallback snapshot is obsolete');
   let offset=0;for(;;){const rows=await this.rows(tx,owned,offset,128);if(!rows.length)break;await this.authorityInTransaction(tx,owned.scope,stage.header.cursor!,rows.map(row=>({entity:stage.header.entity,id:row.id,row:row.row})));offset+=rows.length;}if(offset!==stage.rows)fail('INTENT_CORRUPT','Fallback snapshot lost its membership');
   await request(windows.put({scope:owned.scope,signature:stage.header.signature,stage:owned,header:stage.header,total:stage.rows,generation:owned.snapshot} satisfies Window));await request(stages.put({...stage,published:true}));
  });
 }
 async windowPage(input:SQLiteNativeWindowRead):Promise<SQLiteNativeWindowResult|null>{
  sqlitePayloadBytes(input,8192);const owned=structuredClone(input),offset=owned.offset??0,limit=owned.limit??50;if(!owned.scope||!owned.signature||!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128)throw new TypeError('Invalid bounded fallback window read');
  return this.transaction(['meta','windows','snapshotMembers','canonical','predicted'],'readonly',async tx=>{const owner=await this.fence(tx,owned.expected),window=await request(tx.objectStore('windows').get([owned.scope,owned.signature])) as Window|undefined;if(!window)return null;if(owned.generation!==undefined&&window.generation!==owned.generation)fail('PAGE_CHANGED','Fallback window generation changed');const members=await this.rows<StageMember>(tx,window.stage,offset,limit,'snapshotMembers'),values=await this.values(tx,owned.scope,window.header.entity,members.map(row=>row.id),window.header.columns),byID=new Map(values.map(row=>[row.id,row.row]));const page:SQLiteNativeWindowResult={generation:{epoch:owner.epoch,sequence:owner.sequence},window:{generation:window.generation,header:window.header,total:window.total},offset,nextOffset:offset+members.length<window.total?offset+members.length:null,rows:members.map(row=>({id:row.id,value:byID.get(row.id)??null}))};sqlitePayloadBytes(page,131072);return page;});
 }
 async windowHeader(scope:string,signature:string){sqlitePayloadBytes({scope,signature},8192);return this.transaction(['meta','windows'],'readonly',async tx=>{await this.fence(tx);return await request(tx.objectStore('windows').get([scope,signature])) as Window|undefined;});}
}
