import type {ServerMessage} from '@gonvex/protocol';
import type {ReplicaChange,ReplicaRow} from './local-replica.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes} from './sqlite-admission.js';
const encoder=new TextEncoder();
function object(value:unknown):value is ReplicaRow{return !!value&&typeof value==='object'&&!Array.isArray(value);}
function decode(strings:readonly string[]):unknown[]{
 if(!Array.isArray(strings)||strings.length>128)fail('BUDGET','Native control row count exceeds budget');
 let bytes=64;for(const text of strings){if(typeof text!=='string')fail('INVALID_ROW','Native row must be canonical JSON text');bytes+=decodedJSONBytes(text,262144-bytes);}
 if(bytes>262144)fail('BUDGET','Native control decoded rows exceed budget');
 try{const rows=strings.map(text=>JSON.parse(text));sqlitePayloadBytes(rows,262144);return rows;}catch(error){if(error instanceof SyntaxError)fail('INVALID_ROW','Invalid native row JSON');throw error;}
}
/** Decode only one bounded control frame. Hash original wire text before any
 * number passes through JavaScript, independently of the display row values. */
export async function decodeSQLiteNativeDelta(frame:Extract<ServerMessage,{type:'replica.delta'}>,key:string){
 if(frame.upserts!==undefined||!Array.isArray(frame.canonicalUpserts)||!Array.isArray(frame.deleted??[])||(frame.deleted?.length??0)>128||frame.deleted?.some(id=>typeof id!=='string'||!id||id.length>1024))fail('INVALID_ROW','Native delta requires encoded rows and bounded deleted IDs');
 const encoded=frame.canonicalUpserts,rows=decode(encoded);const ids=new Set<string>();
 const values=rows.map(row=>{if(!object(row)||typeof row[key]!=='string'||!row[key]||String(row[key]).length>1024)fail('INVALID_ROW','Native delta row identity differs from its generated key');const id=row[key] as string;if(ids.has(id))fail('INVALID_ROW','Duplicate native delta row identity');ids.add(id);return {id,row};});
 const hashes=await Promise.all(encoded.map(async text=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(text))),byte=>byte.toString(16).padStart(2,'0')).join('')));
 return values.map((value,index)=>({...value,hash:hashes[index]!}));
}
export function decodeSQLiteNativeTransaction(frame:Extract<ServerMessage,{type:'replica.transactionEncoded'}>,keys:Readonly<Record<string,string>>):ReplicaChange[]{
 const changes=decode(frame.canonicalChanges);
 return changes.map(value=>{
  if(!object(value)||typeof value.entity!=='string'||!Object.hasOwn(keys,value.entity)||typeof value.id!=='string'||!value.id||value.id.length>1024||!['insert','update','delete'].includes(String(value.operation))||value.oldValue!==undefined)fail('INVALID_ROW','Invalid native transaction change');
  if(value.operation!=='delete'&&(!object(value.newValue)||value.newValue[keys[value.entity]!]!==undefined&&value.newValue[keys[value.entity]!]!==value.id))fail('INVALID_ROW','Native transaction row identity differs from its generated key');
  if(value.changedColumns!==undefined&&(!Array.isArray(value.changedColumns)||value.changedColumns.some(column=>typeof column!=='string')))fail('INVALID_ROW','Invalid native transaction columns');
  return value as unknown as ReplicaChange;
 });
}

/** Keep a wire-admitted control frame encoded while decoding bounded chunks.
 * The caller applies every chunk inside one authority transaction. */
function* encodedControlBatches(strings:readonly string[]):Generator<readonly string[]>{
 if(!Array.isArray(strings)||strings.length>128)fail('BUDGET','Native control row count exceeds budget');
 let batch:string[]=[],bytes=64;
 for(const text of strings){if(typeof text!=='string')fail('INVALID_ROW','Native row must be canonical JSON text');const size=decodedJSONBytes(text,262144-64);if(batch.length&&bytes+size>262144){yield batch;batch=[];bytes=64;}batch.push(text);bytes+=size;}
 if(batch.length)yield batch;
}
export function* decodeSQLiteNativeTransactionBatches(frame:Extract<ServerMessage,{type:'replica.transactionEncoded'}>,keys:Readonly<Record<string,string>>):Generator<ReplicaChange[]>{
 for(const canonicalChanges of encodedControlBatches(frame.canonicalChanges))yield decodeSQLiteNativeTransaction({...frame,canonicalChanges:[...canonicalChanges]},keys);
}
export async function* decodeSQLiteNativeDeltaBatches(frame:Extract<ServerMessage,{type:'replica.delta'}>,key:string){
 // Validate the envelope even for an empty update.
 await decodeSQLiteNativeDelta({...frame,canonicalUpserts:[]},key);
 const ids=new Set<string>();
 for(const canonicalUpserts of encodedControlBatches(frame.canonicalUpserts!)){
  const rows=await decodeSQLiteNativeDelta({...frame,canonicalUpserts:[...canonicalUpserts]},key);
  for(const row of rows){if(ids.has(row.id))fail('INVALID_ROW','Duplicate native delta row identity');ids.add(row.id);}
  yield rows;
 }
}
