import {fail,sqlitePayloadBytes} from './sqlite-admission.js';

function frame(op:string,args:unknown):Uint8Array|undefined {
  if(op!=='bootstrapAppend'||!args||typeof args!=='object')return;
  const descriptor=Object.getOwnPropertyDescriptor(args,'encoded');
  if(!descriptor||!('value'in descriptor))return;
  const encoded=descriptor.value;
  if(encoded instanceof Uint8Array && Object.getPrototypeOf(encoded)===Uint8Array.prototype) {
    if(!(encoded.buffer instanceof ArrayBuffer))fail('INVALID_ROW','Shared bootstrap buffers are not supported');
    return encoded;
  }
}
/** Binary admission is limited to the encoded bootstrap frame. Other request
 * fields retain plain-JSON admission; bytes are not expanded into JS numbers. */
export function sqliteRequestBytes(op:string,args:unknown,maxBytes:number):number {
  const encoded=frame(op,args);if(!encoded)return sqlitePayloadBytes(args,maxBytes);
  if(Object.getPrototypeOf(args)!==Object.prototype&&Object.getPrototypeOf(args)!==null)fail('INVALID_ROW','SQLite request must be plain data');
  if(encoded.byteLength*2>maxBytes)fail('BUDGET','SQLite binary frame exceeds budget');
  const metadata:Record<string,unknown>=Object.create(null);
  for(const key of Object.keys(args as object)) {
    const descriptor=Object.getOwnPropertyDescriptor(args,key)!;
    if(!('value'in descriptor))fail('INVALID_ROW','SQLite request accessor rejected');
    metadata[key]=key==='encoded'?null:descriptor.value;
  }
  const bytes=sqlitePayloadBytes(metadata,maxBytes)+encoded.byteLength*2+64;
  if(bytes>maxBytes)fail('BUDGET','SQLite binary request exceeds budget');return bytes;
}
/** The retained replay frame must never be detached. Transfer a fresh bounded
 * copy on each delivery; retries keep the original immutable request bytes. */
export function sqliteTransferRequest(op:string,args:unknown):{args:unknown;transfer:Transferable[]} {
  const encoded=frame(op,args);if(!encoded)return {args,transfer:[]};
  const copy=encoded.slice();return {args:{...args as object,encoded:copy},transfer:[copy.buffer]};
}
