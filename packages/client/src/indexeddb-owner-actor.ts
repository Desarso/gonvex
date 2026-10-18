import {IndexedDBLiveWindows} from './indexeddb-live-windows.js';
import {OriginStorageWorkerLease} from './origin-storage-worker-lease.js';
import {SQLiteOwnerTabs} from './sqlite-owner-tabs.js';
import {sqliteRequestBytes} from './sqlite-request-frame.js';
import {sqliteWorkerProtocol as protocol} from './sqlite-worker-protocol.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {SQLiteOwnerCaller,SQLiteOwnerProgram} from './sqlite-owner-actor.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
export type IndexedDBOwnerProgramContext={engine:IndexedDBLiveWindows;schema:SQLiteTypedSchema;runOwner<T>(bytes:number,run:()=>Promise<T>,scopes?:readonly string[]):Promise<T>;notifyTab(tab:string):Promise<void>};
export type IndexedDBOwnerBinding={schema:SQLiteTypedSchema;schemaVersion:string;createProgram(context:IndexedDBOwnerProgramContext):SQLiteOwnerProgram};
let installed=false;
/** Pure IndexedDB dedicated owner. It shares the primary worker's non-stealable
 * lifetime lease and durable origin epoch, without importing SQLite or WASM. */
export function installIndexedDBOwner(binding:IndexedDBOwnerBinding){
 if(typeof globalThis.postMessage!=='function'||'document'in globalThis||'onconnect'in globalThis)throw Error('IndexedDB owner requires a dedicated worker');if(installed)throw Error('Only one IndexedDB owner may run in a worker');installed=true;
 let engine:IndexedDBLiveWindows|undefined,program:SQLiteOwnerProgram|undefined,token='',closed=false,lane=Promise.resolve(),queued=0,queuedBytes=0,operations=0,operationBytes=0;
 const lease=new OriginStorageWorkerLease(),ports=new Map<string,{port:MessagePort}>();
 const send=(tab:string,message:unknown)=>{const peer=ports.get(tab);if(peer)peer.port.postMessage(message);else globalThis.postMessage(message);};
 const tabs=new SQLiteOwnerTabs((to,change)=>send(to,{type:'change',protocol,to,token,sentAt:Date.now(),...change}),tab=>{ports.get(tab)?.port.close();ports.delete(tab);try{program?.detachTab?.(tab);}catch{globalThis.postMessage({type:'storage-fault'});}}),timer=setInterval(()=>tabs.expire(),5000);
 const queue=<T>(size:number,run:()=>Promise<T>):Promise<T>=>{
  if(closed)return Promise.reject(Object.assign(Error('IndexedDB owner closed'),{code:'FENCED'}));if(!Number.isSafeInteger(size)||size<0||size>2097152||queued>=64||queuedBytes+size>2097152)return Promise.reject(Object.assign(Error('IndexedDB owner queue allowance exceeded'),{code:'BUSY'}));queued++;queuedBytes+=size;
  const operation=lane.then(async()=>{if(closed||token&&!lease.stats().held)fail('FENCED','IndexedDB worker lease released');await engine?.generation();return run();});lane=operation.then(()=>{},()=>{});void operation.then(()=>{queued--;queuedBytes-=size;},()=>{queued--;queuedBytes-=size;});return operation;
 };
 const runOwner=async<T>(bytes:number,run:()=>Promise<T>,scopes:readonly string[]=[])=>{
  sqlitePayloadBytes(scopes,8192);if(scopes.length>8||scopes.some(scope=>!scope||scope.length>512))throw new TypeError('Invalid IndexedDB publication scope');
  return queue(bytes,async()=>{const before=await engine!.generation(),value=await run(),after=await engine!.generation();if(after.sequence!==before.sequence)for(const scope of scopes)tabs.publish(scope,after);return value;});
 };
 const execute=async(op:string,args:any)=>{
  if(op==='open'){
   if(engine||args?.protocol!==protocol||args.schemaVersion!==binding.schemaVersion||typeof args.owner!=='string'||!args.owner||args.owner.length>128)throw Object.assign(Error('Incompatible IndexedDB worker binding'),{code:'SCHEMA_MISMATCH'});token=args.owner;await lease.acquire();globalThis.postMessage({type:'progress',stage:'opening-indexeddb',sentAt:Date.now()});engine=new IndexedDBLiveWindows(binding.schema);const generation=await engine.claimAdapter(token,'indexeddb');await engine.discardFeedStaging();tabs.setGeneration(generation);program=binding.createProgram({engine,schema:binding.schema,runOwner,notifyTab:tab=>queue(0,async()=>{tabs.notify(tab,await engine!.generation());})});if(!program||typeof program.call!=='function')throw new TypeError('IndexedDB program factory must be synchronous');return {epoch:generation.epoch,wasmBytes:0,adapter:'indexeddb'};
  }
  if(!engine||!program)fail('FENCED','IndexedDB owner is not ready');
  if(op==='ping')return {ready:true};
  if(op==='stats')return {adapter:'indexeddb',wasmBytes:0,originEpoch:(await engine.generation()).epoch,workerLease:lease.stats(),queuedOperations:queued,queuedBytes,requests:operations,requestBytes:operationBytes,clients:ports.size,delivery:tabs.stats(),program:program.budgets?.()??[],programDiagnostics:await program.diagnostics?.()};
  // IndexedDB exposes no PRAGMA integrity_check. The generated program supplies
  // bounded logical checks; never report a fabricated SQLite integrity result.
  if(op==='integrity')throw Object.assign(Error('Use IndexedDB logical replica verification'),{code:'UNSUPPORTED'});
  throw Object.assign(Error('Use the authenticated generated client program'),{code:'OPERATION'});
 };
 const serialize=(error:unknown)=>{const value=error as {code?:unknown;name?:unknown;message?:unknown};return {code:String(value.code??(value.name==='QuotaExceededError'?'QUOTA':'INDEXEDDB')),name:String(value.name??'Error'),message:String(value.message??error).slice(0,512)};};
 const reply=(id:string,value:unknown,error:unknown,peer?:{port:MessagePort},tab?:string)=>{const message={id,protocol,...(peer?{from:tab,to:tab,token}:{}),sentAt:Date.now(),...(error?{error:serialize(error)}:{value:value===undefined?null:value})};try{sqlitePayloadBytes(message,262144);if(peer)peer.port.postMessage(message);else globalThis.postMessage(message);}catch(failure){const refused={id,protocol,token,from:tab,to:tab,sentAt:Date.now(),error:serialize(failure)};if(peer)peer.port.postMessage(refused);else globalThis.postMessage(refused);}};
 const admit=(data:any,peer?:{port:MessagePort},tab?:string)=>{
  let size:number,caller:SQLiteOwnerCaller;
  try{if(typeof data?.id!=='string'||!data.id||data.id.length>256||typeof data.op!=='string'||data.requestID!==undefined&&(typeof data.requestID!=='string'||!data.requestID||data.requestID.length>256||peer&&data.requestID!==data.id))throw new TypeError('Invalid IndexedDB worker request');size=sqliteRequestBytes(data.op,data.args,data.op==='open'?1048576:262144);if(operations>=64||operationBytes+size>2097152)throw Object.assign(Error('IndexedDB worker request allowance exceeded'),{code:'BUSY'});const sender=peer?tab:data.tabID;if(typeof sender!=='string'||!sender||sender.length>128)throw new TypeError('IndexedDB requests require a caller');caller={tabID:sender,requestID:data.requestID??data.id,signal:tabs.register(sender)};if(closed)fail('FENCED','IndexedDB owner closed');}catch(error){reply(data?.id,undefined,error,peer,tab);return;}
  operations++;operationBytes+=size;const work=data.op==='clientCall'?Promise.resolve().then(async()=>{if(!program||!engine)fail('FENCED','IndexedDB owner is not ready');await engine.generation();const value=await program.call(data.args,caller);await engine.generation();reply(data.id,value,undefined,peer,tab);}):queue(size,async()=>reply(data.id,await execute(data.op,data.args),undefined,peer,tab));void work.then(()=>{operations--;operationBytes-=size;},error=>{operations--;operationBytes-=size;reply(data.id,undefined,error,peer,tab);});
 };
 const heartbeat=(tab:string)=>{tabs.register(tab);send(tab,{type:'tab-pong',protocol,to:tab,token,sentAt:Date.now()});};
 globalThis.onmessage=({data})=>{
  if(['tab-heartbeat','tab-bye','change-ack'].includes(data?.type)){if(data.protocol!==protocol||data.token!==token||typeof data.tab!=='string'||!data.tab||data.tab.length>128)return;if(data.type==='tab-bye'){tabs.drop(data.tab);return;}if(data.type==='change-ack'){tabs.acknowledge(data.tab,data.notification,{epoch:data.epoch,sequence:data.sequence});return;}try{heartbeat(data.tab);}catch{/* Heartbeat cannot bypass tab admission. */}return;}
  if(data?.type!=='attach'){admit(data);return;}const {tab,token:sentToken,port}=data;if(!engine||typeof tab!=='string'||!tab||tab.length>128||sentToken!==token||!(port instanceof MessagePort)||ports.size>=64&&!ports.has(tab)){port?.close();return;}try{tabs.register(tab);}catch{port.close();return;}ports.get(tab)?.port.close();const peer={port};ports.set(tab,peer);port.onmessage=({data:message})=>{if(ports.get(tab)!==peer)return;if(message?.type==='bye'){tabs.drop(tab);return;}if(message?.protocol!==protocol||message.token!==token||message.from!==tab)return;if(message.type==='tab-heartbeat'){heartbeat(tab);return;}if(message.type==='change-ack'){tabs.acknowledge(tab,message.notification,{epoch:message.epoch,sequence:message.sequence});return;}admit(message,peer,tab);};port.start();port.postMessage({type:'attached'});
 };
 let disposal:Promise<void>|undefined;return ()=>disposal??=(async()=>{closed=true;lease.cancelPending();clearInterval(timer);globalThis.onmessage=null;tabs.close();for(const peer of ports.values())peer.port.close();ports.clear();try{await program?.close?.();await lane;}finally{engine?.close();await lease.close();installed=false;}})();
}
