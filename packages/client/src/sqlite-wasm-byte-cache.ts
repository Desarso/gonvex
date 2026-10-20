import {sqliteWasmSHA256,sqliteWasmLength} from './sqlite-wasm-manifest.js';
type WasmAsset=Readonly<{sha256:string;length:number}>;
const pinnedAsset:WasmAsset=Object.freeze({sha256:sqliteWasmSHA256,length:sqliteWasmLength});
const channelName='gonvex-sqlite-wasm-bytes-v1:'+sqliteWasmSHA256;
type Channel=Pick<BroadcastChannel,'postMessage'|'close'|'onmessage'>;
type Factory=()=>Channel;
const defaultFactory=()=>new BroadcastChannel(channelName);
export async function verifiedSQLiteWasmBytes(value:unknown,asset:WasmAsset=pinnedAsset):Promise<boolean>{
 if(!(value instanceof ArrayBuffer)||value.byteLength!==asset.length)return false;
 const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',value)),byte=>byte.toString(16).padStart(2,'0')).join('');
 return hash===asset.sha256;
}
/** Router-only, one immutable engine binary plus at most one verification in
 * flight. No application data, URLs or arbitrary artifact keys are accepted. */
export function installSQLiteWasmByteCache(factory:Factory=defaultFactory,asset:WasmAsset=pinnedAsset){
 let channel:Channel;try{channel=factory();}catch{return ()=>{};}
 let bytes:ArrayBuffer|undefined,busy=false,closed=false,replyAt=0,replies=0;
 channel.onmessage=async({data:m})=>{
  if(closed||m?.key!==asset.sha256||typeof m.request!=='string'||m.request.length>128)return;
  if(m.type==='get'){
   const now=Date.now();if(now-replyAt>=1000){replyAt=now;replies=0;}if(replies++>=4)return;
   try{channel.postMessage({type:'reply',key:asset.sha256,request:m.request,bytes:bytes??null});}catch{/* Cache failure cannot affect owner election. */}
  }else if(m.type==='put'&&!bytes&&!busy&&m.bytes instanceof ArrayBuffer&&m.bytes.byteLength===asset.length){
   busy=true;try{if(await verifiedSQLiteWasmBytes(m.bytes,asset)&&!closed)bytes=m.bytes;}catch{/* Reject unverifiable bytes. */}finally{busy=false;}
  }
 };
 return ()=>{closed=true;bytes=undefined;channel.close();};
}
export type SQLiteWasmByteProbe={bytes?:Uint8Array;router:boolean;store(bytes:ArrayBuffer):Promise<void>;close():void};
/** The 25 ms ceiling bounds an optional optimization, including a dead router.
 * Start alongside module import, so a normal reply adds no serial wait. */
export function probeSQLiteWasmBytes(factory:Factory=defaultFactory,timeoutMs=25,asset:WasmAsset=pinnedAsset):Promise<SQLiteWasmByteProbe>{
 let channel:Channel;try{channel=factory();}catch{return Promise.resolve({router:false,store:async()=>{},close(){}});}
 return new Promise(resolve=>{
  let finished=false,closed=false,verifying=false,sent=false;const request=crypto.randomUUID();
  const close=()=>{if(!closed){closed=true;channel.close();}};
  const finish=(bytes?:Uint8Array,router=false)=>{if(finished)return;finished=true;clearTimeout(timer);if(!router)close();resolve({bytes,router,close,async store(value){if(closed||!router||sent||!(value instanceof ArrayBuffer)||value.byteLength!==asset.length)return;sent=true;try{channel.postMessage({type:'put',key:asset.sha256,request,bytes:value});}catch{/* Normal loading already succeeded. */}}});};
  const timer=setTimeout(()=>finish(),timeoutMs);
  channel.onmessage=({data:m})=>{if(finished||m?.type!=='reply'||m.key!==asset.sha256||m.request!==request)return;
   if(m.bytes===null){finish(undefined,true);return;}
   if(verifying)return;verifying=true;
   void verifiedSQLiteWasmBytes(m.bytes,asset).then(ok=>finish(ok?new Uint8Array(m.bytes):undefined,true),()=>finish());
  };
  try{channel.postMessage({type:'get',key:asset.sha256,request});}catch{finish();}
 });
}
/** Bound the response clone before retaining a cache seed. The ordinary
 * streaming consumer remains independent of this optional background copy. */
export async function readSQLiteWasmSeed(response:Response,length=sqliteWasmLength):Promise<ArrayBuffer|undefined>{
 if(!response.ok||!response.body)return;
 const declared=response.headers.get('content-length');if(declared!==null&&Number(declared)!==length){await response.body.cancel();return;}
 const reader=response.body.getReader(),bytes=new Uint8Array(length);let offset=0;
 try{for(;;){const {done,value}=await reader.read();if(done)break;if(offset+value.byteLength>bytes.length)return;bytes.set(value,offset);offset+=value.byteLength;}return offset===bytes.length?bytes.buffer:undefined;}
 finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
}
