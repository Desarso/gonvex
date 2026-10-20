import {readFile} from 'node:fs/promises';
import {afterEach,expect,it,vi} from 'vitest';
import {installSQLiteWasmByteCache,probeSQLiteWasmBytes,readSQLiteWasmSeed,verifiedSQLiteWasmBytes} from './sqlite-wasm-bytes.js';
import {sqliteWasmLength,sqliteWasmSHA256} from './sqlite-wasm-manifest.js';
const engine=new Uint8Array(await readFile(new URL('../sqlite-native/sqlite3.wasm',import.meta.url)));
const binary=new TextEncoder().encode('small immutable engine fixture'),asset={length:binary.byteLength,sha256:Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',binary)),byte=>byte.toString(16).padStart(2,'0')).join('')};
function bus(){
 const channels=new Set<any>(),pending:Promise<unknown>[]=[];
 const factory=()=>{const channel={onmessage:null as any,postMessage(data:unknown){for(const peer of channels)if(peer!==channel)pending.push(Promise.resolve().then(()=>peer.onmessage?.({data:structuredClone(data)})));},close(){channels.delete(channel);}};channels.add(channel);return channel;};
 return {factory,async flush(){while(pending.length)await Promise.all(pending.splice(0));},size:()=>channels.size};
}
afterEach(()=>vi.useRealTimers());
it('pins the exact shipped engine length and digest',async()=>{
 expect(engine.byteLength).toBe(sqliteWasmLength);expect(await verifiedSQLiteWasmBytes(engine.slice().buffer)).toBe(true);
 expect(await verifiedSQLiteWasmBytes(new ArrayBuffer(1))).toBe(false);
 const changed=engine.slice();changed[0]^=1;expect(await verifiedSQLiteWasmBytes(changed.buffer)).toBe(false);
});
it('retains one engine across owner probes and never aliases a consumer buffer',async()=>{
 const b=bus(),stop=installSQLiteWasmByteCache(b.factory,asset);
 const cold=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(cold.bytes).toBeUndefined();expect(cold.router).toBe(true);
 await cold.store(binary.slice().buffer);await b.flush();cold.close();
 const warm=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(warm.bytes).toEqual(binary);warm.bytes![0]^=1;warm.close();
 const next=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(next.bytes).toEqual(binary);next.close();stop();expect(b.size()).toBe(0);
});
it('rejects oversized, corrupt and foreign-artifact seeds and keeps the first verified engine',async()=>{
 const b=bus(),stop=installSQLiteWasmByteCache(b.factory,asset),sender=b.factory();
 sender.postMessage({type:'put',key:asset.sha256,request:'a',bytes:new ArrayBuffer(asset.length+1)});await b.flush();
 const corrupt=binary.slice();corrupt[0]^=1;
 sender.postMessage({type:'put',key:asset.sha256,request:'b',bytes:corrupt.buffer});await b.flush();
 sender.postMessage({type:'put',key:'foreign',request:'c',bytes:binary.slice().buffer});await b.flush();
 const miss=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(miss.bytes).toBeUndefined();await miss.store(binary.slice().buffer);await b.flush();miss.close();
 sender.postMessage({type:'put',key:asset.sha256,request:'d',bytes:corrupt.buffer});await b.flush();
 const hit=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(hit.bytes).toEqual(binary);hit.close();sender.close();stop();
});
it('falls back promptly without BroadcastChannel or a responsive router and closes the probe',async()=>{
 expect((await probeSQLiteWasmBytes(()=>{throw Error('disabled');})).router).toBe(false);
 vi.useFakeTimers();const b=bus(),pending=probeSQLiteWasmBytes(b.factory);await vi.advanceTimersByTimeAsync(25);
 expect((await pending).bytes).toBeUndefined();expect(b.size()).toBe(0);
});
it('verifies replies again before exposing cached executable bytes',async()=>{
 const b=bus(),bad=b.factory();bad.onmessage=({data:m}:any)=>bad.postMessage({type:'reply',key:m.key,request:m.request,bytes:new ArrayBuffer(asset.length)});
 const probe=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(probe.bytes).toBeUndefined();probe.close();bad.close();
});
it('bounds streamed seeds even with missing or dishonest content length',async()=>{
 expect(await readSQLiteWasmSeed(new Response(binary),asset.length)).toEqual(binary.buffer);
 expect(await readSQLiteWasmSeed(new Response(new Uint8Array(asset.length+1)),asset.length)).toBeUndefined();
 expect(await readSQLiteWasmSeed(new Response(binary,{headers:{'content-length':'1'}}),asset.length)).toBeUndefined();
 expect(await readSQLiteWasmSeed(new Response(new Uint8Array(1)),asset.length)).toBeUndefined();
 expect(await readSQLiteWasmSeed(new Response(binary,{status:500}),asset.length)).toBeUndefined();
});

it('hashes an admitted seed once and each consumed copy once, with no sender rehash',async()=>{
 const b=bus(),stop=installSQLiteWasmByteCache(b.factory,asset),digest=vi.spyOn(crypto.subtle,'digest');
 try{
  const miss=await probeSQLiteWasmBytes(b.factory,1000,asset);
  await miss.store(binary.slice().buffer);await b.flush();expect(digest).toHaveBeenCalledTimes(1);
  await miss.store(binary.slice().buffer);await b.flush();expect(digest).toHaveBeenCalledTimes(1);miss.close();
  const hit=await probeSQLiteWasmBytes(b.factory,1000,asset);expect(hit.bytes).toEqual(binary);expect(digest).toHaveBeenCalledTimes(2);hit.close();
 }finally{digest.mockRestore();stop();}
});
