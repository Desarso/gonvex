import {readFile} from 'node:fs/promises';
import assert from 'node:assert/strict';
import initialize from '../sqlite-native/sqlite3.mjs';
import {test} from 'node:test';
const root=new URL('../sqlite-native/',import.meta.url),captured=[];
test('actual C VFS pointers preserve checked i64 offsets through generated JS callbacks',async()=>{
const source=await readFile(new URL('sqlite3.mjs',root),'utf8'),binary=await readFile(new URL('sqlite3.wasm',root));
const fakeModule={gonvexSahNumberIO:{xRead(...args){captured.push(['read',...args]);return 71;},xWrite(...args){captured.push(['write',...args]);return 72;}}};
const callbacks=Object.fromEntries(['read','write'].map(name=>{const text=source.match(new RegExp('^function gv_sah_'+name+'_js\\([^\\n]+','m'))?.[0];assert.ok(text);return ['gv_sah_'+name+'_js',Function('Module','return ('+text+');')(fakeModule)];}));
let reject;const failed=new Promise((_yes,no)=>{reject=no;});
const sqlite3=await Promise.race([initialize({instantiateWasm(imports,receive){
 for(const [name,callback]of Object.entries(callbacks)){let count=0;for(const module of new Set(Object.values(imports)))if(Object.hasOwn(module,name)){module[name]=callback;count++;}assert.ok(count>0,'Actual WASM import '+name);}
 WebAssembly.instantiate(binary,imports).then(result=>{this.instantiateWasm={...result,imports};receive(result.instance,result.module);},reject);return {};
},print:()=>{},printErr:()=>{}}),failed]);
const offsets=[0n,1n,2147483647n,2147483648n,4294967295n,4294967296n,8589934591n,9007199254740991n];
let seed=17n;for(let n=0;n<1000;n++){seed=(seed*6364136223846793005n+1442695040888963407n)&((1n<<53n)-1n);offsets.push(seed);}
for(const [method,ptr,rc]of [['read',sqlite3.wasm.exports.gv_sah_read_pointer(),71],['write',sqlite3.wasm.exports.gv_sah_write_pointer(),72]]){
 assert.ok(ptr>0);const callback=sqlite3.wasm.functionEntry(ptr);assert.equal(typeof callback,'function');
 for(const offset of offsets){assert.equal(callback(42,84,4096,offset),rc);assert.deepEqual(captured.pop(),[method,42,84,4096,Number(offset)]);}
 for(const offset of [-1n,9007199254740992n,9223372036854775807n]){assert.equal(callback(42,84,4096,offset),sqlite3.capi.SQLITE_IOERR_SEEK);assert.equal(captured.length,0);}
}
const db=new sqlite3.oo1.DB(':memory:');assert.equal(db.selectValue('SELECT gv_ingress_hash_contract()'),'sha256-v1');assert.equal(db.selectValue('PRAGMA integrity_check'),'ok');db.close();
});

test('borrowed views stay bounded and follow writes, pointer sizes and heap growth',async()=>{
 const source=await readFile(new URL('sqlite3.mjs',root),'utf8');
 const start=source.indexOf('  // Borrowed views are used synchronously.');
 const end=source.indexOf('  const ioMethods = {',start);assert.ok(start>0&&end>start);
 let heap=new Uint8Array(1024*1024);const sqlite3={};
 const {view,options}=Function('wasm','sqlite3',source.slice(start,end)+';return {view:gvView,options:gvOptions};')({heap8u:()=>heap},sqlite3);
 const original=view(64,8);assert.equal(view(64,8),original);heap[64]=71;assert.equal(original[0],71);
 const changed=view(64,16);assert.notEqual(changed,original);assert.equal(changed.length,16);assert.equal(changed.buffer,heap.buffer);
 for(let n=0;n<5000;n++)view(n*32,16);
 const stats=sqlite3.__gonvexVFSProfile();assert.equal(stats.viewEntries,4096);assert.equal(stats.viewMaxEntries,4096);assert.equal(stats.maxViewEntries,4096);
 assert.notEqual(view(64,8),original);assert.equal(view(64,8)[0],71);
 heap=new Uint8Array(2*1024*1024);heap[64]=92;const grown=view(64,8);assert.equal(grown.buffer,heap.buffer);assert.equal(grown[0],92);assert.notEqual(grown,original);
 const reset=sqlite3.__gonvexVFSProfile();assert.equal(reset.heapResets,1);assert.equal(reset.viewEntries,1);assert.equal(options.at,0);
});
