import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import initialize from '@gonvex/client/sqlite-module';
import {sqliteWasmLength,sqliteWasmSHA256} from '../dist/sqlite-wasm-manifest.js';
test('built self-reference exposes the pinned SQLite module and matching engine',async()=>{
 const binary=await readFile(new URL('../dist/vendor/sqlite3.wasm',import.meta.url));
 assert.equal(binary.byteLength,sqliteWasmLength);
 assert.equal(createHash('sha256').update(binary).digest('hex'),sqliteWasmSHA256);
 let reject;const failed=new Promise((_resolve,no)=>{reject=no;});
 const sqlite=await Promise.race([initialize({instantiateWasm(imports,receive){
  void WebAssembly.instantiate(binary,imports).then(result=>{this.instantiateWasm={...result,imports};receive(result.instance,result.module);},reject);return {};
 }}),failed]);
 const db=new sqlite.oo1.DB(':memory:');
 try{assert.equal(db.selectValue('SELECT 42'),42);}finally{db.close();}
});
