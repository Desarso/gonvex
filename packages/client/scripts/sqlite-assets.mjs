import {createRequire} from 'node:module';
import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'..'),native=root+'/sqlite-native',provenance=JSON.parse(await readFile(native+'/source.json','utf8'));
const hash=data=>createHash('sha256').update(data).digest('hex'),require=createRequire(root+'/package.json');
const packageInfo=JSON.parse(await readFile(resolve(dirname(require.resolve('@sqlite.org/sqlite-wasm')),'../package.json'),'utf8'));
if(packageInfo.version!==provenance.typesPackage)throw Error('Unpinned official SQLite type definitions');
if(hash(await readFile(native+'/sqlite_wasm_extra_init.c'))!==provenance.extensionSourceSha256)throw Error('SQLite native extension changed; rebuild and review its artifacts');
const wasmManifest=await readFile(root+'/src/sqlite-wasm-manifest.ts','utf8'),wasmBytes=await readFile(native+'/sqlite3.wasm');
if(!wasmManifest.includes("sqliteWasmSHA256='"+hash(wasmBytes)+"'")||!wasmManifest.includes('sqliteWasmLength='+wasmBytes.length+';'))throw Error('SQLite WASM byte-cache manifest differs from pinned engine');
await mkdir(root+'/dist/vendor',{recursive:true});
if(JSON.stringify(Object.keys(provenance.files).sort())!==JSON.stringify(['sqlite3.mjs','sqlite3.wasm']))throw Error('Unexpected SQLite distribution files');
for(const [file,expected]of Object.entries(provenance.files)){
 const bytes=await readFile(native+'/'+file);if(hash(bytes)!==expected)throw Error('SQLite artifact differs from pinned build: '+file);
 await writeFile(root+'/dist/vendor/'+file,bytes);
}
await writeFile(root+'/dist/vendor/sqlite3.d.mts',"export {default} from '@sqlite.org/sqlite-wasm';\n");
await copyFile(native+'/source.json',root+'/dist/vendor/sqlite-source.json');
await copyFile(root+'/src/vendor/SQLITE-NOTICE.md',root+'/dist/vendor/SQLITE-NOTICE.md');
