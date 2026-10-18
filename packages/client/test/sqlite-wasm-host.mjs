import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
/** Test adapter for the browser-targeted official build. Production uses its
 * normal streaming loader in a dedicated worker; Node supplies module bytes. */
export async function initializePinnedSQLite(){
 const native=new URL('../sqlite-native/',import.meta.url),source=JSON.parse(await readFile(new URL('source.json',native),'utf8'));
 for(const [file,expected]of Object.entries(source.files))if(createHash('sha256').update(await readFile(new URL(file,native))).digest('hex')!==expected)throw Error('Pinned SQLite test asset differs: '+file);
 const {default:initialize}=await import(new URL('sqlite3.mjs',native).href),binary=new Uint8Array(await readFile(new URL('sqlite3.wasm',native)));
 let reject;const failed=new Promise((_yes,no)=>{reject=no;});
 const sqlite=await Promise.race([initialize({instantiateWasm(imports,receive){WebAssembly.instantiate(binary,imports).then(result=>{this.instantiateWasm={...result,imports};receive(result.instance,result.module);},reject);return {};}}),failed]);
 if(sqlite.version.libVersion!==source.sqliteVersion)throw Error('Unexpected SQLite runtime version');return sqlite;
}
