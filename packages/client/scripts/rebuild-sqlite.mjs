import {createHash,randomUUID} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import {dirname,isAbsolute,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {packageSQLiteModule} from './sqlite-module-assets.mjs';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'..'),native=root+'/sqlite-native',mode=process.argv[2];
if(!['--verify','--write'].includes(mode)||process.argv.length!==3)throw Error('Usage: sqlite:rebuild --verify|--write');
const buildRoot=process.env.SQLITE_BUILD_ROOT,emsdk=process.env.EMSDK;
if(!buildRoot||!emsdk||!isAbsolute(buildRoot)||!isAbsolute(emsdk)||resolve(buildRoot).startsWith(root+'/'))throw Error('Absolute SQLITE_BUILD_ROOT outside the package and pinned EMSDK are required');
const manifest=JSON.parse(await readFile(native+'/source.json','utf8')),hash=(data,kind='sha256')=>createHash(kind).update(data).digest('hex');
if(execFileSync('git',['rev-parse','HEAD'],{cwd:emsdk,encoding:'utf8'}).trim()!==manifest.emsdkCommit)throw Error('Emscripten SDK commit differs from source manifest');
const version=execFileSync('bash',['-c','source "$EMSDK/emsdk_env.sh" >/dev/null && emcc --version'],{env:{...process.env,EMSDK_QUIET:'1'},encoding:'utf8'});
if(!new RegExp('\\b'+manifest.emsdk.replaceAll('.','\\.')+'\\b').test(version))throw Error('Active Emscripten compiler differs from source manifest');
const extension=await readFile(native+'/sqlite_wasm_extra_init.c'),extensionHash=hash(extension);
if(mode==='--verify'&&extensionHash!==manifest.extensionSourceSha256)throw Error('Native extension changed; use --write only after reviewing it');
await mkdir(buildRoot,{recursive:true});const work=resolve(buildRoot,'build-'+randomUUID());await mkdir(work);
for(const [name,archive]of Object.entries(manifest.archives)){
 const cache=resolve(buildRoot,'archive-'+name+'.zip');let bytes;
 try{bytes=await readFile(cache);}catch(error){if(error.code!=='ENOENT')throw error;const response=await fetch(archive.url);if(!response.ok)throw Error('SQLite archive download failed: '+response.status);bytes=Buffer.from(await response.arrayBuffer());}
 if(hash(bytes,'sha3-256')!==archive.sha3_256)throw Error('SQLite archive differs from source manifest: '+name);await writeFile(cache,bytes);
 execFileSync('python3',['-c',`import pathlib,sys,zipfile
root=pathlib.Path(sys.argv[2]).resolve()
with zipfile.ZipFile(sys.argv[1]) as archive:
 for item in archive.infolist():
  target=(root/item.filename).resolve()
  if not target.is_relative_to(root):raise SystemExit('Archive path escapes build root')
 archive.extractall(root)
 for item in archive.infolist():
  target=root/item.filename
  target.chmod((item.external_attr>>16)&0o777 or (0o755 if item.is_dir() else 0o644))
`,cache,work],{stdio:'inherit'});
}
const source=resolve(work,'sqlite-src-3540100'),amalgamation=resolve(work,'sqlite-amalgamation-3540100');
for(const name of ['sqlite3.c','sqlite3.h'])await copyFile(amalgamation+'/'+name,source+'/'+name);
const helper=work+'/host-cc.sh';await writeFile(helper,'#!/usr/bin/env bash\nexec cc "$@" -lm\n',{mode:0o755});
await copyFile(native+'/sqlite_wasm_extra_init.c',work+'/sqlite_wasm_extra_init.c');
const cache=process.env.EM_CACHE??resolve(buildRoot,'em-cache');if(!isAbsolute(cache))throw Error('EM_CACHE must be absolute');await mkdir(cache,{recursive:true});
const env={...process.env,EMSDK_QUIET:'1',EM_CACHE:cache,EMCC_CORES:'1',MALLOC_ARENA_MAX:'2',GONVEX_SQLITE_SOURCE:source,GONVEX_SQLITE_EXT:work+'/sqlite_wasm_extra_init.c',GONVEX_SQLITE_CC:helper};
execFileSync('bash',['-c',`set -euo pipefail
source "$EMSDK/emsdk_env.sh"
cd "$GONVEX_SQLITE_SOURCE"
./configure --disable-tcl --with-emsdk="$EMSDK"
cd ext/wasm
make -j1 b-esm emcc_opt=-O2 "bin.wasm-strip=$EMSDK/upstream/bin/llvm-strip" "CC=bash $GONVEX_SQLITE_CC" omit-api="kvvfs opfs opfs-wl vtab worker1" emcc.INITIAL_MEMORY=4 "sqlite3_wasm_extra_init.c=$GONVEX_SQLITE_EXT"
`],{env,stdio:'inherit'});
if(hash(await readFile(native+'/sqlite_wasm_extra_init.c'))!==extensionHash)throw Error('Extension source changed during build');
const raw=await readFile(source+'/ext/wasm/jswasm/esm/sqlite3.mjs','utf8'),module=packageSQLiteModule(raw),wasm=await readFile(source+'/ext/wasm/jswasm/esm/sqlite3.wasm');
if(!module.includes('pool.hasReservedLock(file.path)'))throw Error('Upstream SAH recovery fix is absent');
const files={'sqlite3.mjs':hash(module),'sqlite3.wasm':hash(wasm)};
if(mode==='--verify'){
 if(hash(raw)!==manifest.rawModuleSha256||JSON.stringify(files)!==JSON.stringify(manifest.files))throw Error('Rebuilt SQLite assets differ from pinned hashes; inspect '+work);
 for(const [name,expected]of Object.entries(files))if(hash(await readFile(native+'/'+name))!==expected)throw Error('Packaged asset differs: '+name);
}else{
 await writeFile(native+'/sqlite3.mjs',module);await writeFile(native+'/sqlite3.wasm',wasm);
 await writeFile(native+'/source.json',JSON.stringify({...manifest,rawModuleSha256:hash(raw),extensionSourceSha256:extensionHash,files},null,2)+'\n');
}
console.log(JSON.stringify({mode,work,extensionSourceSha256:extensionHash,files}));
