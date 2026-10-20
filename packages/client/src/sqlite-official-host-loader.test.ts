import {readFile} from 'node:fs/promises';
import {expect,it,vi} from 'vitest';
import {openOfficialSQLiteHost} from './sqlite-official-host.js';
// Isolated from the host bridge suite: SQLite's pinned module bootstrap stashes
// initialization state once at module evaluation and consumes it on first init.
it('loads the pinned engine through the real cache-miss streaming callback and literal asset URL',async()=>{
 const bytes=await readFile(new URL('../sqlite-native/sqlite3.wasm',import.meta.url));
 const expected=new URL('./vendor/sqlite3.wasm',import.meta.url).href;
 const fetched=vi.fn(async(url:URL|RequestInfo)=>{
  expect(String(url)).toBe(expected);
  return new Response(bytes,{headers:{'content-type':'application/wasm'}});
 });
 const warning=vi.spyOn(console,'warn').mockImplementation(()=>{});
 vi.stubGlobal('postMessage',()=>{});vi.stubGlobal('fetch',fetched);vi.stubGlobal('BroadcastChannel',undefined);
 try{
  const host=await openOfficialSQLiteHost();
  try{expect(await host.database.getFirstAsync('SELECT 42 AS n')).toEqual({n:42});}finally{host.close();}
  expect(fetched).toHaveBeenCalledTimes(1);
  expect(warning.mock.calls.some(args=>String(args[0]).includes('sqlite3InitModuleState is missing'))).toBe(false);
 }finally{warning.mockRestore();vi.unstubAllGlobals();}
});
