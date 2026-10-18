import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
const mocked=vi.hoisted(()=>({host:undefined as any}));
vi.mock('./sqlite-official-host.js',()=>({openOfficialSQLiteHost:async()=>mocked.host}));
import {installSQLiteOwner,type SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
const schema={records:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[]}} as const;
it('runs generated-program network waits outside the fenced SQL queue and preserves caller identity through disk phases',async()=>{
 const sql=new DatabaseSync(':memory:');const raw:SQLiteReplicaDatabase={async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}};
 const close=vi.fn();mocked.host={database:raw,memoryBytes:()=>0,close};let release!:(value:unknown)=>void;const network=new Promise(resolve=>{release=resolve;});
 const awaiting=new Map<string,(value:any)=>void>(),changes:any[]=[],callers:any[]=[];let serial=0,context!:SQLiteOwnerProgramContext;
 vi.stubGlobal('postMessage',(message:any)=>{if(message.type==='change'){changes.push(message);queueMicrotask(()=>(globalThis as any).onmessage?.({data:{type:'change-ack',protocol:2,token:'owner',tab:message.to,notification:message.notification,epoch:message.epoch,sequence:message.sequence}}));}else{awaiting.get(message.id)?.(message);awaiting.delete(message.id);}});
 const interval=vi.spyOn(globalThis,'setInterval').mockImplementation(()=>0 as any);
 const dispose=installSQLiteOwner({schema,createProgram(ctx){context=ctx;const initialized=ctx.runOwner(0,()=>ctx.database.execAsync('CREATE TABLE program_ready(n INTEGER)'));return {async call(args:any,caller){await initialized;callers.push(caller);if(args.method==='network')return network;if(args.method==='increment')return ctx.runOwner(128,()=>ctx.engine.apply('s',[{table:'records',id:args.id,version:1,row:{id:args.id,n:1}}],{id:caller.requestID,payload:{id:args.id}}),['s']);return undefined;},close(){release(null);}};}});interval.mockRestore();
 const request=(op:string,args:any={},tabID='tab',requestID='intent-'+(++serial))=>new Promise<any>(resolve=>{const id='transport-'+(++serial);awaiting.set(id,resolve);(globalThis as any).onmessage({data:{id,requestID,tabID,op,args}});});
 try{
  expect(()=>installSQLiteOwner()).toThrow('Only one');expect((await request('open',{protocol:2,owner:'owner'})).error).toBeUndefined();
  const pending=request('clientCall',{method:'network'},'network-tab','network-id');await vi.waitFor(()=>expect(callers).toContainEqual(expect.objectContaining({tabID:'network-tab',requestID:'network-id'})));
  expect((await request('ping')).value).toEqual({ready:true});
  const writes=await Promise.all(['a','b','c'].map(id=>request('clientCall',{method:'increment',id},'write-tab','write-'+id)));expect(writes.every(reply=>!reply.error)).toBe(true);
  expect(sql.prepare('SELECT count(*) n FROM gv_intent_receipts').get()).toEqual({n:3});expect(changes.filter(change=>change.to==='write-tab'&&!change.reset).map(change=>change.sequence)).toEqual([1,2,3]);
  expect((await request('clientCall',{method:'increment',id:'a'},'write-tab','write-a')).value).toEqual(writes[0].value);expect(changes.filter(change=>change.to==='write-tab'&&!change.reset)).toHaveLength(3);
  expect((await request('clientCall',{method:'void'})).value).toBeNull();release({network:'finished'});expect((await pending).value).toEqual({network:'finished'});
  await expect(context.runOwner(2097153,async()=>null)).rejects.toMatchObject({code:'BUSY'});
  await dispose();await dispose();expect(close).toHaveBeenCalledOnce();await expect(context.runOwner(0,async()=>null)).rejects.toMatchObject({code:'FENCED'});
 }finally{await dispose();interval.mockRestore();vi.unstubAllGlobals();delete (globalThis as any).onmessage;sql.close();}
});
