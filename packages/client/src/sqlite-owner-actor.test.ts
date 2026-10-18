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

it('prepares lazy reducer chunks outside the owner queue and fences a changed tab session before retry',async()=>{
 const {PortableReducerRuntime}=await import('@gonvex/local-runtime/portable'),{reducer,schema:validators,getRow}=await import('@gonvex/module-sdk'),{SQLiteReducerOwner}=await import('./sqlite-reducer-owner.js');
 const sql=new DatabaseSync(':memory:');const raw:SQLiteReplicaDatabase={async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}};
 mocked.host={database:raw,memoryBytes:()=>0,close(){}};const awaiting=new Map<string,(value:any)=>void>();let serial=0,generation='session-one',loads=0,executions=0,release!:(value:any)=>void;
 const loading=new Promise<any>(resolve=>{release=resolve;});let endpoint!:InstanceType<typeof SQLiteReducerOwner>,owner!:SQLiteOwnerProgramContext;let persistence:{entered:()=>void;wait:Promise<void>}|undefined;
 const runtime=new PortableReducerRuntime({schema:{records:{key:'id',columns:{id:{type:'text',nullable:false},n:{type:'bigint',nullable:false}}}},artifactHash:'artifact',reducers:{increment:async()=>{loads++;return loading;}}});
 vi.stubGlobal('postMessage',(message:any)=>{if(message.type==='change')queueMicrotask(()=>(globalThis as any).onmessage?.({data:{type:'change-ack',protocol:2,token:'owner',tab:message.to,notification:message.notification,epoch:message.epoch,sequence:message.sequence}}));else{awaiting.get(message.id)?.(message);awaiting.delete(message.id);}});
 const interval=vi.spyOn(globalThis,'setInterval').mockImplementation(()=>0 as any);
 const dispose=installSQLiteOwner({schema,createProgram(ctx){
  owner=ctx;void ctx.runOwner(0,()=>ctx.canonical.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'records',id:'a',operation:'insert',newValue:{id:'a',n:1}}]},{entities:{},liveQueries:{}},'s').then(()=>{}));
  endpoint=new SQLiteReducerOwner(ctx,{runtime,artifactHash:'artifact',async persistIntent(){if(persistence){persistence.entered();await persistence.wait;}},resolveTab(){return {scope:'s',generation};},async context(){return {intentScope:'s',identity:{auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}},coverage:{records:{key:'id',complete:true}}};}});
  return {call(args:any,caller){return endpoint.invoke(args,caller);}};
 }});interval.mockRestore();
 const request=(op:string,args:any={},requestID='command-'+(++serial))=>new Promise<any>(resolve=>{const id='transport-'+(++serial);awaiting.set(id,resolve);(globalThis as any).onmessage({data:{id,requestID,tabID:'tab',op,args}});});
 try{
  expect((await request('open',{protocol:2,owner:'owner'})).error).toBeUndefined();
  const invocation={path:'increment',args:{id:'a'},now:100};const pending=request('clientCall',invocation,'same-command');
  await vi.waitFor(()=>expect(loads).toBe(1));expect((await request('ping')).value).toEqual({ready:true});
  generation='session-two';release(reducer({args:validators.any(),result:validators.any(),run:async ctx=>{executions++;const row=await getRow<{id:string;n:number}>(ctx.db,'records','id','a');await ctx.db.update('records','a',{n:row!.n+1});return row!.n+1;}}));
  expect((await pending).error.code).toBe('FENCED');expect(executions).toBe(0);
  const replies=await Promise.all([request('clientCall',invocation,'same-command'),request('clientCall',invocation,'same-command')]);expect(replies[0].value).toEqual(replies[1].value);expect(replies[0].value.result).toBe(2);expect(executions).toBe(1);expect(loads).toBe(1);
  expect((await owner.runOwner(0,()=>owner.engine.page('s',{table:'records',order:[{field:'n',direction:'asc'}],limit:1}))).rows[0].n).toBe(1);
  expect((await owner.runOwner(0,()=>owner.reader.entities({scope:'s',entity:'records',ids:['a','missing']}))).rows).toEqual([{id:'a',value:{id:'a',n:2}},{id:'missing',value:null}]);
  for(const operation of ['page','nativeEntityPage','nativeWindowPage','canonicalCall','canonicalRead','canonicalGeneration','apply','snapshotActivate','snapshotRead','snapshotAppend','snapshotAppendEncoded','snapshotDiscard','windowBegin','windowAppend','windowPublish','windowPage','windowHashes','windowCollect','bootstrapBegin','bootstrapAppend','bootstrapFinish','outbox','acknowledge','count'])expect((await request(operation,{scope:'other-identity'})).error.code).toBe('OPERATION');
  let persisted!:()=>void,resume!:()=>void;const entered=new Promise<void>(resolve=>{persisted=resolve;});persistence={entered:persisted,wait:new Promise<void>(resolve=>{resume=resolve;})};
  const pendingCommit=request('clientCall',{...invocation,now:101},'fenced-during-persistence');await entered;generation='session-three';resume();expect((await pendingCommit).error.code).toBe('FENCED');expect((await endpoint.journal.summary('s')).count).toBe(1);expect((await endpoint.predictions.entities('s','records',['a']))[0].row.n).toBe(2);expect(sql.prepare('SELECT 1 FROM gv_reducer_invocations WHERE command_id=?').get('fenced-during-persistence')).toBeUndefined();

  persistence=undefined;let enteredReply!:()=>void,releaseReply!:()=>void;const replyEntered=new Promise<void>(resolve=>{enteredReply=resolve;}),replyBarrier=new Promise<void>(resolve=>{releaseReply=resolve;}),write=owner.database.runAsync.bind(owner.database);
  const held=vi.spyOn(owner.database,'runAsync').mockImplementation(async(query,...args)=>{const value=await write(query,...args);if(query.startsWith('INSERT INTO gv_reducer_reply_context')&&args[1]==='fenced-after-receipt'){enteredReply();await replyBarrier;}return value;});
  const afterReceipt=request('clientCall',{...invocation,now:102},'fenced-after-receipt');await replyEntered;expect(sql.prepare('SELECT 1 FROM gv_reducer_invocations WHERE command_id=?').get('fenced-after-receipt')).toBeDefined();generation='session-four';releaseReply();expect((await afterReceipt).error.code).toBe('FENCED');held.mockRestore();expect(sql.prepare('SELECT 1 FROM gv_reducer_invocations WHERE command_id=?').get('fenced-after-receipt')).toBeUndefined();expect(sql.prepare('SELECT 1 FROM gv_reducer_reply_context WHERE command_id=?').get('fenced-after-receipt')).toBeUndefined();expect((await endpoint.journal.summary('s')).count).toBe(1);

  await dispose();
 }finally{await dispose();interval.mockRestore();vi.unstubAllGlobals();delete (globalThis as any).onmessage;sql.close();}
});
