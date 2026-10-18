import {afterEach,expect,it} from 'vitest';
import {DatabaseSync} from 'node:sqlite';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteCanonicalDispatcher} from './sqlite-canonical-dispatch.js';
import {brokerSQLiteStorage} from './broker-sqlite-storage.js';
import {sqliteOperationPublishes} from './sqlite-worker-protocol.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
const connections:DatabaseSync[]=[];
afterEach(()=>{for(const db of connections.splice(0))db.close();});
const schema={records:{key:'_id',fields:{_id:{kind:'text'},rank:{kind:'number'},title:{kind:'text'}},indexes:[{columns:['rank']}]}} as const;
const empty={entities:{},liveQueries:{}};
async function fixture(){
  const db=new DatabaseSync(':memory:');connections.push(db);
  const host:SQLiteReplicaDatabase={
    async execAsync(sql){db.exec(sql);},async runAsync(sql,...args){return db.prepare(sql).run(...args as any[]);},
    async getFirstAsync<T>(sql:string,...args:unknown[]){return db.prepare(sql).get(...args as any[]) as T??null;},
    async getAllAsync<T>(sql:string,...args:unknown[]){return db.prepare(sql).all(...args as any[]) as T[];},
    async withTransactionAsync(run){db.exec('BEGIN');try{await run();db.exec('COMMIT');}catch(error){db.exec('ROLLBACK');throw error;}},
  };
  const database=sqliteOwnerTransactions(host);
  let engine=new SQLiteTypedReplicaEngine(database,schema);await engine.open('one');
  let dispatcher=new SQLiteCanonicalDispatcher(database,sqliteTypedReplicaStorage(database,schema),engine),lane=Promise.resolve(),id=0;
  const listeners=new Set<(scope:string,sequence:number)=>void>();
  const request=<T>(op:string,args:any,requestID=String(++id)):Promise<T>=>{
    const run=lane.then(async()=>{
      let value:any;
      if(op==='canonicalCall')value=await dispatcher.call(requestID,args);
      else if(op==='canonicalRead')value=await dispatcher.read(args);
      else if(op==='canonicalGeneration')value=await dispatcher.generation(args.scope,args.expected);
      else throw Error('unsupported');
      if(sqliteOperationPublishes(op,args))for(const listener of listeners)listener(args.scope,value.sequence);
      return value as T;
    });lane=run.then(()=>undefined,()=>undefined);return run;
  };
  const storage=brokerSQLiteStorage({request,subscribeChanges(listener){listeners.add(listener);return()=>{listeners.delete(listener);};}});
  const transaction=(rank:number)=>({cursor:{epoch:'e',revision:rank},changes:[{entity:'records',id:'a',operation:'insert' as const,newValue:{_id:'a',rank,title:'Record'}}]});
  return {db,storage,request,transaction,listeners,engine,async replaceOwner(){await lane;engine=new SQLiteTypedReplicaEngine(database,schema);await engine.open('two');dispatcher=new SQLiteCanonicalDispatcher(database,sqliteTypedReplicaStorage(database,schema),engine);}};
}
it('uses the owner canonical corpus for scoped pages, reads, sessions and peer changes',async()=>{
  const f=await fixture(),changes:string[]=[];const release=f.storage.subscribePeer!(scope=>changes.push(scope));
  await f.storage.applyTransaction(f.transaction(1),empty,'s');
  expect(changes).toEqual(['s']);
  expect((await f.storage.loadEntityRows!('s','records',['a']))[0]!.row.rank).toBe(1);
  expect(await f.storage.loadEntityRows!('other','records',['a'])).toEqual([]);
  expect((await f.storage.readViewPage!('s','grid',{table:'records',order:[{field:'rank',direction:'asc'}],limit:32})).rows.map(row=>row._id)).toEqual(['a']);
  const read=await f.storage.withReadView!('s',{records:{key:'_id',complete:true}},view=>view.select({table:'records',columns:['_id','rank'],where:{column:'rank',op:'eq',value:1}}));
  expect(read).toEqual({rows:[{_id:'a',rank:1}],complete:true});
  await f.storage.saveSession!('identity',undefined);expect(await f.storage.loadSession!('identity')).toBeUndefined();
  release();f.storage.dispose();expect(f.listeners.size).toBe(0);
  await expect(f.storage.load('s')).rejects.toThrow('closed');
});
it('replays canonical writes after replacement without reapplying an earlier change',async()=>{
  const f=await fixture(),firstInput={scope:'s',method:'applyTransaction',args:[f.transaction(1),empty,'s']};
  const first=await f.request('canonicalCall',firstInput,'same-request');
  await f.storage.applyTransaction(f.transaction(2),empty,'s');await f.replaceOwner();
  expect(await f.request('canonicalCall',firstInput,'same-request')).toEqual(first);
  expect((await f.storage.loadEntityRows!('s','records',['a']))[0]!.row.rank).toBe(2);
  await expect(f.request('canonicalCall',{...firstInput,args:[f.transaction(3),empty,'s']},'same-request')).rejects.toMatchObject({code:'INTENT_CONFLICT'});
});
it('rejects stale multi-read callbacks on a commit or owner change, including swallowed read errors',async()=>{
  const f=await fixture();await f.storage.applyTransaction(f.transaction(1),empty,'s');
  const coverage={records:{key:'_id',complete:true}},read={table:'records',limit:1};
  await expect(f.storage.withReadView!('s',coverage,async view=>{
    await view.select(read);await f.storage.applyTransaction(f.transaction(2),empty,'s');
    await expect(view.select(read)).rejects.toMatchObject({code:'PAGE_CHANGED'});return 'mixed';
  })).rejects.toMatchObject({code:'PAGE_CHANGED'});
  await expect(f.storage.withReadView!('s',coverage,async view=>{await view.select(read);await f.replaceOwner();return 'stale';})).rejects.toMatchObject({code:'PAGE_CHANGED'});
  const released=await f.storage.withReadView!('s',coverage,async view=>view);
  await expect(released.select(read)).rejects.toThrow('released');
});
it('rejects scope mismatch, unsupported methods and oversized hydration before any write',async()=>{
  const f=await fixture();
  await expect(f.request('canonicalCall',{scope:'s',method:'applyTransaction',args:[f.transaction(1),empty,'other']})).rejects.toThrow('scope');
  await expect(f.request('canonicalCall',{scope:'s',method:'constructor',args:['s']})).rejects.toMatchObject({code:'OPERATION'});
  await expect(f.storage.loadEntityRows!('s','records',Array(257).fill('a'))).rejects.toMatchObject({code:'BUDGET'});
  expect(await f.storage.loadEntityRows!('s','records',['a'])).toEqual([]);
});
it('clamps caller working-set allowances and rejects invalid change budgets',async()=>{
  const f=await fixture();
  await f.engine.importScope('s',(async function*(){for(let start=0;start<600;start+=50)yield JSON.stringify(Array.from({length:50},(_,n)=>{const id=String(start+n);return {table:'records',id,version:1,row:{_id:id,rank:n,title:'Row '+id}};}));})());
  const working=await f.storage.loadWorkingSet!('s',{maxRows:1000000,maxBytes:1000000000});
  expect(Object.keys(working!.entities.records!).length).toBeLessThanOrEqual(256);
  await expect(f.request('canonicalCall',{scope:'s',method:'readChanges',args:['s',0,{maxRows:'unbounded',maxBytes:'unbounded'}]})).rejects.toThrow('budget');
});
it('routes bounded native reads without legacy membership hydration and releases them with the adapter',async()=>{
 const calls:Array<{op:string;args:unknown}>=[];
 const storage=brokerSQLiteStorage({async request<T>(op,args){calls.push({op,args});return (op==='nativeWindowPage'?null:{generation:{epoch:1,sequence:2},rows:[{id:'a',value:null}]}) as T;},subscribeChanges(){return()=>{};}});
 expect(await storage.readNativeWindowPage({scope:'s',signature:'large',limit:50})).toBeUndefined();
 expect((await storage.readNativeEntityPage({scope:'s',entity:'records',ids:['a']})).rows).toEqual([{id:'a',value:null}]);
 expect(calls.map(call=>call.op)).toEqual(['nativeWindowPage','nativeEntityPage']);storage.dispose();
 await expect(storage.readNativeWindowPage({scope:'s',signature:'large'})).rejects.toThrow('closed');
});
