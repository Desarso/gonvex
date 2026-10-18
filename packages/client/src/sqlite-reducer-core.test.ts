import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import {reducer,schema,getRow} from '@gonvex/module-sdk';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import {SQLiteReducerCore} from './sqlite-reducer-core.js';
import {SQLitePredictions} from './sqlite-predictions.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
const typed={tasks:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[{columns:['n']}]}} as const;
const local={tasks:{key:'id',columns:{id:{type:'text',nullable:false},n:{type:'bigint',nullable:false}}}} as const;
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
async function fixture(){
 const sql=new DatabaseSync(':memory:');const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,typed);await engine.open('core');const canonical=sqliteTypedReplicaStorage(db,typed);await canonical.ready();await canonical.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:{id:'a',n:1}}]},{entities:{},liveQueries:{}},'s');
 const fence=()=>engine.verifyOwner(),journal=new SQLiteIntentJournal(db,fence),predictions=new SQLitePredictions(db,canonical,typed,fence);let calls=0;
 const runtime=new PortableReducerRuntime({schema:local,artifactHash:'artifact',reducers:{increment:reducer({args:schema.any(),result:schema.any(),run:async(ctx,args:any)=>{calls++;const before=await getRow<{id:string;n:number}>(ctx.db,'tasks','id',args.id);if(!before)throw Error('missing task');await ctx.db.update('tasks',args.id,{n:before.n+1});if(args.fail)throw Error('reducer failed');return {id:args.id,n:before.n+1,member:ctx.member!.id};}}),giantReply:reducer({args:schema.any(),result:schema.any(),run:async()=> '東京'.repeat(30000)}),create:reducer({args:schema.any(),result:schema.any(),run:async ctx=>ctx.db.insert('tasks',{n:10})})}});
 const options={database:db,engine,journal,predictions,runtime,artifactHash:'artifact',async context(scope:string){if(scope!=='s')throw Error('unauthenticated scope');return {identity,coverage:{tasks:{key:'id',complete:true}}};}},core=new SQLiteReducerCore(options);await core.ready();return {sql,db,engine,canonical,journal,predictions,core,options,get calls(){return calls;}};
}
it('executes the same generated reducer against predictions and atomically queues an intent without changing canonical rows',async()=>{
 const f=await fixture();try{
  const first={scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}};const receipt=await f.core.invoke(first);expect(receipt.result).toEqual({id:'a',n:2,member:'member'});expect(f.calls).toBe(1);
  expect(await f.core.invoke(first)).toEqual(receipt);expect(f.calls).toBe(1);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect((await f.canonical.loadEntityRows('s','tasks',['a']))[0].row.n).toBe(1);
  expect((await f.core.invoke({...first,commandId:'two',now:101})).result).toMatchObject({n:3});expect((await f.journal.summary('s')).count).toBe(2);
  expect((await f.journal.readyBatch('s',1000)).map(entry=>entry.idempotencyKey)).toEqual(['one']);
  const generation=await f.engine.verifyOwner();expect(generation.sequence).toBe(receipt.originalGeneration.sequence+1);
  await expect(f.core.invoke({...first,args:{id:'other'}})).rejects.toMatchObject({code:'INTENT_CONFLICT'});
  const reopened=new SQLiteReducerCore(f.options);expect(await reopened.invoke(first)).toEqual(receipt);expect(f.calls).toBe(2);
 }finally{f.sql.close();}
});
it('rolls back failed reducers and later persistence failures, including their predictions, receipts and generation',async()=>{
 const f=await fixture();try{
  const before=await f.engine.verifyOwner();await expect(f.core.invoke({scope:'s',path:'increment',commandId:'failed',now:100,args:{id:'a',fail:true}})).rejects.toThrow('reducer failed');expect((await f.journal.summary('s')).count).toBe(0);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect(await f.engine.verifyOwner()).toEqual(before);
  await expect(f.core.invoke({scope:'s',path:'giantReply',commandId:'large-reply',now:100,args:{}})).rejects.toMatchObject({code:'BUDGET'});expect((await f.journal.summary('s')).count).toBe(0);expect(await f.engine.verifyOwner()).toEqual(before);
  await f.db.execAsync("CREATE TRIGGER reject_receipt BEFORE INSERT ON gv_reducer_invocations BEGIN SELECT RAISE(ABORT,'quota failure surrogate'); END;");
  await expect(f.core.invoke({scope:'s',path:'increment',commandId:'late-failure',now:101,args:{id:'a'}})).rejects.toThrow('quota failure surrogate');expect((await f.journal.summary('s')).count).toBe(0);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect(await f.engine.verifyOwner()).toEqual(before);
  await expect(f.core.invoke({scope:'other',path:'increment',commandId:'wrong',now:102,args:{id:'a'}})).rejects.toThrow('unauthenticated');
 }finally{f.sql.close();}
});
it('retains the original stable insert ID and reply after intent acknowledgment and owner reopen',async()=>{
 const f=await fixture();try{
  const input={scope:'s',path:'create',commandId:'new-row',now:100,args:{}};const first=await f.core.invoke(input);expect(typeof first.result).toBe('object');
  const rows=(await f.journal.page('s')).entries;expect(rows).toHaveLength(1);const inserted=rows[0].patches![0];expect((await f.predictions.entities('s','tasks',[inserted.rowId]))[0].row.n).toBe(10);
  await f.journal.acknowledge('s',[first.intentId]);const reopened=new SQLiteReducerCore(f.options);expect(await reopened.invoke(input)).toEqual(first);expect((await f.journal.summary('s')).count).toBe(0);
  const offline=new SQLiteReducerCore({...f.options,runtime:{isPrepared:()=>false,execute:async()=>{throw Error('must not execute');},prepare:async()=>{throw Error('chunk unavailable offline');}}});expect(await offline.invoke(input)).toEqual(first);
 }finally{f.sql.close();}
});

it('rebases a causal prediction chain atomically over authority without changing original command replies',async()=>{
 const f=await fixture();try{
  const one={scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}},two={...one,commandId:'two',now:101};
  const first=await f.core.invoke(one),second=await f.core.invoke(two);
  const applied=await f.core.rebase('s',async()=>{
   await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:10}}]},{entities:{},liveQueries:{}},'s');
   await f.journal.acknowledge('s',[first.intentId]);
  });
  expect(applied).toMatchObject({replayed:1,retained:0,unavailable:0});expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(11);
  expect((await f.canonical.loadEntityRows('s','tasks',['a']))[0].row.n).toBe(10);
  expect(await f.core.invoke(one)).toEqual(first);expect(await f.core.invoke(two)).toEqual(second);
  expect((await f.journal.page('s')).entries[0].patches).toEqual([{entity:'tasks',rowId:'a',op:'patch',fields:{n:11}}]);
  await f.journal.update('s',second.intentId,['pending'],entry=>({...entry,state:'rejected'}));await f.core.rebase('s');
  expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(10);expect((await f.journal.summary('s')).rejected).toBe(1);
 }finally{f.sql.close();}
});
it('rolls back authority, acknowledgments and predictions on a cold module or a storage failure',async()=>{
 const f=await fixture();try{
  const input={scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}};const first=await f.core.invoke(input),before=await f.engine.verifyOwner();
  let prepared=false;const runtime={isPrepared:()=>prepared,prepare:async()=>{prepared=true;},execute:f.options.runtime.execute.bind(f.options.runtime)};
  const cold=new SQLiteReducerCore({...f.options,runtime});
  const authority=async()=>{await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:20}}]},{entities:{},liveQueries:{}},'s');};
  await expect(cold.rebase('s',authority)).rejects.toMatchObject({code:'MODULE_REQUIRED',path:'increment'});
  expect(await f.engine.verifyOwner()).toEqual(before);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect((await f.canonical.loadEntityRows('s','tasks',['a']))[0].row.n).toBe(1);
  await cold.prepare('increment');await cold.rebase('s',authority);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(21);
  const generation=await f.engine.verifyOwner();
  await f.db.execAsync("CREATE TRIGGER fail_rebase BEFORE UPDATE ON gv_reducer_journal BEGIN SELECT RAISE(ABORT,'disk I/O error surrogate'); END;");
  await expect(cold.rebase('s',async()=>{await f.journal.acknowledge('s',[first.intentId]);throw Error('late failure');})).rejects.toThrow('late failure');
  expect((await f.journal.summary('s')).count).toBe(1);expect(await f.engine.verifyOwner()).toEqual(generation);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(21);
  await expect(cold.rebase('s')).rejects.toThrow('disk I/O error surrogate');expect(await f.engine.verifyOwner()).toEqual(generation);
 }finally{f.sql.close();}
});
it('retains inflight predictions and keeps a locally unavailable intent durable without predicting its obsolete patch',async()=>{
 const f=await fixture();try{
  const first=await f.core.invoke({scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}});
  await f.journal.claim('s',1000);expect(await f.core.rebase('s')).toMatchObject({replayed:0,retained:1});expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);
  await f.journal.recoverInflight('s');
  const unavailable=new SQLiteReducerCore({...f.options,runtime:{isPrepared:()=>true,prepare:async()=>{},execute:async()=>{throw Error('local validation no longer holds');}}});
  expect(await unavailable.rebase('s')).toMatchObject({unavailable:1});expect((await f.journal.summary('s')).queued).toBe(1);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect((await f.journal.page('s')).entries[0].patches).toEqual([]);
  expect((await f.journal.page('s')).entries[0].id).toBe(first.intentId);
 }finally{f.sql.close();}
});

it('aborts a storage read failure instead of converting it into an unavailable local prediction',async()=>{
 const f=await fixture();try{
  await f.core.invoke({scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}});const before=await f.engine.verifyOwner();
  const failed=vi.spyOn(f.predictions,'withReadView').mockImplementation(async(_scope,_coverage,run)=>run({async select(){throw Object.assign(Error('disk I/O failed'),{resultCode:10});}}));
  await expect(f.core.rebase('s')).rejects.toThrow('disk I/O failed');failed.mockRestore();
  expect(await f.engine.verifyOwner()).toEqual(before);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect((await f.journal.page('s')).entries[0].patches).toEqual([{entity:'tasks',rowId:'a',op:'patch',fields:{n:2}}]);
 }finally{vi.restoreAllMocks();f.sql.close();}
});
it('replays a long causal chain through bounded pages rather than loading the backlog',async()=>{
 const f=await fixture();try{
  for(let n=0;n<250;n++)await f.core.invoke({scope:'s',path:'increment',commandId:'intent-'+n,now:100+n,args:{id:'a'}});
  const original=f.journal.page.bind(f.journal);let pages=0;
  const paged=vi.spyOn(f.journal,'page').mockImplementation(async(...args)=>{const page=await original(...args);expect(page.entries.length).toBeLessThanOrEqual(8);pages++;return page;});
  expect(await f.core.rebase('s',()=>f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:10}}]},{entities:{},liveQueries:{}},'s').then(()=>{}))).toMatchObject({replayed:250,retained:0,unavailable:0});
  expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(260);expect(pages).toBe(32);paged.mockRestore();
 }finally{vi.restoreAllMocks();f.sql.close();}
},30000);
