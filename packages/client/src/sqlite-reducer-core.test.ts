import {DatabaseSync} from 'node:sqlite';
import {expect,it} from 'vitest';
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
  const offline=new SQLiteReducerCore({...f.options,runtime:{execute:async()=>{throw Error('must not execute');},prepare:async()=>{throw Error('chunk unavailable offline');}}});expect(await offline.invoke(input)).toEqual(first);
 }finally{f.sql.close();}
});
