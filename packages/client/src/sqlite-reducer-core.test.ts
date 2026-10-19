import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import {reducer,schema,getRow} from '@gonvex/module-sdk';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import {SQLiteReducerCore,type SQLitePreparedReducer} from './sqlite-reducer-core.js';
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
 const runtime=new PortableReducerRuntime({schema:local,artifactHash:'artifact',reducers:{scan:reducer({args:schema.any(),result:schema.any(),run:async ctx=>{calls++;await ctx.db.update('tasks','a',{n:99});return await ctx.db.select({table:'tasks'});}}),increment:reducer({args:schema.any(),result:schema.any(),run:async(ctx,args:any)=>{calls++;const before=await getRow<{id:string;n:number}>(ctx.db,'tasks','id',args.id);if(!before)throw Error('missing task');await ctx.db.update('tasks',args.id,{n:before.n+1});if(args.fail)throw Error('reducer failed');return {id:args.id,n:before.n+1,member:ctx.member!.id};}}),giantReply:reducer({args:schema.any(),result:schema.any(),run:async()=> '東京'.repeat(30000)}),create:reducer({args:schema.any(),result:schema.any(),run:async ctx=>ctx.db.insert('tasks',{n:10})})}});
 const options={database:db,engine,journal,predictions,runtime,artifactHash:'artifact',async context(scope:string){if(scope!=='s')throw Error('unauthenticated scope');return {intentScope:'s',identity,coverage:{tasks:{key:'id',complete:true}}};}},core=new SQLiteReducerCore(options);await core.ready();return {sql,db,engine,canonical,journal,predictions,core,options,get calls(){return calls;}};
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
it('persists independent recovery only after local validation and before the SQLite commit, retaining it on a late SQL failure',async()=>{
 const f=await fixture(),recovery:any[]=[];try{
  const persist=vi.fn(async(input,context)=>{expect(f.db.ownerTransactionDepth).toBeGreaterThan(0);expect(context.intentScope).toBe('s');recovery.push(structuredClone(input));}),input={scope:'s',path:'increment',commandId:'durable',now:100,args:{id:'a'}};
  await expect(f.core.invoke({...input,args:{id:'a',fail:true}},persist)).rejects.toThrow('reducer failed');expect(persist).not.toHaveBeenCalled();expect((await f.journal.summary('s')).count).toBe(0);
  await f.db.execAsync("CREATE TRIGGER fail_original_receipt BEFORE INSERT ON gv_reducer_invocations BEGIN SELECT RAISE(ABORT,'late receipt failure');END;");
  await expect(f.core.invoke(input,persist)).rejects.toThrow('late receipt failure');expect(recovery).toEqual([input]);expect((await f.journal.summary('s')).count).toBe(0);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);
  await f.db.execAsync('DROP TRIGGER fail_original_receipt');const saved=await f.core.invoke(input,persist);expect(saved.result).toMatchObject({n:2});expect(persist).toHaveBeenCalledTimes(2);expect(await f.core.invoke(input,persist)).toEqual(saved);expect(persist).toHaveBeenCalledTimes(2);
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
it('retains an unknown-outcome intent after inflight recovery instead of executing it over its own canonical commit',async()=>{
 const f=await fixture();try{
  const first=await f.core.invoke({scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}}),second=await f.core.invoke({scope:'s',path:'increment',commandId:'two',now:101,args:{id:'a'}});
  expect((await f.journal.claim('s',1000,8)).map(entry=>entry.id)).toEqual([first.intentId]);await f.journal.recoverInflight('s');expect((await f.journal.page('s')).entries[0]).toMatchObject({state:'pending',sentAt:1000});
  const rebased=await f.core.rebase('s',async()=>{await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:2}}]},{entities:{},liveQueries:{}},'s');});
  expect(rebased).toMatchObject({replayed:1,retained:1});expect(f.calls).toBe(3);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(3);
  const reopened=new SQLiteReducerCore({...f.options,runtime:{isPrepared:()=>false,prepare:async()=>{throw Error('offline');},execute:async()=>{throw Error('must not rerun sent body');}}});
  await f.journal.update('s',second.intentId,['pending'],entry=>({...entry,state:'inflight',sentAt:1001}));expect(await reopened.rebase('s')).toMatchObject({replayed:0,retained:2});expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(3);
  expect(await reopened.invoke({scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}})).toEqual(first);expect(await reopened.invoke({scope:'s',path:'increment',commandId:'two',now:101,args:{id:'a'}})).toEqual(second);
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
it('retains inflight predictions across recovery without asking an unavailable local body to execute',async()=>{
 const f=await fixture();try{
  const first=await f.core.invoke({scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}});
  await f.journal.claim('s',1000);expect(await f.core.rebase('s')).toMatchObject({replayed:0,retained:1});expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);
  await f.journal.recoverInflight('s');
  const unavailable=new SQLiteReducerCore({...f.options,runtime:{isPrepared:()=>true,prepare:async()=>{},execute:async()=>{throw Error('local validation no longer holds');}}});
  expect(await unavailable.rebase('s')).toMatchObject({unavailable:0,retained:1});expect((await f.journal.summary('s')).queued).toBe(1);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect((await f.journal.page('s')).entries[0].patches).toEqual([{entity:'tasks',rowId:'a',op:'patch',fields:{n:2}}]);
  expect((await f.journal.page('s')).entries[0].id).toBe(first.intentId);
 }finally{f.sql.close();}
});

it('keeps an unsent locally unavailable intent durable without predicting its obsolete patch',async()=>{
 const f=await fixture();try{
  const first=await f.core.invoke({scope:'s',path:'increment',commandId:'one',now:100,args:{id:'a'}});
  const unavailable=new SQLiteReducerCore({...f.options,runtime:{isPrepared:()=>true,prepare:async()=>{},execute:async()=>{throw Error('local validation no longer holds');}}});
  expect(await unavailable.rebase('s')).toMatchObject({unavailable:1,retained:0});expect((await f.journal.summary('s')).queued).toBe(1);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect((await f.journal.page('s')).entries[0].patches).toEqual([]);expect((await f.journal.page('s')).entries[0].id).toBe(first.intentId);
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

it('keeps queued commands and original replies in the stable identity scope across visibility changes',async()=>{
 const f=await fixture();try{
  const intentScope='identity\u0000deployment\u0000tenant\u0000account';let active='s';
  const options={...f.options,async context(scope:string){if(scope!==active)throw Error('obsolete visibility');return {intentScope,identity,coverage:{tasks:{key:'id',complete:true}}};}};
  const core=new SQLiteReducerCore(options),input={scope:'s',path:'increment',commandId:'same',now:100,args:{id:'a'}};const first=await core.invoke(input);
  expect((await f.journal.summary(intentScope)).count).toBe(1);expect((await f.journal.summary('s')).count).toBe(0);
  await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:{id:'a',n:10}}]},{entities:{},liveQueries:{}},'new-visible');active='new-visible';
  const offline=new SQLiteReducerCore({...options,runtime:{isPrepared:()=>false,prepare:async()=>{throw Error('offline');},execute:async()=>{throw Error('must not execute');}}});
  expect(await offline.invoke({...input,scope:active})).toEqual(first);expect(await core.rebase(active)).toMatchObject({replayed:1});expect((await f.predictions.entities(active,'tasks',['a']))[0].row.n).toBe(11);
  expect((await core.invoke({...input,scope:active,commandId:'next',now:101})).result).toMatchObject({n:12});const queued=(await f.journal.page(intentScope)).entries;expect(queued).toHaveLength(2);expect(queued.every(entry=>entry.scope===intentScope&&entry.localExecution!.scope===active)).toBe(true);expect(await core.invoke({...input,scope:active})).toEqual(first);
 }finally{f.sql.close();}
});

it('restores prepared predictions and the exact original reply without loading or rerunning a reducer body',async()=>{
 const before=await fixture(),after=await fixture();try{
  const input={scope:'s',path:'increment',commandId:'survives-opfs-loss',now:100,args:{id:'a'}};let prepared!:SQLitePreparedReducer;
  const receipt=await before.core.invoke(input,async(_input,_context,value)=>{prepared=structuredClone(value);});expect(receipt.result).toMatchObject({n:2});
  const offline=new SQLiteReducerCore({...after.options,runtime:{isPrepared:()=>false,prepare:async()=>{throw Error('module unavailable');},execute:async()=>{throw Error('must not execute');}}});
  await after.canonical.applyTransaction({cursor:{epoch:'e',revision:12},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:2}}]},{entities:{},liveQueries:{}},'s');
  expect(await offline.recover(input,prepared)).toEqual(receipt);expect(await offline.invoke(input)).toEqual(receipt);expect(after.calls).toBe(0);expect((await after.journal.page('s')).entries[0]).toMatchObject({idempotencyKey:input.commandId,sentAt:100,state:'pending'});
  expect(await offline.rebase('s')).toMatchObject({retained:1,replayed:0});expect((await after.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect(await offline.recover(input,prepared)).toEqual(receipt);expect((await after.journal.summary('s')).count).toBe(1);
  await expect(offline.recover({...input,args:{id:'other'}},prepared)).rejects.toMatchObject({code:'FENCED'});expect(await after.engine.integrity()).toBe('ok');
 }finally{before.sql.close();after.sql.close();}
});

it('durably queues missing-coverage commands without a fabricated result or partial local patches',async()=>{
 const f=await fixture();try{
  const core=new SQLiteReducerCore({...f.options,async context(){return {intentScope:'s',identity,coverage:{}};}}),input={scope:'s',path:'scan',commandId:'missing-coverage',now:100,args:{}};
  const receipt=await core.invoke(input);expect(receipt).toMatchObject({queuedOnly:true,result:null});expect(f.calls).toBe(1);expect((await f.journal.page('s')).entries[0]).toMatchObject({entityKeys:['__gonvex_local_intents'],patches:[],localExecution:{commandId:input.commandId}});expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);
  expect(await core.rebase('s')).toMatchObject({retained:1,replayed:0});expect(f.calls).toBe(1);expect(await core.invoke(input)).toEqual(receipt);expect((await f.journal.readyBatch('s',1000)).map(entry=>entry.idempotencyKey)).toEqual([input.commandId]);
 }finally{f.sql.close();}
});

it('does not reset every typed prediction table during first sync when there are no intents or predictions',async()=>{
 const f=await fixture();try{const reset=vi.spyOn(f.predictions,'reset');expect(await f.core.rebase('s',async()=>{await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:2}}]},{entities:{},liveQueries:{}},'s');})).toMatchObject({replayed:0,retained:0});expect(reset).not.toHaveBeenCalled();expect((await f.canonical.loadEntityRows('s','tasks',['a']))[0].row.n).toBe(2);}finally{f.sql.close();}
});

it('publishes first-sync authority without capturing unused reducer read coverage',async()=>{
 const f=await fixture();try{
  const modes:boolean[]=[];
  const core=new SQLiteReducerCore({...f.options,async context(scope,includeCoverage=true){modes.push(includeCoverage);if(includeCoverage)throw Error('No reducer requires coverage');return {...await f.options.context(scope),coverage:{}};}});
  expect(await core.rebase('s',async()=>{await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:10}}]},{entities:{},liveQueries:{}},'s');})).toMatchObject({replayed:0,retained:0});
  expect(modes).toEqual([false,false]);expect((await f.canonical.loadEntityRows('s','tasks',['a']))[0].row.n).toBe(10);
 }finally{f.sql.close();}
});

it('captures coverage after authority when an unsent prediction still needs replay',async()=>{
 const f=await fixture();try{
  await f.core.invoke({scope:'s',path:'increment',commandId:'coverage-replay',now:100,args:{id:'a'}});
  const modes:boolean[]=[];let published=false;
  const core=new SQLiteReducerCore({...f.options,async context(scope,includeCoverage=true){modes.push(includeCoverage);if(includeCoverage)expect(published).toBe(true);return {...await f.options.context(scope),...(!includeCoverage?{coverage:{}}:{})};}});
  expect(await core.rebase('s',async()=>{await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:10}}]},{entities:{},liveQueries:{}},'s');published=true;})).toMatchObject({replayed:1});
  expect(modes).toEqual([false,true]);expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(11);
 }finally{f.sql.close();}
});

it('clears acknowledged predictions without building coverage for an empty remaining journal',async()=>{
 const f=await fixture();try{
  const receipt=await f.core.invoke({scope:'s',path:'increment',commandId:'coverage-ack',now:100,args:{id:'a'}});
  const core=new SQLiteReducerCore({...f.options,async context(scope,includeCoverage=true){if(includeCoverage)throw Error('Acknowledged reducer requires no coverage');return {...await f.options.context(scope),coverage:{}};}});
  expect(await core.rebase('s',async()=>{await f.journal.acknowledge('s',[receipt.intentId]);await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:20}}]},{entities:{},liveQueries:{}},'s');})).toMatchObject({replayed:0,retained:0});
  expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(20);expect((await f.journal.summary('s')).count).toBe(0);
 }finally{f.sql.close();}
});

it('rolls authority back when identity changes even with no prediction coverage to capture',async()=>{
 const f=await fixture();try{
  let changed=false;const generation=await f.engine.verifyOwner();
  const core=new SQLiteReducerCore({...f.options,async context(scope){return {...await f.options.context(scope),coverage:{},intentScope:changed?'other-account':'s'};}});
  await expect(core.rebase('s',async()=>{await f.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:20}}]},{entities:{},liveQueries:{}},'s');changed=true;})).rejects.toMatchObject({code:'FENCED'});
  expect((await f.canonical.loadEntityRows('s','tasks',['a']))[0].row.n).toBe(1);expect(await f.engine.verifyOwner()).toEqual(generation);
 }finally{f.sql.close();}
});

it('keeps prepared commands queueable across an artifact or permission change without exposing stale predictions or replies',async()=>{
 const before=await fixture(),after=await fixture();try{
  const input={scope:'s',path:'increment',commandId:'updated-artifact',now:100,args:{id:'a'}};let prepared!:SQLitePreparedReducer;await before.core.invoke(input,async(_input,_context,value)=>{prepared=structuredClone(value);});
  const changed=new SQLiteReducerCore({...after.options,artifactHash:'new-artifact',async context(){return {intentScope:'s',identity:{...identity,member:{...identity.member,permissions:{tasks:false}}},coverage:{}};},runtime:{isPrepared:()=>false,prepare:async()=>{throw Error('old module unavailable');},execute:async()=>{throw Error('must not execute');}}});
  const receipt=await changed.recover(input,prepared);expect(receipt).toMatchObject({queuedOnly:true,result:null});expect((await after.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect((await after.journal.page('s')).entries[0]).toMatchObject({patches:[],localExecution:{artifactHash:'artifact'},sentAt:100});expect(await changed.rebase('s')).toMatchObject({retained:1,replayed:0});expect(await changed.invoke(input)).toEqual(receipt);
  const differentAccount=new SQLiteReducerCore({...after.options,async context(){return {intentScope:'s',identity:{...identity,auth:{account:{id:'another-account'}}},coverage:{}};}});await expect(differentAccount.recover(input,prepared)).rejects.toMatchObject({code:'FENCED'});expect((await after.journal.summary('s')).count).toBe(1);
 }finally{before.sql.close();after.sql.close();}
});
it('restores a migrated legacy intent in place with its stable IDs and unknown-send fence, without inventing an original local result',async()=>{
 const f=await fixture();try{
  const input={scope:'s',path:'increment',commandId:'legacy-command',now:100,args:{id:'a'}},legacy={id:77,scope:'s',path:input.path,args:input.args,idempotencyKey:input.commandId,entityKeys:[],patches:[{entity:'tasks',rowId:'a',op:'patch' as const,fields:{n:2}}],localExecution:{scope:'s',commandId:input.commandId,now:100,artifactHash:'artifact',identity},createdAt:101,attempts:2,nextAttemptAt:200,state:'inflight' as const,sentAt:150};
  const {id:_,...draft}=legacy,migrated=await f.journal.append({...draft,state:'pending'});const receipt=await f.core.recoverLegacy(input,legacy);expect(receipt).toMatchObject({queuedOnly:true,result:null,commandId:input.commandId});expect(f.calls).toBe(0);expect((await f.journal.page('s')).entries).toHaveLength(1);expect((await f.journal.page('s')).entries[0]).toMatchObject({id:migrated.id,idempotencyKey:input.commandId,sentAt:150,createdAt:101,state:'pending'});expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect(await f.core.recoverLegacy(input,legacy)).toEqual(receipt);expect(await f.core.invoke(input)).toEqual(receipt);expect(f.calls).toBe(0);
 }finally{f.sql.close();}
});

it('redacts an existing original reply after permission change while retaining it for its original admitted context',async()=>{
 const f=await fixture();try{
  const input={scope:'s',path:'increment',commandId:'permission-context',now:100,args:{id:'a'}};let prepared!:SQLitePreparedReducer;const original=await f.core.invoke(input,async(_input,_context,value)=>{prepared=structuredClone(value);});
  const changed=new SQLiteReducerCore({...f.options,async context(){return {intentScope:'s',identity:{...identity,member:{...identity.member,permissions:{tasks:false}}},coverage:{}};}});expect(await changed.invoke(input)).toMatchObject({queuedOnly:true,result:null});expect(await changed.recover(input,prepared)).toMatchObject({queuedOnly:true,result:null});await changed.rebase('s');expect((await f.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect((await f.journal.page('s')).entries[0]).toMatchObject({patches:[],sentAt:100});
  expect(await f.core.invoke(input)).toEqual(original);expect(JSON.parse((await f.db.getFirstAsync<{receipt:string}>('SELECT receipt FROM gv_reducer_invocations'))!.receipt)).toEqual(original);expect(f.calls).toBe(1);
 }finally{f.sql.close();}
});
it('recovers a prepared reply before a lost canonical base returns without blocking bootstrap or inventing a partial row',async()=>{
 const before=await fixture(),after=await fixture();try{const input={scope:'s',path:'increment',commandId:'missing-base',now:100,args:{id:'a'}};let prepared!:SQLitePreparedReducer;const receipt=await before.core.invoke(input,async(_input,_context,value)=>{prepared=structuredClone(value);});await after.canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'delete'}]},{entities:{},liveQueries:{}},'s');expect(await after.core.recover(input,prepared)).toEqual(receipt);expect(await after.predictions.entities('s','tasks',['a'])).toEqual([]);expect(await after.core.rebase('s')).toMatchObject({retained:1,unavailable:1});expect((await after.journal.page('s')).entries[0]).toMatchObject({sentAt:100,patches:[{entity:'tasks',rowId:'a',op:'patch',fields:{n:2}}]});await after.core.rebase('s',()=>after.canonical.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:{id:'a',n:2}}]},{entities:{},liveQueries:{}},'s').then(()=>{}));expect((await after.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect(after.calls).toBe(0);expect(await after.core.invoke(input)).toEqual(receipt);}finally{before.sql.close();after.sql.close();}
});
