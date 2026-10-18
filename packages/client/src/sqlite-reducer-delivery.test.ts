import {DatabaseSync} from 'node:sqlite';
import {afterEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import {reducer,schema,getRow} from '@gonvex/module-sdk';
import {GonvexClientError} from './index.js';
import {SQLiteReducerDelivery} from './sqlite-reducer-delivery.js';
import {SQLiteReducerOwner} from './sqlite-reducer-owner.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
import {SQLitePendingSnapshotReader} from './sqlite-pending-snapshot-reader.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
const cleanup:Array<()=>void>=[];afterEach(()=>{vi.restoreAllMocks();for(const close of cleanup.splice(0))close();});
const typed={tasks:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[]}} as const;
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
const session={directive:{protocolVersion:1 as const,scope:'tenant',visibilityScope:'s',epoch:'e'},identity,artifactHash:'artifact',lastOnlineAtMs:100};
async function fixture(){
 const sql=new DatabaseSync(':memory:'),db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,typed);await engine.open('delivery');const canonical=sqliteTypedReplicaStorage(db,typed);await canonical.ready();await canonical.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:{id:'a',n:1}}]},{entities:{},liveQueries:{}},'s');
 let lane=Promise.resolve();const runOwner=<T>(_bytes:number,run:()=>Promise<T>)=>{const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;};
 const fence=()=>engine.verifyOwner(),snapshots=new SQLiteSnapshotPages(db,fence,typed),reader=new SQLiteNativeReplicaReader(db,canonical,engine),pendingReader=new SQLitePendingSnapshotReader({database:db,canonical,snapshots,schema:typed,fence});
 const owner={database:db,engine,canonical,reader,pendingReader,snapshots,runOwner} as SQLiteOwnerProgramContext;let generation='one',now=1000,calls=0;
 const runtime=new PortableReducerRuntime({schema:{tasks:{key:'id',columns:{id:{type:'text',nullable:false},n:{type:'bigint',nullable:false}}}},artifactHash:'artifact',reducers:{increment:reducer({args:schema.any(),result:schema.any(),run:async ctx=>{calls++;const row=await getRow<{id:string;n:number}>(ctx.db,'tasks','id','a');await ctx.db.update('tasks','a',{n:row!.n+1});return row!.n+1;}})}});
 const endpoint=new SQLiteReducerOwner(owner,{runtime,artifactHash:'artifact',resolveTab:()=>({scope:'s',generation}),async context(){return {intentScope:'identity',identity,coverage:{tasks:{key:'id',complete:true}}};}}),safety=new SQLiteSafetyJournal('delivery',new IDBFactory(),IDBKeyRange),drivers:SQLiteReducerDelivery[]=[];
 const options={owner,reducer:endpoint,safety,admission:()=>({scope:'s',intentScope:'identity',generation,online:true}),now:()=>now};
 const delivery=(send:ConstructorParameters<typeof SQLiteReducerDelivery>[0]['send'],extra:Partial<ConstructorParameters<typeof SQLiteReducerDelivery>[0]>={})=>{const driver=new SQLiteReducerDelivery({...options,send,...extra});drivers.push(driver);return driver;};
 const input={scope:'s',path:'increment',args:{},commandId:'command',now:100};
 const invoke=async()=>{await safety.append(input,session,'identity');return endpoint.invoke({path:input.path,args:input.args,now:input.now},{tabID:'tab',requestID:input.commandId,signal:new AbortController().signal});};
 const authoritative=()=>runOwner(128,()=>canonical.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:2}}]},{entities:{},liveQueries:{}},'s'));
 cleanup.push(()=>{for(const driver of drivers)driver.close();safety.close();sql.close();});return {sql,db,engine,canonical,endpoint,safety,delivery,invoke,authoritative,runOwner,get calls(){return calls;},change(){generation='two';},advance(){now+=10000;}};
}
it('claims before network delivery, leaves SQL available, deduplicates drains and retires safety only after atomic settlement',async()=>{
 const f=await fixture(),receipt=await f.invoke();let release!:()=>void,entered!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 const send=vi.fn(async entry=>{expect(entry.idempotencyKey).toBe('command');expect(entry.state).toBe('inflight');expect(f.db.ownerTransactionDepth).toBe(0);entered();await barrier;await f.authoritative();return 2;});const driver=f.delivery(send),pending=driver.drainOnce();expect(driver.drainOnce()).toBe(pending);await waiting;
 expect(await f.runOwner(0,()=>f.engine.integrity())).toBe('ok');expect(await f.safety.count('identity')).toBe(1);release();expect(await pending).toEqual({sent:1,nextDelay:null});expect(send).toHaveBeenCalledOnce();expect((await f.endpoint.journal.summary('identity')).count).toBe(0);expect(await f.safety.count('identity')).toBe(0);expect((await f.endpoint.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect(f.calls).toBe(1);expect(receipt.result).toBe(2);expect(driver.stats().bytes).toBeLessThanOrEqual(driver.stats().maxBytes);
});
it('keeps a server-accepted unknown outcome after late SQLite rollback and retries its immutable command without double prediction',async()=>{
 const f=await fixture();await f.invoke();const commands:string[]=[];const send=async(entry:any)=>{commands.push(entry.idempotencyKey);await f.authoritative();return 2;};const driver=f.delivery(send);
 await f.db.execAsync("CREATE TRIGGER fail_settle BEFORE DELETE ON gv_reducer_journal BEGIN SELECT RAISE(ABORT,'late SQL failure');END;");await expect(driver.drainOnce()).rejects.toThrow('late SQL failure');expect(await f.safety.count('identity')).toBe(1);expect((await f.endpoint.journal.page('identity')).entries[0].state).toBe('inflight');expect(await f.db.getFirstAsync('SELECT 1 FROM gv_reducer_delivery_settlements')).toBeNull();
 await f.db.execAsync('DROP TRIGGER fail_settle');expect(await driver.drainOnce()).toEqual({sent:1,nextDelay:null});expect(commands).toEqual(['command','command']);expect(f.calls).toBe(1);expect((await f.endpoint.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);expect(await f.safety.count('identity')).toBe(0);
});
it('resumes independent safety retirement after SQL settlement without sending or executing the accepted intent again',async()=>{
 const f=await fixture();await f.invoke();const settle=vi.spyOn(f.safety,'settle').mockRejectedValueOnce(Error('IDB quota surrogate'));const send=vi.fn(async()=>{await f.authoritative();return 2;});await expect(f.delivery(send).drainOnce()).rejects.toThrow('IDB quota surrogate');expect((await f.endpoint.journal.summary('identity')).count).toBe(0);expect(await f.safety.count('identity')).toBe(1);expect((await f.db.getFirstAsync<{safety_settled:number}>('SELECT safety_settled FROM gv_reducer_delivery_settlements'))?.safety_settled).toBe(0);
 expect(await f.delivery(send).drainOnce()).toEqual({sent:0,nextDelay:null});expect(send).toHaveBeenCalledOnce();expect(settle).toHaveBeenCalledTimes(2);expect(await f.safety.count('identity')).toBe(0);expect(f.calls).toBe(1);
});
it('retains unknown network outcomes without spending retries and removes only authoritative rejection predictions',async()=>{
 const f=await fixture();await f.invoke();const offline=f.delivery(async()=>{throw new GonvexClientError('connection lost',{code:'disconnected'});});expect(await offline.drainOnce()).toEqual({sent:1,nextDelay:1000});expect((await f.endpoint.journal.page('identity')).entries[0]).toMatchObject({state:'pending',attempts:0,sentAt:1000,lastErrorClass:'network'});expect(await f.safety.count('identity')).toBe(1);expect((await f.endpoint.predictions.entities('s','tasks',['a']))[0].row.n).toBe(2);f.advance();const rejected=f.delivery(async()=>{throw new GonvexClientError('permission denied',{code:'server',errorClass:'rejected'});});expect(await rejected.drainOnce()).toEqual({sent:1,nextDelay:null});expect((await f.endpoint.journal.page('identity')).entries[0]).toMatchObject({state:'rejected',lastError:'permission denied'});expect((await f.endpoint.predictions.entities('s','tasks',['a']))[0].row.n).toBe(1);expect(await f.safety.count('identity')).toBe(0);
});
it('parks counted failures with intent safety intact and blocks an update-required driver',async()=>{
 for(const classification of ['transient','update_required'] as const){const f=await fixture();await f.invoke();const updateRequired=vi.fn(),send=vi.fn(async()=>{throw new GonvexClientError('server response',{code:'server',errorClass:classification});}),driver=f.delivery(send,{maxAttempts:1,updateRequired});expect(await driver.drainOnce()).toEqual({sent:1,nextDelay:null});expect(await f.safety.count('identity')).toBe(1);expect((await f.endpoint.journal.page('identity')).entries[0]).toMatchObject({state:classification==='transient'?'failed':'pending',attempts:classification==='transient'?1:0});expect(await driver.drainOnce()).toEqual({sent:0,nextDelay:null});expect(send).toHaveBeenCalledOnce();expect(updateRequired).toHaveBeenCalledTimes(classification==='update_required'?1:0);}
});
it('refuses stale identity settlement after a network wait, preserving the unknown command for recovery',async()=>{
 const f=await fixture();await f.invoke();const driver=f.delivery(async()=>{f.change();return 2;});await expect(driver.drainOnce()).rejects.toMatchObject({code:'FENCED'});expect((await f.endpoint.journal.page('identity')).entries[0].state).toBe('inflight');expect(await f.safety.count('identity')).toBe(1);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_reducer_delivery_settlements')).toBeNull();
});
