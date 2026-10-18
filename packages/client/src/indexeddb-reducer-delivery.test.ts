import {afterEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import {reducer,schema,getRow} from '@gonvex/module-sdk';
import {IndexedDBReplicaEngine} from './indexeddb-replica-engine.js';
import {IndexedDBReducerCore} from './indexeddb-reducer-core.js';
import {IndexedDBReducerDelivery} from './indexeddb-reducer-delivery.js';
import {indexedDBEffectiveIntent} from './indexeddb-intent-ledger.js';
import {GonvexClientError} from './client-error.js';
const cleanup:Array<()=>void>=[];
afterEach(()=>{for(const close of cleanup.splice(0))close();vi.restoreAllMocks();});
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
async function fixture(){
 const engine=new IndexedDBReplicaEngine({tasks:{key:'id',fields:{id:{},n:{}}}},'delivery',new IDBFactory(),IDBKeyRange);await engine.claim('owner');await engine.applyCanonical('visible',{epoch:'e',revision:1},['a','b','c'].map(id=>({entity:'tasks',id,row:{id,n:1}})));
 let now=1000,generation='one',calls=0,lane=Promise.resolve();
 const runOwner=<T>(_bytes:number,run:()=>Promise<T>)=>{const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;};
 const runtime=new PortableReducerRuntime({artifactHash:'artifact',schema:{tasks:{key:'id',columns:{id:{type:'text',nullable:false},n:{type:'bigint',nullable:false}}}},reducers:{increment:reducer({args:schema.any(),result:schema.any(),run:async(ctx,args:any)=>{calls++;const row=await getRow<{id:string;n:number}>(ctx.db,'tasks','id',args.id);await ctx.db.update('tasks',args.id,{n:row!.n+1});return row!.n+1;}})}});
 const core=new IndexedDBReducerCore({engine,runtime,artifactHash:'artifact',context:async()=>({identity,intentScope:'identity',generation,coverage:{tasks:{key:'id',complete:true}}}),assertCurrent:(_scope,expected)=>{if(expected!==generation)throw Object.assign(Error('admission changed'),{code:'FENCED'});}});
 const settled=vi.fn(async()=>{}),drivers:IndexedDBReducerDelivery[]=[];
 const driver=(send:ConstructorParameters<typeof IndexedDBReducerDelivery>[0]['send'],extra:Partial<ConstructorParameters<typeof IndexedDBReducerDelivery>[0]>={})=>{const result=new IndexedDBReducerDelivery({engine,core,safety:{settle:settled},runOwner,admission:()=>({scope:'visible',intentScope:'identity',generation,online:true}),now:()=>now,send,...extra});drivers.push(result);return result;};
 cleanup.push(()=>{drivers.forEach(driver=>driver.close());engine.close();});
 return {engine,core,driver,settled,runOwner,invoke:(commandId='command',id='a')=>runOwner(128,()=>core.invoke({scope:'visible',path:'increment',args:{id},commandId,now:100})),advance(){now+=10000;},change(){generation='two';},get calls(){return calls;},async records(){return (await engine.deliveryPage('identity')).records.map(indexedDBEffectiveIntent);}};
}
it('commits the first-send fence before network work and allows owner work while the request waits',async()=>{
 const f=await fixture();await f.invoke();let release!:()=>void,entered!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 const driver=f.driver(async entry=>{expect(entry).toMatchObject({state:'inflight',sentAt:1000,idempotencyKey:'command'});entered();await barrier;await f.runOwner(128,()=>f.engine.applyCanonical('visible',{epoch:'e',revision:2},[{entity:'tasks',id:'a',row:{id:'a',n:2}}]));return 2;});
 const pending=driver.drainOnce();expect(driver.drainOnce()).toBe(pending);await waiting;expect((await f.runOwner(128,()=>f.engine.generation())).epoch).toBe(1);expect((await f.records())[0]?.state).toBe('inflight');release();expect((await pending).sent).toBe(1);expect(await f.records()).toEqual([]);expect(f.calls).toBe(1);expect(f.settled).toHaveBeenCalledWith('identity','command',{status:'accepted'});expect((await f.engine.entities('visible','tasks',['a'])).rows[0]?.value?.n).toBe(2);expect(driver.stats().bytes).toBeLessThan(driver.stats().maxBytes);
});
it('sends independent intents concurrently but waits for every earlier conflicting intent',async()=>{
 const f=await fixture();await f.invoke('a1');await f.invoke('a2');await f.invoke('b1','b');await f.invoke('c1','c');const commands:string[]=[];
 const driver=f.driver(async entry=>{commands.push(entry.idempotencyKey);return null;});expect((await driver.drainOnce()).sent).toBe(3);expect(commands).toEqual(['a1','b1','c1']);expect((await f.records()).map(entry=>entry.idempotencyKey)).toEqual(['a2']);expect((await driver.drainOnce()).sent).toBe(1);expect(commands).toEqual(['a1','b1','c1','a2']);
});
it('retries an unknown outcome with the same command without spending attempts or executing its body again',async()=>{
 const f=await fixture();const receipt=await f.invoke();const commands:string[]=[];const offline=f.driver(async entry=>{commands.push(entry.idempotencyKey);throw new GonvexClientError('socket lost',{code:'disconnected'});});await offline.drainOnce();expect((await f.records())[0]).toMatchObject({state:'pending',attempts:0,sentAt:1000,nextAttemptAt:2000});expect(f.calls).toBe(1);f.advance();await f.driver(async entry=>{commands.push(entry.idempotencyKey);return null;}).drainOnce();expect(commands).toEqual(['command','command']);expect(f.calls).toBe(1);expect(await f.invoke()).toEqual(receipt);
});
it('recovers independent safety retirement after settlement without delivering the intent again',async()=>{
 const f=await fixture();await f.invoke();f.settled.mockRejectedValueOnce(Error('safety quota'));const send=vi.fn(async()=>null);await expect(f.driver(send).drainOnce()).rejects.toThrow('safety quota');expect(await f.records()).toEqual([]);expect(await f.engine.safetySettlementPage('identity')).toHaveLength(1);await f.driver(send).drainOnce();expect(send).toHaveBeenCalledOnce();expect(await f.engine.safetySettlementPage('identity')).toEqual([]);expect(f.calls).toBe(1);
});
it('parks counted failures, retires authoritative rejections and blocks update-required delivery',async()=>{
 for(const classification of ['transient','rejected','update_required'] as const){const f=await fixture();await f.invoke();const updateRequired=vi.fn(),rejected=vi.fn(),send=vi.fn(async()=>{throw new GonvexClientError(classification,{code:'server',errorClass:classification});}),driver=f.driver(send,{maxAttempts:1,updateRequired,rejected});await driver.drainOnce();const entry=(await f.records())[0]!;expect(entry.state).toBe(classification==='transient'?'failed':classification==='rejected'?'rejected':'pending');expect(f.settled).toHaveBeenCalledTimes(Number(classification==='rejected'));expect(rejected).toHaveBeenCalledTimes(Number(classification==='rejected'));if(classification==='update_required'){expect(updateRequired).toHaveBeenCalledOnce();await driver.drainOnce();expect(send).toHaveBeenCalledOnce();}}
});
it('leaves the claimed unknown outcome durable when admission changes during delivery',async()=>{
 const f=await fixture();await f.invoke();await expect(f.driver(async()=>{f.change();return null;}).drainOnce()).rejects.toMatchObject({code:'FENCED'});expect((await f.records())[0]).toMatchObject({state:'inflight',sentAt:1000});expect(f.settled).not.toHaveBeenCalled();const send=vi.fn(async()=>null);await f.driver(send).drainOnce();expect(send).toHaveBeenCalledOnce();expect(f.calls).toBe(1);
});
