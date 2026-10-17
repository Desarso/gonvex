import { afterEach, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteLocalReplicaStorage, type SQLiteReplicaDatabase } from './sqlite-replica-storage';
import { remoteSQLiteStorage, serveSQLiteStorage, type RemoteStorageLimits } from './remote-sqlite-storage';
const limits:RemoteStorageLimits={maxInputBytes:16384,maxQueuedBytes:32768,maxOperations:4,maxResultBytes:32768,timeoutMs:1000,
 readView:{maxInputBytes:16384,maxResultBytes:16384,maxResultRows:2,maxViewBytes:32768,timeoutMs:500}};
const cleanup:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const close of cleanup.splice(0))await close();});
function fixture(options=limits,hostOptions=options){
 const native=new DatabaseSync(':memory:'),events:string[]=[];
 const db:SQLiteReplicaDatabase={
  async execAsync(sql){native.exec(sql);},
  async runAsync(sql,...args){return native.prepare(sql).run(...args as any[]);},
  async getAllAsync<T>(sql,...args){return native.prepare(sql).all(...args as any[]) as T[];},
  async getFirstAsync<T>(sql,...args){return (native.prepare(sql).get(...args as any[]) as T)??null;},
  async withTransactionAsync(run){native.exec('BEGIN');events.push('begin');try{await run();native.exec('COMMIT');events.push('commit');}catch(error){native.exec('ROLLBACK');events.push('rollback');throw error;}}
 };
 const local=new SQLiteLocalReplicaStorage(db),channel=new MessageChannel();
 const host=serveSQLiteStorage(channel.port2,local,hostOptions),remote=remoteSQLiteStorage(channel.port1,options);
 cleanup.push(async()=>{remote.dispose();await host.dispose();native.close();});
 return {local,remote,events};
}
const empty={entities:{},liveQueries:{}};
it('serializes a write behind an entire coherent remote read lease',async()=>{
 const {remote,events}=fixture();
 await remote.replaceSnapshot!({entities:{tasks:{t1:{_id:'t1',name:'before'}}},liveQueries:{}},'s');
 let update:Promise<unknown>|undefined;
 const names=await remote.withReadView!('s',{tasks:{key:'_id',complete:true}},async view=>{
  const first=await view.select({table:'tasks',where:{column:'_id',op:'eq',value:'t1'}});
  update=remote.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'t1',operation:'update',newValue:{_id:'t1',name:'after'}}]},empty,'s');
  const second=await view.select({table:'tasks',where:{column:'_id',op:'eq',value:'t1'}});
  return [first.rows[0]?.name,second.rows[0]?.name];
 });
 expect(names).toEqual(['before','before']);await update;
 expect((await remote.loadEntityRows!('s','tasks',['t1']))[0]?.row.name).toBe('after');
 expect(events.filter(x=>x==='begin').length).toBe(events.filter(x=>x==='commit').length);
});
it('does not clone the whole resident snapshot during a normalized transaction or window delta',async()=>{
 const {remote}=fixture();const huge={entities:{unused:{u:{note:'x'.repeat(100000)}}},liveQueries:{}};
 await remote.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'t1',operation:'insert',newValue:{_id:'t1',name:'kept'}}]},huge,'s');
 const window={signature:'w',entity:'tasks',key:'_id',ids:['t1'],cursor:{epoch:'e',revision:2}};
 await remote.applyWindowDelta!(window,{upserts:[{_id:'t1',name:'delta'}],deleted:[]},huge,'s');
 expect((await remote.loadEntityRows!('s','tasks',['t1']))[0]?.row.name).toBe('delta');
 expect(await remote.loadEntityRows!('s','unused',['u'])).toEqual([]);
});
it('keeps bounded warm rows partial and cold rows available across the channel',async()=>{
 const {remote}=fixture();await remote.replaceSnapshot!({entities:{tasks:{t1:{_id:'t1'},t2:{_id:'t2'}}},liveQueries:{}},'s');
 const warm=await remote.loadWorkingSet!('s',{maxRows:1,maxBytes:1000});
 expect(Object.keys(warm!.entities.tasks!)).toHaveLength(1);expect(warm!.residentPartialTables).toContain('tasks');
 expect((await remote.loadEntityRows!('s','tasks',['t2']))[0]?.row._id).toBe('t2');
 expect(await remote.loadEntityRows!('other','tasks',['t2'])).toEqual([]);
});
it('rejects oversized requests before invoking storage and can still execute a small request',async()=>{
 const {remote}=fixture();
 await expect(remote.replaceSnapshot!({entities:{tasks:{t:{name:'x'.repeat(20000)}}},liveQueries:{}},'s')).rejects.toThrow('budget');
 expect(await remote.loadEntityRows!('s','tasks',['t'])).toEqual([]);
});
it('rejects a large result without silently truncating and releases the actor',async()=>{
 const {local,remote}=fixture();await local.replaceSnapshot({entities:{tasks:{t:{_id:'t',name:'x'.repeat(20000)}}},liveQueries:{}},'s');
 await expect(remote.loadEntityRows!('s','tasks',['t'])).rejects.toThrow('budget');
 expect(await remote.loadEntityRows!('other','tasks',['t'])).toEqual([]);
});
it('bounds admitted operations while a read callback holds the actor',async()=>{
 const {remote}=fixture({...limits,maxOperations:2});
 let release!:()=>void,started!:()=>void;
 const gate=new Promise<void>(resolve=>{release=resolve;}),ready=new Promise<void>(resolve=>{started=resolve;});
 const view=remote.withReadView!('s',{},async()=>{started();await gate;});await ready;
 const first=remote.loadEntityRows!('s','tasks',['a']);
 await expect(remote.loadEntityRows!('s','tasks',['b'])).rejects.toThrow('queue budget');
 release();await view;expect(await first).toEqual([]);
});
it('rolls back a failed callback and then allows another durable write',async()=>{
 const {remote,events}=fixture();await remote.loadEntityRows!('s','tasks',[]);
 await expect(remote.withReadView!('s',{},async()=>{throw Error('invalid draft');})).rejects.toThrow('invalid draft');
 expect(events).toContain('rollback');
 await remote.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'t',operation:'insert',newValue:{_id:'t'}}]},empty,'s');
 expect((await remote.loadEntityRows!('s','tasks',['t']))[0]?.row._id).toBe('t');
});
it('disposal aborts a waiting callback, drains cleanup and rejects future calls',async()=>{
 const {remote}=fixture();let entered!:()=>void;const ready=new Promise<void>(resolve=>{entered=resolve;});
 const result=remote.withReadView!('s',{},async()=>{entered();return new Promise(()=>{});});void result.catch(()=>{});
 await ready;remote.dispose();await expect(result).rejects.toThrow('closed');
 await expect(remote.load('s')).rejects.toThrow('closed');
});

it('enforces actor-side queued bytes even if a caller uses looser limits',async()=>{
 const {remote}=fixture(limits,{...limits,maxQueuedBytes:2200});
 let release!:()=>void,entered!:()=>void;
 const ready=new Promise<void>(resolve=>{entered=resolve;}),gate=new Promise<void>(resolve=>{release=resolve;});
 const view=remote.withReadView!('s',{},async()=>{entered();await gate;});await ready;
 const first=remote.loadEntityRows!('s','tasks',['a'.repeat(700)]);
 await expect(remote.loadEntityRows!('s','tasks',['b'.repeat(700)])).rejects.toThrow('actor queue budget');
 release();await view;expect(await first).toEqual([]);
});

it('forwards committed authority and bounded peer reconciliation through the actor',async()=>{
 const {remote}=fixture();
 const receipt=await remote.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'tasks',id:'t',operation:'insert',newValue:{_id:'t',name:'Fresh'}}]},empty,'s');
 expect(receipt).toMatchObject({scope:'s',previousSequence:0,sequence:1,changes:{entities:{tasks:{t:{name:'Fresh'}}}}});
 const stale=await remote.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',name:'Old'}}]},empty,'s');
 expect(stale).toMatchObject({previousSequence:1,sequence:2,changes:{entities:{tasks:{t:{name:'Fresh'}}}}});
 const changes=await remote.readChanges!('s',0,{rows:{tasks:['t']},windows:[],availableRows:0,availableBytes:0,maxRows:1,maxBytes:10000});
 expect(changes.entities.tasks!.t!.name).toBe('Fresh');expect(changes.sequence).toBe(2);
});
