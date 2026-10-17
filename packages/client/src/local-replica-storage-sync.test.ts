import { expect, it } from 'vitest';
import { LocalReplica, type LocalReplicaStorage } from './local-replica';
function fixture(blockFirst=false){
 let reads=0,revision=1,release!:()=>void,entered!:()=>void;
 const gate=new Promise<void>(resolve=>{release=resolve;}),started=new Promise<void>(resolve=>{entered=resolve;});
 const storage:LocalReplicaStorage={load:async()=>undefined,applyTransaction:async()=>{},readChanges:async()=>{
  const current=revision;reads++;if(reads===1&&blockFirst){entered();await gate;}
  return {sequence:current,entities:{tasks:{t:{_id:'t',name:'Version '+current}}},windows:{}};
 }};
 const replica=new LocalReplica(storage);
 return {replica,started,release:()=>release(),advance:()=>{revision++;},reads:()=>reads};
}
it('coalesces a burst of pending storage synchronizations into one database read',async()=>{
 const f=fixture();try{
  await f.replica.activateScope('s');const operations=Array.from({length:1000},()=>f.replica.synchronizeStorage());
  await Promise.all(operations);expect(f.reads()).toBe(1);expect(f.replica.entity('tasks','t')?.name).toBe('Version 1');
 }finally{f.replica.dispose();}
});
it('rechecks once for a burst arriving during an active snapshot instead of resolving with stale data',async()=>{
 const f=fixture(true);try{
  await f.replica.activateScope('s');const first=f.replica.synchronizeStorage();await f.started;
  f.advance();const later=Array.from({length:100},()=>f.replica.synchronizeStorage());f.release();
  await Promise.all([first,...later]);expect(f.reads()).toBe(2);expect(f.replica.entity('tasks','t')?.name).toBe('Version 2');
 }finally{f.replica.dispose();}
});
it('allows a new synchronization after the prior shared operation completes',async()=>{
 const f=fixture();try{
  await f.replica.activateScope('s');await f.replica.synchronizeStorage();f.advance();await f.replica.synchronizeStorage();
  expect(f.reads()).toBe(2);expect(f.replica.entity('tasks','t')?.name).toBe('Version 2');
 }finally{f.replica.dispose();}
});
it('does not publish a retired scope snapshot after an auth-scope change',async()=>{
 const f=fixture(true);try{
  await f.replica.activateScope('old');const first=f.replica.synchronizeStorage();await f.started;
  const activation=f.replica.activateScope('new');f.release();await Promise.all([first,activation]);
  expect(f.replica.entity('tasks','t')).toBeUndefined();
 }finally{f.replica.dispose();}
});
it('disposal releases a shared synchronization without publishing its completed read',async()=>{
 const f=fixture(true);await f.replica.activateScope('s');const first=f.replica.synchronizeStorage();await f.started;
 const later=Array.from({length:100},()=>f.replica.synchronizeStorage());
 const outcomes=Promise.allSettled([first,...later]);f.replica.dispose();f.release();await outcomes;
 expect(f.replica.cachedEntityRows('tasks')).toEqual([]);expect(f.reads()).toBe(1);
});
it('releases a failed shared read so a later synchronization can recover',async()=>{
 let failed=true,reads=0;const error=Error('temporary storage failure');
 const replica=new LocalReplica({load:async()=>undefined,applyTransaction:async()=>{},readChanges:async()=>{
  reads++;if(failed)throw error;return {sequence:1,entities:{tasks:{t:{_id:'t',name:'Recovered'}}},windows:{}};
 }});
 try{
  await replica.activateScope('s');const results=await Promise.allSettled(Array.from({length:100},()=>replica.synchronizeStorage()));
  expect(reads).toBe(1);expect(results.every(result=>result.status==='rejected'&&result.reason===error)).toBe(true);
  failed=false;await replica.synchronizeStorage();expect(reads).toBe(2);expect(replica.entity('tasks','t')?.name).toBe('Recovered');
 }finally{replica.dispose();}
});
it('updates the resident allowance between coalesced passes',async()=>{
 let release!:()=>void,entered!:()=>void,reads=0;const allowances:number[]=[];
 const gate=new Promise<void>(resolve=>{release=resolve;}),started=new Promise<void>(resolve=>{entered=resolve;});
 const replica=new LocalReplica({load:async()=>undefined,loadWorkingSet:async()=>undefined,loadWindowRows:async()=>undefined,
  withReadView:async(_scope,_coverage,run)=>run({select:async()=>({rows:[],complete:false})}),applyTransaction:async()=>{},
  readChanges:async(_scope,_after,interest)=>{
   allowances.push(interest!.availableRows);reads++;if(reads===1){entered();await gate;}
   return {sequence:reads,entities:reads===1?{tasks:{a:{_id:'a'}}}:{},windows:{}};
  }},{maxResidentRows:1,maxResidentBytes:100000});
 try{
  await replica.activateScope('s');const first=replica.synchronizeStorage();await started;
  const second=replica.synchronizeStorage();release();await Promise.all([first,second]);
  expect(allowances).toEqual([1,0]);expect(replica.cachedEntityRows('tasks')).toHaveLength(1);
 }finally{replica.dispose();}
});
it('lets an already queued write run before the follow-up synchronization',async()=>{
 let release!:()=>void,entered!:()=>void,reads=0,revision=1;const events:string[]=[];
 const gate=new Promise<void>(resolve=>{release=resolve;}),started=new Promise<void>(resolve=>{entered=resolve;});
 const replica=new LocalReplica({load:async()=>undefined,applyTransaction:async transaction=>{events.push('write');revision=transaction.cursor.revision;},readChanges:async()=>{
  const current=revision;events.push('read');reads++;if(reads===1){entered();await gate;}
  return {sequence:current,entities:{tasks:{t:{_id:'t',name:'Version '+current}}},windows:{}};
 }});
 try{
  await replica.activateScope('s');const first=replica.synchronizeStorage();await started;
  const write=replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',name:'Version 2'}}]});
  const later=replica.synchronizeStorage();release();await Promise.all([first,write,later]);
  expect(events.slice(0,3)).toEqual(['read','write','read']);expect(replica.entity('tasks','t')?.name).toBe('Version 2');
 }finally{replica.dispose();}
});
