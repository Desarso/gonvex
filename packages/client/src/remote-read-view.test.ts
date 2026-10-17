import { expect, it } from 'vitest';
import { withRemoteReadView, serveRemoteReadView, readViewMessageBytes, type RemoteReadLimits } from './remote-read-view';
import type { ReducerReadView } from '@gonvex/local-runtime/portable';
const limits:RemoteReadLimits={maxInputBytes:16384,maxResultBytes:16384,maxResultRows:2,maxViewBytes:32768,timeoutMs:1000};
function fixture(view:ReducerReadView,options=limits){
 const {port1,port2}=new MessageChannel(),events:string[]=[];
 const host=serveRemoteReadView(port2,async run=>{events.push('begin');try{await run(view);events.push('commit');}catch(error){events.push('rollback');throw error;}},options);
 return {port:port1,host,events};
}
it('keeps all selects in one transaction and acknowledges only after commit',async()=>{
 let revision=1;const captured=revision;
 const f=fixture({select:async()=>({rows:[{revision:captured}],complete:true})});
 const value=await withRemoteReadView(f.port,async view=>{
  const a=await view.select({table:'tasks'});revision=2;
  const b=await view.select({table:'tasks'});expect(f.events).toEqual(['begin']);return [a,b];
 },limits);
 await f.host;expect(value.map(v=>v.rows[0]!.revision)).toEqual([1,1]);expect(f.events).toEqual(['begin','commit']);
});
it('rolls back when the main callback fails and preserves its original error',async()=>{
 const f=fixture({select:async()=>({rows:[],complete:true})}),error=new Error('invalid draft');
 await expect(withRemoteReadView(f.port,async()=>{throw error;},limits)).rejects.toBe(error);
 await f.host;expect(f.events).toEqual(['begin','rollback']);
});
it('preserves a typed incomplete-read failure across the channel',async()=>{
 const f=fixture({select:async()=>{throw Object.assign(new Error('not covered'),{name:'IncompleteReplicaError',code:'incomplete',read:{table:'tasks',where:{column:'id',op:'eq',value:'missing'}}});}});
 await expect(withRemoteReadView(f.port,view=>view.select({table:'tasks'}),limits)).rejects.toMatchObject({name:'IncompleteReplicaError',message:'not covered',code:'incomplete',read:{table:'tasks',where:{column:'id',op:'eq',value:'missing'}}});
 await f.host;expect(f.events).toContain('rollback');
});
it('detects oversized result sets using one extra row instead of asking for an entire table',async()=>{
 let requested:number|undefined;
 const f=fixture({select:async read=>{requested=read.limit;return {rows:Array.from({length:read.limit!},(_,id)=>({id})),complete:true};}});
 await expect(withRemoteReadView(f.port,view=>view.select({table:'tasks'}),limits)).rejects.toThrow('row budget');
 await f.host;expect(requested).toBe(3);expect(f.events).toContain('rollback');
});
it('rejects an oversized request before posting it and releases the transaction',async()=>{
 const f=fixture({select:async()=>{throw Error('must not reach backend');}});
 await expect(withRemoteReadView(f.port,view=>view.select({table:'x'.repeat(20000)}),limits)).rejects.toThrow('budget');
 await f.host;expect(f.events).toContain('rollback');
});
it('times out an abandoned callback and closes both ports',async()=>{
 const options={...limits,timeoutMs:30},f=fixture({select:async()=>({rows:[],complete:true})},options);
 await expect(withRemoteReadView(f.port,async()=>new Promise(()=>{}),options)).rejects.toThrow('timed out');
 await f.host;expect(f.events).toContain('rollback');
});
it('does not let a caller use a captured view after its lease closes',async()=>{
 let captured:ReducerReadView|undefined;const f=fixture({select:async()=>({rows:[],complete:true})});
 await withRemoteReadView(f.port,async view=>{captured=view;return 1;},limits);await f.host;
 await expect(captured!.select({table:'tasks'})).rejects.toThrow('closed');
});
it('bounds traversal of large arrays and rejects cycles without stringifying them',()=>{
 expect(()=>readViewMessageBytes(Array.from({length:10000},()=>1),100)).toThrow('budget');
 const cycle:any={};cycle.self=cycle;expect(()=>readViewMessageBytes(cycle,10000)).toThrow('Cyclic');
 const shared={v:1};expect(readViewMessageBytes([shared,shared],10000)).toBeGreaterThan(0);
});
it('does not convert an invalid infinite limit into a successful bounded read',async()=>{
 const f=fixture({select:async()=>{throw Error('must validate first');}});
 await expect(withRemoteReadView(f.port,view=>view.select({table:'tasks',limit:Infinity}),limits)).rejects.toThrow('Invalid');await f.host;
});
it('propagates cancellation to an active interruptible backend read',async()=>{
 const {port1,port2}=new MessageChannel(),controller=new AbortController(),reason=new Error('scope retired');let rolledBack=false;
 const host=serveRemoteReadView(port2,async(run,signal)=>{
  try{await run({select:async()=>new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true}))});}
  catch{rolledBack=true;throw Error('rolled back');}
 },limits);
 const result=withRemoteReadView(port1,view=>view.select({table:'tasks'}),limits,controller.signal);
 setTimeout(()=>controller.abort(reason),10);
 await expect(result).rejects.toBe(reason);await host;expect(rolledBack).toBe(true);
});

it('rejects opaque buffers and accessors before posting them',()=>{
 for(const value of [new ArrayBuffer(100000),new Date(),new Map(),Object.create({custom:true})])
  expect(()=>readViewMessageBytes(value,10000)).toThrow('Non-data object');
 let invoked=false;
 const value={get payload(){invoked=true;return 'x';}};
 expect(()=>readViewMessageBytes(value,10000)).toThrow('Accessor');expect(invoked).toBe(false);
 const array:unknown[]=[];Object.defineProperty(array,'0',{get(){invoked=true;return 'x';}});
 expect(()=>readViewMessageBytes(array,10000)).toThrow('Accessor');expect(invoked).toBe(false);
});
it('awaits the worker rollback acknowledgement after caller cancellation',async()=>{
 const {port1,port2}=new MessageChannel(),controller=new AbortController();
 let finishRollback!:()=>void,rollbackStarted!:()=>void;
 const started=new Promise<void>(resolve=>{rollbackStarted=resolve;});
 const rollback=new Promise<void>(resolve=>{finishRollback=resolve;});
 const host=serveRemoteReadView(port2,async run=>{
  try{await run({select:async()=>({rows:[],complete:true})});}
  catch(error){rollbackStarted();await rollback;throw error;}
 },limits);
 let settled=false;
 const result=withRemoteReadView(port1,async()=>{controller.abort(Error('retired'));await new Promise(()=>{});},limits,controller.signal);
 void result.then(()=>{settled=true;},()=>{settled=true;});
 await started;await new Promise(resolve=>setTimeout(resolve,10));expect(settled).toBe(false);
 finishRollback();await expect(result).rejects.toThrow('retired');await host;
});
