import {expect,it,vi} from 'vitest';
import {SQLiteOwnerWorkQueue} from './sqlite-owner-work-queue.js';

it('batches adjacent reads without overtaking the write which supplies their state',async()=>{
 const events:string[]=[],fence=vi.fn(async()=>{}),snapshot=vi.fn(async(run:()=>Promise<void>)=>{events.push('begin');await run();events.push('commit');});
 const queue=new SQLiteOwnerWorkQueue(fence,snapshot);
 const write=queue.submit(1,async()=>{events.push('ack');});
 const reads=Array.from({length:10},(_,i)=>queue.submit(1,async()=>{events.push('read'+i);return i;},true));
 await Promise.all([write,...reads]);
 expect(events).toEqual(['ack','begin',...Array.from({length:8},(_,i)=>'read'+i),'commit','begin','read8','read9','commit']);
 expect(snapshot).toHaveBeenCalledTimes(2);expect(queue.stats()).toMatchObject({readOperations:10,readSnapshots:2,writeOperations:1});expect(await Promise.all(reads)).toEqual([0,1,2,3,4,5,6,7,8,9]);
 expect(queue.stats()).toMatchObject({queuedOperations:0,queuedBytes:0});await queue.close();
});
it('never overlaps active writes and rejects a batch when its snapshot commit fails',async()=>{
 let release!:()=>void,entered!:()=>void;const gate=new Promise<void>(r=>release=r),started=new Promise<void>(r=>entered=r);
 const queue=new SQLiteOwnerWorkQueue(async()=>{},async run=>{await run();throw Error('commit failed');});
 const write=queue.submit(1,async()=>{entered();await gate;});await started;
 const callback=vi.fn(async()=>42),read=queue.submit(1,callback,true),rejected=expect(read).rejects.toThrow('commit failed');
 expect(callback).not.toHaveBeenCalled();release();await write;await rejected;await queue.close();
});
it('isolates an individual read error and fences queued reads on shutdown',async()=>{
 const queue=new SQLiteOwnerWorkQueue(async()=>{},async run=>run());
 const bad=queue.submit(1,async()=>{throw Error('denied');},true),rejected=expect(bad).rejects.toThrow('denied');
 const good=queue.submit(1,async()=>7,true);await rejected;expect(await good).toBe(7);
 let release!:()=>void,entered!:()=>void;const gate=new Promise<void>(r=>release=r),started=new Promise<void>(r=>entered=r);
 const write=queue.submit(1,async()=>{entered();await gate;});await started;
 const read=queue.submit(1,vi.fn(async()=>1),true),fenced=expect(read).rejects.toMatchObject({code:'FENCED'});
 const closing=queue.close();await fenced;release();await write;await closing;
 expect(queue.stats()).toMatchObject({queuedOperations:0,queuedBytes:0});await expect(queue.submit(0,async()=>0)).rejects.toMatchObject({code:'FENCED'});
});
it('keeps the existing global operation and byte ceilings',async()=>{
 const queue=new SQLiteOwnerWorkQueue(async()=>{},async run=>run());
 const held=Array.from({length:16},()=>queue.submit(131072,async()=>1,true));
 await expect(queue.submit(1,async()=>0)).rejects.toMatchObject({name:'SQLiteOwnerQueueBusyError'});
 await Promise.all(held);await expect(queue.submit(2097153,async()=>0)).rejects.toMatchObject({code:'BUSY'});await queue.close();
});

it('completes a supplying write under continuous read demand before a dependent read runs',async()=>{
 const queue=new SQLiteOwnerWorkQueue(async()=>{},async run=>run());
 let done=false,seen=0;
 const stream=async():Promise<void>=>{await queue.submit(1,async()=>{seen++;},true);if(!done)await stream();};
 const streams=Array.from({length:8},()=>stream());
 const supplying=queue.submit(1,async()=>{done=true;});
 const dependent=queue.submit(1,async()=>{await supplying;return 'resident';},true);
 expect(await dependent).toBe('resident');await Promise.all(streams);
 expect(seen).toBeLessThanOrEqual(16);await queue.close();
});
it('uses task messages instead of scheduling a clamped timer for every owner operation',async()=>{
 const timers=vi.spyOn(globalThis,'setTimeout'),queue=new SQLiteOwnerWorkQueue(async()=>{},async run=>run());
 await Promise.all(Array.from({length:32},()=>queue.submit(1,async()=>1)));
 expect(timers).not.toHaveBeenCalled();timers.mockRestore();await queue.close();
});
