import { expect, it, vi } from 'vitest';
import { SQLiteTaskYield } from './sqlite-task-yield.js';
it('coalesces a pending yield and crosses a real task boundary', async () => {
  const tasks = new SQLiteTaskYield();
  try {
    let completed = false;
    const first = tasks.yield(); expect(tasks.yield()).toBe(first);
    void first.then(() => { completed = true; });
    await Promise.resolve(); expect(completed).toBe(false);
    await first; expect(completed).toBe(true);
    const next = tasks.yield(); expect(next).not.toBe(first); await next;
  } finally { tasks.close(); }
});
it('closes resources and rejects pending and future work', async () => {
  const tasks = new SQLiteTaskYield();
  const pending = expect(tasks.yield()).rejects.toThrow('scheduler closed'); tasks.close();
  await pending; await expect(tasks.yield()).rejects.toThrow('scheduler closed'); tasks.close();
});
it('uses a real task fallback when MessageChannel is unavailable', async () => {
  vi.stubGlobal('MessageChannel', undefined); const tasks = new SQLiteTaskYield();
  try { let complete = false; const pending = tasks.yield().then(() => { complete = true; }); await Promise.resolve(); expect(complete).toBe(false); await pending; }
  finally { tasks.close(); vi.unstubAllGlobals(); }
});

it('continues yielding when background timers cannot fire',async()=>{
 const tasks=new SQLiteTaskYield();const timer=vi.spyOn(globalThis,'setTimeout').mockImplementation(()=>{throw Error('Background timer used');});
 try{for(let n=0;n<12;n++)await tasks.yield();}finally{timer.mockRestore();tasks.close();}
});

it('lets an inbound read run within 50ms while background work continues yielding',async()=>{
 const tasks=new SQLiteTaskYield(),inbound=new MessageChannel();let completed=0;
 const background=(async()=>{for(let n=0;n<100;n++){const start=performance.now();while(performance.now()-start<1){}completed++;await tasks.yield();}})();
 const requested=performance.now();const read=new Promise<number>(resolve=>{inbound.port1.onmessage=()=>resolve(performance.now()-requested);inbound.port2.postMessage('read');});
 try{expect(await read).toBeLessThan(50);expect(completed).toBeLessThan(100);await background;}finally{tasks.close();inbound.port1.close();inbound.port2.close();}
});
it('uses the host task scheduler without demoting an active transaction to idle work',async()=>{
 const postTask=vi.fn((run:()=>void)=>new Promise<void>(resolve=>{const port=new MessageChannel();port.port1.onmessage=()=>{run();resolve();port.port1.close();port.port2.close();};port.port2.postMessage(0);}));vi.stubGlobal('scheduler',{postTask});const tasks=new SQLiteTaskYield();
 try{await tasks.yield();expect(postTask).toHaveBeenCalledWith(expect.any(Function),{priority:'user-visible'});}finally{tasks.close();vi.unstubAllGlobals();}
});
