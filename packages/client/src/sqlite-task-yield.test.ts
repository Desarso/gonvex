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
it('uses a microtask fallback without background-clamped timers when MessageChannel is unavailable', async () => {
  vi.stubGlobal('MessageChannel', undefined); const tasks = new SQLiteTaskYield();
  try { let complete = false; const pending = tasks.yield().then(() => { complete = true; }); await Promise.resolve(); expect(complete).toBe(false); await pending; }
  finally { tasks.close(); vi.unstubAllGlobals(); }
});

it('continues yielding when background timers cannot fire',async()=>{
 const tasks=new SQLiteTaskYield();const timer=vi.spyOn(globalThis,'setTimeout').mockImplementation(()=>{throw Error('Background timer used');});
 try{for(let n=0;n<12;n++)await tasks.yield();}finally{timer.mockRestore();tasks.close();}
});
