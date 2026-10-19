import {expect,it} from 'vitest';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
it('queues under the same byte cap and resumes in order after release',async()=>{
 const budget=new SQLiteTransientBudget(100,1),release=budget.reserve(60);
 const next=budget.acquire(40);
 expect(budget.stats()).toMatchObject({bytes:100,maxBytes:100});
 expect(()=>budget.reserve(1)).toThrow();
 await expect(budget.acquire(1)).rejects.toMatchObject({code:'BUDGET'});
 release();const done=await next;
 expect(budget.stats().bytes).toBe(40);done();done();expect(budget.stats().bytes).toBe(0);
});
it('rejects queued requests on close without granting them later',async()=>{
 const budget=new SQLiteTransientBudget(100,1),release=budget.reserve(40),next=budget.acquire(40);
 budget.close();await expect(next).rejects.toMatchObject({code:'FENCED'});release();
 await expect(budget.acquire(1)).rejects.toMatchObject({code:'FENCED'});expect(budget.stats().bytes).toBe(0);
});
it('charges held metadata to the same byte cap without consuming foreground slots',async()=>{
 const budget=new SQLiteTransientBudget(16,2),held=budget.hold(12);
 await expect(budget.acquire(8)).rejects.toMatchObject({code:'BUDGET'});const first=await budget.acquire(4);
 expect(budget.stats()).toMatchObject({bytes:16,entries:2,maxBytes:16});held();held();const second=await budget.acquire(8);
 expect(()=>budget.hold(5)).toThrow();first();second();expect(budget.stats()).toMatchObject({bytes:0,entries:0});
});
it('keeps a foreground slot available at the bounded background metadata limit',async()=>{
 const budget=new SQLiteTransientBudget(1024,1),held=Array.from({length:budget.maxWaitingEntries},()=>budget.hold(1));
 expect(()=>budget.hold(1)).toThrow();const foreground=await budget.acquire(32);
 expect(budget.stats()).toMatchObject({bytes:288,entries:257,maxEntries:257});
 await expect(budget.acquire(1)).rejects.toMatchObject({code:'BUDGET'});budget.close();foreground();for(const release of held)release();
 expect(budget.stats()).toMatchObject({bytes:0,entries:0});expect(()=>budget.hold(1)).toThrow();
});
