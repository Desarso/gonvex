import {expect,it} from 'vitest';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
it('queues under the same byte cap and resumes in order after release',async()=>{
 const budget=new SQLiteTransientBudget(100,1),release=budget.reserve(60);
 const next=budget.acquire(40);
 expect(budget.stats()).toMatchObject({bytes:100,maxBytes:100});
 await expect(budget.acquire(1)).rejects.toMatchObject({code:'BUDGET'});
 release();const done=await next;
 expect(budget.stats().bytes).toBe(40);done();done();expect(budget.stats().bytes).toBe(0);
});
it('rejects queued requests on close without granting them later',async()=>{
 const budget=new SQLiteTransientBudget(100,1),release=budget.reserve(40),next=budget.acquire(40);
 budget.close();await expect(next).rejects.toMatchObject({code:'FENCED'});release();
 await expect(budget.acquire(1)).rejects.toMatchObject({code:'FENCED'});expect(budget.stats().bytes).toBe(0);
});
