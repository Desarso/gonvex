import {expect,it,vi} from 'vitest';
import {ByteLedger} from './byte-budget.js';
it('releases snapshot callbacks on close without keeping a live registration',()=>{
 const ledger=new ByteLedger(1024),read=vi.fn(()=>({name:'owned',bytes:12,maxBytes:1024,entries:1,maxEntries:1,evictions:0,refusals:0}));
 const release=ledger.register('owned',read);expect(ledger.snapshot()).toHaveLength(1);
 const calls=read.mock.calls.length;ledger.close();expect(ledger.snapshot()).toEqual([]);expect(ledger.bytes).toBe(0);
 expect(read).toHaveBeenCalledTimes(calls);release();ledger.close();expect(ledger.snapshot()).toEqual([]);
 expect(()=>ledger.register('owned',read)).toThrow('closed');
});
