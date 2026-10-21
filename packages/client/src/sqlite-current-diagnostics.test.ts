import {expect,it} from 'vitest';
import {sqliteCurrentDiagnostics} from './sqlite-current-diagnostics.js';
import {sqlitePayloadBytes} from './sqlite-admission.js';
it('keeps current budget peaks without copying unbounded histories',()=>{
 const value={wasmBytes:16777216,program:[{name:'ingress',bytes:123,peakBytes:456}],ownerQueue:{maxQueueWaitMs:8,recent:Array(10000).fill('large')},transactions:{transactionCount:100,recent:Array(10000).fill('large')},nativeProfile:{history:'x'.repeat(200000)},programDiagnostics:{history:'x'.repeat(200000)}};
 const result=sqliteCurrentDiagnostics(value);expect(result.program).toEqual(value.program);expect(result.ownerQueue).toEqual({maxQueueWaitMs:8});expect(result.transactions).toEqual({transactionCount:100});expect(result).not.toHaveProperty('nativeProfile');expect(sqlitePayloadBytes(result,65536)).toBeLessThan(65536);
});
it('omits optional oversized diagnostic detail without refusing current counters',()=>{
 const result=sqliteCurrentDiagnostics({wasmBytes:42,program:[{name:'x'.repeat(100000)}]});expect(result.wasmBytes).toBe(42);expect(result).not.toHaveProperty('program');
});
