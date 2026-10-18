import {expect,it} from 'vitest';
import {sqliteRequestBytes,sqliteTransferRequest} from './sqlite-request-frame.js';
it('transfers each bounded delivery without detaching immutable replay bytes',()=>{
  const encoded=new TextEncoder().encode('東京'),args={scope:'s',encoded};
  expect(sqliteRequestBytes('bootstrapAppend',args,1024)).toBeGreaterThan(encoded.byteLength*2);
  for(const op of ['bootstrapAppend','snapshotAppendEncoded'])for(let i=0;i<2;i++) {
    const outgoing=sqliteTransferRequest(op,args);
    const result=structuredClone(outgoing.args,{transfer:outgoing.transfer}) as typeof args;
    expect(result.encoded).toEqual(encoded);expect(encoded.byteLength).toBe(6);
    expect((outgoing.args as typeof args).encoded.byteLength).toBe(0);
  }
});
it('rejects shared byte frames and non-JSON metadata instead of retaining unbounded graphs',()=>{
  expect(()=>sqliteRequestBytes('bootstrapAppend',{encoded:new Uint8Array(new SharedArrayBuffer(4))},1024)).toThrow('Shared');
  expect(()=>sqliteRequestBytes('bootstrapAppend',{encoded:new Uint8Array(4),extra:new Map()},1024)).toThrow('plain JSON');
  expect(()=>sqliteRequestBytes('bootstrapAppend',{encoded:new Uint8Array(128)},128)).toThrow('budget');
  expect(sqliteTransferRequest('page',{scope:'s'}).transfer).toEqual([]);
});
