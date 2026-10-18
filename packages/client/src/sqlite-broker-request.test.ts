import { expect, it, vi } from 'vitest';
import { SQLiteReplicaBroker } from './sqlite-broker.js';

// Exercise public admission without a browser election. Transport dispatch is
// replaced at its boundary; real multi-tab tests cover the actual transports.
function client() {
  const broker = Object.assign(Object.create(SQLiteReplicaBroker.prototype), {
    closed: false, failed: false, changeListeners:new Set(), pending: new Map(), inflight: new Map(), inflightBytes: 0, pendingBytes: 0, serial: 0, tabID: 'test',
    send: vi.fn((call: any) => {
      clearTimeout(call.timer); broker.pending.delete(call.id); broker.pendingBytes -= call.bytes;
      call.resolve({ accepted: call.op, args: call.args });
    }),
  }) as SQLiteReplicaBroker;
  return broker;
}
it('dispatches native view pages through the public broker admission path', async () => {
  const broker = client();
  await expect(broker.request('viewPage', { scope: 's', signature: 'grid', offset: 1000, plan: { table: 'tasks', limit: 32 } })).resolves.toMatchObject({ accepted: 'viewPage', args: { offset: 1000 } });
  expect(broker.stats()).toMatchObject({ pending: 0, pendingBytes: 0 });
});
it('admits staged membership operations and rejects internal owner operations', async () => {
  const broker = client();
  for (const op of ['windowBegin', 'windowAppend', 'windowPublish', 'windowHeader', 'windowPage', 'windowHashes', 'windowCollect']) {
    await expect(broker.request(op, { scope: 's' })).resolves.toMatchObject({ accepted: op });
  }
  await expect(broker.request('open', { owner: 'intruder' })).rejects.toMatchObject({ code: 'OPERATION' });
});

it('admits restartable bootstrap operations through the same bounded replay channel', async () => {
  const broker = client();
  for (const op of ['bootstrapBegin', 'bootstrapAppend', 'bootstrapFinish'])
    await expect(broker.request(op, { scope: 's', token: 'first-sync' })).resolves.toMatchObject({ accepted: op });
  await expect(broker.request('bootstrapAppend', { scope: 's', encoded: 'x'.repeat(131072) })).rejects.toMatchObject({ code: 'BUDGET' });
  expect(broker.stats()).toMatchObject({ pending: 0, pendingBytes: 0 });
});

it('accepts actual progress only from the current ready worker without settling requests', () => {
  const broker = client() as any;
  const generation = { ready: true, lastProgress: 1000 };
  broker.generation = generation; broker.announce = vi.fn();
  const now = vi.spyOn(Date, 'now').mockReturnValue(3000);
  try {
    broker.progress(generation, 2500); expect(generation.lastProgress).toBe(2500);
    broker.progress(generation, 2400); broker.progress(generation, 5000);
    broker.progress({ ...generation }, 2700); broker.progress(generation, NaN);
    generation.ready = false; broker.progress(generation, 2700);
    expect(generation.lastProgress).toBe(2500); expect(broker.announce).toHaveBeenCalledOnce();
    generation.ready = true; broker.progress(generation, 2800);
    expect(generation.lastProgress).toBe(2800); expect(broker.announce).toHaveBeenCalledTimes(2);
    expect(broker.pending.size).toBe(0);
  } finally { now.mockRestore(); }
});

it('bounds change subscriptions and releases their retained listeners',()=>{
  const broker=client() as any;broker.options={};const calls:number[]=[];
  const release=broker.subscribeChanges((_scope:string,sequence:number)=>calls.push(sequence));
  broker.subscribeChanges(()=>{throw Error('observer');});
  broker.receiveControl({protocol:1,type:'change',scope:'s',sequence:3});expect(calls).toEqual([3]);
  release();broker.receiveControl({protocol:1,type:'change',scope:'s',sequence:4});expect(calls).toEqual([3]);
  const releases=Array.from({length:63},()=>broker.subscribeChanges(()=>{}));
  expect(()=>broker.subscribeChanges(()=>{})).toThrow('listener budget');
  for(const close of releases)close();expect(broker.stats().listeners).toBe(1);
});

it('admits binary bootstrap frames and snapshots bytes before retaining replay requests', async()=>{
  const broker=client(); const input=new TextEncoder().encode('[{"row":"東京"}]');
  const result=await broker.request<any>('bootstrapAppend',{scope:'s',token:'t',page:0,encoded:input});
  const expected=input.slice();input.fill(0);
  expect(result.args.encoded).toEqual(expected);
  await expect(broker.request('apply',{scope:'s',encoded:expected})).rejects.toMatchObject({code:'INVALID_ROW'});
  await expect(broker.request('bootstrapAppend',{scope:'s',encoded:new Uint8Array(131072)})).rejects.toMatchObject({code:'BUDGET'});
  let read=false;
  await expect(broker.request('bootstrapAppend',{encoded:expected,get scope(){read=true;return 's';}})).rejects.toMatchObject({code:'INVALID_ROW'});
  expect(read).toBe(false);expect(broker.stats()).toMatchObject({pending:0,pendingBytes:0});
});

it('admits encoded native snapshot bytes under the same immutable transport budget',async()=>{
 const broker=client(),encoded=new TextEncoder().encode('{"type":"replica.snapshotEncodedPage"}');
 const result=await broker.request<any>('snapshotAppendEncoded',{scope:'s',encoded});
 const expected=encoded.slice();encoded.fill(0);expect(result.args.encoded).toEqual(expected);
 await expect(broker.request('snapshotAppendEncoded',{scope:'s',encoded:new Uint8Array(131072)})).rejects.toMatchObject({code:'BUDGET'});
});
