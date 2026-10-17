import { expect, it, vi } from 'vitest';
import { LocalReplica } from './local-replica';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

it('drops abandoned row restores immediately but preserves an overlapping visible range', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async (_scope: string, _entity: string, ids: readonly string[]) => ids.map(id => ({ id, row: { id, value: 'disk' } })));
  const replica = new LocalReplica({ load: async () => undefined, applyTransaction: async () => { entered.resolve(); await gate.promise; }, loadEntityRows: read });
  const writing = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] });
  await entered.promise;
  const first = replica.retainRows('tasks', ['a', 'b']);
  const second = replica.retainRows('tasks', ['b', 'c']);
  try {
    expect((replica as any).applicationJobs.length).toBe(2);
    first();
    expect((replica as any).applicationJobs.length).toBe(1);
    gate.resolve(); await writing; await replica.activateScope();
    expect(read).toHaveBeenCalledTimes(1);
    expect(read.mock.calls[0]?.[2]).toEqual(['b', 'c']);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(replica.entity('tasks', 'b')).toMatchObject({ value: 'disk' });
    expect(replica.entity('tasks', 'c')).toMatchObject({ value: 'disk' });
  } finally { first(); second(); gate.resolve(); await writing; replica.dispose(); }
});

it('shares a pending row restore by owner count and requeues a same-turn remount', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async (_scope: string, _entity: string, ids: readonly string[]) => ids.map(id => ({ id, row: { id } })));
  const replica = new LocalReplica({ load: async () => undefined, applyTransaction: async () => { entered.resolve(); await gate.promise; }, loadEntityRows: read });
  const writing = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] });
  await entered.promise;
  const first = replica.retainRows('tasks', ['a', 'b']);
  const second = replica.retainRows('tasks', ['b', 'a']);
  let third: (() => void) | undefined;
  try {
    first(); expect((replica as any).applicationJobs.length).toBe(1);
    second(); expect((replica as any).applicationJobs.length).toBe(0);
    third = replica.retainRows('tasks', ['a', 'b']);
    expect((replica as any).applicationJobs.length).toBe(1);
    gate.resolve(); await writing; await replica.activateScope();
    expect(read).toHaveBeenCalledTimes(1);
    expect(replica.entity('tasks', 'a')).toEqual({ id: 'a' });
    expect(replica.entity('tasks', 'b')).toEqual({ id: 'b' });
  } finally { first(); second(); third?.(); gate.resolve(); await writing; replica.dispose(); }
});

it('keeps an active row read available to a remount and admits only still-retained IDs', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async (_scope: string, _entity: string, ids: readonly string[]) => { entered.resolve(); await gate.promise; return ids.map(id => ({ id, row: { id } })); });
  const replica = new LocalReplica({ load: async () => undefined, applyTransaction: async () => {}, loadEntityRows: read });
  const first = replica.retainRows('tasks', ['a', 'b']); await entered.promise;
  first(); const second = replica.retainRows('tasks', ['b']);
  try {
    gate.resolve(); await replica.activateScope();
    expect(read).toHaveBeenCalledTimes(1);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(replica.entity('tasks', 'b')).toEqual({ id: 'b' });
  } finally { first(); second(); gate.resolve(); replica.dispose(); }
});

it('does not retain queued lookups for rows that leave the visible range', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async () => []);
  const replica = new LocalReplica({ load: async () => undefined, applyTransaction: async () => { entered.resolve(); await gate.promise; }, loadEntityRows: read });
  const writing = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] });
  await entered.promise;
  try {
    for (let index = 0; index < 1_000; index++) replica.retainRows('tasks', ['scrolled-' + index])();
    expect((replica as any).applicationJobs.length).toBe(0);
    expect((replica as any).pendingRetains.size).toBe(0);
  } finally { gate.resolve(); await writing; await replica.activateScope(); replica.dispose(); }
  expect(read).not.toHaveBeenCalled();
});
