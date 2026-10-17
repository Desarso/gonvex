import { expect, it, vi } from 'vitest';
import { LocalReplica } from './local-replica';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

it('removes an abandoned queued window restore without waiting for a slow durable write', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async () => undefined);
  const replica = new LocalReplica({
    load: async () => undefined,
    applyTransaction: async () => { entered.resolve(); await gate.promise; },
    loadWindowRows: read,
  });
  const writing = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] });
  await entered.promise;
  const release = replica.retainWindow('abandoned');
  try {
    expect((replica as any).applicationJobs.length).toBe(1);
    release();
    expect((replica as any).applicationJobs.length).toBe(0);
    expect(read).not.toHaveBeenCalled();
  } finally { gate.resolve(); await writing; await replica.activateScope(); replica.dispose(); }
});

it('keeps a shared restore until its last owner leaves and requeues an immediate remount', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async (scope: string, signature: string) => ({
    window: { signature, kind: 'replica' as const, entity: 'tasks', key: 'id', ids: ['a'], completeness: 'complete' as const, source: 'cache' as const },
    rows: [{ id: 'a', title: 'from disk' }],
  }));
  const replica = new LocalReplica({
    load: async () => undefined,
    applyTransaction: async () => { entered.resolve(); await gate.promise; },
    loadWindowRows: read,
  });
  const writing = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] });
  await entered.promise;
  const a = replica.retainWindow('shared'), b = replica.retainWindow('shared');
  let c: (() => void) | undefined;
  try {
    expect((replica as any).applicationJobs.length).toBe(1);
    a(); expect((replica as any).applicationJobs.length).toBe(1);
    b(); expect((replica as any).applicationJobs.length).toBe(0);
    // No await: the old promise's cleanup must not erase this new owner/job.
    c = replica.retainWindow('shared');
    expect((replica as any).applicationJobs.length).toBe(1);
    gate.resolve(); await writing; await replica.activateScope();
    expect(read).toHaveBeenCalledTimes(1);
    expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', title: 'from disk' });
  } finally { a(); b(); c?.(); gate.resolve(); await writing; replica.dispose(); }
});

it('does not cancel an active restore needed by a remounted window', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async (_scope: string, signature: string) => {
    entered.resolve(); await gate.promise;
    return { window: { signature, kind: 'replica' as const, entity: 'tasks', key: 'id', ids: ['a'], completeness: 'complete' as const, source: 'cache' as const }, rows: [{ id: 'a' }] };
  });
  const replica = new LocalReplica({ load: async () => undefined, applyTransaction: async () => {}, loadWindowRows: read });
  const first = replica.retainWindow('active'); await entered.promise;
  first(); const second = replica.retainWindow('active');
  try {
    gate.resolve(); await replica.activateScope();
    expect(read).toHaveBeenCalledTimes(1);
    expect(replica.entity('tasks', 'a')).toEqual({ id: 'a' });
  } finally { first(); second(); gate.resolve(); replica.dispose(); }
});

it('does not accumulate restores for a tour of windows closed behind a blocked commit', async () => {
  const gate = deferred(), entered = deferred();
  const read = vi.fn(async () => undefined);
  const replica = new LocalReplica({
    load: async () => undefined,
    applyTransaction: async () => { entered.resolve(); await gate.promise; },
    loadWindowRows: read,
  });
  const writing = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] });
  await entered.promise;
  try {
    for (let index = 0; index < 1_000; index++) replica.retainWindow('tour-' + index)();
    expect((replica as any).applicationJobs.length).toBe(0);
    expect((replica as any).pendingRetains.size).toBe(0);
  } finally { gate.resolve(); await writing; await replica.activateScope(); replica.dispose(); }
  expect(read).not.toHaveBeenCalled();
});
