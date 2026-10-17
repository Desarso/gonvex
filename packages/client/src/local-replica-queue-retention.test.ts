import { writeHeapSnapshot } from 'node:v8';
import { Session } from 'node:inspector';
import { setTimeout as nextTurn } from 'node:timers/promises';
import { expect, it } from 'vitest';
import { LocalReplica, type LocalReplicaStorage } from './local-replica';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

async function collected(ref: WeakRef<object>) {
  const inspector = new Session();
  inspector.connect();
  try {
    for (let attempt = 0; attempt < 8; attempt++) {
      // A dereference keeps its target alive until the current JS job ends.
      await nextTurn(0);
      await new Promise<void>((resolve, reject) => inspector.post('HeapProfiler.collectGarbage', error => error ? reject(error) : resolve()));
    }
    return ref.deref() === undefined;
  } finally { inspector.disconnect(); }
}

it('releases a completed urgent read result while an earlier queued durable write is still blocked', async () => {
  const first = deferred(), second = deferred(), firstEntered = deferred(), secondEntered = deferred();
  let writes = 0;
  const storage: LocalReplicaStorage = {
    load: async () => undefined,
    applyTransaction: async () => {
      if (++writes === 1) { firstEntered.resolve(); await first.promise; }
      else { secondEntered.resolve(); await second.promise; }
    },
  };
  const replica = new LocalReplica(storage);
  const write = (revision: number) => replica.applyTransaction({ cursor: { epoch: 'test', revision }, changes: [] });
  const a = write(1); await firstEntered.promise;
  const b = write(2);
  // The read runs ahead of b between atomic writes. Neither the caller nor
  // normalized replica rows retain its temporary result after this helper exits.
  async function temporaryRead() {
    class TemporaryReadScratch { scratch = new Uint8Array(2 ** 20); }
    const value = await replica.withReadView(() => ({}), false, async () => new TemporaryReadScratch());
    return new WeakRef(value);
  }
  const pending = temporaryRead();
  first.resolve();
  const ref = await pending;
  await secondEntered.promise;
  let barrierDone = false;
  const barrier = replica.activateScope().then(() => { barrierDone = true; });
  try {
    const released = await collected(ref);
    if (!released && process.env.QUEUE_RETENTION_HEAP) writeHeapSnapshot(process.env.QUEUE_RETENTION_HEAP);
    expect(released).toBe(true);
    expect(barrierDone).toBe(false);
  } finally {
    second.resolve(); await Promise.all([a, b, barrier]); replica.dispose();
  }
}, 15_000);

it('keeps same-scope activation waiting for all earlier jobs, including rejected writes', async () => {
  const gate = deferred(), entered = deferred();
  const replica = new LocalReplica({
    load: async () => undefined,
    applyTransaction: async () => { entered.resolve(); await gate.promise; throw new Error('disk unavailable'); },
  });
  const failed = replica.applyTransaction({ cursor: { epoch: 'test', revision: 1 }, changes: [] }).catch(error => error.message);
  await entered.promise;
  let activated = false;
  const activation = replica.activateScope().then(() => { activated = true; });
  await nextTurn(0);
  expect(activated).toBe(false);
  gate.resolve();
  expect(await failed).toBe('disk unavailable');
  await activation;
  expect(activated).toBe(true);
  replica.dispose();
});
