import { expect, it, vi } from 'vitest';
import { applyReplicaHashDelta, replicaRowsHashes } from './replica-integrity';
it('incremental hashes equal a full hash after updates, additions and removals', async () => {
  const original = [{ id: 'a', name: 'á' }, { id: 'b', name: 'B' }, { id: 'c', name: 'C' }];
  const hashes = await replicaRowsHashes(original, 'id');
  const upserts = [{ id: 'a', name: 'changed' }, { id: 'd', name: 'new' }];
  const next = await applyReplicaHashDelta(hashes, upserts, ['b'], 'id');
  expect(next).toEqual(await replicaRowsHashes([upserts[0], original[2], upserts[1]], 'id'));
  expect(hashes).toHaveProperty('b');
});

it('maintains an order-independent aggregate through replacements, deletes and inserts', async () => {
  const { replicaAggregateDigest } = await import('./replica-integrity');
  const hashes = await replicaRowsHashes([{ id: 'a', value: 1 }, { id: 'b', value: 2 }], 'id');
  const initial = replicaAggregateDigest(hashes);
  expect(replicaAggregateDigest(Object.fromEntries(Object.entries(hashes).reverse()))).toBe(initial);
  const upserts = [{ id: 'a', value: 3 }, { id: 'c', value: 4 }];
  const next = await applyReplicaHashDelta(hashes, upserts, ['b', 'b'], 'id', initial);
  expect(replicaAggregateDigest(next)).toBe(replicaAggregateDigest(await replicaRowsHashes(upserts, 'id')));
  expect(replicaAggregateDigest(hashes)).toBe(initial);
});

it('yields between full-window hashing chunks and still rejects duplicate keys across chunks', async () => {
  const digest = vi.spyOn(globalThis.crypto.subtle, 'digest').mockResolvedValue(new ArrayBuffer(32));
  let callsAtYield = 0;
  const timer = setTimeout(() => { callsAtYield = digest.mock.calls.length; }, 0);
  try {
    const rows = Array.from({ length: 129 }, (_, id) => ({ id: String(id) }));
    await replicaRowsHashes(rows, 'id');
    expect(callsAtYield).toBe(128);
    expect(digest).toHaveBeenCalledTimes(129);
    rows[128] = rows[0]!;
    await expect(replicaRowsHashes(rows, 'id')).rejects.toThrow('duplicate key');
  } finally {
    clearTimeout(timer);
    digest.mockRestore();
  }
});
