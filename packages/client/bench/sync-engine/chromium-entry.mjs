import { IndexedDBLocalReplicaStorage } from '../../dist/indexeddb-replica.js';
import { IndexedDBEngineReplicaStorage } from '../../dist/indexeddb-engine.js';
import { replicaRowsHashes } from '../../dist/replica-integrity.js';

let state;
window.prepareStorageCase = async (data, variant) => {
  state?.storage.close();
  const storage = variant === 0 ? new IndexedDBLocalReplicaStorage(`native-baseline-${data.rows.length}`)
    : new IndexedDBEngineReplicaStorage(`native-engine-${data.rows.length}`);
  await storage.replaceWindow(data.window, { entities: {}, liveQueries: {}, cursor: data.window.cursor }, 'default', data.rows);
  // Retain only the inputs a row delta needs. The seed's decoded corpus is not
  // the store's working set and must not dominate its garbage collector.
  const row = data.rows[Math.floor(data.rows.length / 2)];
  state = { storage, window: data.window, row, originalHash: data.window.hashes[row.id],
    aggregate: Uint8Array.from({ length: 32 }, (_, index) => parseInt(data.window.aggregateDigest.slice(5 + index * 2, 7 + index * 2), 16)) };
};
window.applyStorageSample = async index => {
  const row = { ...state.row, field1: `delta-${index}` };
  const hash = (await replicaRowsHashes([row], 'id'))[row.id];
  const hashes = state.window.hashes;
  hashes[row.id] = hash;
  // Prepare the existing XOR certificate without allocating a 50k-key input
  // map per sample. This is fixture setup, outside the timed storage call.
  const bytes = state.aggregate.slice();
  for (let index = 0; index < 32; index++) bytes[index] ^= parseInt(state.originalHash.slice(index * 2, index * 2 + 2), 16) ^ parseInt(hash.slice(index * 2, index * 2 + 2), 16);
  const aggregateDigest = 'xor1:' + Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
  const cursor = { epoch: 'fixture', revision: index + 2 };
  const window = { ...state.window, cursor, hashes, aggregateDigest };
  const start = performance.now();
  await state.storage.applyWindowDelta(window, { upserts: [row], deleted: [], memberships: [{ id: row.id }] },
    { entities: {}, liveQueries: {}, cursor });
  return performance.now() - start;
};
window.closeStorageCase = () => state?.storage.close();
