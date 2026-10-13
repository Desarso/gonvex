import { IndexedDBLocalReplicaStorage } from '../../dist/indexeddb-replica.js';
import { IndexedDBEngineReplicaStorage } from '../../dist/indexeddb-engine.js';
import { replicaAggregateDigest, replicaRowsHashes } from '../../dist/replica-integrity.js';

window.runStorageCase = async data => {
  const stores = [new IndexedDBLocalReplicaStorage(`native-baseline-${data.rows.length}`), new IndexedDBEngineReplicaStorage(`native-engine-${data.rows.length}`)];
  const samples = [[], []], empty = { entities: {}, liveQueries: {} };
  try {
    for (const storage of stores) await storage.replaceWindow(data.window, { ...empty, cursor: data.window.cursor }, 'default', data.rows);
    for (let index = 0; index < 25; index++) {
      const row = { ...data.rows[Math.floor(data.rows.length / 2)], field1: `delta-${index}` };
      const hashes = { ...data.window.hashes, ...await replicaRowsHashes([row], 'id') };
      const cursor = { epoch: 'fixture', revision: index + 2 };
      const window = { ...data.window, cursor, hashes, aggregateDigest: replicaAggregateDigest(hashes) };
      for (const variant of index % 2 ? [1, 0] : [0, 1]) {
        const start = performance.now();
        await stores[variant].applyWindowDelta(window, { upserts: [row], deleted: [], memberships: [{ id: row.id }] }, { ...empty, cursor });
        if (index >= 5) samples[variant].push(performance.now() - start);
      }
    }
    const summary = values => { const sorted = [...values].sort((a, b) => a - b); return { median: sorted[10], p95: sorted[18], samples: values }; };
    return { metric: `single-row-${data.rows.length}`, encodedBytes: data.encodedBytes, legacy: summary(samples[0]), engine: summary(samples[1]) };
  } finally { stores.forEach(storage => storage.close()); }
};
