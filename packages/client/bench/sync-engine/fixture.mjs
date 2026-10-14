import { createHash } from 'node:crypto';
import { replicaAggregateDigest } from '../../dist/replica-integrity.js';
// Committed seed and artifact identify this synthetic corpus, never production data.
export const seed = 0x47565801;
export const artifactHash = createHash('sha256').update('sync-engine-fixture-v1-47565801').digest('hex');
export const budget = { maxRows: Number.MAX_SAFE_INTEGER, maxBytes: 200 * 1024 * 1024 };
export function fixture(count) {
  const rows = Array.from({ length: count }, (_, index) => {
    const row = { id: `task-${String(index).padStart(6, '0')}` };
    for (let column = 1; column < 50; column++) {
      const value = (Math.imul(index + seed, 1664525) + column * 1013904223) >>> 0;
      const scalar = column % 5 === 0 ? { value, text: 'deterministic-payload-'.repeat(3) }
        : column % 5 === 1 ? `value-${value}` : column % 5 === 2 ? value
        : column % 5 === 3 ? value % 2 === 0 : ['fixture', value];
      // Keep four indexed columns. The other mixed values model JSON payloads,
      // so this fixture isolates membership costs from scalar-index fanout.
      row[`field${column}`] = column < 4 || column % 5 === 0 || column % 5 === 4 ? scalar
        : column % 5 === 1 ? { value: scalar } : column % 5 === 2 ? [scalar] : null;
    }
    return row;
  });
  const hashes = Object.fromEntries(rows.map((row, index) => [row.id, rowHash(row)]));
  const cursor = { epoch: 'fixture', revision: 1 };
  const window = { signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', ids: rows.map(row => row.id),
    completeness: 'complete', source: 'server', cursor, hashes, aggregateDigest: replicaAggregateDigest(hashes) };
  const related = Array.from({ length: 5 }, (_, table) => ({ entity: `related${table}`, rows: Array.from({ length: 100 }, (_, index) => ({ id: `related-${index}`, taskId: rows[index % rows.length].id })) }));
  return { rows, window, related, visibleIDs: window.ids.slice(0, 250), exactIDs: window.ids.slice(0, 1000),
    encodedBytes: rows.reduce((bytes, row) => bytes + Buffer.byteLength(JSON.stringify(row)), 0) };
}

function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
}
export function rowHash(row) { return createHash('sha256').update(canonical(row)).digest('hex'); }
