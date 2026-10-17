import assert from 'node:assert/strict';
import test from 'node:test';
import { replicaCollection } from '../dist/index.js';
const base = { table: 'records', key: 'id', columns: ['id'] };
test('resident declarations require both finite reference-data budgets', () => {
  assert.doesNotThrow(() => replicaCollection({ replica: { ...base, residency: 'paged' } }));
  assert.doesNotThrow(() => replicaCollection({ replica: { ...base, residency: 'resident', maxRows: 2000, maxBytes: 2097152 } }));
  for (const limits of [{}, { maxRows: 100 }, { maxBytes: 1000 }, { maxRows: 2001, maxBytes: 1000 }, { maxRows: 100, maxBytes: 2097153 }]) {
    assert.throws(() => replicaCollection({ replica: { ...base, residency: 'resident', ...limits } }), /resident replica collection/);
  }
  assert.throws(() => replicaCollection({ replica: { ...base, residency: 'anything' } }), /invalid residency/);
});
