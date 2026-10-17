import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { sqliteBindings } from '../dist/sqlite-bindings.js';

const local = { things: { key: '_id', columns: { _id: { type: 'text', nullable: false }, workspace: { type: 'text', nullable: true }, title: { type: 'text', nullable: true }, secret: { type: 'text', nullable: true }, due: { type: 'bigint', nullable: true } } } };
const manifest = { functions: {
  '__local.things': { replica: { table: 'things', key: '_id', columns: ['_id', 'workspace', 'title', 'due'], maxRows: 100000, maxBytes: 100000000 } },
  grid: { dependencies: { liveQueryPlan: { table: 'things', key: '_id', where: { operator: 'eq', column: 'workspace', value: { argument: 'workspace' } }, sort: { allowedColumns: ['title', 'due'] }, search: { columns: ['title', 'secret'] } } } },
} };
test('generates executable typed tables/indexes/FTS/counts without unexposed fields', () => {
  const output = sqliteBindings(local, manifest);
  const source = output['sqlite-schema.ts'];
  assert.doesNotMatch(source, /secret/);
  assert.match(source, /"columns":\["workspace","due"\]/);
  assert.match(source, /"search":\["title"\]/);
  assert.match(source, /"counts":\[\{"columns":\["workspace"\]\}\]/);
  assert.match(source, /"things":"paged"/);
  const db = new DatabaseSync(':memory:');
  try { db.exec(output['sqlite-schema.sql']); assert.equal(db.prepare('PRAGMA integrity_check').get().integrity_check, 'ok'); }
  finally { db.close(); }
});
test('generation is stable and changed exposed layouts change the version', () => {
  const first = sqliteBindings(local, manifest);
  assert.deepEqual(first, sqliteBindings(local, manifest));
  const changed = structuredClone(manifest); changed.functions['__local.things'].replica.columns.push('secret');
  assert.notEqual(first['sqlite-schema.ts'].match(/sqliteSchemaVersion = "(.*?)"/)[1], sqliteBindings(local, changed)['sqlite-schema.ts'].match(/sqliteSchemaVersion = "(.*?)"/)[1]);
});
