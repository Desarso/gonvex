import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { PGlite } from '@electric-sql/pglite';
import { SQLiteTypedReplicaEngine, lowerSQLiteLivePlan } from '@gonvex/client/sqlite';

// Disposable embedded PostgreSQL only. This test never opens a socket or reads
// credentials. The explicit C text order is a contract control, not a claim of
// parity with the application's current localeCompare/default-PG collation.
test('typed plans agree with PostgreSQL for scalar/null/C-order/JSON/timestamp contracts', async () => {
  const pg = new PGlite();
  const sqlite = new DatabaseSync(':memory:');
  const host = {
    async execAsync(sql) { sqlite.exec(sql); },
    async runAsync(sql, ...values) { sqlite.prepare(sql).run(...values); },
    async getFirstAsync(sql, ...values) { return sqlite.prepare(sql).get(...values) ?? null; },
    async getAllAsync(sql, ...values) { return sqlite.prepare(sql).all(...values); },
    async withTransactionAsync(run) { sqlite.exec('BEGIN'); try { await run(); sqlite.exec('COMMIT'); } catch (e) { sqlite.exec('ROLLBACK'); throw e; } },
  };
  const schema = { records: { key: 'id', fields: { id: { kind: 'text' }, name: { kind: 'text' }, n: { kind: 'number' }, active: { kind: 'boolean' }, data: { kind: 'json' }, at: { kind: 'text' } }, indexes: [{ columns: ['n'] }, { columns: ['name'] }, { columns: ['at'] }] } };
  const engine = new SQLiteTypedReplicaEngine(host, schema);
  const names = [null, '', 'A', 'a', 'ä', 'é', 'e\u0301', '東京', '𐀀', '😀', 'İ', 'ı'];
  const rows = names.map((name, index) => ({ id: String(index).padStart(2, '0'), name, n: index === 0 ? null : [0, -0.5, 0.1, 1e20, -1e20, 42][index % 6], active: index % 3 === 0 ? null : index % 2 === 0, data: index % 2 ? { z: [null, true, 1.25], text: name } : ['x', false, index], at: index === 0 ? null : `2026-10-${String(index).padStart(2, '0')}T01:02:03.004Z` }));
  try {
    await engine.open('conformance');
    await engine.apply('s', rows.map(row => ({ table: 'records', id: row.id, version: 1, row })));
    await pg.exec('CREATE TABLE records(id TEXT PRIMARY KEY,name TEXT,n DOUBLE PRECISION,active BOOLEAN,data JSONB,at TIMESTAMPTZ)');
    for (const row of rows) await pg.query('INSERT INTO records VALUES($1,$2,$3,$4,$5::jsonb,$6::timestamptz)', [row.id, row.name, row.n, row.active, JSON.stringify(row.data), row.at]);
    const cases = [
      ['n', undefined, '', []],
      ['name', undefined, '', []],
      ['at', undefined, '', []],
      ['n', { field: 'n', op: 'ne', value: 42 }, 'n <> $1', [42]],
      ['n', { field: 'n', op: 'eq', value: null }, 'n IS NULL', []],
      ['n', { field: 'n', op: 'ne', value: null }, 'n IS NOT NULL', []],
      ['n', { field: 'n', op: 'gte', value: -0.5 }, 'n >= $1', [-0.5]],
      ['name', { field: 'name', op: 'ne', value: 'A' }, 'name <> $1', ['A']],
      ['n', { field: 'active', op: 'eq', value: false }, 'active = $1', [false]],
      ['n', { field: 'active', op: 'ne', value: true }, 'active <> $1', [true]],
      ['n', { op: 'not', child: { field: 'n', op: 'eq', value: 42 } }, 'NOT (n = $1)', [42]],
      ['name', { op: 'in', field: 'name', values: ['A', null, '東京'] }, 'name IN ($1,$2,$3)', ['A', null, '東京']],
      ['name', { op: 'in', field: 'name', values: [] }, 'FALSE', []],
      ['n', { op: 'or', children: [{ field: 'n', op: 'lt', value: 0 }, { field: 'n', op: 'eq', value: null }] }, '(n < 0 OR n IS NULL)', []],
    ];
    let comparisons = 0;
    for (const [field, where, sqlWhere, args] of cases) for (const direction of ['asc', 'desc']) {
      const plan = { table: 'records', order: [{ field, direction }], textOrder: 'binary', limit: 3, where };
      const sql = `SELECT id FROM records ${sqlWhere ? 'WHERE ' + sqlWhere : ''} ORDER BY ${field}${field === 'name' ? ' COLLATE "C"' : ''} ${direction} NULLS ${direction === 'asc' ? 'LAST' : 'FIRST'},id COLLATE "C" ASC`;
      const expected = (await pg.query(sql, args)).rows.map(row => row.id);
      const actual = []; let after;
      do {
        const page = await engine.page('s', { ...plan, after });
        actual.push(...page.rows.map(row => row.id)); after = page.rows.length ? page.cursor : null;
      } while (after);
      assert.deepEqual(actual, expected, JSON.stringify({ field, direction, where }));
      const positional = await engine.viewPage('s', String(comparisons), { ...plan, limit: 256 });
      assert.deepEqual(positional.rows.map(row => row.id), expected);
      assert.equal(positional.total, expected.length); comparisons++;
    }
    const projection = await engine.page('s', { table: 'records', order: [{ field: 'id', direction: 'asc' }], textOrder: 'binary', select: ['data', 'active'], limit: 256 });
    assert.deepEqual(projection.rows, (await pg.query('SELECT id,data,active FROM records ORDER BY id COLLATE "C"')).rows);
    assert.equal(comparisons, 28);
    const source = { table: 'records', key: 'id', sort: { columnArgument: 'sort', directionArgument: 'direction', allowedColumns: ['n', 'name'], defaultColumn: 'n', defaultDirection: 'asc' },
      window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 3, maxLimit: 100, count: 'exact' },
      where: { operator: 'and', children: [
        { operator: 'range', column: 'n', value: { argument: 'low' }, valueTo: { argument: 'high' } },
        { operator: 'not', children: [{ operator: 'eq', column: 'name', value: { context: 'member.id' } }] },
      ] } };
    const lowered = lowerSQLiteLivePlan(schema, source, { low: -1, high: 43, sort: 'name', direction: 'desc', offset: 1 }, { 'member.id': 'A' }, { textOrder: 'binary' });
    assert.equal(lowered.supported, true);
    const loweredPage = await engine.viewPage('s', 'lowered', lowered.plan, lowered.offset);
    assert.deepEqual(loweredPage.rows.map(row => row.id), (await pg.query(`SELECT id FROM records WHERE n BETWEEN -1 AND 43 AND NOT (name = 'A') ORDER BY name COLLATE "C" DESC NULLS FIRST,id COLLATE "C" ASC LIMIT 3 OFFSET 1`)).rows.map(row => row.id));
    assert.deepEqual(lowerSQLiteLivePlan(schema, source, { sort: 'name' }, { 'member.id': 'A' }), { supported: false, reason: 'textOrderContract' });
    assert.deepEqual(lowerSQLiteLivePlan(schema, source, {}, {}), { supported: false, reason: 'missingIdentityContext' });
    assert.deepEqual(lowerSQLiteLivePlan(schema, { ...source, serverOnly: true }, {}, {}), { supported: false, reason: 'serverOnly' });
  } finally { sqlite.close(); await pg.close(); }
});
