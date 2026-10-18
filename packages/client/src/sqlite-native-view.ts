import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import type { SQLiteTypedViewPage } from './sqlite-typed-engine.js';
import { sqlitePayloadBytes, decodedPageBytes, pageBudgetError } from './sqlite-admission.js';
import { compileSQLiteTypedView, type SQLitePagePlan } from './sqlite-typed-query.js';
import { sqliteIdentifier as q, sqliteTableName, sqliteFieldName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';

export const sqliteNativeViewSchema = `CREATE TABLE IF NOT EXISTS gv_views(scope TEXT NOT NULL,signature TEXT NOT NULL,plan TEXT NOT NULL,sequence INTEGER NOT NULL,n INTEGER NOT NULL,touched INTEGER NOT NULL,PRIMARY KEY(scope,signature)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_views_recency ON gv_views(touched);
CREATE TABLE IF NOT EXISTS gv_view_members(scope TEXT NOT NULL,signature TEXT NOT NULL,position INTEGER NOT NULL,rid INTEGER NOT NULL,PRIMARY KEY(scope,signature,position)) WITHOUT ROWID;`;

/** Caller owns one serialized read/write transaction and its authority fence.
 * Canonical commits supply their sequence; standalone engines supply theirs.
 * This helper never publishes membership or rows outside that transaction. */
export async function readSQLiteTypedView(database: SQLiteReplicaDatabase, schema: SQLiteTypedSchema,
  scope: string, physical: string, signature: string, admitted: SQLitePagePlan, offset: number, sequence: number,
  pageBytes: number): Promise<SQLiteTypedViewPage> {
  const encoded = JSON.stringify(admitted);
  let header = await database.getFirstAsync<{ plan: string; sequence: number; n: number }>('SELECT plan,sequence,n FROM gv_views WHERE scope=? AND signature=?', scope, signature);
  if (!header || header.plan !== encoded || header.sequence !== sequence) {
    await database.runAsync('DELETE FROM gv_view_members WHERE scope=? AND signature=?', scope, signature);
    const statement = compileSQLiteTypedView(schema, physical, admitted);
    await database.runAsync(`INSERT INTO gv_view_members SELECT ?,?,row_number() OVER ()-1,rid FROM (${statement.sql})`, scope, signature, ...statement.bindings);
    const n = (await database.getFirstAsync<{ n: number }>('SELECT count(*) n FROM gv_view_members WHERE scope=? AND signature=?', scope, signature))!.n;
    await database.runAsync('INSERT INTO gv_views VALUES(?,?,?,?,?,?) ON CONFLICT(scope,signature) DO UPDATE SET plan=excluded.plan,sequence=excluded.sequence,n=excluded.n,touched=excluded.touched', scope, signature, encoded, sequence, n, Date.now());
    header = { plan: encoded, sequence, n };
    // A global disk-header cap also bounds retained plans across scopes.
    // Evict one exact view, never scan or decode its member IDs in JS.
    const count = (await database.getFirstAsync<{ n: number }>('SELECT count(*) n FROM gv_views'))!.n;
    if (count > 32) {
      const oldest = await database.getFirstAsync<{ scope: string; signature: string }>('SELECT scope,signature FROM gv_views WHERE scope<>? OR signature<>? ORDER BY touched,scope,signature LIMIT 1', scope, signature);
      if (oldest) {
        await database.runAsync('DELETE FROM gv_view_members WHERE scope=? AND signature=?', oldest.scope, oldest.signature);
        await database.runAsync('DELETE FROM gv_views WHERE scope=? AND signature=?', oldest.scope, oldest.signature);
      }
    }
  } else await database.runAsync('UPDATE gv_views SET touched=? WHERE scope=? AND signature=?', Date.now(), scope, signature);
  const table = schema[admitted.table]!;
  const fields = admitted.select ? [...new Set([table.key, ...admitted.select])] : undefined;
  const selected = fields ? fields.map((name, i) => {
    const path = ('$.' + JSON.stringify(name)).replaceAll("'", "''");
    return `t.${q(sqliteFieldName(name))} AS ${q('p' + i)},json_type(t.body,'${path}') AS ${q('h' + i)}`;
  }).join(',') : 't.body';
  let records: Record<string, unknown>[], bytes: number, limit = admitted.limit;
  for (;;) {
    try {
      records = await database.getAllAsync<Record<string, unknown>>(`SELECT ${selected} FROM gv_view_members m JOIN ${q(sqliteTableName(admitted.table))} t ON t.scope=? AND t.rid=m.rid
        WHERE m.scope=? AND m.signature=? AND m.position>=? AND m.position<? ORDER BY m.position`, physical, scope, signature, offset, offset + limit);
      bytes = Math.max(sqlitePayloadBytes(records, pageBytes), decodedPageBytes(records, table, fields, pageBytes)); break;
    } catch (error) {
      if (!pageBudgetError(error) || limit === 1) throw error;
      limit = Math.max(1, Math.floor(limit / 2));
    }
  }
  const rows = records.map(record => !fields ? JSON.parse(String(record.body)) : Object.fromEntries(fields.flatMap((name, i) => {
    if (record['h' + i] === null) return [];
    const value = record['p' + i], kind = table.fields[name]!.kind;
    return [[name, value === null ? null : kind === 'json' ? JSON.parse(String(value)) : kind === 'boolean' ? Boolean(value) : value]];
  })));
  return { rows, sequence, bytes, offset, total: header.n, nextOffset: offset + rows.length < header.n ? offset + rows.length : null };

}
