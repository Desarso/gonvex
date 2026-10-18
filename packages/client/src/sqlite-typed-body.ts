import { sqliteFieldName, sqliteIdentifier as q, type SQLiteTypedSchema, type SQLiteTypedTable } from './sqlite-typed-schema.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

export const sqliteTypedBodyFormat = 6;
const path = (field: string) => "'" + ('$.' + JSON.stringify(field)).replaceAll("'", "''") + "'";
function replace(source: string, fields: readonly [string, string][]): string {
  // Stay below older native SQLite's 127 function-argument limit as well.
  for (let start = 0; start < fields.length; start += 48)
    source = `jsonb_replace(${source},${fields.slice(start, start + 48).flatMap(([name, value]) => [path(name), value]).join(',')})`;
  return `json(${source})`;
}
/** Keep unknown fields, original key order and missing-vs-null in the skeleton.
 * Values of generated fields have exactly one canonical home: typed columns. */
export function sqliteBodySkeleton(row: Record<string, unknown>, table: SQLiteTypedTable): string {
  const body = { ...row };
  for (const name of Object.keys(table.fields)) if (Object.hasOwn(body, name)) body[name] = null;
  return JSON.stringify(body);
}
export function sqliteBodySkeletonSQL(source: string, table: SQLiteTypedTable): string {
  return replace(source, Object.keys(table.fields).map(name => [name, 'NULL']));
}
export function sqliteRowBodySQL(table: SQLiteTypedTable, alias = 't'): string {
  return replace(`${alias}.body`, Object.entries(table.fields).map(([name, shape]) => {
    const column = `${alias}.${q(sqliteFieldName(name))}`;
    const value = shape.kind === 'json' ? `json(${column})` : shape.kind === 'boolean'
      ? `json(CASE WHEN ${column} IS NULL THEN 'null' WHEN ${column}<>0 THEN 'true' ELSE 'false' END)` : column;
    return [name, value];
  }));
}
/** Canonical and typed-engine entry points must fence old body decoders even
 * when they share a physical database. Existing full bodies remain readable. */
export async function installSQLiteTypedBodyFence(db: SQLiteReplicaDatabase, schema: SQLiteTypedSchema): Promise<void> {
  await db.execAsync('CREATE TABLE IF NOT EXISTS gv_storage_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema TEXT NOT NULL,epoch INTEGER NOT NULL,owner TEXT NOT NULL,sequence INTEGER NOT NULL)');
  const signature = JSON.stringify({ format: sqliteTypedBodyFormat, tables: schema });
  // Compare in SQLite. A generated schema can exceed the row bridge budget;
  // initialization must never copy that signature back into JavaScript.
  const old = await db.getFirstAsync<{ matches: number; epoch: number }>('SELECT schema=? matches,epoch FROM gv_storage_meta WHERE id=1', signature);
  if (old?.matches) return;
  if (old) {
    let compatible = false;
    for (const format of [3, 4, 5]) {
      const match = await db.getFirstAsync<{ matches: number }>('SELECT schema=? matches FROM gv_storage_meta WHERE id=1', JSON.stringify({ format, tables: schema }));
      if (match?.matches) { compatible = true; break; }
    }
    if (!compatible) throw new Error('Typed body storage requires an explicit schema migration');
  }
  if (old && (!Number.isSafeInteger(old.epoch + 1) || old.epoch < 0)) throw new Error('SQLite owner epoch exhausted');
  await db.runAsync("INSERT INTO gv_storage_meta VALUES(1,?,0,'canonical-layout',0) ON CONFLICT(id) DO UPDATE SET schema=excluded.schema,epoch=epoch+1,owner=excluded.owner", signature);
}
