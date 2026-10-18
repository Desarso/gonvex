import { decodedJSONBytes } from './sqlite-admission.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';
const literal = (value: string) => "'" + value.replaceAll("'", "''") + "'";
const path = (...parts: string[]) => literal('$.' + parts.map(name => JSON.stringify(name)).join('.'));

/** The common wire path never creates a complete row graph. SQL checks strict
 * JSON, numeric finiteness, key uniqueness and declared field types before
 * returning only the bounded text projection needed for Unicode folding.
 * Unusual/legacy inputs return to the canonical JS decoder, preserving its
 * duplicate-key and numeric-key semantics rather than changing them silently. */
export async function writeSQLiteEncodedFrame(db: SQLiteReplicaDatabase, schema: SQLiteTypedSchema,
  each: 'json_each' | 'jsonb_each', encoded: string, maxBytes: number,
  insert: (entity: string, encoded: string, normalized: string) => Promise<void>,
): Promise<{ rows: number; validationMs: number; projectionMs: number; normalizationMs: number } | undefined> {
  const began = performance.now();
  decodedJSONBytes(encoded, maxBytes);
  if (!(await db.getFirstAsync<{ valid: number }>('SELECT json_valid(?) valid', encoded))?.valid) return;
  const shape = await db.getFirstAsync<{ kind: string }>('SELECT json_type(?) kind', encoded);
  if (shape?.kind !== 'array') return;
  const size = await db.getFirstAsync<{ n: number; bad: number }>(`SELECT count(*) n,coalesce(sum(type<>'object'),0) bad FROM ${each}(jsonb(?))`, encoded);
  if (!size || size.n > 512 || size.bad) return;
  const structure = await db.getFirstAsync<{ duplicate: number; nonfinite: number; tooDeep: number }>(`WITH RECURSIVE tree AS MATERIALIZED (
    SELECT id,parent,key,type,atom FROM json_tree(jsonb(?))
  ), depths(id,depth) AS (SELECT id,0 FROM tree WHERE parent IS NULL UNION ALL SELECT t.id,d.depth+1 FROM tree t JOIN depths d ON t.parent=d.id WHERE d.depth<33) SELECT EXISTS(SELECT 1 FROM tree WHERE key IS NOT NULL GROUP BY parent,key HAVING count(*)>1) duplicate,
    EXISTS(SELECT 1 FROM tree WHERE type IN ('integer','real') AND NOT(atom<=1.7976931348623157e308 AND atom>=-1.7976931348623157e308)) nonfinite,EXISTS(SELECT 1 FROM depths WHERE depth>32) tooDeep`, encoded);
  if (structure?.duplicate || structure?.nonfinite || structure?.tooDeep) return;
  const entities = await db.getAllAsync<{ entity: unknown }>(`SELECT DISTINCT json_extract(value,'$.table') entity FROM ${each}(jsonb(?))`, encoded);
  if (entities.length > 32) return;
  // Validate every table before inserting any of them. The caller's enclosing
  // transaction is still the rollback boundary for a later native failure.
  for (const { entity } of entities) {
    if (typeof entity !== 'string' || !Object.hasOwn(schema, entity)) return;
    const table = schema[entity]!;
    if (table.fields[table.key]?.kind !== 'text' || Object.keys(table.fields).length > 128) return;
    const id = "json_extract(value,'$.id')", version = "json_extract(value,'$.version')";
    const checks = ["json_type(value,'$.id')='text'", `length(${id})>0`,
      "json_type(value,'$.version') IN ('integer','real')", `${version}>=0 AND ${version}<=9007199254740991 AND ${version}=CAST(${version} AS INTEGER)`,
      "json_type(value,'$.row')='object'", `json_type(value,${path('row',table.key)})='text'`, `json_extract(value,${path('row',table.key)})=${id}`];
    for (const [name, field] of Object.entries(table.fields)) if (field.kind !== 'json') {
      const type = `json_type(value,${path('row',name)})`;
      const types = field.kind === 'text' ? "'text'" : field.kind === 'boolean' ? "'true','false'" : "'integer','real'";
      checks.push(`(${type} IS NULL OR ${type} IN ('null',${types}))`);
    }
    const invalid = await db.getFirstAsync<{ bad: number }>(`SELECT count(*) bad FROM ${each}(jsonb(?)) WHERE json_extract(value,'$.table')=? AND CASE WHEN ${checks.map(check => '('+check+')').join(' AND ')} THEN 0 ELSE 1 END`, encoded, entity);
    if (invalid?.bad) return;
  }
  const validationMs = performance.now() - began;
  let projectionMs = 0, normalizationMs = 0;
  for (const { entity } of entities as { entity: string }[]) {
    const table = schema[entity]!;
    let normalized = '[]';
    if (table.search?.length) {
      let timed = performance.now();
      const records = await db.getAllAsync<Record<string, unknown>>(`SELECT CAST(key AS INTEGER) pos,${table.search.map((name,index)=>`json_extract(value,${path('row',name)}) s${index}`).join(',')} FROM ${each}(jsonb(?)) WHERE json_extract(value,'$.table')=? ORDER BY CAST(key AS INTEGER)`, encoded, entity);
      projectionMs += performance.now() - timed; timed = performance.now();
      const values: Array<string[] | null> = Array(size.n).fill(null);
      for (const record of records) values[Number(record.pos)] = table.search.map((_,index)=>String(record['s'+index] ?? '').toLowerCase());
      normalized = JSON.stringify(values); normalizationMs += performance.now() - timed;
    }
    await insert(entity, encoded, normalized);
  }
  return { rows: size.n, validationMs, projectionMs, normalizationMs };
}
