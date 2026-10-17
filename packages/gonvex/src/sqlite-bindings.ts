import { createHash } from 'node:crypto';
import type { LocalSchema } from '@gonvex/local-runtime/schema';
import { compileSQLiteTypedSchema, type SQLiteTypedSchema, type SQLiteField } from '@gonvex/client/sqlite';
import type { Manifest, LiveExpression } from './manifest-types.js';

/** Generate only columns exposed by local replica declarations. PostgreSQL
 * schema introspection supplies types, never authority to replicate a field. */
export function sqliteBindings(local: LocalSchema, manifest: Manifest): Record<string, string> {
  const declarations = Object.values(manifest.functions).flatMap(entry => entry.replica ? [entry.replica] : []);
  const tables: Record<string, { key: string; fields: Record<string, SQLiteField>; indexes: { columns: string[] }[]; search?: string[]; counts?: { columns: string[] }[] }> = Object.create(null);
  const residency: Record<string, 'resident' | 'paged'> = Object.create(null);
  for (const declaration of declarations) {
    const source = local[declaration.table];
    if (!source) throw new Error('Replica table missing from local schema: ' + declaration.table);
    const table = tables[declaration.table] ??= { key: declaration.key, fields: Object.create(null), indexes: [] };
    if (table.key !== declaration.key) throw new Error('Conflicting replica identity for ' + declaration.table);
    for (const column of [...declaration.columns, declaration.key]) {
      const type = source.columns[column]?.type.toLowerCase();
      if (!type) throw new Error('Replica column missing from local schema: ' + declaration.table + '.' + column);
      const kind: SQLiteField['kind'] = /^(smallint|integer|bigint|real|double precision|numeric|decimal)/.test(type) ? 'number'
        : type === 'boolean' ? 'boolean' : /json|\[\]$/.test(type) ? 'json' : 'text';
      table.fields[column] = { kind, nullable: source.columns[column]!.nullable };
    }
    // Delivery limits describe a server window, not permission to retain a
    // whole table. Only explicit reference-data declarations may be resident.
    const resident = declaration.residency === 'resident';
    if (resident && !(declaration.maxRows !== undefined && declaration.maxRows > 0 && declaration.maxRows <= 2000 && declaration.maxBytes !== undefined && declaration.maxBytes > 0 && declaration.maxBytes <= 2097152)) throw new Error('Resident collection requires bounded row and byte budgets: ' + declaration.table);
    if (!resident || !residency[declaration.table]) residency[declaration.table] = resident ? 'resident' : 'paged';
  }
  const addIndex = (entity: string, columns: readonly string[]) => {
    const table = tables[entity];
    const unique = [...new Set(columns)].filter(field => table && Object.hasOwn(table.fields, field));
    if (!table || !unique.length) return;
    if (!table.indexes.some(index => JSON.stringify(index.columns) === JSON.stringify(unique))) table.indexes.push({ columns: unique });
  };
  const visit = (entity: string, expression: LiveExpression | undefined): string[] => {
    if (!expression) return [];
    if (expression.relation) {
      addIndex(expression.relation.table, [expression.relation.column]);
      const related = visit(expression.relation.table, expression.relation.where);
      addIndex(expression.relation.table, [...related, expression.relation.column]);
    }
    const fields = (expression.children ?? []).flatMap(child => visit(entity, child));
    if (expression.column) addIndex(entity, [expression.column]);
    return [...new Set([...fields, ...expression.operator === 'eq' && expression.column ? [expression.column] : []])];
  };
  for (const entry of Object.values(manifest.functions)) {
    const plan = entry.dependencies?.liveQueryPlan;
    if (!plan || !tables[plan.table]) continue;
    const table = tables[plan.table]!, equalities = visit(plan.table, plan.where);
    for (const column of plan.sort?.allowedColumns ?? []) {
      const fields = plan.index?.sortColumns?.[column] ?? [column];
      addIndex(plan.table, fields);
      for (const equality of equalities) addIndex(plan.table, [equality, ...fields]);
      if (equalities.length) addIndex(plan.table, [...equalities, ...fields]);
    }
    for (const column of plan.filters?.allowedColumns ?? []) addIndex(plan.table, [column]);
    const search = [...new Set([...(table.search ?? []), ...plan.search?.offlineColumns ?? plan.search?.columns ?? []])].filter(field => table.fields[field]?.kind === 'text');
    if (search.length) table.search = search.sort();
  }
  for (const [entity, table] of Object.entries(tables)) {
    addIndex(entity, [table.key]);
    // Count groups come from declared equality predicates, not a handwritten
    // application list. NULL is represented by JSON null in a bucket key.
    const groups = new Map<string, string[]>();
    for (const entry of Object.values(manifest.functions)) {
      const plan = entry.dependencies?.liveQueryPlan;
      if (plan?.table !== entity) continue;
      const fields = visit(entity, plan.where).filter(field => Object.hasOwn(table.fields, field)).sort();
      for (const field of fields) groups.set(JSON.stringify([field]), [field]);
      if (fields.length) groups.set(JSON.stringify(fields), fields);
    }
    if (groups.size) table.counts = [...groups.values()].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))).map(columns => ({ columns }));
    table.fields = Object.fromEntries(Object.entries(table.fields).sort(([a], [b]) => a.localeCompare(b)));
    table.indexes.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  }
  if (!Object.keys(tables).length) return {};
  const schema: SQLiteTypedSchema = Object.fromEntries(Object.entries(tables).sort(([a], [b]) => a.localeCompare(b)));
  const ddl = compileSQLiteTypedSchema(schema);
  const version = createHash('sha256').update(JSON.stringify(schema)).digest('hex');
  return { 'sqlite-schema.ts': `// Generated by Gonvex. Do not edit.\nexport const sqliteSchemaVersion = ${JSON.stringify(version)};\nexport const sqliteSchema = ${JSON.stringify(schema)} as const;\nexport const replicaResidency = ${JSON.stringify(residency)} as const;\n`,
    'sqlite-schema.sql': `-- Generated by Gonvex. Schema ${version}\n${ddl.tables}\n${ddl.indexes}\n${ddl.search}\n` };
}
