/** Generated, application-independent layout for the worker's disk replica.
 * SQL names are encoded, never interpolated from a query argument. */
export type SQLiteField = { kind: 'text' | 'number' | 'boolean' | 'json'; nullable?: boolean };
export type SQLiteTypedTable = {
  key: string;
  fields: Readonly<Record<string, SQLiteField>>;
  indexes: readonly { columns: readonly string[] }[];
  search?: readonly string[];
  counts?: readonly { columns: readonly string[] }[];
};
export type SQLiteTypedSchema = Readonly<Record<string, SQLiteTypedTable>>;
export const sqliteIdentifier = (name: string) => '"' + name.replaceAll('"', '""') + '"';
// Encoding every UTF-16 code unit also handles names containing NUL or unpaired
// surrogates without aliasing distinct application tables or SQLite internals.
export const sqliteTableName = (entity: string) => 'gv_t_' + Array.from({ length: entity.length }, (_, i) => entity.charCodeAt(i).toString(16).padStart(4, '0')).join('');
export const sqliteFieldName = (field: string) => 'f_' + sqliteTableName(field).slice(5);
export const sqliteSearchName = (entity: string) => sqliteTableName(entity) + '_fts';
export const sqliteCountName = (entity: string, index: number) => sqliteTableName(entity) + '_count_' + index;
const q = sqliteIdentifier;

export function validateSQLiteTypedSchema(schema: SQLiteTypedSchema): void {
  if (!Object.keys(schema).length) throw new TypeError('SQLite schema must declare a table');
  for (const [entity, table] of Object.entries(schema)) {
    if (!entity || !Object.hasOwn(table.fields, table.key) || !['text', 'number'].includes(table.fields[table.key]!.kind))
      throw new TypeError('SQLite replica tables require a declared scalar identity');
    for (const [field, shape] of Object.entries(table.fields)) {
      if (!field || !['text', 'number', 'boolean', 'json'].includes(shape.kind)) throw new TypeError('Invalid SQLite field');
    }
    for (const index of [...table.indexes, ...table.counts ?? []]) {
      if (!index.columns.length || new Set(index.columns).size !== index.columns.length)
        throw new TypeError('SQLite index/count requires distinct fields');
      for (const field of index.columns) if (!Object.hasOwn(table.fields, field)) throw new TypeError('Unknown SQLite indexed field');
    }
    if (table.search && (!table.search.length || new Set(table.search).size !== table.search.length)) throw new TypeError('Search requires distinct fields');
    for (const field of table.search ?? []) if (table.fields[field]?.kind !== 'text') throw new TypeError('Search fields must be text');
  }
}

/** Index creation is separate so a bounded bootstrap can build indexes after
 * ingest. Count and FTS maintenance always shares the row write transaction. */
export function compileSQLiteTypedSchema(schema: SQLiteTypedSchema): { tables: string; indexes: string; search: string } {
  validateSQLiteTypedSchema(schema);
  const tables: string[] = [], indexes: string[] = [], search: string[] = [];
  for (const [entity, table] of Object.entries(schema)) {
    const name = sqliteTableName(entity), tn = q(name);
    const fields = Object.entries(table.fields).map(([field, shape]) =>
      `${q(sqliteFieldName(field))} ${shape.kind === 'number' ? 'REAL' : shape.kind === 'boolean' ? 'INTEGER' : 'TEXT'}`);
    tables.push(`CREATE TABLE IF NOT EXISTS ${tn}(rid INTEGER PRIMARY KEY, scope TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL, version INTEGER NOT NULL, ${fields.concat((table.search ?? []).map((_, i) => q('s' + i) + ' TEXT')).join(',')}, UNIQUE(scope,id));`);
    table.indexes.forEach((index, i) => indexes.push(`CREATE INDEX IF NOT EXISTS ${q(name + '_i_' + i)} ON ${tn}(scope,${[...new Set([...index.columns, table.key])].map(field => q(sqliteFieldName(field))).join(',')});`));
    (table.counts ?? []).forEach((group, i) => {
      const cn = q(sqliteCountName(entity, i));
      const key = (prefix: string) => `json_array(${group.columns.map(field => `${prefix}.${q(sqliteFieldName(field))}`).join(',')})`;
      tables.push(`CREATE TABLE IF NOT EXISTS ${cn}(scope TEXT NOT NULL, bucket TEXT NOT NULL, n INTEGER NOT NULL CHECK(n>=0), PRIMARY KEY(scope,bucket)) WITHOUT ROWID;`);
      const add = `INSERT INTO ${cn} VALUES(NEW.scope,${key('NEW')},1) ON CONFLICT(scope,bucket) DO UPDATE SET n=n+1;`;
      const drop = `UPDATE ${cn} SET n=n-1 WHERE scope=OLD.scope AND bucket=${key('OLD')}; DELETE FROM ${cn} WHERE scope=OLD.scope AND bucket=${key('OLD')} AND n=0;`;
      tables.push(`CREATE TRIGGER IF NOT EXISTS ${q(name + '_c_' + i + '_a')} AFTER INSERT ON ${tn} BEGIN ${add} END;`,
        `CREATE TRIGGER IF NOT EXISTS ${q(name + '_c_' + i + '_u')} AFTER UPDATE ON ${tn} BEGIN ${drop} ${add} END;`,
        `CREATE TRIGGER IF NOT EXISTS ${q(name + '_c_' + i + '_d')} AFTER DELETE ON ${tn} BEGIN ${drop} END;`);
    });
    if (table.search?.length) {
      const fn = q(sqliteSearchName(entity));
      const cols = table.search.map((_, i) => q('s' + i));
      // Content lives in this index. Lowercasing happens once on ingestion using
      // JS Unicode rules; SQLite never calls JS to filter or compare a row.
      search.push(`CREATE VIRTUAL TABLE IF NOT EXISTS ${fn} USING fts5(${cols.join(',')},tokenize='trigram case_sensitive 1');`);
      search.push(`CREATE TRIGGER IF NOT EXISTS ${q(name + '_fts_d')} AFTER DELETE ON ${tn} BEGIN DELETE FROM ${fn} WHERE rowid=OLD.rid; END;`);
    }
  }
  return { tables: tables.join('\n'), indexes: indexes.join('\n'), search: search.join('\n') };
}
