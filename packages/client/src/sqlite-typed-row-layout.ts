import { SQLiteLocalReplicaStorage, type SQLiteReplicaDatabase, type SQLiteReplicaRowLayout } from './sqlite-replica-storage.js';
import type { ReplicaRow } from './local-replica.js';
import { compileSQLiteTypedSchema, sqliteIdentifier as q, sqliteTableName, sqliteFieldName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
import { sqlitePayloadBytes } from './sqlite-typed-engine.js';

const literal = (value: string) => "'" + value.replaceAll("'", "''") + "'";

/** Physical row layout for the existing canonical adapter. The compatibility
 * relation is a SQL view, not a duplicate durable JSON corpus. Both Expo and
 * browser hosts use the same schema and authority merge implementation. */
export function sqliteTypedRowLayout(schema: SQLiteTypedSchema): SQLiteReplicaRowLayout {
  const ddl = compileSQLiteTypedSchema(schema);
  const signature = JSON.stringify({ format: 1, schema });
  return {
    async install(db) {
      const legacy = await db.getFirstAsync<{ type: string }>("SELECT type FROM sqlite_master WHERE name='_gonvex_replica_entities'");
      if (legacy?.type === 'table') throw new Error('Existing generic SQLite rows require a verified migration before typed storage opens');
      await db.execAsync(`CREATE TABLE IF NOT EXISTS gv_canonical_layout(id INTEGER PRIMARY KEY CHECK(id=1),signature TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gv_canonical_tombstones(scope TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(scope,entity,id)) WITHOUT ROWID;`);
      const previous = await db.getFirstAsync<{ signature: string }>('SELECT signature FROM gv_canonical_layout WHERE id=1');
      if (previous && previous.signature !== signature) throw new Error('Typed canonical layout requires an explicit schema migration');
      await db.execAsync(ddl.tables + '\n' + ddl.indexes + '\n' + ddl.search);
      await db.runAsync('INSERT OR IGNORE INTO gv_canonical_layout VALUES(1,?)', signature);
      // This lookup serves legacy portable ReducerReadView predicates. Typed
      // view plans use the generated column indexes directly, without it.
      await db.execAsync(`CREATE TABLE IF NOT EXISTS _gonvex_replica_lookup (
 scope TEXT NOT NULL,entity TEXT NOT NULL,column_name TEXT NOT NULL,kind TEXT NOT NULL,scalar BLOB NOT NULL,id TEXT NOT NULL,
 PRIMARY KEY(scope,entity,column_name,kind,scalar,id));
CREATE INDEX IF NOT EXISTS _gonvex_replica_lookup_row ON _gonvex_replica_lookup(scope,entity,id);`);
      const branches: string[] = [];
      for (const entity of Object.keys(schema)) {
        const table = sqliteTableName(entity), name = q(table), entitySQL = literal(entity);
        branches.push(`SELECT scope,${entitySQL} entity,id,body value,rid rowid FROM ${name}`);
        const drop = `DELETE FROM _gonvex_replica_lookup WHERE scope=OLD.scope AND entity=${entitySQL} AND id=OLD.id;`;
        const add = `INSERT INTO _gonvex_replica_lookup SELECT NEW.scope,${entitySQL},j.key,
 CASE WHEN j.type IN ('integer','real') THEN 'number' WHEN j.type IN ('true','false') THEN 'boolean' ELSE 'string' END,j.atom,NEW.id
 FROM json_each(NEW.body) j WHERE j.type IN ('integer','real','true','false') OR (j.type='text' AND length(j.atom)<=256);`;
        await db.execAsync(`CREATE TRIGGER IF NOT EXISTS ${q(table + '_lookup_a')} AFTER INSERT ON ${name} BEGIN ${add} END;
CREATE TRIGGER IF NOT EXISTS ${q(table + '_lookup_u')} AFTER UPDATE ON ${name} BEGIN ${drop} ${add} END;
CREATE TRIGGER IF NOT EXISTS ${q(table + '_lookup_d')} AFTER DELETE ON ${name} BEGIN ${drop} END;`);
      }
      branches.push("SELECT scope,entity,id,'null' value,NULL rowid FROM gv_canonical_tombstones");
      if (branches.length > 500) throw new RangeError('Typed canonical schema exceeds SQLite compound table limit');
      await db.execAsync(`CREATE VIEW IF NOT EXISTS _gonvex_replica_entities AS ${branches.join(' UNION ALL ')};`);
      const deletes = Object.keys(schema).map(entity => `DELETE FROM ${q(sqliteTableName(entity))} WHERE scope=OLD.scope AND id=OLD.id AND OLD.entity=${literal(entity)};`).join('\n');
      await db.execAsync(`CREATE TRIGGER IF NOT EXISTS gv_canonical_delete INSTEAD OF DELETE ON _gonvex_replica_entities BEGIN
${deletes}
DELETE FROM gv_canonical_tombstones WHERE scope=OLD.scope AND entity=OLD.entity AND id=OLD.id;
END;`);
    },
    async upsert(db, scope, entity, rows) {
      const table = Object.hasOwn(schema, entity) && schema[entity];
      if (!table) throw new TypeError('Canonical write references an undeclared SQLite table');
      // Preserve per-field authority from the shared storage adapter. Version
      // here is not a substitute for its independently retained field stamps.
      sqlitePayloadBytes(rows, 262144);
      const fields = Object.entries(table.fields), name = q(sqliteTableName(entity));
      const columns = ['scope', 'id', 'body', 'version', ...fields.map(([field]) => sqliteFieldName(field)), ...(table.search ?? []).map((_, i) => 's' + i)];
      for (let offset = 0; offset < rows.length;) {
        const item = rows[offset]!;
        if (!item.id) throw new TypeError('Canonical row requires an identity');
        if (item.row === null) {
          await db.runAsync(`DELETE FROM ${name} WHERE scope=? AND id=?`, scope, item.id);
          await db.runAsync('INSERT OR IGNORE INTO gv_canonical_tombstones VALUES(?,?,?)', scope, entity, item.id);
          offset++; continue;
        }
        const values: unknown[] = [], page: string[] = [], ids: string[] = [];
        const maxRows = Math.min(100, Math.floor(32000 / columns.length));
        while (offset < rows.length && page.length < maxRows && rows[offset]!.row !== null) {
          const { id, row } = rows[offset++]!;
          if (!id || String(row![table.key]) !== id) throw new TypeError('Canonical SQLite key differs from its row identity');
          values.push(scope, id, JSON.stringify(row), 0, ...fields.map(([field, shape]) => {
            const value = row![field];
            if (value == null) return null;
            if (shape.kind !== 'json' && typeof value !== (shape.kind === 'text' ? 'string' : shape.kind === 'number' ? 'number' : 'boolean')) throw new TypeError('Canonical SQLite field type differs from schema');
            return shape.kind === 'json' ? JSON.stringify(value) : shape.kind === 'boolean' ? Number(value) : value;
          }), ...(table.search ?? []).map(field => String(row![field] ?? '').toLowerCase()));
          ids.push(id); page.push('(' + columns.map(() => '?').join(',') + ')');
        }
        await db.runAsync(`DELETE FROM gv_canonical_tombstones WHERE scope=? AND entity=? AND id IN (${ids.map(() => '?').join(',')})`, scope, entity, ...ids);
        await db.runAsync(`INSERT INTO ${name}(${columns.map(q).join(',')}) VALUES ${page.join(',')} ON CONFLICT(scope,id) DO UPDATE SET ${columns.slice(2).map(column => `${q(column)}=excluded.${q(column)}`).join(',')}`, ...values);
      }
    },
  };
}

export function sqliteTypedReplicaStorage(db: SQLiteReplicaDatabase, schema: SQLiteTypedSchema): SQLiteLocalReplicaStorage {
  return new SQLiteLocalReplicaStorage(db, sqliteTypedRowLayout(schema));
}
