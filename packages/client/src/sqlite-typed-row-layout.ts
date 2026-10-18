import { fail } from './sqlite-admission.js';
import { yieldSQLiteTask } from './sqlite-task-yield.js';
import { installSQLiteTypedBodyFence, sqliteBodySkeleton, sqliteRowBodySQL } from './sqlite-typed-body.js';
import { sqliteNativeViewSchema, readSQLiteTypedView } from './sqlite-native-view.js';
import { compileSQLiteTypedView } from './sqlite-typed-query.js';
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
  const signature = JSON.stringify({ format: 3, schema });
  let lease: { epoch: number; owner: string } | undefined;
  async function assertOwner(db: SQLiteReplicaDatabase) {
    const current = await db.getFirstAsync<{ epoch: number; owner: string; sequence: number }>('SELECT epoch,owner,sequence FROM gv_storage_meta WHERE id=1');
    if (!lease || current?.epoch !== lease.epoch || current.owner !== lease.owner) fail('FENCED', 'Canonical SQLite owner was replaced');
    return current;
  }
  return {
    async assertOwner(db) { await assertOwner(db); },
    async afterCommit(db) {
      const current = await assertOwner(db);
      if (!Number.isSafeInteger(current.sequence) || current.sequence >= Number.MAX_SAFE_INTEGER) fail('FENCED', 'SQLite storage sequence exhausted');
      await db.runAsync('UPDATE gv_storage_meta SET sequence=sequence+1 WHERE id=1');
    },
    async install(db) {
      const flags = await db.getFirstAsync("SELECT name FROM sqlite_master WHERE type='table' AND name='gv_storage_flags'");
      if (flags && await db.getFirstAsync("SELECT value FROM gv_storage_flags WHERE name='bootstrap-indexes'")) throw new Error('Finish or abort the restartable bootstrap before installing the canonical adapter');
      const legacy = await db.getFirstAsync<{ type: string }>("SELECT type FROM sqlite_master WHERE name='_gonvex_replica_entities'");
      if (legacy?.type === 'table') throw new Error('Existing generic SQLite rows require a verified migration before typed storage opens');
      await db.execAsync(`CREATE TABLE IF NOT EXISTS gv_canonical_layout(id INTEGER PRIMARY KEY CHECK(id=1),signature TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gv_canonical_tombstones(scope TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(scope,entity,id)) WITHOUT ROWID;`);
      const previous = await db.getFirstAsync<{ signature: string }>('SELECT signature FROM gv_canonical_layout WHERE id=1');
      if (previous && previous.signature !== signature && previous.signature !== JSON.stringify({ format: 1, schema }) && previous.signature !== JSON.stringify({ format: 2, schema })) throw new Error('Typed canonical layout requires an explicit schema migration');
      await installSQLiteTypedBodyFence(db, schema);
      await db.execAsync('DROP TRIGGER IF EXISTS gv_canonical_delete; DROP VIEW IF EXISTS _gonvex_replica_entities;');
      await db.execAsync(sqliteNativeViewSchema);
      await db.execAsync(ddl.tables + '\n' + ddl.indexes + '\n' + ddl.search);
      // This lookup serves legacy portable ReducerReadView predicates. Typed
      // view plans use the generated column indexes directly, without it.
      await db.execAsync(`CREATE TABLE IF NOT EXISTS _gonvex_replica_lookup (
 scope TEXT NOT NULL,entity TEXT NOT NULL,column_name TEXT NOT NULL,kind TEXT NOT NULL,scalar BLOB NOT NULL,id TEXT NOT NULL,
 PRIMARY KEY(scope,entity,column_name,kind,scalar,id));
CREATE INDEX IF NOT EXISTS _gonvex_replica_lookup_row ON _gonvex_replica_lookup(scope,entity,id);`);
      const branches: string[] = [];
      for (const entity of Object.keys(schema)) {
        const table = sqliteTableName(entity), name = q(table), entitySQL = literal(entity);
        branches.push(`SELECT scope,${entitySQL} entity,id,${sqliteRowBodySQL(schema[entity]!, "t")} value,rid rowid FROM ${name} t`);
        const drop = `DELETE FROM _gonvex_replica_lookup WHERE scope=OLD.scope AND entity=${entitySQL} AND id=OLD.id;`;
        const add = `INSERT INTO _gonvex_replica_lookup SELECT NEW.scope,${entitySQL},j.key,
 CASE WHEN j.type IN ('integer','real') THEN 'number' WHEN j.type IN ('true','false') THEN 'boolean' ELSE 'string' END,j.atom,NEW.id
 FROM json_each(${sqliteRowBodySQL(schema[entity]!, "NEW")}) j WHERE j.type IN ('integer','real','true','false') OR (j.type='text' AND length(j.atom)<=256);`;
        await db.execAsync(`DROP TRIGGER IF EXISTS ${q(table + '_lookup_a')}; DROP TRIGGER IF EXISTS ${q(table + '_lookup_u')}; DROP TRIGGER IF EXISTS ${q(table + '_lookup_d')};
CREATE TRIGGER IF NOT EXISTS ${q(table + '_lookup_a')} AFTER INSERT ON ${name} BEGIN ${add} END;
CREATE TRIGGER IF NOT EXISTS ${q(table + '_lookup_u')} AFTER UPDATE ON ${name} BEGIN ${drop} ${add} END;
CREATE TRIGGER IF NOT EXISTS ${q(table + '_lookup_d')} AFTER DELETE ON ${name} BEGIN ${drop} END;`);
      }
      // Typed imports can precede adapter installation. Triggers alone would
      // leave those rows absent from portable equality/relation reads. Build
      // the compatibility scalar index natively in bounded rowid slices, in
      // the same installation transaction as the signature and triggers.
      if (previous?.signature !== signature) {
        for (const entity of Object.keys(schema)) {
          const name = q(sqliteTableName(entity)); let after = 0;
          await db.runAsync('DELETE FROM _gonvex_replica_lookup WHERE entity=?', entity);
          for (;;) {
            const batch = await db.getFirstAsync<{ last: number | null }>(`SELECT max(rid) last FROM (SELECT rid FROM ${name} WHERE rid>? ORDER BY rid LIMIT 256)`, after);
            if (batch?.last == null) break;
            await db.runAsync(`INSERT INTO _gonvex_replica_lookup SELECT t.scope,?,j.key,
 CASE WHEN j.type IN ('integer','real') THEN 'number' WHEN j.type IN ('true','false') THEN 'boolean' ELSE 'string' END,j.atom,t.id
 FROM ${name} t,json_each(${sqliteRowBodySQL(schema[entity]!, 't')}) j WHERE t.rid>? AND t.rid<=? AND (j.type IN ('integer','real','true','false') OR (j.type='text' AND length(j.atom)<=256))`, entity, after, batch.last);
            after = batch.last; await yieldSQLiteTask();
          }
        }
      }
      await db.runAsync('INSERT INTO gv_canonical_layout VALUES(1,?) ON CONFLICT(id) DO UPDATE SET signature=excluded.signature', signature);
      branches.push("SELECT scope,entity,id,'null' value,NULL rowid FROM gv_canonical_tombstones");
      if (branches.length > 500) throw new RangeError('Typed canonical schema exceeds SQLite compound table limit');
      await db.execAsync(`CREATE VIEW IF NOT EXISTS _gonvex_replica_entities AS ${branches.join(' UNION ALL ')};`);
      const deletes = Object.keys(schema).map(entity => `DELETE FROM ${q(sqliteTableName(entity))} WHERE scope=OLD.scope AND id=OLD.id AND OLD.entity=${literal(entity)};`).join('\n');
      await db.execAsync(`CREATE TRIGGER IF NOT EXISTS gv_canonical_delete INSTEAD OF DELETE ON _gonvex_replica_entities BEGIN
${deletes}
DELETE FROM gv_canonical_tombstones WHERE scope=OLD.scope AND entity=OLD.entity AND id=OLD.id;
END;`);
      lease = await db.getFirstAsync<{ epoch: number; owner: string }>('SELECT epoch,owner FROM gv_storage_meta WHERE id=1') ?? undefined;
    },
    async resetViews(db, scope) {
      await db.runAsync('DELETE FROM gv_view_members WHERE scope=?', scope);
      await db.runAsync('DELETE FROM gv_views WHERE scope=?', scope);
    },
    async readViewPage(db, scope, signature, plan, offset, sequence) {
      compileSQLiteTypedView(schema, scope, plan);
      return readSQLiteTypedView(db, schema, scope, scope, 'canonical:' + signature, plan, offset, sequence, 262144);
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
          values.push(scope, id, sqliteBodySkeleton(row!, table), 0, ...fields.map(([field, shape]) => {
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
