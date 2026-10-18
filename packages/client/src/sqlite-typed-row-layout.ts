import {sqliteWindowAuxiliarySchema} from './sqlite-window-store.js';
import {sqliteTypedIndexSQL} from './sqlite-typed-schema.js';
import {sqliteEntityAliasSchema,sqlitePhysicalEntityScope,sqliteQueryEntityScopes} from './sqlite-entity-scope.js';
import { readSQLitePortablePage } from './sqlite-portable-page.js';
import { fail } from './sqlite-admission.js';
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
  let lease: { epoch: number; owner: string } | undefined;
  async function assertOwner(db: SQLiteReplicaDatabase) {
    const current = await db.getFirstAsync<{ epoch: number; owner: string; sequence: number }>('SELECT epoch,owner,sequence FROM gv_storage_meta WHERE id=1');
    if (!lease || current?.epoch !== lease.epoch || current.owner !== lease.owner) fail('FENCED', 'Canonical SQLite owner was replaced');
    return current;
  }
  return {
    async initializeSequences(db) {
      // Scope discovery must not expand every generated row reconstruction.
      // It is metadata-only and creates no decoded rows or JS scope corpus.
      const scopes = Object.keys(schema).map(entity => `SELECT scope FROM ${q(sqliteTableName(entity))}`).concat([
        'SELECT scope FROM gv_canonical_tombstones',
        'SELECT scope FROM gv_scope_alias',
        'SELECT scope FROM gv_entity_alias',
        'SELECT scope FROM _gonvex_replica_queries',
        "SELECT scope FROM _gonvex_replica_meta WHERE key='cursor'",
      ]);
      const groups: string[] = [];
      for (let offset = 0; offset < scopes.length; offset += 128) groups.push('SELECT scope FROM (' + scopes.slice(offset, offset + 128).join(' UNION ') + ')');
      await db.runAsync(`INSERT OR IGNORE INTO _gonvex_replica_meta SELECT scope,'sequence','0' FROM (${groups.join(' UNION ')}) WHERE instr(scope,char(0))=0`);
    },
    async readEntityRows(db, scope, entity, ids, maxBytes) {
      const table = Object.hasOwn(schema, entity) && schema[entity];
      if (!table) throw new TypeError('Canonical read references an undeclared SQLite table');
      if (!ids.length) return [];
      if (ids.length > 256) throw new RangeError('Canonical entity read exceeds page budget');
      const physical = await sqlitePhysicalEntityScope(db, scope, entity);
      const body = sqliteRowBodySQL(table, 't');
      const from = `${q(sqliteTableName(entity))} t WHERE t.scope=? AND t.id IN (${ids.map(() => '?').join(',')})`;
      const args = [physical, ...ids];
      const size = await db.getFirstAsync<{ bytes: number }>(`SELECT coalesce(sum(length(CAST(${body} AS BLOB))*2+length(t.id)*2+128),0) bytes FROM ${from}`, ...args);
      if (!size || size.bytes > maxBytes) fail('BUDGET', 'Native entity bodies exceed page budget');
      // Restrict to the generated physical table before preparing the plan.
      // Expanding the all-table compatibility view for one row retains SQL
      // plans proportional to the entire application schema.
      return db.getAllAsync<{ id: string; value: string }>(`SELECT t.id,${body} value FROM ${from}`, ...args);
    },
    async readVersions(db, scope, entity, ids) {
      if (!Object.hasOwn(schema, entity) || ids.length > 100) throw new RangeError('Invalid native authority page');
      if (!ids.length) return new Map();
      const physical=await sqlitePhysicalEntityScope(db,scope,entity);
      const rows = await db.getAllAsync<{id:string;version:number}>(`SELECT id,version FROM ${q(sqliteTableName(entity))} WHERE scope=? AND id IN (${ids.map(()=>'?').join(',')})`, physical, ...ids);
      return new Map(rows.map(row => [row.id,row.version]));
    },
    readPage: (db,scope,read,after,excluded,columns) => readSQLitePortablePage(db,schema,scope,read,after,excluded,columns),
    async assertOwner(db) { await assertOwner(db); },
    async afterCommit(db) {
      const current = await assertOwner(db);
      if (!Number.isSafeInteger(current.sequence) || current.sequence >= Number.MAX_SAFE_INTEGER) fail('FENCED', 'SQLite storage sequence exhausted');
      await db.runAsync('UPDATE gv_storage_meta SET sequence=sequence+1 WHERE id=1');
    },
    async install(db) {
      const ddl = compileSQLiteTypedSchema(schema);
      const signature = JSON.stringify({ format: 6, schema });
      const flags = await db.getFirstAsync("SELECT name FROM sqlite_master WHERE type='table' AND name='gv_storage_flags'");
      if (flags && await db.getFirstAsync("SELECT value FROM gv_storage_flags WHERE name='bootstrap-indexes'")) throw new Error('Finish or abort the restartable bootstrap before installing the canonical adapter');
      const legacy = await db.getFirstAsync<{ type: string }>("SELECT type FROM sqlite_master WHERE name='_gonvex_replica_entities'");
      if (legacy?.type === 'table') throw new Error('Existing generic SQLite rows require a verified migration before typed storage opens');
      await db.execAsync(`CREATE TABLE IF NOT EXISTS gv_canonical_layout(id INTEGER PRIMARY KEY CHECK(id=1),signature TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gv_scope_alias(scope TEXT PRIMARY KEY,physical TEXT NOT NULL) WITHOUT ROWID;
${sqliteEntityAliasSchema}
CREATE INDEX IF NOT EXISTS gv_scope_alias_physical ON gv_scope_alias(physical);
CREATE TABLE IF NOT EXISTS gv_canonical_tombstones(scope TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(scope,entity,id)) WITHOUT ROWID;`);
      const previous = await db.getFirstAsync<{ matches: number }>('SELECT signature=? matches FROM gv_canonical_layout WHERE id=1', signature);
      if (previous && !previous.matches) {
        let compatible = false;
        for (const format of [1, 2, 3, 4, 5]) {
          const match = await db.getFirstAsync<{ matches: number }>('SELECT signature=? matches FROM gv_canonical_layout WHERE id=1', JSON.stringify({ format, schema }));
          if (match?.matches) { compatible = true; break; }
        }
        if (!compatible) throw new Error('Typed canonical layout requires an explicit schema migration');
      }
      await installSQLiteTypedBodyFence(db, schema);
      await db.execAsync('DROP TRIGGER IF EXISTS gv_canonical_delete; DROP VIEW IF EXISTS _gonvex_replica_entities;');
      await db.execAsync(sqliteNativeViewSchema);
      await db.execAsync(ddl.tables+'\n'+sqliteWindowAuxiliarySchema);
      await db.execAsync(await sqliteTypedIndexSQL(db,schema) + '\n' + ddl.search);
      const branches: string[] = [];
      for (const entity of Object.keys(schema)) {
        const table = sqliteTableName(entity), name = q(table), entitySQL = literal(entity);
        const values = ['id', 'body', 'version', ...Object.keys(schema[entity]!.fields).map(sqliteFieldName), ...(schema[entity]!.search ?? []).map((_, i) => 's' + i)].map(field => 't.' + q(field)).join(',');
        // Reconstruct each body's typed fields once, outside the three scope
        // alias branches. The compatibility view must not retain three copies
        // of every generated JSON expression in SQLite's schema tree.
        branches.push(`SELECT t.scope,${entitySQL} entity,t.id,${sqliteRowBodySQL(schema[entity]!, "t")} value,t.rid rowid FROM (
 SELECT t.* FROM ${name} t
 WHERE NOT EXISTS(SELECT 1 FROM gv_scope_alias a WHERE a.physical=t.scope OR a.scope=t.scope)
 AND NOT EXISTS(SELECT 1 FROM gv_entity_alias a WHERE a.entity=${entitySQL} AND (a.physical=t.scope OR a.scope=t.scope))
 UNION ALL SELECT t.rid,a.scope,${values} FROM gv_scope_alias a JOIN ${name} t ON t.scope=a.physical
 WHERE NOT EXISTS(SELECT 1 FROM gv_entity_alias e WHERE e.scope=a.scope AND e.entity=${entitySQL})
 UNION ALL SELECT t.rid,a.scope,${values} FROM gv_entity_alias a JOIN ${name} t ON t.scope=a.physical WHERE a.entity=${entitySQL}) t`);
        await db.execAsync(`DROP TRIGGER IF EXISTS ${q(table + '_lookup_a')}; DROP TRIGGER IF EXISTS ${q(table + '_lookup_u')}; DROP TRIGGER IF EXISTS ${q(table + '_lookup_d')};`);
      }
      // Portable reads use generated columns and their native indexes. Remove
      // the legacy scalar-copy index atomically only after its triggers retire.
      await db.execAsync(`DROP TRIGGER IF EXISTS gv_canonical_alias_insert; DROP TRIGGER IF EXISTS gv_canonical_alias_update;
DROP TABLE IF EXISTS _gonvex_replica_lookup;`);
      await db.runAsync('INSERT INTO gv_canonical_layout VALUES(1,?) ON CONFLICT(id) DO UPDATE SET signature=excluded.signature', signature);
      branches.push("SELECT scope,entity,id,'null' value,NULL rowid FROM gv_canonical_tombstones");
      if (branches.length > 500) throw new RangeError('Typed canonical schema exceeds SQLite compound table limit');
      await db.execAsync(`CREATE VIEW IF NOT EXISTS _gonvex_replica_entities AS ${branches.join(' UNION ALL ')};`);
      const deletes = Object.keys(schema).map(entity => `DELETE FROM ${q(sqliteTableName(entity))} WHERE scope=coalesce((SELECT physical FROM gv_entity_alias WHERE scope=OLD.scope AND entity=${literal(entity)}),(SELECT physical FROM gv_scope_alias WHERE scope=OLD.scope),OLD.scope) AND id=OLD.id AND OLD.entity=${literal(entity)};`).join('\n');
      await db.execAsync(`CREATE TRIGGER IF NOT EXISTS gv_canonical_delete INSTEAD OF DELETE ON _gonvex_replica_entities BEGIN
${deletes}
DELETE FROM gv_canonical_tombstones WHERE scope=OLD.scope AND entity=OLD.entity AND id=OLD.id;
END;`);
      lease = await db.getFirstAsync<{ epoch: number; owner: string }>('SELECT epoch,owner FROM gv_storage_meta WHERE id=1') ?? undefined;
    },
    async resetViews(db, scope) {
      if(await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gv_windows'")) {
        for(const table of ['gv_windows','gv_window_generations','gv_window_members','gv_window_hashes','gv_window_frames','gv_snapshot_window_alias','gv_window_mutations','gv_window_certificates','gv_window_delta_receipts'])await db.runAsync('DELETE FROM '+table+' WHERE scope=?',scope);
      }
      await db.runAsync('DELETE FROM gv_view_members WHERE scope=?', scope);
      await db.runAsync('DELETE FROM gv_views WHERE scope=?', scope);
    },
    async readViewPage(db, scope, signature, plan, offset, sequence) {
      compileSQLiteTypedView(schema, scope, plan);
      const resolved=await sqliteQueryEntityScopes(db,scope,plan),physical=resolved.physical;
      const generation = (await assertOwner(db)).sequence;
      return readSQLiteTypedView(db, schema, scope, physical, 'canonical:' + signature, plan, offset, sequence, 262144, generation,undefined,resolved.sources);
    },
    async upsert(db, scope, entity, rows) {
      const table = Object.hasOwn(schema, entity) && schema[entity];
      if (!table) throw new TypeError('Canonical write references an undeclared SQLite table');
      // Preserve per-field authority from the shared storage adapter. Version
      // is the baseline for fields present in native imports; sparse metadata
      // retains later per-field overrides. Preserve the baseline on updates.
      sqlitePayloadBytes(rows, 262144);
      const physical=await sqlitePhysicalEntityScope(db,scope,entity);
      const fields = Object.entries(table.fields), name = q(sqliteTableName(entity));
      const columns = ['scope', 'id', 'body', 'version', ...fields.map(([field]) => sqliteFieldName(field)), ...(table.search ?? []).map((_, i) => 's' + i)];
      for (let offset = 0; offset < rows.length;) {
        const item = rows[offset]!;
        if (!item.id) throw new TypeError('Canonical row requires an identity');
        if (item.row === null) {
          await db.runAsync(`DELETE FROM ${name} WHERE scope=? AND id=?`, physical, item.id);
          await db.runAsync('INSERT OR IGNORE INTO gv_canonical_tombstones VALUES(?,?,?)', scope, entity, item.id);
          offset++; continue;
        }
        const values: unknown[] = [], page: string[] = [], ids: string[] = [];
        const maxRows = Math.min(100, Math.floor(32000 / columns.length));
        while (offset < rows.length && page.length < maxRows && rows[offset]!.row !== null) {
          const { id, row } = rows[offset++]!;
          if (!id || String(row![table.key]) !== id) throw new TypeError('Canonical SQLite key differs from its row identity');
          values.push(physical, id, sqliteBodySkeleton(row!, table), 0, ...fields.map(([field, shape]) => {
            const value = row![field];
            if (value == null) return null;
            if (shape.kind !== 'json' && typeof value !== (shape.kind === 'text' ? 'string' : shape.kind === 'number' ? 'number' : 'boolean')) throw new TypeError('Canonical SQLite field type differs from schema');
            return shape.kind === 'json' ? JSON.stringify(value) : shape.kind === 'boolean' ? Number(value) : value;
          }), ...(table.search ?? []).map(field => String(row![field] ?? '').toLowerCase()));
          ids.push(id); page.push('(' + columns.map(() => '?').join(',') + ')');
        }
        await db.runAsync(`DELETE FROM gv_canonical_tombstones WHERE scope=? AND entity=? AND id IN (${ids.map(() => '?').join(',')})`, scope, entity, ...ids);
        await db.runAsync(`INSERT INTO ${name}(${columns.map(q).join(',')}) VALUES ${page.join(',')} ON CONFLICT(scope,id) DO UPDATE SET ${columns.slice(2).filter(column => column !== 'version').map(column => `${q(column)}=excluded.${q(column)}`).join(',')}`, ...values);
      }
    },
  };
}

export function sqliteTypedReplicaStorage(db: SQLiteReplicaDatabase, schema: SQLiteTypedSchema): SQLiteLocalReplicaStorage {
  return new SQLiteLocalReplicaStorage(db, sqliteTypedRowLayout(schema));
}
