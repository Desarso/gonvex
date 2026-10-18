import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import type { ReplicaRow, ReplicaWindow, ReplicaStorageCommit, ReplicaStorageChanges, LocalReplicaStorage } from './local-replica.js';

type Interest = Parameters<NonNullable<LocalReplicaStorage['readChanges']>>[2];
type ChangeKey = { kind: 'row' | 'window'; entity: string; id: string; bytes: number | null; windowKind: string | null; deleted: number | null };
const schema = `CREATE TABLE IF NOT EXISTS _gonvex_replica_changes (
 scope TEXT NOT NULL,kind TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,sequence INTEGER NOT NULL,
 PRIMARY KEY(scope,kind,entity,id));
 CREATE INDEX IF NOT EXISTS _gonvex_replica_changed_sequence ON _gonvex_replica_changes(scope,sequence);`;
function integer(value: string | undefined): number {
  const result = Number(value ?? 0);
  if (!Number.isSafeInteger(result) || result < 0) throw new Error('Invalid SQLite replica sequence');
  return result;
}

/** Latest-change index, not another row-value store or an in-memory event log.
 * The storage actor serializes whole calls on this connection. Every recorded
 * key and its canonical value/cursor must commit in the same SQL transaction.
 * Deleted rows stay canonical tombstones; removed windows join to no value.
 * This does not normalize large window JSON or solve snapshot staging. */
export class SQLiteReplicaCommits {
  private initialized?: Promise<void>;
  private active?: ReplicaStorageCommit;
  private committing = false;
  constructor(private readonly database: SQLiteReplicaDatabase, private readonly onCommit?: (scope: string) => void, private readonly controls: { before?: () => Promise<void>; after?: () => Promise<void> } = {}) {}
  initialize() {
    return this.initialized ??= this.database.withTransactionAsync(async () => {
      const present=await this.database.getFirstAsync<{name:string}>("SELECT name FROM sqlite_master WHERE type='table' AND name='_gonvex_replica_changes'");
      if(present)return;
      await this.database.execAsync(schema);
      // Establish the logical zero sequence for existing scopes without reading
      // row payloads into JS. The table and these counters publish atomically.
      await this.database.runAsync(`INSERT OR IGNORE INTO _gonvex_replica_meta
        SELECT scope,'sequence','0' FROM (
          SELECT scope FROM _gonvex_replica_entities UNION SELECT scope FROM _gonvex_replica_queries
          UNION SELECT scope FROM _gonvex_replica_meta WHERE key='cursor')`);
    });
  }
  private receipt(scope: string) {
    if (!this.active || this.active.scope !== scope) throw new Error('SQLite change recording outside its commit');
    return this.active;
  }
  async commit(scope: string, run: () => Promise<void>): Promise<ReplicaStorageCommit> {
    await this.initialize();
    if (this.committing) throw new Error('Concurrent SQLite commits outside the storage actor');
    this.committing = true;
    let receipt!: ReplicaStorageCommit;
    try { await this.database.withTransactionAsync(async () => {
      await this.controls.before?.();
      const previous = integer((await this.database.getFirstAsync<{ value: string }>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='sequence'", scope))?.value);
      if (previous === Number.MAX_SAFE_INTEGER) throw new RangeError('SQLite replica sequence exhausted');
      receipt = { scope, previousSequence: previous, sequence: previous + 1, changes: { sequence: previous + 1, entities: Object.create(null), windows: Object.create(null) } };
      this.active = receipt;
      try {
        await run();
        await this.controls.after?.();
        await this.database.runAsync("INSERT OR REPLACE INTO _gonvex_replica_meta VALUES(?,'sequence',?)", scope, String(receipt.sequence));
      } finally { this.active = undefined; }
    });
    } finally { this.committing = false; this.active = undefined; }
    // A failed notification must never turn a durable commit into a rejection.
    try { this.onCommit?.(scope); } catch { /* Peers can reconcile on next read. */ }
    return receipt;
  }
  private async keys(scope: string, kind: ChangeKey['kind'], entity: string, ids: readonly string[]) {
    const receipt = this.receipt(scope);
    for (let offset = 0; offset < ids.length; offset += 100) {
      const page = ids.slice(offset, offset + 100);
      if (!page.length) continue;
      await this.database.runAsync(`INSERT INTO _gonvex_replica_changes VALUES ${page.map(() => '(?,?,?,?,?)').join(',')}
       ON CONFLICT(scope,kind,entity,id) DO UPDATE SET sequence=excluded.sequence`,
      ...page.flatMap(id => [scope, kind, entity, id, receipt.sequence]));
    }
  }
  async rows(scope: string, entity: string, rows: readonly { id: string; row: ReplicaRow | null }[]) {
    const receipt = this.receipt(scope);
    await this.keys(scope, 'row', entity, rows.map(entry => entry.id));
    // Snapshot activation publishes one reset receipt. Its complete change-key
    // index remains on disk; never retain all activated row values in a receipt.
    if (!receipt.changes.reset)
      for (const { id, row } of rows) (receipt.changes.entities[entity] ??= Object.create(null))[id] = row;
  }
  async window(scope: string, signature: string, window: ReplicaWindow | null) {
    await this.keys(scope, 'window', '', [signature]);
    this.receipt(scope).changes.windows[signature] = window;
  }
  changes(scope: string): ReplicaStorageChanges { return this.receipt(scope).changes; }
  async reset(scope: string) {
    const receipt = this.receipt(scope);
    await this.database.runAsync('DELETE FROM _gonvex_replica_changes WHERE scope=?', scope);
    await this.database.runAsync("INSERT OR REPLACE INTO _gonvex_replica_meta VALUES(?,'resetSequence',?)", scope, String(receipt.sequence));
    receipt.changes.reset = true;
  }
  /** A full replacement can index its newly stored rows without decoding a
   * duplicate whole-corpus receipt. The caller already owns the input snapshot. */
  async indexReplacement(scope: string) {
    const receipt = this.receipt(scope);
    await this.database.runAsync("INSERT OR REPLACE INTO _gonvex_replica_changes SELECT scope,'row',entity,id,? FROM _gonvex_replica_entities WHERE scope=?", receipt.sequence, scope);
    await this.database.runAsync("INSERT OR REPLACE INTO _gonvex_replica_changes SELECT scope,'window','',signature,? FROM _gonvex_replica_queries WHERE scope=?", receipt.sequence, scope);
  }
  async sequence(scope: string) {
    await this.initialize();
    return integer((await this.database.getFirstAsync<{ value: string }>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='sequence'", scope))?.value);
  }
  async readChanges(scope: string, afterSequence: number, interest?: Interest): Promise<ReplicaStorageChanges> {
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) throw new RangeError('Invalid prior SQLite sequence');
    await this.initialize();
    let result!: ReplicaStorageChanges;
    await this.database.withTransactionAsync(async () => {
      await this.controls.before?.();
      const sequence = await this.sequence(scope);
      const reset = afterSequence < integer((await this.database.getFirstAsync<{ value: string }>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='resetSequence'", scope))?.value);
      if (sequence < afterSequence) throw new Error('SQLite replica sequence moved backwards');
      result = { sequence, ...(reset ? { reset: true } : {}), entities: Object.create(null), windows: Object.create(null) };
      if (sequence === afterSequence) return;
      const wanted = new Map(Object.entries(interest?.rows ?? {}).map(([table, ids]) => [table, new Set(ids)]));
      const windows = new Set(interest?.windows ?? []);
      let availableRows = interest ? reset ? interest.maxRows : interest.availableRows : Infinity;
      let availableBytes = interest ? reset ? interest.maxBytes : interest.availableBytes : Infinity;
      let outputRows = 0, outputBytes = 0;
      let after: [string, string, string] = ['\uffff', '', ''];
      for (;;) {
        // Fetch small key/size records first. Cold row JSON is never decoded
        // just to decide whether a resident consumer is interested in it.
        const page = await this.database.getAllAsync<ChangeKey>(`SELECT c.kind,c.entity,c.id,
          CASE WHEN c.kind='row' THEN length(CAST(e.value AS BLOB))*2+256 ELSE length(CAST(q.value AS BLOB))*2+256 END AS bytes,json_extract(q.value,'$.kind') AS windowKind,e.value='null' AS deleted
          FROM _gonvex_replica_changes c
          LEFT JOIN _gonvex_replica_entities e ON c.kind='row' AND e.scope=c.scope AND e.entity=c.entity AND e.id=c.id
          LEFT JOIN _gonvex_replica_queries q ON c.kind='window' AND q.scope=c.scope AND q.signature=c.id
          WHERE c.scope=? AND c.sequence>? AND (c.kind<? OR (c.kind=? AND (c.entity,c.id)>(?,?))) ORDER BY c.kind DESC,c.entity,c.id LIMIT 128`, scope, afterSequence, after[0], ...after);
        if (!page.length) break;
        for (const key of page) {
          after = [key.kind, key.entity, key.id];
          const requested = !interest || (key.kind === 'row'
            ? interest.hasRow?.(key.entity, key.id) || wanted.get(key.entity)?.has(key.id)
            : windows.has(key.id) || key.windowKind === 'replica' || key.bytes === null);
          if (key.kind === 'window' && !requested) continue;
          const bytes = key.bytes ?? 256;
          if (!requested && (key.deleted || availableRows < 1 || bytes > availableBytes)) continue;
          if (interest && (outputRows + (key.kind === 'row' ? 1 : 0) > interest.maxRows || outputBytes + bytes > interest.maxBytes)) {
            if (!requested) continue;
            throw new RangeError('SQLite replica change result budget exceeded');
          }
          const record = key.kind === 'row'
            ? await this.database.getFirstAsync<{ value: string }>('SELECT value FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?', scope, key.entity, key.id)
            : await this.database.getFirstAsync<{ value: string }>('SELECT value FROM _gonvex_replica_queries WHERE scope=? AND signature=?', scope, key.id);
          const value = record ? JSON.parse(record.value) : null;
          if (key.kind === 'row') (result.entities[key.entity] ??= Object.create(null))[key.id] = value;
          else {
            result.windows[key.id] = value;
            if (value && windows.has(key.id)) {
              let ids = wanted.get(value.entity); if (!ids) wanted.set(value.entity, ids = new Set());
              for (const id of value.ids) ids.add(id);
            }
          }
          if (key.kind === 'row') outputRows++; outputBytes += bytes;
          if (!requested) { availableRows--; availableBytes -= bytes; }
        }
        if (page.length < 128) break;
      }
    });
    return result;
  }
}
