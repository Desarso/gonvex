import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { compileSQLiteTypedSchema, sqliteIdentifier as q, sqliteTableName, sqliteFieldName, sqliteSearchName, sqliteCountName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
import { compileSQLiteTypedPage, type SQLitePagePlan, type SQLiteKeyset } from './sqlite-typed-query.js';

export class SQLiteReplicaEngineError extends Error {
  constructor(readonly code: 'FENCED' | 'SCHEMA_MISMATCH' | 'BUDGET' | 'INTENT_CONFLICT' | 'INVALID_ROW' | 'PAGE_CHANGED', message: string) { super(message); this.name = 'SQLiteReplicaEngineError'; }
}
export type SQLiteTypedWrite = { table: string; id: string; version: number; row: Record<string, unknown> | null };
export type SQLiteTypedPage = { rows: Record<string, unknown>[]; cursor: SQLiteKeyset | null; bytes: number; sequence: number };
export type SQLiteBootstrapState = { token: string; nextPage: number; rows: number; root: string; complete: boolean };
type SQLiteBootstrapRecord = { scope: string; token: string; physical: string; next_page: number; rows: number; root: string; base_sequence: number; complete: number };
function bootstrapState(row: SQLiteBootstrapRecord): SQLiteBootstrapState { return { token: row.token, nextPage: row.next_page, rows: row.rows, root: row.root, complete: !!row.complete }; }
async function sha256(text: string): Promise<string> {
  const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text)));
  return Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
}
function pageBudgetError(error: unknown): boolean { return !!error && typeof error === 'object' && 'code' in error && (error.code === 'BUDGET' || error.code === 'sqlite-bridge-budget'); }
function fail(code: SQLiteReplicaEngineError['code'], message: string): never { throw new SQLiteReplicaEngineError(code, message); }

/** Conservative JS admission accounting before JSON serialization or cloning.
 * Only own plain JSON data is accepted, with bounded depth and property count. */
export function sqlitePayloadBytes(value: unknown, maxBytes: number): number {
  let bytes = 0, nodes = 0;
  const stack = new Set<object>();
  const visit = (current: unknown, depth: number): void => {
    if (++nodes > 100_000 || depth > 32) fail('BUDGET', 'SQLite payload structure exceeds budget');
    bytes += 16;
    if (typeof current === 'string') bytes += current.length * 2;
    else if (typeof current === 'number') { if (!Number.isFinite(current)) fail('INVALID_ROW', 'Nonfinite SQLite JSON number'); }
    else if (current !== null && typeof current !== 'boolean') {
      if (typeof current !== 'object' || !Array.isArray(current) && Object.getPrototypeOf(current) !== Object.prototype && Object.getPrototypeOf(current) !== null)
        fail('INVALID_ROW', 'SQLite payload must be plain JSON data');
      const object = current as object;
      if (Array.isArray(object) && (object.length > 100_000 || object.length * 16 > maxBytes || Object.keys(object).length !== object.length)) fail('BUDGET', 'SQLite arrays must be dense and fit the byte budget');
      if (stack.has(object)) fail('INVALID_ROW', 'Cyclic SQLite payload');
      stack.add(object);
      for (const key of Object.keys(object)) {
        bytes += key.length * 2 + 16;
        if (bytes > maxBytes) fail('BUDGET', 'SQLite payload byte budget exceeded');
        const descriptor = Object.getOwnPropertyDescriptor(object, key)!;
        if (!('value' in descriptor)) fail('INVALID_ROW', 'SQLite payload accessor rejected');
        if (descriptor.value !== undefined || Array.isArray(object)) visit(descriptor.value, depth + 1);
      }
      stack.delete(object);
    }
    if (bytes > maxBytes) fail('BUDGET', 'SQLite payload byte budget exceeded');
  };
  visit(value, 0);
  return bytes;
}

/** JSON.parse owns this graph: no accessors, cycles or foreign prototypes.
 * Avoid allocating a descriptor and key array for every field on the trusted
 * wire path. Still enforce graph/depth/finite-number budgets before ingestion. */
function parsedPayloadBytes(value: unknown, maxBytes: number): number {
  let bytes = 0, nodes = 0;
  const visit = (item: any, depth: number): void => {
    if (++nodes > 100000 || depth > 32) fail('BUDGET', 'SQLite parsed payload structure exceeds budget');
    bytes += 16;
    if (typeof item === 'string') bytes += item.length * 2;
    else if (typeof item === 'number' && !Number.isFinite(item)) fail('INVALID_ROW', 'Nonfinite SQLite JSON number');
    else if (item && typeof item === 'object') for (const key in item) {
      if (!Object.hasOwn(item, key)) continue;
      bytes += key.length * 2 + 16; visit(item[key], depth + 1);
    }
    if (bytes > maxBytes) fail('BUDGET', 'SQLite parsed payload byte budget exceeded');
  };
  visit(value, 0); return bytes;
}

/** One instance runs inside the elected owner worker. Every public operation
 * enters this bounded FIFO. Intent, row, FTS, counts and sequence commit together.
 * It never exposes raw SQL or accepts tenant routing as an application argument. */
export class SQLiteTypedReplicaEngine {
  private lane = Promise.resolve();
  private pending = 0;
  private pendingBytes = 0;
  private epoch = 0;
  private owner = '';
  private initialized = false;
  constructor(private readonly db: SQLiteReplicaDatabase, readonly schema: SQLiteTypedSchema,
    private readonly limits = { operations: 64, queuedBytes: 2_097_152, batchBytes: 262_144, pageBytes: 262_144 }) {
    compileSQLiteTypedSchema(schema);
    for (const value of Object.values(limits)) if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Positive SQLite limits required');
  }
  private enqueue<T>(bytes: number, task: () => Promise<T>): Promise<T> {
    if (this.pending >= this.limits.operations || this.pendingBytes + bytes > this.limits.queuedBytes)
      return Promise.reject(new SQLiteReplicaEngineError('BUDGET', 'SQLite operation queue is full'));
    this.pending++; this.pendingBytes += bytes;
    const result = this.lane.then(task);
    const release = () => { this.pending--; this.pendingBytes -= bytes; };
    this.lane = result.then(release, release);
    return result;
  }
  async open(owner: string): Promise<number> {
    if (!owner || owner.length > 128) throw new TypeError('Invalid SQLite owner');
    return this.enqueue(256, async () => {
      if (this.initialized) throw new Error('SQLite owner already opened');
      const ddl = compileSQLiteTypedSchema(this.schema);
      await this.db.withTransactionAsync(async () => {
        await this.db.execAsync(`CREATE TABLE IF NOT EXISTS gv_storage_meta(id INTEGER PRIMARY KEY CHECK(id=1), schema TEXT NOT NULL, epoch INTEGER NOT NULL, owner TEXT NOT NULL, sequence INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS gv_tombstones(scope TEXT NOT NULL, entity TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(scope,entity,id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_intent_receipts(scope TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(scope,id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_outbox(scope TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY(scope,id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_outbox_sequence ON gv_outbox(scope,sequence);
CREATE TABLE IF NOT EXISTS gv_scope_alias(scope TEXT PRIMARY KEY,physical TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_bootstrap(scope TEXT PRIMARY KEY,token TEXT UNIQUE NOT NULL,physical TEXT NOT NULL,next_page INTEGER NOT NULL,rows INTEGER NOT NULL,root TEXT NOT NULL,base_sequence INTEGER NOT NULL,complete INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_bootstrap_pages(token TEXT NOT NULL,page INTEGER NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(token,page)) WITHOUT ROWID;`);
        const signature = JSON.stringify({ format: 3, tables: this.schema });
        const old = await this.db.getFirstAsync<{ schema: string; epoch: number }>('SELECT schema,epoch FROM gv_storage_meta WHERE id=1');
        if (old && old.schema !== signature) fail('SCHEMA_MISMATCH', 'SQLite layout requires an explicit migration');
        await this.db.execAsync(ddl.tables + '\n' + ddl.indexes + '\n' + ddl.search);
        this.epoch = (old?.epoch ?? 0) + 1;
        if (!Number.isSafeInteger(this.epoch)) fail('FENCED', 'SQLite epoch exhausted');
        await this.db.runAsync('INSERT INTO gv_storage_meta VALUES(1,?,?,?,0) ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch,owner=excluded.owner', signature, this.epoch, owner);
      });
      this.owner = owner; this.initialized = true;
      return this.epoch;
    });
  }
  private async fence(): Promise<number> {
    if (!this.initialized) fail('FENCED', 'SQLite owner is not ready');
    const lease = await this.db.getFirstAsync<{ epoch: number; owner: string; sequence: number }>('SELECT epoch,owner,sequence FROM gv_storage_meta WHERE id=1');
    if (!lease || lease.epoch !== this.epoch || lease.owner !== this.owner) fail('FENCED', 'SQLite owner epoch is stale');
    return lease.sequence;
  }
  private async physicalScope(scope: string): Promise<string> {
    if (!scope || scope.length > 512 || scope.startsWith('\u0000bootstrap:')) throw new TypeError('Invalid SQLite public scope');
    return (await this.db.getFirstAsync<{ physical: string }>('SELECT physical FROM gv_scope_alias WHERE scope=?', scope))?.physical ?? scope;
  }
  private validateWrite(input: SQLiteTypedWrite) {
    const table = Object.hasOwn(this.schema, input.table) && this.schema[input.table];
    if (!table || typeof input.id !== 'string' || !input.id || !Number.isSafeInteger(input.version) || input.version < 0) fail('INVALID_ROW', 'Invalid SQLite row identity/version');
    if (input.row === null) return table;
    if (input.row[table.key] == null || String(input.row[table.key]) !== input.id) fail('INVALID_ROW', 'SQLite row key does not match its identity');
    for (const field in table.fields) {
      const shape = table.fields[field]!;
      const value = input.row[field];
      if (value != null && (shape.kind === 'text' && typeof value !== 'string' || shape.kind === 'number' && typeof value !== 'number' || shape.kind === 'boolean' && typeof value !== 'boolean'))
        fail('INVALID_ROW', 'SQLite field type differs from the generated schema');
    }
    return table;
  }
  private async writeBatch(scope: string, writes: readonly SQLiteTypedWrite[]): Promise<void> {
    // Preserve transaction order, including delete/reinsert chains. Only
    // consecutive upserts for the same generated table share a SQL statement.
    for (let offset = 0; offset < writes.length;) {
      const input = writes[offset]!, table = this.validateWrite(input), tn = q(sqliteTableName(input.table));
      if (input.row === null) {
        await this.db.runAsync('INSERT INTO gv_tombstones VALUES(?,?,?,?) ON CONFLICT(scope,entity,id) DO UPDATE SET version=max(version,excluded.version)', scope, input.table, input.id, input.version);
        await this.db.runAsync(`DELETE FROM ${tn} WHERE scope=? AND id=? AND version<=?`, scope, input.id, input.version);
        offset++; continue;
      }
      const fields = Object.entries(table.fields);
      const columns = ['scope', 'id', 'body', 'version', ...fields.map(([field]) => sqliteFieldName(field)), ...(table.search ?? []).map((_, i) => 's' + i)];
      const selects: string[] = [], bindings: unknown[] = [];
      const select = `SELECT ${columns.map(() => '?').join(',')} WHERE NOT EXISTS(SELECT 1 FROM gv_tombstones WHERE scope=? AND entity=? AND id=? AND version>=?)`;
      let end = offset;
      const batchRows = Math.max(1, Math.min(100, Math.floor(32000 / (columns.length + 4))));
      while (end < writes.length && end - offset < batchRows && writes[end]!.table === input.table && writes[end]!.row !== null) {
        const item = writes[end]!; this.validateWrite(item);
        bindings.push(scope, item.id, JSON.stringify(item.row), item.version, ...fields.map(([field, shape]) => {
          const value = item.row![field];
          return value == null ? null : shape.kind === 'json' ? JSON.stringify(value) : shape.kind === 'boolean' ? Number(value) : value;
        }), ...(table.search ?? []).map(field => String(item.row![field] ?? '').toLowerCase()));
        selects.push(select);
        bindings.push(scope, item.table, item.id, item.version);
        end++;
      }
      const head = `INSERT INTO ${tn}(${columns.map(q).join(',')}) `;
      const tail = ` ON CONFLICT(scope,id) DO UPDATE SET ${columns.slice(2).map(column => `${q(column)}=excluded.${q(column)}`).join(',')} WHERE ${tn}.version<=excluded.version`;
      await this.db.runAsync(head + selects.join(' UNION ALL ') + tail, ...bindings);
      offset = end;
    }
  }
  /** A durable delivery uses the same immutable ID on every retry. The receipt
   * is separate from the outbox, so retiring delivery cannot forget dedupe. */
  apply(scope: string, writes: readonly SQLiteTypedWrite[], intent?: { id: string; payload: unknown }): Promise<{ sequence: number }> {
    const bytes = sqlitePayloadBytes({ scope, writes, intent: intent ?? null }, this.limits.batchBytes);
    if (!scope || scope.length > 512 || writes.length > 512 || intent && (!intent.id || intent.id.length > 128)) throw new TypeError('Invalid SQLite batch');
    // Serialize once at admission. A caller cannot mutate a queued intent or
    // row after it has passed byte validation and change the durable operation.
    const admitted = JSON.parse(JSON.stringify({ scope, writes, intent: intent ?? null })) as { scope: string; writes: SQLiteTypedWrite[]; intent: { id: string; payload: unknown } | null };
    return this.enqueue(bytes, async () => {
      let result!: { sequence: number };
      await this.db.withTransactionAsync(async () => {
        const previous = await this.fence(), payload = admitted.intent ? JSON.stringify({ payload: admitted.intent.payload, writes: admitted.writes }) : null;
        if (admitted.intent) {
          const receipt = await this.db.getFirstAsync<{ payload: string; result: string }>('SELECT payload,result FROM gv_intent_receipts WHERE scope=? AND id=?', admitted.scope, admitted.intent.id);
          if (receipt) {
            if (receipt.payload !== payload) fail('INTENT_CONFLICT', 'SQLite intent ID reused with different payload');
            result = JSON.parse(receipt.result); return;
          }
        }
        await this.writeBatch(await this.physicalScope(admitted.scope), admitted.writes);
        if (previous >= Number.MAX_SAFE_INTEGER) fail('FENCED', 'SQLite storage sequence exhausted');
        result = { sequence: previous + 1 };
        if (admitted.intent) {
          await this.db.runAsync('INSERT INTO gv_intent_receipts VALUES(?,?,?,?)', admitted.scope, admitted.intent.id, payload, JSON.stringify(result));
          await this.db.runAsync('INSERT INTO gv_outbox VALUES(?,?,?,?)', admitted.scope, admitted.intent.id, JSON.stringify(admitted.intent.payload), result.sequence);
        }
        await this.db.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1', result.sequence);
      });
      return result;
    });
  }
  /** Restartable bootstrap keeps unverified rows in an invisible generation.
   * Its page receipts and chain survive worker recycling. This entry point is
   * deliberately limited to first sync with no pending intents. Existing data
   * and migrations require a separately verified source/overlay transition. */
  beginBootstrap(scope: string, token: string): Promise<SQLiteBootstrapState> {
    sqlitePayloadBytes({ scope, token }, 2048);
    if (!token || token.length > 128) throw new TypeError('Invalid SQLite bootstrap token');
    return this.enqueue(2048, async () => {
      let state!: SQLiteBootstrapState;
      await this.db.withTransactionAsync(async () => {
        const sequence = await this.fence(), physical = await this.physicalScope(scope);
        const old = await this.db.getFirstAsync<SQLiteBootstrapRecord>('SELECT * FROM gv_bootstrap WHERE scope=?', scope);
        if (old) {
          if (old.token !== token) fail('INTENT_CONFLICT', 'Another bootstrap owns this scope');
          state = bootstrapState(old); return;
        }
        for (const entity of Object.keys(this.schema)) if (await this.db.getFirstAsync(`SELECT 1 FROM ${q(sqliteTableName(entity))} WHERE scope=? LIMIT 1`, physical)) fail('INVALID_ROW', 'Bootstrap requires an empty scope');
        for (const table of ['gv_outbox', 'gv_intent_receipts', 'gv_tombstones']) if (await this.db.getFirstAsync(`SELECT 1 FROM ${table} WHERE scope=? LIMIT 1`, scope)) fail('INVALID_ROW', 'Bootstrap cannot replace pending work');
        const stage = '\u0000bootstrap:' + token;
        await this.db.runAsync('INSERT INTO gv_bootstrap VALUES(?,?,?,0,0,?,?,0)', scope, token, stage, '0'.repeat(64), sequence);
        state = { token, nextPage: 0, rows: 0, root: '0'.repeat(64), complete: false };
      }); return state;
    });
  }
  appendBootstrap(scope: string, token: string, page: number, encoded: string): Promise<SQLiteBootstrapState> {
    return this.appendBootstrapBatch(scope, token, (async function* () { yield { page, encoded }; })());
  }
  /** A bounded streamed credit group commits together. The producer yields one
   * frame at a time; it never needs a 1,000-row object or worker message. */
  appendBootstrapBatch(scope: string, token: string, source: AsyncIterable<{ page: number; encoded: string }>): Promise<SQLiteBootstrapState> {
    sqlitePayloadBytes({ scope, token }, 2048);
    return this.enqueue(Math.min(this.limits.queuedBytes, this.limits.batchBytes * 2), async () => {
      let state: SQLiteBootstrapState | undefined;
      await this.db.withTransactionAsync(async () => {
        let pages = 0, bytes = 0;
        for await (const frame of source) {
          const page = frame.page, encoded = frame.encoded;
          if (++pages > 16 || typeof encoded !== 'string' || (bytes += encoded.length * 2) > this.limits.queuedBytes) fail('BUDGET', 'Bootstrap credit group exceeds budget');
          state = await this.appendBootstrapPage(scope, token, page, encoded);
        }
        if (!state) fail('INVALID_ROW', 'Empty bootstrap credit group');
      });
      return state!;
    });
  }
  private async appendBootstrapPage(scope: string, token: string, page: number, encoded: string): Promise<SQLiteBootstrapState> {
    if (typeof encoded !== 'string' || encoded.length * 2 > this.limits.batchBytes || !Number.isSafeInteger(page) || page < 0) throw new RangeError('Invalid SQLite bootstrap page');
    const admitted = JSON.parse(encoded) as SQLiteTypedWrite[];
    parsedPayloadBytes(admitted, this.limits.batchBytes);
    if (!Array.isArray(admitted) || admitted.length > 512 || admitted.some(write => write.row === null)) fail('INVALID_ROW', 'Bootstrap accepts bounded live rows only');
    const hash = await sha256(encoded);
    await this.fence();
    const old = await this.db.getFirstAsync<SQLiteBootstrapRecord>('SELECT * FROM gv_bootstrap WHERE scope=? AND token=?', scope, token);
    if (!old) fail('INVALID_ROW', 'Bootstrap is not open');
    if (page < old.next_page) {
      const receipt = await this.db.getFirstAsync<{ hash: string }>('SELECT hash FROM gv_bootstrap_pages WHERE token=? AND page=?', token, page);
      if (receipt?.hash !== hash) fail('INTENT_CONFLICT', 'Bootstrap page replay changed its payload');
      return bootstrapState(old);
    }
    if (old.complete || page !== old.next_page) fail('PAGE_CHANGED', 'Bootstrap page is out of sequence');
    const groups = new Map<string, string[]>(), seen = new Set<string>();
    for (const write of admitted) {
      this.validateWrite(write);
      const key = JSON.stringify([write.table, write.id]);
      if (seen.has(key)) fail('INVALID_ROW', 'Duplicate bootstrap row'); seen.add(key);
      const ids = groups.get(write.table) ?? []; ids.push(write.id); groups.set(write.table, ids);
    }
    for (const [entity, ids] of groups) if (await this.db.getFirstAsync(`SELECT 1 FROM ${q(sqliteTableName(entity))} WHERE scope=? AND id IN (${ids.map(() => '?').join(',')}) LIMIT 1`, old.physical, ...ids)) fail('INVALID_ROW', 'Duplicate bootstrap row');
    await this.writeBatch(old.physical, admitted);
    const root = await sha256(old.root + hash), rows = old.rows + admitted.length;
    await this.db.runAsync('INSERT INTO gv_bootstrap_pages VALUES(?,?,?)', token, page, hash);
    await this.db.runAsync('UPDATE gv_bootstrap SET next_page=?,rows=?,root=? WHERE scope=? AND token=?', page + 1, rows, root, scope, token);
    return { token, nextPage: page + 1, rows, root, complete: false };
  }
  abortBootstrap(scope: string, token: string): Promise<void> {
    sqlitePayloadBytes({ scope, token }, 2048);
    return this.enqueue(2048, () => this.db.withTransactionAsync(async () => {
      await this.fence();
      const old = await this.db.getFirstAsync<SQLiteBootstrapRecord>('SELECT * FROM gv_bootstrap WHERE scope=? AND token=?', scope, token);
      if (!old) return;
      if (old.complete) fail('INVALID_ROW', 'A published bootstrap cannot be discarded');
      for (const entity of Object.keys(this.schema)) await this.db.runAsync(`DELETE FROM ${q(sqliteTableName(entity))} WHERE scope=?`, old.physical);
      await this.db.runAsync('DELETE FROM gv_bootstrap_pages WHERE token=?', token);
      await this.db.runAsync('DELETE FROM gv_bootstrap WHERE scope=? AND token=?', scope, token);
    }));
  }
  finishBootstrap(scope: string, token: string, expected: { pages: number; rows: number; root: string }): Promise<SQLiteBootstrapState> {
    sqlitePayloadBytes({ scope, token, expected }, 2048);
    return this.enqueue(2048, async () => {
      let state!: SQLiteBootstrapState;
      await this.db.withTransactionAsync(async () => {
        const sequence = await this.fence();
        const old = await this.db.getFirstAsync<SQLiteBootstrapRecord>('SELECT * FROM gv_bootstrap WHERE scope=? AND token=?', scope, token);
        if (!old || old.rows !== expected.rows || old.next_page !== expected.pages || old.root !== expected.root) fail('INVALID_ROW', 'Bootstrap manifest does not match');
        if (!old.complete) {
          if (sequence !== old.base_sequence) fail('PAGE_CHANGED', 'Canonical writes raced bootstrap activation');
          if (sequence >= Number.MAX_SAFE_INTEGER) fail('FENCED', 'SQLite storage sequence exhausted');
          await this.db.runAsync('INSERT INTO gv_scope_alias VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET physical=excluded.physical', scope, old.physical);
          await this.db.runAsync('UPDATE gv_bootstrap SET complete=1 WHERE scope=? AND token=?', scope, token);
          await this.db.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1', sequence + 1);
        }
        state = { ...bootstrapState(old), complete: true };
      }); return state;
    });
  }
  /** Stream a previously empty scope in bounded pages with one atomic commit.
   * Only bootstrap uses this lane. Readers wait for activation, and a failed
   * source or owner crash leaves the scope empty. The producer must stream
   * rather than retaining the complete fixture. No outbox intent is accepted
   * or acknowledged until the import transaction finishes. */
  importScope(scope: string, source: AsyncIterable<readonly SQLiteTypedWrite[] | string>): Promise<{ sequence: number; rows: number }> {
    if (!scope || scope.length > 512) throw new TypeError('Invalid SQLite import scope');
    return this.enqueue(this.limits.batchBytes, async () => {
      let result!: { sequence: number; rows: number };
      await this.db.withTransactionAsync(async () => {
        const previous = await this.fence();
        if (previous >= Number.MAX_SAFE_INTEGER) fail('FENCED', 'SQLite storage sequence exhausted');
        const physical = await this.physicalScope(scope);
        if (physical !== scope) fail('INVALID_ROW', 'Bootstrap import cannot replace an activated scope');
        for (const entity of Object.keys(this.schema)) {
          if (await this.db.getFirstAsync(`SELECT 1 FROM ${q(sqliteTableName(entity))} WHERE scope=? LIMIT 1`, physical))
            fail('INVALID_ROW', 'Bootstrap import requires an empty scope');
        }
        for (const table of ['gv_tombstones', 'gv_intent_receipts', 'gv_outbox'])
          if (await this.db.getFirstAsync(`SELECT 1 FROM ${table} WHERE scope=? LIMIT 1`, scope))
            fail('INVALID_ROW', 'Bootstrap import cannot replace pending work or tombstones');
        // Only a completely empty canonical database may defer global indexes.
        // Existing scopes keep every index/trigger available throughout import.
        let deferIndexes = true;
        for (const entity of Object.keys(this.schema)) if (await this.db.getFirstAsync(`SELECT 1 FROM ${q(sqliteTableName(entity))} LIMIT 1`)) { deferIndexes = false; break; }
        if (deferIndexes) for (const [entity, table] of Object.entries(this.schema)) {
          const name = sqliteTableName(entity);
          for (let i = 0; i < table.indexes.length; i++) await this.db.execAsync(`DROP INDEX IF EXISTS ${q(name + '_i_' + i)}`);
          if (table.search?.length) {
            for (const suffix of ['a', 'u', 'd']) await this.db.execAsync(`DROP TRIGGER IF EXISTS ${q(name + '_fts_' + suffix)}`);
            await this.db.execAsync(`DROP TABLE IF EXISTS ${q(sqliteSearchName(entity))}`);
          }
        }
        let rows = 0, sliceStarted = performance.now();
        for await (const page of source) {
          let admitted: SQLiteTypedWrite[];
          if (typeof page === 'string') {
            if (page.length * 2 > this.limits.batchBytes) fail('BUDGET', 'SQLite encoded import page exceeds byte budget');
            admitted = JSON.parse(page) as SQLiteTypedWrite[];
            parsedPayloadBytes(admitted, this.limits.batchBytes);
          } else {
            sqlitePayloadBytes(page, this.limits.batchBytes);
            admitted = JSON.parse(JSON.stringify(page)) as SQLiteTypedWrite[];
          }
          if (!Array.isArray(admitted) || admitted.length > 512) fail('BUDGET', 'SQLite import page exceeds row budget');
          await this.writeBatch(physical, admitted);
          rows += admitted.length;
          if (!Number.isSafeInteger(rows)) fail('BUDGET', 'SQLite import row count exhausted');
          // Promise-only ingestion monopolizes a worker task, withholding both
          // progress messages and idle GC for the entire bootstrap. Yield real
          // event-loop tasks within the ingestion slice budget.
          if (performance.now() - sliceStarted >= 8) {
            await new Promise<void>(resolve => setTimeout(resolve, 0));
            sliceStarted = performance.now();
          }
        }
        if (deferIndexes) {
          const ddl = compileSQLiteTypedSchema(this.schema);
          await this.db.execAsync(ddl.indexes + '\n' + ddl.search);
          for (const [entity, table] of Object.entries(this.schema)) if (table.search?.length) {
            const tn = q(sqliteTableName(entity)), fn = q(sqliteSearchName(entity)), columns = table.search.map((_, i) => q('s' + i)).join(',');
            let after = 0;
            for (;;) {
              const last = (await this.db.getFirstAsync<{ last: number | null }>(`SELECT max(rid) last FROM (SELECT rid FROM ${tn} WHERE rid>? ORDER BY rid LIMIT 512)`, after))?.last;
              if (last == null) break;
              await this.db.runAsync(`INSERT INTO ${fn}(rowid,${columns}) SELECT rid,${columns} FROM ${tn} WHERE rid>? AND rid<=? ORDER BY rid`, after, last);
              after = last;
              await new Promise<void>(resolve => setTimeout(resolve, 0));
            }
          }
        }
        result = { sequence: previous + 1, rows };
        await this.db.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1', result.sequence);
      });
      return result;
    });
  }
  page(scope: string, plan: SQLitePagePlan): Promise<SQLiteTypedPage> {
    const bytes = sqlitePayloadBytes({ scope, plan }, 16_384);
    const admitted = JSON.parse(JSON.stringify(plan)) as SQLitePagePlan;
    compileSQLiteTypedPage(this.schema, scope, admitted);
    return this.enqueue(bytes, async () => {
      let result!: SQLiteTypedPage;
      await this.db.withTransactionAsync(async () => {
        const sequence = await this.fence();
        if (admitted.after && admitted.after.sequence !== sequence) fail('PAGE_CHANGED', 'SQLite keyset belongs to an older replica snapshot');
        const physical = await this.physicalScope(scope);
        let access: 'trigram' | 'ordered' = 'trigram';
        let selectedStatement = compileSQLiteTypedPage(this.schema, physical, admitted);
        if (admitted.search) {
          // Inspect at most 1025 matching rowids. A common term uses the native
          // sort/filter index and stops at the requested page, avoiding a huge
          // FTS candidate materialization followed by a global sort.
          const table = this.schema[admitted.table]!;
          const pattern = '*' + admitted.search.toLowerCase().replace(/[\[\]*?]/g, char => char === '[' ? '[[]' : char === ']' ? '[]]' : '[' + char + ']') + '*';
          const candidates = Array.from(admitted.search).length < 3 ? 1025 : (await this.db.getFirstAsync<{ n: number }>(
            `SELECT count(*) n FROM (SELECT rowid FROM ${q(sqliteSearchName(admitted.table))} WHERE ${(table.search ?? []).map((_, i) => `${q('s' + i)} GLOB ?`).join(' OR ')} LIMIT 1025)`,
            ...(table.search ?? []).map(() => pattern)))!.n;
          if (candidates > 1024) { access = 'ordered'; selectedStatement = compileSQLiteTypedPage(this.schema, physical, admitted, access); }
        }
        // The host must enforce a column byte limit before decoding as well.
        let records: Record<string, unknown>[], outputBytes: number, limit = admitted.limit;
        for (;;) {
          try {
            records = await this.db.getAllAsync<Record<string, unknown>>(selectedStatement.sql, ...selectedStatement.bindings);
            outputBytes = sqlitePayloadBytes(records, this.limits.pageBytes); break;
          } catch (error) {
            if (!pageBudgetError(error) || limit === 1) throw error;
            limit = Math.max(1, Math.floor(limit / 2));
            selectedStatement = compileSQLiteTypedPage(this.schema, physical, { ...admitted, limit }, access);
          }
        }
        const last = records.at(-1);
        const fields = admitted.select ? [...new Set([this.schema[admitted.table]!.key, ...admitted.select])] : undefined;
        const decode = (record: Record<string, unknown>): Record<string, unknown> => {
          if (!fields) return JSON.parse(String(record.body));
          return Object.fromEntries(fields.flatMap((field, i) => {
            if (record['h' + i] === null) return [];
            const value = record['p' + i], kind = this.schema[admitted.table]!.fields[field]!.kind;
            return [[field, value === null ? null : kind === 'json' ? JSON.parse(String(value)) : kind === 'boolean' ? Boolean(value) : value]];
          }));
        };
        result = { rows: records.map(decode), sequence, bytes: outputBytes,
          cursor: last ? { id: String(last.id), sequence, values: admitted.order.map((_, i) => last['k' + i] as string | number | null) } : null };
      });
      return result;
    });
  }
  count(scope: string, entity: string, group: number, values: readonly (string | number | boolean | null)[]): Promise<number> {
    const table = Object.hasOwn(this.schema, entity) && this.schema[entity], descriptor = table && table.counts?.[group];
    if (!descriptor || descriptor.columns.length !== values.length) throw new TypeError('Unknown SQLite count');
    const bytes = sqlitePayloadBytes({ scope, values }, 16_384);
    const bucket = `json_array(${descriptor.columns.map(field => `CAST(? AS ${table!.fields[field]!.kind === 'number' ? 'REAL' : table!.fields[field]!.kind === 'boolean' ? 'INTEGER' : 'TEXT'})`).join(',')})`;
    const bindings = values.map(value => typeof value === 'boolean' ? Number(value) : value);
    return this.enqueue(bytes, async () => {
      let n = 0;
      await this.db.withTransactionAsync(async () => {
        await this.fence();
        n = (await this.db.getFirstAsync<{ n: number }>(`SELECT n FROM ${q(sqliteCountName(entity, group))} WHERE scope=? AND bucket=${bucket}`, await this.physicalScope(scope), ...bindings))?.n ?? 0;
      });
      return n;
    });
  }
  outbox(scope: string, afterSequence = 0, limit = 32): Promise<{ id: string; payload: unknown; sequence: number }[]> {
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 32) throw new TypeError('Invalid SQLite outbox page');
    return this.enqueue(1024, async () => {
      let result!: { id: string; payload: unknown; sequence: number }[];
      await this.db.withTransactionAsync(async () => {
        await this.fence();
        let count = limit;
        for (;;) {
          try {
            const rows = await this.db.getAllAsync<{ id: string; payload: string; sequence: number }>('SELECT id,payload,sequence FROM gv_outbox WHERE scope=? AND sequence>? ORDER BY sequence LIMIT ?', scope, afterSequence, count);
            sqlitePayloadBytes(rows, this.limits.pageBytes);
            result = rows.map(row => ({ ...row, payload: JSON.parse(row.payload) }));
            sqlitePayloadBytes(result, this.limits.pageBytes); break;
          } catch (error) {
            if (!pageBudgetError(error) || count === 1) throw error;
            count = Math.max(1, Math.floor(count / 2));
          }
        }
      });
      return result;
    });
  }
  acknowledge(scope: string, ids: readonly string[]): Promise<void> {
    const bytes = sqlitePayloadBytes({ scope, ids }, 16_384);
    if (ids.length > 32 || ids.some(id => typeof id !== 'string' || !id || id.length > 128)) throw new TypeError('Invalid SQLite intent acknowledgment');
    const admitted = [...ids];
    return this.enqueue(bytes, () => this.db.withTransactionAsync(async () => {
      await this.fence();
      if (admitted.length) await this.db.runAsync(`DELETE FROM gv_outbox WHERE scope=? AND id IN (${admitted.map(() => '?').join(',')})`, scope, ...admitted);
      // Receipt retention is independent. A delayed tab retry must still see
      // the original result after server delivery has left the outbox.
    }));
  }
  integrity(): Promise<string> {
    return this.enqueue(256, async () => {
      let result = '';
      await this.db.withTransactionAsync(async () => {
        await this.fence();
        const row = await this.db.getFirstAsync<{ integrity_check: string }>('PRAGMA integrity_check');
        result = row?.integrity_check ?? 'unavailable';
      });
      return result;
    });
  }
  stats() { return { pendingOperations: this.pending, pendingBytes: this.pendingBytes, epoch: this.epoch }; }
}
