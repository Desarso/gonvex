import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { compileSQLiteTypedSchema, sqliteIdentifier as q, sqliteTableName, sqliteFieldName, sqliteSearchName, sqliteCountName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
import { compileSQLiteTypedPage, type SQLitePagePlan, type SQLiteKeyset } from './sqlite-typed-query.js';

export class SQLiteReplicaEngineError extends Error {
  constructor(readonly code: 'FENCED' | 'SCHEMA_MISMATCH' | 'BUDGET' | 'INTENT_CONFLICT' | 'INVALID_ROW', message: string) { super(message); this.name = 'SQLiteReplicaEngineError'; }
}
export type SQLiteTypedWrite = { table: string; id: string; version: number; row: Record<string, unknown> | null };
export type SQLiteTypedPage = { rows: Record<string, unknown>[]; cursor: SQLiteKeyset | null; bytes: number; sequence: number };
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
CREATE TABLE IF NOT EXISTS gv_outbox(scope TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(scope,id)) WITHOUT ROWID;`);
        const signature = JSON.stringify(this.schema);
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
  private async write(scope: string, input: SQLiteTypedWrite): Promise<void> {
    const table = Object.hasOwn(this.schema, input.table) && this.schema[input.table];
    if (!table || typeof input.id !== 'string' || !input.id || !Number.isSafeInteger(input.version) || input.version < 0) fail('INVALID_ROW', 'Invalid SQLite row identity/version');
    const tn = q(sqliteTableName(input.table));
    const old = await this.db.getFirstAsync<{ rid: number; version: number }>(`SELECT rid,version FROM ${tn} WHERE scope=? AND id=?`, scope, input.id);
    const tombstone = await this.db.getFirstAsync<{ version: number }>('SELECT version FROM gv_tombstones WHERE scope=? AND entity=? AND id=?', scope, input.table, input.id);
    if (Math.max(old?.version ?? -1, tombstone?.version ?? -1) > input.version || tombstone?.version === input.version && input.row !== null) return;
    if (input.row === null) {
      await this.db.runAsync(`DELETE FROM ${tn} WHERE scope=? AND id=?`, scope, input.id);
      await this.db.runAsync('INSERT INTO gv_tombstones VALUES(?,?,?,?) ON CONFLICT(scope,entity,id) DO UPDATE SET version=excluded.version', scope, input.table, input.id, input.version);
      return;
    }
    await this.db.runAsync('DELETE FROM gv_tombstones WHERE scope=? AND entity=? AND id=?', scope, input.table, input.id);
    if (input.row[table.key] == null || String(input.row[table.key]) !== input.id) fail('INVALID_ROW', 'SQLite row key does not match its identity');
    const fields = Object.entries(table.fields), values: unknown[] = [];
    for (const [field, shape] of fields) {
      const value = input.row[field];
      if (value == null) { values.push(null); continue; }
      if (shape.kind === 'text' && typeof value !== 'string' || shape.kind === 'number' && typeof value !== 'number' || shape.kind === 'boolean' && typeof value !== 'boolean')
        fail('INVALID_ROW', 'SQLite field type differs from the generated schema');
      values.push(shape.kind === 'json' ? JSON.stringify(value) : shape.kind === 'boolean' ? Number(value) : value);
    }
    const searchValues = (table.search ?? []).map(field => String(input.row![field] ?? '').toLowerCase());
    const columns = ['scope', 'id', 'body', 'version', ...fields.map(([field]) => sqliteFieldName(field)), ...(table.search ?? []).map((_, i) => 's' + i)];
    await this.db.runAsync(`INSERT INTO ${tn}(${columns.map(q).join(',')}) VALUES(${columns.map(() => '?').join(',')}) ON CONFLICT(scope,id) DO UPDATE SET ${columns.slice(2).map(column => `${q(column)}=excluded.${q(column)}`).join(',')}`,
      scope, input.id, JSON.stringify(input.row), input.version, ...values, ...searchValues);
    if (table.search?.length) {
      const fn = q(sqliteSearchName(input.table));
      const row = await this.db.getFirstAsync<{ rid: number }>(`SELECT rid FROM ${tn} WHERE scope=? AND id=?`, scope, input.id);
      await this.db.runAsync(`DELETE FROM ${fn} WHERE rowid=?`, row!.rid);
      await this.db.runAsync(`INSERT INTO ${fn}(rowid,${table.search.map((_, i) => q('s' + i)).join(',')}) VALUES(${table.search.map(() => '?').concat('?').join(',')})`,
        row!.rid, ...searchValues);
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
        for (const write of admitted.writes) await this.write(admitted.scope, write);
        result = { sequence: previous + 1 };
        if (admitted.intent) {
          await this.db.runAsync('INSERT INTO gv_intent_receipts VALUES(?,?,?,?)', admitted.scope, admitted.intent.id, payload, JSON.stringify(result));
          await this.db.runAsync('INSERT INTO gv_outbox VALUES(?,?,?)', admitted.scope, admitted.intent.id, JSON.stringify(admitted.intent.payload));
        }
        await this.db.runAsync('UPDATE gv_storage_meta SET sequence=? WHERE id=1', result.sequence);
      });
      return result;
    });
  }
  page(scope: string, plan: SQLitePagePlan): Promise<SQLiteTypedPage> {
    const bytes = sqlitePayloadBytes({ scope, plan }, 16_384);
    const admitted = JSON.parse(JSON.stringify(plan)) as SQLitePagePlan;
    const statement = compileSQLiteTypedPage(this.schema, scope, admitted);
    return this.enqueue(bytes, async () => {
      let result!: SQLiteTypedPage;
      await this.db.withTransactionAsync(async () => {
        const sequence = await this.fence();
        let selectedStatement = statement;
        if (admitted.search) {
          // Inspect at most 1025 matching rowids. A common term uses the native
          // sort/filter index and stops at the requested page, avoiding a huge
          // FTS candidate materialization followed by a global sort.
          const table = this.schema[admitted.table]!;
          const pattern = '*' + admitted.search.toLowerCase().replace(/[\[\]*?]/g, char => char === '[' ? '[[]' : char === ']' ? '[]]' : '[' + char + ']') + '*';
          const candidates = Array.from(admitted.search).length < 3 ? 1025 : (await this.db.getFirstAsync<{ n: number }>(
            `SELECT count(*) n FROM (SELECT rowid FROM ${q(sqliteSearchName(admitted.table))} WHERE ${(table.search ?? []).map((_, i) => `${q('s' + i)} GLOB ?`).join(' OR ')} LIMIT 1025)`,
            ...(table.search ?? []).map(() => pattern)))!.n;
          if (candidates > 1024) selectedStatement = compileSQLiteTypedPage(this.schema, scope, admitted, 'ordered');
        }
        // The host must enforce a column byte limit before decoding as well.
        const records = await this.db.getAllAsync<Record<string, unknown>>(selectedStatement.sql, ...selectedStatement.bindings);
        const outputBytes = sqlitePayloadBytes(records, this.limits.pageBytes);
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
          cursor: last ? { id: String(last.id), values: admitted.order.map((_, i) => last['k' + i] as string | number | null) } : null };
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
        n = (await this.db.getFirstAsync<{ n: number }>(`SELECT n FROM ${q(sqliteCountName(entity, group))} WHERE scope=? AND bucket=${bucket}`, scope, ...bindings))?.n ?? 0;
      });
      return n;
    });
  }
  stats() { return { pendingOperations: this.pending, pendingBytes: this.pendingBytes, epoch: this.epoch }; }
}
