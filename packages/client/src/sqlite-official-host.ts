import type { BindableValue, Database, PreparedStatement, Sqlite3Static } from '@sqlite.org/sqlite-wasm';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { SQLiteBridgeBudgetError } from './wasm-sqlite-database.js';

export type OfficialSQLiteHost = {
  database: SQLiteReplicaDatabase;
  memoryBytes(): number;
  statementBytes(): number;
  close(): void;
};

/** Lazy worker-only factory. The origin broker must hold its ownership lease
 * before invoking this. Followers never import WASM or acquire OPFS handles. */
export async function openOfficialSQLiteHost(): Promise<OfficialSQLiteHost> {
  if (typeof globalThis.postMessage !== 'function' || 'document' in globalThis || 'onconnect' in globalThis)
    throw new Error('SQLite OPFS must open in the elected dedicated worker');
  // Disable unused proxy VFS initialization and its helper workers. Configure
  // this before importing/initializing the pinned official module.
  (globalThis as typeof globalThis & { sqlite3ApiConfig?: unknown }).sqlite3ApiConfig = {
    disable: { vfs: { opfs: true, 'opfs-vfs': true, 'opfs-wl': true } },
  };
  const { default: initialize } = await import('@gonvex/client/sqlite-module');
  const sqlite3 = await initialize();
  // On failed initialization preserve existing files and the unsent outbox.
  const pool = await sqlite3.installOpfsSAHPoolVfs({ directory: '.gonvex-sqlite-v1', initialCapacity: 6, preserveOnInitFailure: true } as Parameters<typeof sqlite3.installOpfsSAHPoolVfs>[0]);
  let db: Database | undefined;
  try {
    db = new pool.OpfsSAHPoolDb('/replica.sqlite');
    const host = officialSQLiteDatabase(sqlite3, db);
    // EXCLUSIVE must precede the very first database read for single-connection
    // WAL without shared-memory methods. A mode mismatch is a startup failure.
    await host.database.execAsync(`PRAGMA locking_mode=EXCLUSIVE;
PRAGMA page_size=4096; PRAGMA cache_size=-2048; PRAGMA mmap_size=0;
PRAGMA temp_store=FILE; PRAGMA synchronous=FULL; PRAGMA journal_mode=WAL;
PRAGMA hard_heap_limit=33554432; PRAGMA wal_autocheckpoint=256;
PRAGMA recursive_triggers=ON; PRAGMA busy_timeout=3000;`);
    for (const [pragma, expected] of [['locking_mode', 'exclusive'], ['journal_mode', 'wal'], ['synchronous', 2], ['page_size', 4096], ['cache_size', -2048], ['temp_store', 1], ['hard_heap_limit', 33554432]] as const) {
      const row = await host.database.getFirstAsync<Record<string, unknown>>('PRAGMA ' + pragma);
      if (!row || Object.values(row)[0] !== expected) throw new Error('SQLite storage mode invariant failed: ' + pragma);
    }
    return { ...host, close() { host.close(); pool.pauseVfs(); } };
  } catch (error) { db?.close(); pool.pauseVfs(); throw error; }
}

/** Official oo1 bridge shared by the owner and real-WASM tests. Statements are
 * read statements are finalized immediately; write plans use a byte-capped LRU. */
export function officialSQLiteDatabase(sqlite3: Sqlite3Static, db: Database,
  budget = { maxRows: 512, maxBytes: 262_144 }): OfficialSQLiteHost {
  let transaction = false, closed = false, statementBytes = 0;
  const statements = new Map<string, { statement: PreparedStatement; bytes: number }>();
  const statementBudget = 262144, maxStatements = 4;
  function retire(sql: string) {
    const cached = statements.get(sql); if (!cached) return;
    statements.delete(sql); statementBytes -= cached.bytes; cached.statement.finalize();
  }
  function check() { if (closed) throw new Error('SQLite host closed'); }
  function bind(statement: PreparedStatement, values: unknown[]) {
    for (const value of values) if (value !== null && typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean' && !(value instanceof Uint8Array)) throw new TypeError('Invalid SQLite binding');
    if (values.length) statement.bind(values as BindableValue[]);
  }
  function read<T>(sql: string, values: unknown[], first: boolean): T[] {
    check(); const statement = db.prepare(sql);
    try {
      bind(statement, values);
      const rows: T[] = []; let bytes = 0;
      const columns = statement.getColumnNames();
      while (statement.step()) {
        if (rows.length >= budget.maxRows) throw new SQLiteBridgeBudgetError('rows');
        const entries: [string, unknown][] = [];
        for (let index = 0; index < columns.length; index++) {
          const name = columns[index]!;
          const type = sqlite3.capi.sqlite3_column_type(statement.pointer!, index);
          const size = type === 3 || type === 4 ? sqlite3.capi.sqlite3_column_bytes(statement.pointer!, index) : 8;
          // Reserve conservatively before decoding UTF-8 or copying a blob.
          bytes += name.length * 2 + size * (type === 3 ? 2 : 1) + 32;
          if (bytes > budget.maxBytes) throw new SQLiteBridgeBudgetError('text');
          entries.push([name, statement.get(index)]);
        }
        rows.push(Object.fromEntries(entries) as T);
        if (first) break;
      }
      return rows;
    } finally { statement.finalize(); }
  }
  const database: SQLiteReplicaDatabase = {
    async execAsync(sql) { check(); db.exec(sql); },
    async runAsync(sql, ...values) {
      check();
      let cached = statements.get(sql);
      if (cached) { statements.delete(sql); statements.set(sql, cached); }
      const statement = cached?.statement ?? db.prepare(sql);
      let retain = !!cached;
      try {
        bind(statement, values); while (statement.step()) { /* No retained output. */ }
        statement.reset(true); // Never retain bound row strings or buffers.
        const size = sql.length * 2 + 128 + sqlite3.capi.sqlite3_stmt_status(statement.pointer!, sqlite3.capi.SQLITE_STMTSTATUS_MEMUSED, 0);
        if (cached) {
          // SQLite may recompile after a schema/index change. Charge the new
          // native plan as well as its SQL key before retaining it again.
          statementBytes += size - cached.bytes; cached.bytes = size;
          while (statementBytes > statementBudget) retire(statements.keys().next().value!);
        } else {
          if (size <= statementBudget) {
            while (statements.size >= maxStatements || statementBytes + size > statementBudget) retire(statements.keys().next().value!);
            statements.set(sql, { statement, bytes: size }); statementBytes += size; retain = true;
          }
        }
      } catch (e) {
        if (cached) { retire(sql); retain = true; }
        throw e;
      } finally { if (!retain) statement.finalize(); }
    },
    async getAllAsync<T>(sql: string, ...values: unknown[]) { return read<T>(sql, values, false); },
    async getFirstAsync<T>(sql: string, ...values: unknown[]) { return read<T>(sql, values, true)[0] ?? null; },
    async withTransactionAsync(run) {
      check(); if (transaction) throw new Error('Overlapping SQLite operation outside owner FIFO');
      transaction = true; let began = false;
      try { db.exec('BEGIN IMMEDIATE'); began = true; await run(); db.exec('COMMIT'); }
      catch (error) { if (began) try { db.exec('ROLLBACK'); } catch { /* Preserve original failure. */ } throw error; }
      finally { transaction = false; }
    },
  };
  return { database, memoryBytes: () => sqlite3.wasm.heap8u().buffer.byteLength, statementBytes: () => statementBytes,
    close() { if (!closed) { closed = true; for (const sql of statements.keys()) retire(sql); db.close(); } } };
}
