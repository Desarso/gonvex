import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

/** Minimal host surface provided by the pinned browser SQLite factory. No
 * dependency, database, worker or WASM instance is created by this module. */
export interface WasmSQLiteAPI {
  statements(database: number, sql: string, options?: { unscoped?: boolean }): AsyncIterable<number>;
  finalize(statement: number): Promise<number>;
  bind_collection(statement: number, values: unknown[]): unknown;
  step(statement: number): Promise<number>;
  column_names(statement: number): string[];
  column(statement: number, index: number): unknown;
  column_type?(statement: number, index: number): number;
  column_bytes?(statement: number, index: number): number;
}
export type WasmSQLiteReadBudget = { maxRows: number; maxTextBytes: number };
export class SQLiteBridgeBudgetError extends Error {
  readonly code = 'sqlite-bridge-budget';
  constructor(readonly limit: 'rows' | 'text') {
    super(`SQLite bridge ${limit} budget exceeded`);
    this.name = 'SQLiteBridgeBudgetError';
  }
}
const settled = () => {};

/** A database connection for one storage-operation actor in a dedicated worker.
 * The actor must serialize complete storage/query operations. The SQL lane
 * below also serializes statements, including Promise.all during initialization;
 * reentering SQLite while a JSPI VFS call is suspended is unsafe.
 * withTransactionAsync intentionally allows its callback to call these methods.
 * It is not an exclusive transaction API for unrelated concurrent callers.
 * Native/cache/input/resident budgets remain the host's responsibility. */
export function wasmSQLiteDatabase(api: WasmSQLiteAPI, database: number,
  budget: WasmSQLiteReadBudget): SQLiteReplicaDatabase {
  for (const value of [budget.maxRows, budget.maxTextBytes])
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Positive finite SQLite bridge budget required');
  let lane = Promise.resolve(), transaction = false;
  function serialize<T>(run: () => Promise<T>): Promise<T> {
    const result = lane.then(run);
    // A completed statement must not leave its result in the lane barrier.
    lane = result.then(settled, settled);
    return result;
  }
  const bindings = (values: unknown[]) => values.map(value => typeof value === 'boolean' ? Number(value) : value);
  async function owned<T>(statement: number, run: () => Promise<T>): Promise<T> {
    let failed = false;
    try { return await run(); }
    catch (error) { failed = true; throw error; }
    finally {
      // The pinned iterator starts async finalization without awaiting it.
      // Own this lifecycle so the SQL lane includes all native VFS cleanup.
      try { await api.finalize(statement); }
      catch (error) { if (!failed) throw error; }
    }
  }

  async function discard(sql: string, params: unknown[] = []) {
    for await (const statement of api.statements(database, sql, { unscoped: true })) {
      await owned(statement, async () => {
        if (params.length) api.bind_collection(statement, bindings(params));
        while (await api.step(statement) === 100) { /* Drain without retaining rows. */ }
      });
    }
  }
  async function read<T>(sql: string, params: unknown[], first: boolean): Promise<T[]> {
    const rows: T[] = [];
    let bytes = 0;
    for await (const statement of api.statements(database, sql, { unscoped: true })) {
      const stop = await owned(statement, async () => {
        if (params.length) api.bind_collection(statement, bindings(params));
        let columns: string[] | undefined;
        while (await api.step(statement) === 100) {
          if (rows.length >= budget.maxRows) throw new SQLiteBridgeBudgetError('rows');
          columns ??= api.column_names(statement);
          const entries: Array<[string, unknown]> = [];
          for (const [index, name] of columns.entries()) {
            const remaining = budget.maxTextBytes - bytes - name.length * 2;
            if (remaining < 0) throw new SQLiteBridgeBudgetError('text');
            if (api.column_type && api.column_bytes) {
              const type = api.column_type(statement, index);
              const size = type === 3 || type === 4 ? api.column_bytes(statement, index) : 0;
              // Check a guaranteed lower bound before creating a JS copy. UTF-8
              // text can use more bytes than UTF-16; leave the exact check below.
              if (type === 4 && size > remaining || type === 3 && size > remaining * 2)
                throw new SQLiteBridgeBudgetError('text');
            }
            const value = api.column(statement, index);
            bytes += name.length * 2 + (typeof value === 'string' ? value.length * 2
              : value instanceof Uint8Array ? value.byteLength : 8);
            if (bytes > budget.maxTextBytes) throw new SQLiteBridgeBudgetError('text');
            entries.push([name, value]);
          }
          rows.push(Object.fromEntries(entries) as T);
          if (first) return true;
        }
        return false;
      });
      if (stop) return rows;
    }
    return rows;
  }
  const connection: SQLiteReplicaDatabase = {
    execAsync: sql => serialize(() => discard(sql)),
    runAsync: (sql, ...params) => serialize(() => discard(sql, params)),
    getAllAsync: <T>(sql: string, ...params: unknown[]) => serialize(() => read<T>(sql, params, false)),
    getFirstAsync: <T>(sql: string, ...params: unknown[]) => serialize(async () => (await read<T>(sql, params, true))[0] ?? null),
    async withTransactionAsync(run) {
      if (transaction) throw new Error('Concurrent or nested SQLite transaction outside the storage actor');
      transaction = true;
      let began = false;
      try {
        await connection.execAsync('BEGIN'); began = true;
        await run();
        await connection.execAsync('COMMIT');
      } catch (error) {
        if (began) await connection.execAsync('ROLLBACK').catch(settled);
        throw error;
      } finally { transaction = false; }
    },
  };
  return connection;
}
