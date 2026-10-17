import type { JsonValue } from '@gonvex/protocol';
import type { LiveQueryPlan, OfflineLiveQueryResult } from './query-expression.js';
import { compileSQLiteLiveQuery, type SQLiteLiveStatement } from './sqlite-query-expression.js';

type Row = Record<string, unknown>;
/** The adapter owns BEGIN/COMMIT, serialization with writes, and optimistic
 * overlays. It must interrupt retired statements when possible, rather than
 * queueing cancellation behind the read. Coverage and rows must describe the
 * same durable read version.
 * This interface does not install an adapter or create another entity cache. */
export interface SQLiteLiveReadSnapshot {
  /** Internal normalized source selected after capturing the coherent overlay.
   * The compiler validates the identifier; application values remain bound. */
  rowsTable?: string;
  rows(statement: SQLiteLiveStatement): AsyncIterable<readonly unknown[]>;
  coverage(plan: LiveQueryPlan, args: Record<string, JsonValue>,
    context: Readonly<Record<string, JsonValue>>): Promise<'complete' | 'partial'>;
}
export interface SQLiteLiveReadDatabase {
  readSnapshot<T>(scope: string, run: (snapshot: SQLiteLiveReadSnapshot) => Promise<T>,
    signal?: AbortSignal): Promise<T>;
}
export type SQLiteLiveReadBudget = {
  maxRows: number;
  /** UTF-16 text returned by SQLite, checked before JSON.parse. This bounds the
   * query bridge, not the JS heap cost of decoded rows or the replica ledger. */
  maxTextBytes: number;
};

/** Internal candidate reader. One disk scan supplies exact counts, preflights
 * and the bounded row window. No whole table, deep-offset heap or query cache.
 * The bridge must finalize the statement when its iterator is closed, including
 * cancellation and budget failures. Its own native/temp/WASM limits are separate.
 * A budget failure is explicit; it never becomes a silently truncated result. */
export async function readSQLiteLiveWindow<T extends Row = Row>(database: SQLiteLiveReadDatabase,
  plan: LiveQueryPlan, args: Record<string, JsonValue>, scope: string,
  context: Readonly<Record<string, JsonValue>>, budget: SQLiteLiveReadBudget,
  signal?: AbortSignal, source: { rowsTable?: string } = {}): Promise<OfflineLiveQueryResult<T>> {
  for (const value of [budget.maxRows, budget.maxTextBytes])
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Positive finite query budget required');
  const abort = () => signal?.throwIfAborted();
  abort();
  const partial = (reason: string): OfflineLiveQueryResult<T> =>
    ({ rows: [], completeness: 'partial', supported: false, unsupportedOperator: reason });
  const prepared = compileSQLiteLiveQuery(plan, args, scope, context, source);
  if (!prepared.supported) return partial(prepared.unsupportedOperator);
  if (prepared.limit > budget.maxRows) return partial('queryRowBudget');
  const result = await database.readSnapshot(scope, async (snapshot): Promise<OfflineLiveQueryResult<T>> => {
    abort();
    const compiled = snapshot.rowsTable === undefined || snapshot.rowsTable === source.rowsTable
      ? prepared : compileSQLiteLiveQuery(plan, args, scope, context, { rowsTable: snapshot.rowsTable });
    if (!compiled.supported) return partial(compiled.unsupportedOperator);
    const coverage = await snapshot.coverage(plan, args, context);
    abort();
    let summary: readonly unknown[] | undefined, bytes = 0;
    const rows: T[] = [];
    for await (const record of snapshot.rows(compiled.combined)) {
      abort();
      if (!summary) {
        if (record.length !== 5 || record[0] !== 0 || record[1] !== null
          || (compiled.count ? !Number.isSafeInteger(record[2]) || Number(record[2]) < 0 : record[2] !== null)
          || (record[3] !== 0 && record[3] !== 1) || (record[4] !== 0 && record[4] !== 1))
          throw new Error('Invalid SQLite Live Query summary');
        summary = record;
        if (record[3]) return partial('missingSortField');
        continue;
      }
      if (record.length !== 5 || record[0] !== 1 || typeof record[1] !== 'string'
        || record[2] !== null || record[3] !== null || record[4] !== null)
        throw new Error('Invalid SQLite Live Query row');
      if (rows.length >= compiled.limit) throw new Error('SQLite Live Query exceeded its window');
      bytes += record[1].length * 2;
      if (bytes > budget.maxTextBytes) return partial('queryTextBudget');
      const row: unknown = JSON.parse(record[1]);
      if (!row || typeof row !== 'object' || Array.isArray(row)) throw new Error('Invalid normalized SQLite row');
      rows.push(row as T);
    }
    abort();
    if (!summary) throw new Error('Missing SQLite Live Query summary');
    return { rows, ...(compiled.count ? { total: Number(summary[2]), offset: compiled.offset, limit: compiled.limit } : {}), supported: true,
      completeness: coverage === 'complete' && !compiled.searchSourcesPartial && !summary[4] ? 'complete' : 'partial' };
  }, signal);
  abort();
  return result;
}
