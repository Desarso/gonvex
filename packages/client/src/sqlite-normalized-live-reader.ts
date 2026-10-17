import type { JsonValue } from '@gonvex/protocol';
import type { OptimisticPatch } from './optimistic.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import type { LiveQueryPlan, OfflineLiveQueryResult } from './query-expression.js';
import { readSQLiteLiveWindow, type SQLiteLiveReadBudget, type SQLiteLiveReadSnapshot } from './sqlite-live-reader.js';
import { readViewMessageBytes } from './remote-read-view.js';

type QueryContext = {
  patches: readonly OptimisticPatch[];
  /** Called inside the same canonical read transaction as the query. The host
   * must establish complete coverage for this exact plan, including relations,
   * projected fields and search sources. Resident row presence is not proof. */
  coverage: SQLiteLiveReadSnapshot['coverage'];
};
export type NormalizedLiveReadBudget = SQLiteLiveReadBudget & {
  maxPatches: number;
  maxOverlayBytes: number;
};
const rowSource = '_gonvex_live_rows';
const schema = `CREATE TEMP TABLE IF NOT EXISTS _gonvex_live_overlay (
 scope TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,value TEXT NOT NULL,position INTEGER NOT NULL,
 PRIMARY KEY(scope,entity,id));
 CREATE TEMP VIEW IF NOT EXISTS ${rowSource} AS
 SELECT e.scope,e.entity,e.id,e.value,e.rowid AS position FROM main._gonvex_replica_entities e
 WHERE e.value <> 'null' AND NOT EXISTS(SELECT 1 FROM _gonvex_live_overlay p WHERE p.scope=e.scope AND p.entity=e.entity AND p.id=e.id)
 UNION ALL SELECT scope,entity,id,value,position FROM _gonvex_live_overlay WHERE value <> 'null';`;

/** Query the existing normalized SQL replica, not a second durable task store.
 * The caller's worker actor serializes this whole operation with storage writes
 * and holds the SDK intent coordination needed to capture a consistent pending
 * overlay. It also fences scope/query/prediction generations at publication.
 * The connection must have the compiler's JS-semantic functions/collation and
 * bounded result/native/temp storage installed. This helper neither creates a
 * database nor establishes visibility, coverage, migration or transport credit.
 * Pending rows exist only in a transaction-local TEMP overlay. Canonical rows
 * are never overwritten by predictions. Limits fail explicitly, not by dropping
 * intents. Large journals need disk paging before this can serve arbitrary use.
 */
export async function readNormalizedSQLiteLiveWindow<T extends Record<string, unknown>>(
  database: SQLiteReplicaDatabase, plan: LiveQueryPlan, args: Record<string, JsonValue>, scope: string,
  context: Readonly<Record<string, JsonValue>>, budget: NormalizedLiveReadBudget,
  capture: () => Promise<QueryContext>, signal?: AbortSignal,
): Promise<OfflineLiveQueryResult<T>> {
  for (const value of [budget.maxPatches, budget.maxOverlayBytes])
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Positive finite overlay limits required');
  return readSQLiteLiveWindow<T>({ async readSnapshot(readScope, run) {
    let result!: Awaited<ReturnType<typeof run>>;
    await database.withTransactionAsync(async () => {
      signal?.throwIfAborted();
      await database.execAsync(schema);
      await database.runAsync('DELETE FROM _gonvex_live_overlay');
      const captured = await capture();
      signal?.throwIfAborted();
      if (captured.patches.length > budget.maxPatches) throw new RangeError('Pending SQLite overlay count budget exceeded');
      readViewMessageBytes(captured.patches, budget.maxOverlayBytes);
      let bytes = 0;
      const last = await database.getFirstAsync<{ position: number | null }>('SELECT max(rowid) AS position FROM _gonvex_replica_entities');
      let nextPosition = Number(last?.position ?? 0);
      for (const patch of captured.patches) {
        signal?.throwIfAborted();
        const entity = patch.entity ?? patch.collection!;
        const pending = await database.getFirstAsync<{ value: string; position: number }>(
          'SELECT value,position FROM _gonvex_live_overlay WHERE scope=? AND entity=? AND id=?', readScope, entity, patch.rowId);
        const previous = pending ?? await database.getFirstAsync<{ value: string; position: number }>(
            'SELECT value,rowid AS position FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?', readScope, entity, patch.rowId);
        const old = previous && previous.value !== 'null' ? JSON.parse(previous.value) as Record<string, JsonValue> : undefined;
        const value = patch.op === 'delete' ? null : patch.op === 'patch' ? { ...old, ...patch.fields } : patch.fields;
        // Charge every intermediate merge as well as the input patch graph.
        // A long chain cannot bypass the decoding/bridge work allowance.
        bytes += readViewMessageBytes(value, budget.maxOverlayBytes);
        if (bytes > budget.maxOverlayBytes) throw new RangeError('Pending SQLite overlay byte budget exceeded');
        // LocalReplica keeps a pending delete as an ordering tombstone.
        // Reinsertions retain that established slot, including locale-equal IDs.
        const position = pending?.position ?? (old ? previous!.position : ++nextPosition);
        if (!Number.isSafeInteger(position)) throw new RangeError('SQLite overlay position exceeds safe integer range');
        await database.runAsync('INSERT INTO _gonvex_live_overlay VALUES(?,?,?,?,?) ON CONFLICT(scope,entity,id) DO UPDATE SET value=excluded.value,position=excluded.position',
          readScope, entity, patch.rowId, JSON.stringify(value), position);
      }
      result = await run({ coverage: captured.coverage, async *rows(statement) {
        signal?.throwIfAborted();
        const records = await database.getAllAsync<{ kind: unknown; value: unknown; total: unknown; missingSortFields: unknown; missingReferences: unknown }>(statement.sql, ...statement.bindings);
        for (const row of records) { signal?.throwIfAborted(); yield [row.kind, row.value, row.total, row.missingSortFields, row.missingReferences]; }
      } });
      signal?.throwIfAborted();
      await database.runAsync('DELETE FROM _gonvex_live_overlay');
    });
    return result;
  } }, plan, args, scope, context, budget, signal, { rowsTable: rowSource });
}
