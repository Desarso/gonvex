import type { JsonValue, QueryCacheDirective } from "@gonvex/protocol";
import type { MutationOutboxEntry, OutboxStore } from "./outbox.js";
import {
  defaultQueryCacheMaxAgeMs,
  defaultQueryCacheMaxBytes,
  defaultQueryCacheMaxBytesPerScope,
  defaultQueryCacheMaxEntriesPerScope,
  defaultQueryCacheMaxEntryBytes,
  type QueryCacheLookup,
  type QueryCacheStatus,
  type QueryCacheStore,
  type QueryCacheWrite,
  type QueryCacheWriteOutcome,
} from "./query-cache.js";
import {
  defaultSyncStoreMaxBytes,
  type StoredSyncCollection,
  type SyncStore,
} from "./sync-store.js";

/**
 * Injectable Gonvex persistence over a tiny key-value backend.
 *
 * The client defaults to Dexie/IndexedDB. React Native has neither, so these
 * factories implement {@link QueryCacheStore}, {@link SyncStore}, and
 * {@link OutboxStore} against a five-method string store the host binds once —
 * an expo-sqlite table in production, {@link createMemoryGonvexKv} in tests.
 */
export type GonvexKv = {
  get(table: string, key: string): Promise<string | undefined>;
  set(table: string, key: string, value: string): Promise<void>;
  delete(table: string, key: string): Promise<void>;
  list(table: string): Promise<Array<{ key: string; value: string }>>;
  clear(table: string, keyPrefix?: string): Promise<void>;
};

export const kvQueryCacheTable = "query_cache";
export const kvSyncCollectionTable = "sync_collections";
export const kvSyncDirectiveTable = "sync_directives";
export const kvMutationOutboxTable = "mutation_outbox";

/** An in-memory {@link GonvexKv} for tests and ephemeral sessions. */
export function createMemoryGonvexKv(): GonvexKv {
  const tables = new Map<string, Map<string, string>>();
  const table = (name: string) => {
    let rows = tables.get(name);
    if (!rows) {
      rows = new Map();
      tables.set(name, rows);
    }
    return rows;
  };
  return {
    async get(name, key) {
      return table(name).get(key);
    },
    async set(name, key, value) {
      table(name).set(key, value);
    },
    async delete(name, key) {
      table(name).delete(key);
    },
    async list(name) {
      return [...table(name).entries()].map(([key, value]) => ({ key, value }));
    },
    async clear(name, keyPrefix) {
      const rows = table(name);
      if (!keyPrefix) {
        rows.clear();
        return;
      }
      for (const key of [...rows.keys()]) {
        if (key.startsWith(keyPrefix)) rows.delete(key);
      }
    },
  };
}

export type KvQueryCacheStoreOptions = {
  maxAgeMs?: number;
  maxEntryBytes?: number;
  maxEntriesPerScope?: number;
  maxBytesPerScope?: number;
  maxBytes?: number;
  /** Invoked whenever a write lands under a scope not seen since the last one. */
  onScope?: (scope: string) => void | Promise<void>;
};

type KvQueryCacheRecord = {
  scope: string;
  path: string;
  result: JsonValue;
  revision: string;
  writtenAt: number;
  lastAccessedAt: number;
  sizeBytes: number;
};

/**
 * A {@link QueryCacheStore} over an injected {@link GonvexKv}. Records are
 * keyed by `scope\u0000path\u0000stableArgs`, revision-guarded like the Dexie
 * store, and LRU-evicted by `lastAccessedAt` against per-scope and global
 * caps. A failing backend permanently disables the cache for the session;
 * server data remains authoritative.
 */
export function createKvQueryCacheStore(
  kv: GonvexKv,
  options: KvQueryCacheStoreOptions = {},
): QueryCacheStore {
  const maxAgeMs = positive(options.maxAgeMs, defaultQueryCacheMaxAgeMs);
  const maxEntryBytes = positive(options.maxEntryBytes, defaultQueryCacheMaxEntryBytes);
  const limits = {
    maxEntriesPerScope: positive(options.maxEntriesPerScope, defaultQueryCacheMaxEntriesPerScope),
    maxBytesPerScope: positive(options.maxBytesPerScope, defaultQueryCacheMaxBytesPerScope),
    maxBytes: positive(options.maxBytes, defaultQueryCacheMaxBytes),
  };
  const onScope = options.onScope;
  let lastEmittedScope: string | undefined;
  let readsEnabled = true;
  let writesEnabled = true;
  let disabledReason: string | undefined;

  const disable = (reason: string) => {
    readsEnabled = false;
    writesEnabled = false;
    disabledReason = reason;
  };

  return {
    async read(scope, path, args, ageMs = maxAgeMs) {
      if (!readsEnabled || !scope) return undefined;
      try {
        const key = storageKey(scope, path, args);
        const record = parseJson<KvQueryCacheRecord>(await kv.get(kvQueryCacheTable, key));
        if (!record) return undefined;
        const now = Date.now();
        if (now - record.writtenAt > Math.min(ageMs, maxAgeMs)) {
          await kv.delete(kvQueryCacheTable, key);
          return undefined;
        }
        record.lastAccessedAt = now;
        await kv.set(kvQueryCacheTable, key, JSON.stringify(record));
        return {
          result: record.result,
          revision: record.revision,
          writtenAt: record.writtenAt,
          ageMs: now - record.writtenAt,
        } satisfies QueryCacheLookup;
      } catch (error) {
        disable(errorReason(error, "kv-query-cache-read-failed"));
        return undefined;
      }
    },
    async write(value: QueryCacheWrite): Promise<QueryCacheWriteOutcome> {
      if (!writesEnabled || !value.scope || !value.revision) return "disabled";
      try {
        const sizeBytes = jsonSize(value.result);
        if (sizeBytes > maxEntryBytes) return "oversize";
        const key = storageKey(value.scope, value.path, value.args);
        const existing = parseJson<KvQueryCacheRecord>(await kv.get(kvQueryCacheTable, key));
        if (existing && existing.revision >= value.revision) return "older";
        const now = Date.now();
        const record: KvQueryCacheRecord = {
          scope: value.scope,
          path: value.path,
          result: value.result,
          revision: value.revision,
          writtenAt: now,
          lastAccessedAt: now,
          sizeBytes,
        };
        await kv.set(kvQueryCacheTable, key, JSON.stringify(record));
        await evictQueryCache(kv, limits);
        if (onScope && lastEmittedScope !== value.scope) {
          lastEmittedScope = value.scope;
          await onScope(value.scope);
        }
        return "written";
      } catch (error) {
        disable(errorReason(error, "kv-query-cache-write-failed"));
        return "error";
      }
    },
    async delete(scope, path, args) {
      try {
        await kv.delete(kvQueryCacheTable, storageKey(scope, path, args));
      } catch {
        // Cache deletion is best-effort; server data remains authoritative.
      }
    },
    async clear(scope) {
      try {
        await kv.clear(kvQueryCacheTable, scope ? `${scope}\u0000` : undefined);
      } catch {
        // Clearing a disposable cache must not affect the application.
      }
    },
    status(): QueryCacheStatus {
      return {
        enabled: readsEnabled || writesEnabled,
        readsEnabled,
        writesEnabled,
        reason: disabledReason,
      };
    },
    close() {
      // The KV connection belongs to the host and outlives this store.
    },
  };
}

async function evictQueryCache(
  kv: GonvexKv,
  limits: { maxEntriesPerScope: number; maxBytesPerScope: number; maxBytes: number },
): Promise<void> {
  const rows: Array<{ key: string; record: KvQueryCacheRecord }> = [];
  const corrupt: string[] = [];
  for (const { key, value } of await kv.list(kvQueryCacheTable)) {
    const record = parseJson<KvQueryCacheRecord>(value);
    if (record) rows.push({ key, record });
    else corrupt.push(key);
  }
  rows.sort((left, right) => left.record.lastAccessedAt - right.record.lastAccessedAt);
  const byScope = new Map<string, typeof rows>();
  for (const row of rows) {
    const list = byScope.get(row.record.scope) ?? [];
    list.push(row);
    byScope.set(row.record.scope, list);
  }
  const evict = new Set<string>(corrupt);
  let totalBytes = 0;
  for (const list of byScope.values()) {
    let scopeBytes = 0;
    let scopeCount = 0;
    // Newest first: eviction always drops the least recently used entries.
    for (let index = list.length - 1; index >= 0; index -= 1) {
      const row = list[index]!;
      if (
        scopeCount >= limits.maxEntriesPerScope
        || scopeBytes + row.record.sizeBytes > limits.maxBytesPerScope
      ) {
        evict.add(row.key);
        continue;
      }
      scopeCount += 1;
      scopeBytes += row.record.sizeBytes;
      totalBytes += row.record.sizeBytes;
    }
  }
  for (const row of rows) {
    if (totalBytes <= limits.maxBytes) break;
    if (evict.has(row.key)) continue;
    evict.add(row.key);
    totalBytes -= row.record.sizeBytes;
  }
  for (const key of evict) await kv.delete(kvQueryCacheTable, key);
}

export type KvSyncStoreOptions = {
  maxBytes?: number;
};

type KvSyncCollectionRecord = StoredSyncCollection & {
  lastAccessedAt: number;
  sizeBytes: number;
};

/**
 * A {@link SyncStore} over an injected {@link GonvexKv}. Each collection is
 * one record holding its rows, cursor, and metadata; deltas are applied by
 * key against the stored rows. Collections are LRU-evicted by
 * `lastAccessedAt` once the table exceeds `maxBytes`. A failing backend
 * permanently disables the store for the session.
 */
export function createKvSyncStore(
  kv: GonvexKv,
  options: KvSyncStoreOptions = {},
): SyncStore {
  const maxBytes = positive(options.maxBytes, defaultSyncStoreMaxBytes);
  let disabled = false;

  const loadRecord = async (key: string) =>
    parseJson<KvSyncCollectionRecord>(await kv.get(kvSyncCollectionTable, key));

  const persist = async (key: string, value: StoredSyncCollection) => {
    const record: KvSyncCollectionRecord = {
      ...value,
      rowsUnchanged: undefined,
      lastAccessedAt: Date.now(),
      sizeBytes: jsonSize(value.rows),
    };
    await kv.set(kvSyncCollectionTable, key, JSON.stringify(record));
    await evictSyncCollections(kv, maxBytes);
  };

  return {
    async load(scope, path, args) {
      if (disabled || !scope) return undefined;
      try {
        const key = storageKey(scope, path, args);
        const record = await loadRecord(key);
        if (!record) return undefined;
        record.lastAccessedAt = Date.now();
        await kv.set(kvSyncCollectionTable, key, JSON.stringify(record));
        const { lastAccessedAt: _lastAccessedAt, sizeBytes: _sizeBytes, ...collection } = record;
        return collection;
      } catch {
        disabled = true;
        return undefined;
      }
    },
    async replace(scope, path, args, value) {
      if (disabled || !scope) return;
      try {
        const key = storageKey(scope, path, args);
        if (value.rowsUnchanged) {
          const existing = await loadRecord(key);
          // The caller proved the rows are byte-identical, so only advance
          // the cursor/metadata. A missing record falls through to a full
          // write.
          if (existing) {
            await persist(key, {
              ...existing,
              cursor: value.cursor,
              keyField: value.keyField,
              mode: value.mode,
              truncated: value.truncated,
              orderBy: value.orderBy,
              orderDirection: value.orderDirection,
              maxRows: value.maxRows,
              maxBytes: value.maxBytes,
              hashes: value.hashes,
            });
            return;
          }
        }
        await persist(key, value);
      } catch {
        disabled = true;
      }
    },
    async applyDelta(scope, path, args, value) {
      if (disabled || !scope) return;
      try {
        const key = storageKey(scope, path, args);
        const existing = await loadRecord(key);
        const keyField = value.keyField || existing?.keyField || "_id";
        const deleted = new Set(value.deleted.map(String));
        const kept = (existing?.rows ?? []).filter((row) => !deleted.has(rowKey(row, keyField)));
        const byKey = new Map(kept.map((row) => [rowKey(row, keyField), row] as const));
        for (const upsert of value.upserts) {
          const id = rowKey(upsert, keyField);
          if (!id) continue;
          byKey.set(id, upsert);
        }
        await persist(key, {
          rows: [...byKey.values()],
          cursor: value.cursor,
          keyField,
          mode: value.mode ?? existing?.mode,
          truncated: value.truncated,
          orderBy: value.orderBy ?? existing?.orderBy,
          orderDirection: value.orderDirection ?? existing?.orderDirection,
          maxRows: value.maxRows ?? existing?.maxRows,
          maxBytes: value.maxBytes ?? existing?.maxBytes,
          hashes: value.hashes ?? existing?.hashes,
        });
      } catch {
        disabled = true;
      }
    },
    async delete(scope, path, args) {
      try {
        await kv.delete(kvSyncCollectionTable, storageKey(scope, path, args));
      } catch {
        // Disposable local data must never break the application.
      }
    },
    async loadDirective(identity) {
      if (!identity) return undefined;
      try {
        return parseJson<QueryCacheDirective>(await kv.get(kvSyncDirectiveTable, identity));
      } catch {
        return undefined;
      }
    },
    async saveDirective(identity, directive) {
      if (!identity) return;
      try {
        await kv.set(kvSyncDirectiveTable, identity, JSON.stringify(directive));
      } catch {
        // Warm-start metadata is an optimization, never a request prerequisite.
      }
    },
    async clear(scope) {
      try {
        await kv.clear(kvSyncCollectionTable, scope ? `${scope}\u0000` : undefined);
        if (!scope) await kv.clear(kvSyncDirectiveTable);
      } catch {
        // Disposable local data must never break the application.
      }
    },
    close() {
      // The KV connection belongs to the host and outlives this store.
    },
  };
}

async function evictSyncCollections(kv: GonvexKv, maxBytes: number): Promise<void> {
  const rows: Array<{ key: string; record: KvSyncCollectionRecord }> = [];
  for (const { key, value } of await kv.list(kvSyncCollectionTable)) {
    const record = parseJson<KvSyncCollectionRecord>(value);
    if (record) rows.push({ key, record });
    else await kv.delete(kvSyncCollectionTable, key);
  }
  rows.sort((left, right) => left.record.lastAccessedAt - right.record.lastAccessedAt);
  let total = rows.reduce((sum, row) => sum + (row.record.sizeBytes || 0), 0);
  for (const row of rows) {
    if (total <= maxBytes) break;
    await kv.delete(kvSyncCollectionTable, row.key);
    total -= row.record.sizeBytes || 0;
  }
}

/**
 * An {@link OutboxStore} over an injected {@link GonvexKv}. Entries are
 * whole JSON records keyed by their queue id; ordering, causal barriers, and
 * inflight recovery stay in {@link StoreMutationOutbox}.
 */
export function createKvOutboxStore(kv: GonvexKv): OutboxStore {
  return {
    async load() {
      const entries: MutationOutboxEntry[] = [];
      for (const { value } of await kv.list(kvMutationOutboxTable)) {
        // A corrupt row must not strand every other queued mutation.
        const entry = parseJson<MutationOutboxEntry>(value);
        if (entry && typeof entry.id === "number") entries.push(entry);
      }
      return entries.sort((left, right) => left.id - right.id);
    },
    async put(entry) {
      await kv.set(kvMutationOutboxTable, String(entry.id), JSON.stringify(entry));
    },
    async delete(id) {
      await kv.delete(kvMutationOutboxTable, String(id));
    },
    async clear(scope) {
      if (scope === undefined) {
        await kv.clear(kvMutationOutboxTable);
        return;
      }
      for (const { key, value } of await kv.list(kvMutationOutboxTable)) {
        if (parseJson<MutationOutboxEntry>(value)?.scope === scope) {
          await kv.delete(kvMutationOutboxTable, key);
        }
      }
    },
  };
}

function storageKey(scope: string, path: string, args: unknown): string {
  return `${scope}\u0000${path}\u0000${stableStringify(args ?? {})}`;
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
    .join(",")}}`;
}

function parseJson<T>(value: string | undefined): T | undefined {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(value) as T;
  } catch {
    return undefined;
  }
}

function rowKey(row: JsonValue, keyField: string): string {
  if (!row || Array.isArray(row) || typeof row !== "object") return "";
  const candidate = row[keyField];
  return typeof candidate === "string" || typeof candidate === "number" ? String(candidate) : "";
}

function jsonSize(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value ?? null)).byteLength;
}

function errorReason(error: unknown, fallback: string) {
  return error instanceof Error ? error.message || error.name : fallback;
}

function positive(value: number | undefined, fallback: number) {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : fallback;
}
