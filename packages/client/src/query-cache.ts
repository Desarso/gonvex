import type { Dexie as DexieDatabase, Table } from "dexie";
import type { JsonValue } from "@gonvex/protocol";

export const defaultQueryCacheMaxAgeMs = 24 * 60 * 60 * 1000;
export const defaultQueryCacheMaxEntryBytes = 2 * 1024 * 1024;
export const defaultQueryCacheMaxEntriesPerScope = 100;
export const defaultQueryCacheMaxBytesPerScope = 20 * 1024 * 1024;
export const defaultQueryCacheMaxBytes = 50 * 1024 * 1024;

export type QueryCacheLookup = {
  result: JsonValue;
  revision: string;
  writtenAt: number;
  ageMs: number;
};

export type QueryCacheWrite = {
  scope: string;
  path: string;
  args: JsonValue;
  result: JsonValue;
  revision: string;
  maxAgeMs: number;
};

export type QueryCacheWriteOutcome = "written" | "older" | "oversize" | "disabled" | "error";

export type QueryCacheStatus = {
  enabled: boolean;
  readsEnabled: boolean;
  writesEnabled: boolean;
  reason?: string;
};

export type QueryCacheStore = {
  read(scope: string, path: string, args: JsonValue, maxAgeMs?: number): Promise<QueryCacheLookup | undefined>;
  write(value: QueryCacheWrite): Promise<QueryCacheWriteOutcome>;
  delete(scope: string, path: string, args: JsonValue): Promise<void>;
  clear(scope?: string): Promise<void>;
  status(): QueryCacheStatus;
  close(): void;
};

export type QueryCacheOptions = {
  enabled?: boolean;
  maxAgeMs?: number;
  maxEntryBytes?: number;
  maxEntriesPerScope?: number;
  maxBytesPerScope?: number;
  maxBytes?: number;
  store?: QueryCacheStore;
  databaseName?: string;
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
};

type QueryCacheRecord = {
  scope: string;
  queryHash: string;
  result: JsonValue;
  revision: string;
  writtenAt: number;
  lastAccessedAt: number;
  expiresAt: number;
  sizeBytes: number;
};

type QueryCacheDatabase = DexieDatabase & {
  results: Table<QueryCacheRecord, [string, string]>;
};

export class DexieQueryCacheStore implements QueryCacheStore {
  private readonly databaseName: string;
  private readonly maxAgeMs: number;
  private readonly maxEntryBytes: number;
  private readonly maxEntriesPerScope: number;
  private readonly maxBytesPerScope: number;
  private readonly maxBytes: number;
  private readonly indexedDB?: IDBFactory;
  private readonly keyRange?: typeof IDBKeyRange;
  private readonly memory = new Map<string, QueryCacheRecord>();
  private databasePromise: Promise<QueryCacheDatabase> | undefined;
  private database: QueryCacheDatabase | undefined;
  private readsEnabled = true;
  private writesEnabled = true;
  private disabledReason: string | undefined;
  private cleanupScheduled = false;

  constructor(options: QueryCacheOptions = {}) {
    this.databaseName = options.databaseName ?? "gonvex-query-cache";
    this.maxAgeMs = positive(options.maxAgeMs, defaultQueryCacheMaxAgeMs);
    this.maxEntryBytes = positive(options.maxEntryBytes, defaultQueryCacheMaxEntryBytes);
    this.maxEntriesPerScope = positive(options.maxEntriesPerScope, defaultQueryCacheMaxEntriesPerScope);
    this.maxBytesPerScope = positive(options.maxBytesPerScope, defaultQueryCacheMaxBytesPerScope);
    this.maxBytes = positive(options.maxBytes, defaultQueryCacheMaxBytes);
    this.indexedDB = options.indexedDB;
    this.keyRange = options.IDBKeyRange;
    if (options.enabled === false) {
      this.disable("disabled-by-client");
    }
  }

  async read(scope: string, path: string, args: JsonValue, maxAgeMs = this.maxAgeMs): Promise<QueryCacheLookup | undefined> {
    if (!this.readsEnabled || !validScope(scope)) return undefined;
    try {
      const queryHash = await hashQuery(path, args);
      const memoryKey = recordMemoryKey(scope, queryHash);
      const now = Date.now();
      const memoryRecord = this.memory.get(memoryKey);
      if (memoryRecord) {
        if (recordExpired(memoryRecord, now, maxAgeMs)) {
          this.memory.delete(memoryKey);
          void this.deleteByHash(scope, queryHash).catch(() => undefined);
          return undefined;
        }
        this.touchMemory(memoryKey, memoryRecord, now);
        return lookup(memoryRecord, now);
      }

      const database = await this.open();
      const record = await database.results.get([scope, queryHash]);
      if (!record) return undefined;
      if (recordExpired(record, now, maxAgeMs)) {
        await database.results.delete([scope, queryHash]);
        return undefined;
      }
      this.touchMemory(memoryKey, record, now);
      void database.results.update([scope, queryHash], { lastAccessedAt: now }).catch(() => undefined);
      return lookup(record, now);
    } catch (error) {
      this.disable(errorReason(error));
      return undefined;
    }
  }

  async write(value: QueryCacheWrite): Promise<QueryCacheWriteOutcome> {
    if (!this.writesEnabled || !validScope(value.scope) || !value.revision) return "disabled";
    try {
      const queryHash = await hashQuery(value.path, value.args);
      const sizeBytes = jsonSize(value.result);
      if (sizeBytes > this.maxEntryBytes) return "oversize";
      const now = Date.now();
      const maxAgeMs = Math.min(this.maxAgeMs, positive(value.maxAgeMs, this.maxAgeMs));
      const record: QueryCacheRecord = {
        scope: value.scope,
        queryHash,
        result: value.result,
        revision: value.revision,
        writtenAt: now,
        lastAccessedAt: now,
        expiresAt: now + maxAgeMs,
        sizeBytes,
      };
      const outcome = await this.putRecord(record, false);
      if (outcome === "written") {
        this.touchMemory(recordMemoryKey(value.scope, queryHash), record, now);
        this.scheduleCleanup();
      }
      return outcome;
    } catch (error) {
      if (quotaExceeded(error)) {
        try {
          await this.cleanup(true);
          const queryHash = await hashQuery(value.path, value.args);
          const now = Date.now();
          const record: QueryCacheRecord = {
            scope: value.scope,
            queryHash,
            result: value.result,
            revision: value.revision,
            writtenAt: now,
            lastAccessedAt: now,
            expiresAt: now + Math.min(this.maxAgeMs, positive(value.maxAgeMs, this.maxAgeMs)),
            sizeBytes: jsonSize(value.result),
          };
          return await this.putRecord(record, true);
        } catch {
          this.writesEnabled = false;
          this.disabledReason = "quota-exceeded";
          return "error";
        }
      }
      this.writesEnabled = false;
      this.disabledReason = errorReason(error);
      return "error";
    }
  }

  async delete(scope: string, path: string, args: JsonValue) {
    if (!validScope(scope)) return;
    try {
      const queryHash = await hashQuery(path, args);
      this.memory.delete(recordMemoryKey(scope, queryHash));
      await this.deleteByHash(scope, queryHash);
    } catch {
      // Cache deletion is best-effort; server data remains authoritative.
    }
  }

  async clear(scope?: string) {
    this.memory.clear();
    if (!this.readsEnabled && !this.database) return;
    try {
      const database = await this.open();
      if (scope) {
        await database.results.where("scope").equals(scope).delete();
      } else {
        await database.results.clear();
      }
    } catch {
      // Clearing a disposable cache must not affect the application.
    }
  }

  status(): QueryCacheStatus {
    return {
      enabled: this.readsEnabled || this.writesEnabled,
      readsEnabled: this.readsEnabled,
      writesEnabled: this.writesEnabled,
      reason: this.disabledReason,
    };
  }

  close() {
    this.database?.close();
    this.database = undefined;
    this.databasePromise = undefined;
    this.memory.clear();
  }

  private async open() {
    if (!this.databasePromise) {
      this.databasePromise = this.createDatabase().catch((error) => {
        this.databasePromise = undefined;
        throw error;
      });
    }
    return this.databasePromise;
  }

  private async createDatabase(): Promise<QueryCacheDatabase> {
    const indexedDBValue = this.indexedDB ?? globalThis.indexedDB;
    const keyRangeValue = this.keyRange ?? globalThis.IDBKeyRange;
    if (!indexedDBValue || !keyRangeValue) throw new Error("indexeddb-unavailable");
    if (!globalThis.crypto?.subtle) throw new Error("web-crypto-unavailable");
    const { Dexie } = await import("dexie");
    const database = new Dexie(this.databaseName, {
      indexedDB: indexedDBValue,
      IDBKeyRange: keyRangeValue,
    }) as QueryCacheDatabase;
    database.version(1).stores({
      results: "[scope+queryHash], scope, [scope+lastAccessedAt], lastAccessedAt, expiresAt",
    });
    database.on("versionchange", () => database.close());
    await database.open();
    this.database = database;
    return database;
  }

  private async putRecord(record: QueryCacheRecord, afterCleanup: boolean): Promise<QueryCacheWriteOutcome> {
    if (record.sizeBytes > this.maxEntryBytes) return "oversize";
    const database = await this.open();
    const written = await database.transaction("rw", database.results, async () => {
      const previous = await database.results.get([record.scope, record.queryHash]);
      if (previous && previous.revision >= record.revision) return false;
      await database.results.put(record);
      return true;
    });
    const outcome: QueryCacheWriteOutcome = written ? "written" : "older";
    if (afterCleanup && outcome === "written") {
      this.touchMemory(recordMemoryKey(record.scope, record.queryHash), record, record.lastAccessedAt);
    }
    return outcome;
  }

  private async deleteByHash(scope: string, queryHash: string) {
    if (!this.readsEnabled && !this.database) return;
    const database = await this.open();
    await database.results.delete([scope, queryHash]);
  }

  private touchMemory(key: string, record: QueryCacheRecord, now: number) {
    this.memory.delete(key);
    this.memory.set(key, { ...record, lastAccessedAt: now });
    while (this.memory.size > this.maxEntriesPerScope) {
      const oldest = this.memory.keys().next().value as string | undefined;
      if (!oldest) break;
      this.memory.delete(oldest);
    }
  }

  private scheduleCleanup() {
    if (this.cleanupScheduled) return;
    this.cleanupScheduled = true;
    const run = () => {
      this.cleanupScheduled = false;
      void this.cleanup(false).catch(() => undefined);
    };
    if (typeof globalThis.requestIdleCallback === "function") {
      globalThis.requestIdleCallback(run, { timeout: 2_000 });
    } else {
      setTimeout(run, 0);
    }
  }

  private async cleanup(force: boolean) {
    const database = await this.open();
    const records = await database.results.orderBy("lastAccessedAt").toArray();
    const now = Date.now();
    const deleteKeys = new Set<string>();
    const scopeBytes = new Map<string, number>();
    const scopeCounts = new Map<string, number>();
    let globalBytes = 0;

    for (const record of [...records].reverse()) {
      const encodedKey = recordMemoryKey(record.scope, record.queryHash);
      if (record.expiresAt <= now) {
        deleteKeys.add(encodedKey);
        continue;
      }
      const nextScopeBytes = (scopeBytes.get(record.scope) ?? 0) + record.sizeBytes;
      const nextScopeCount = (scopeCounts.get(record.scope) ?? 0) + 1;
      const globalLimit = force ? Math.floor(this.maxBytes * 0.8) : this.maxBytes;
      if (
        nextScopeBytes > this.maxBytesPerScope
        || nextScopeCount > this.maxEntriesPerScope
        || globalBytes + record.sizeBytes > globalLimit
      ) {
        deleteKeys.add(encodedKey);
        continue;
      }
      scopeBytes.set(record.scope, nextScopeBytes);
      scopeCounts.set(record.scope, nextScopeCount);
      globalBytes += record.sizeBytes;
    }

    if (deleteKeys.size === 0) return;
    const keys: Array<[string, string]> = [];
    for (const record of records) {
      if (deleteKeys.has(recordMemoryKey(record.scope, record.queryHash))) {
        keys.push([record.scope, record.queryHash]);
        this.memory.delete(recordMemoryKey(record.scope, record.queryHash));
      }
    }
    await database.results.bulkDelete(keys);
  }

  private disable(reason: string) {
    this.readsEnabled = false;
    this.writesEnabled = false;
    this.disabledReason = reason;
  }
}

export function createQueryCacheStore(options: QueryCacheOptions | false | undefined): QueryCacheStore | undefined {
  if (options === false || options?.enabled === false) return undefined;
  if (options?.store) return options.store;
  return new DexieQueryCacheStore(options);
}

async function hashQuery(path: string, args: JsonValue) {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error("web-crypto-unavailable");
  const payload = new TextEncoder().encode(`${path}\u0000${stableStringify(args)}`);
  const digest = await subtle.digest("SHA-256", payload);
  return Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, "0")).join("");
}

function stableStringify(value: JsonValue): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key]!)}`)
    .join(",")}}`;
}

function lookup(record: QueryCacheRecord, now: number): QueryCacheLookup {
  return {
    result: record.result,
    revision: record.revision,
    writtenAt: record.writtenAt,
    ageMs: Math.max(0, now - record.writtenAt),
  };
}

function recordExpired(record: QueryCacheRecord, now: number, maxAgeMs: number) {
  return record.expiresAt <= now || now - record.writtenAt > Math.min(maxAgeMs, defaultQueryCacheMaxAgeMs);
}

function recordMemoryKey(scope: string, queryHash: string) {
  return `${scope}\u0000${queryHash}`;
}

function jsonSize(value: JsonValue) {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function quotaExceeded(error: unknown) {
  return (typeof DOMException !== "undefined" && error instanceof DOMException)
    ? error.name === "QuotaExceededError"
    : error instanceof Error && error.name === "QuotaExceededError";
}

function errorReason(error: unknown) {
  return error instanceof Error ? error.message || error.name : "query-cache-unavailable";
}

function validScope(scope: string) {
  return scope.length >= 16;
}

function positive(value: number | undefined, fallback: number) {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : fallback;
}
