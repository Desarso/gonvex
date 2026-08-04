import type { JsonValue, QueryCacheDirective, SyncCursor } from "@gonvex/protocol";
import type { Dexie as DexieDatabase, Table } from "dexie";

export const defaultSyncStoreMaxBytes = 100 * 1024 * 1024;

export type SyncStoreOptions = {
  enabled?: boolean;
  databaseName?: string;
  maxBytes?: number;
  hashes?: Record<string, string>;
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
  store?: SyncStore;
};

export type StoredSyncCollection = {
  rows: JsonValue[];
  cursor: SyncCursor;
  keyField: string;
  mode?: "eager" | "progressive";
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  maxRows?: number;
  maxBytes?: number;
  hashes?: Record<string, string>;
  /**
   * The caller proved these rows are byte-identical to what is already
   * stored, so only the collection's cursor/metadata needs to advance.
   * Rewriting unchanged rows is what turns a quiet reload into a full
   * delete+insert of every row of every collection.
   */
  rowsUnchanged?: boolean;
};

export type SyncStore = {
  load(scope: string, path: string, args: JsonValue): Promise<StoredSyncCollection | undefined>;
  replace(
    scope: string,
    path: string,
    args: JsonValue,
    value: StoredSyncCollection,
  ): Promise<void>;
  applyDelta(
    scope: string,
    path: string,
    args: JsonValue,
    value: {
      cursor: SyncCursor;
      keyField: string;
      orderBy?: string;
      orderDirection?: "asc" | "desc";
      upserts: JsonValue[];
      deleted: string[];
      maxRows?: number;
      maxBytes?: number;
      hashes?: Record<string, string>;
    },
  ): Promise<void>;
  delete(scope: string, path: string, args: JsonValue): Promise<void>;
  loadDirective(identity: string): Promise<QueryCacheDirective | undefined>;
  saveDirective(identity: string, directive: QueryCacheDirective): Promise<void>;
  clear(scope?: string): Promise<void>;
  close(): void;
};

type CollectionRecord = {
  key: string;
  scope: string;
  path: string;
  argsHash: string;
  cursor: SyncCursor;
  keyField: string;
  mode?: "eager" | "progressive";
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  maxRows?: number;
  maxBytes?: number;
  hashes?: Record<string, string>;
  rowCount: number;
  sizeBytes: number;
  lastAccessedAt: number;
};

type RowRecord = {
  collectionKey: string;
  rowId: string;
  value: JsonValue;
  rank: number;
  sizeBytes: number;
};

type IdentityRecord = {
  identity: string;
  directive: QueryCacheDirective;
  lastAccessedAt: number;
};

type SyncDatabase = DexieDatabase & {
  collections: Table<CollectionRecord, string>;
  rows: Table<RowRecord, [string, string]>;
  identities: Table<IdentityRecord, string>;
};

export class DexieSyncStore implements SyncStore {
  private readonly databaseName: string;
  private readonly maxBytes: number;
  private readonly indexedDB?: IDBFactory;
  private readonly keyRange?: typeof IDBKeyRange;
  private database?: SyncDatabase;
  private databasePromise?: Promise<SyncDatabase>;
  private disabled = false;
  private cleanupScheduled = false;

  constructor(options: SyncStoreOptions = {}) {
    this.databaseName = options.databaseName ?? "gonvex-sync";
    this.maxBytes = positive(options.maxBytes, defaultSyncStoreMaxBytes);
    this.indexedDB = options.indexedDB;
    this.keyRange = options.IDBKeyRange;
    this.disabled = options.enabled === false;
  }

  async load(scope: string, path: string, args: JsonValue): Promise<StoredSyncCollection | undefined> {
    if (this.disabled || !scope) return undefined;
    try {
      const key = await collectionKey(scope, path, args);
      const database = await this.open();
      let collection: CollectionRecord | undefined;
      let storedRows: RowRecord[] = [];
      await database.transaction("r", database.collections, database.rows, async () => {
        collection = await database.collections.get(key);
        if (collection) {
          storedRows = await database.rows.where("collectionKey").equals(key).sortBy("rank");
        }
      });
      if (!collection) return undefined;
      const rows = sortSyncRows(storedRows, collection.orderBy, collection.orderDirection);
      void database.collections.update(key, { lastAccessedAt: Date.now() }).catch(() => undefined);
      return {
        rows: rows.map((row) => row.value),
        cursor: collection.cursor,
        keyField: collection.keyField,
        mode: collection.mode,
        orderBy: collection.orderBy,
        orderDirection: collection.orderDirection,
        maxRows: collection.maxRows,
        maxBytes: collection.maxBytes,
        hashes: collection.hashes,
      };
    } catch {
      this.disabled = true;
      return undefined;
    }
  }

  async replace(scope: string, path: string, args: JsonValue, value: StoredSyncCollection): Promise<void> {
    if (this.disabled || !scope) return;
    try {
      const key = await collectionKey(scope, path, args);
      const argsHash = await hashValue(stableStringify(args));
      const database = await this.open();
      // A quiet reload re-advertises every collection at a new revision with
      // byte-identical rows. Rewriting them would delete and re-insert every
      // row of every collection on every load; IndexedDB keeps the superseded
      // versions until LevelDB compacts, so that churn is what grows the
      // origin's store into the gigabytes. Advance the cursor only.
      if (value.rowsUnchanged) {
        let metadataOnly = false;
        await database.transaction("rw", database.collections, async () => {
          const existing = await database.collections.get(key);
          if (!existing) return;
          if (
            existing.cursor.epoch === value.cursor.epoch
            && existing.cursor.revision > value.cursor.revision
          ) {
            metadataOnly = true;
            return;
          }
          await database.collections.put({
            ...existing,
            cursor: value.cursor,
            keyField: value.keyField,
            mode: value.mode,
            orderBy: value.orderBy,
            orderDirection: value.orderDirection,
            maxRows: value.maxRows,
            maxBytes: value.maxBytes,
            hashes: value.hashes,
            lastAccessedAt: Date.now(),
          });
          metadataOnly = true;
        });
        // Only a missing collection record falls through to a full write.
        if (metadataOnly) return;
      }
      const rows = boundedRows(
        value.rows,
        value.keyField,
        value.maxRows,
        value.maxBytes,
        value.orderBy,
        value.orderDirection,
      )
        .map((row, rank): RowRecord => ({
          collectionKey: key,
          rowId: rowKey(row, value.keyField),
          value: row,
          rank,
          sizeBytes: jsonSize(row),
        }));
      const sizeBytes = rows.reduce((sum, row) => sum + row.sizeBytes, 0);
      await database.transaction("rw", database.collections, database.rows, async () => {
        const existing = await database.collections.get(key);
        if (
          existing?.cursor.epoch === value.cursor.epoch
          && existing.cursor.revision > value.cursor.revision
        ) return;
        await database.rows.where("collectionKey").equals(key).delete();
        if (rows.length > 0) await database.rows.bulkPut(rows);
        await database.collections.put({
          key,
          scope,
          path,
          argsHash,
          cursor: value.cursor,
          keyField: value.keyField,
          mode: value.mode,
          orderBy: value.orderBy,
          orderDirection: value.orderDirection,
          maxRows: value.maxRows,
          maxBytes: value.maxBytes,
          hashes: value.hashes,
          rowCount: rows.length,
          sizeBytes,
          lastAccessedAt: Date.now(),
        });
      });
      this.scheduleCleanup();
    } catch {
      this.disabled = true;
    }
  }

  async applyDelta(
    scope: string,
    path: string,
    args: JsonValue,
    value: {
      cursor: SyncCursor;
      keyField: string;
      mode?: "eager" | "progressive";
      orderBy?: string;
      orderDirection?: "asc" | "desc";
      upserts: JsonValue[];
      deleted: string[];
      maxRows?: number;
      maxBytes?: number;
      hashes?: Record<string, string>;
    },
  ): Promise<void> {
    if (this.disabled || !scope) return;
    try {
      const key = await collectionKey(scope, path, args);
      const database = await this.open();
      await database.transaction("rw", database.collections, database.rows, async () => {
        const collection = await database.collections.get(key);
        if (!collection) return;
        if (
          collection.cursor.epoch !== value.cursor.epoch
          || collection.cursor.revision > value.cursor.revision
        ) return;
        const deleted = new Set(value.deleted);
        for (const row of value.upserts) deleted.delete(rowKey(row, value.keyField));
        if (deleted.size > 0) {
          await database.rows.bulkDelete([...deleted].map((rowId) => [key, rowId] as [string, string]));
        }

        const currentRows = await database.rows.where("collectionKey").equals(key).sortBy("rank");
        let nextRank = currentRows.length === 0 ? 0 : currentRows[0]!.rank - 1;
        const upsertRecords: RowRecord[] = [];
        for (const row of value.upserts) {
          const rowId = rowKey(row, value.keyField);
          if (!rowId) continue;
          upsertRecords.push({
            collectionKey: key,
            rowId,
            value: row,
            rank: nextRank--,
            sizeBytes: jsonSize(row),
          });
        }
        if (upsertRecords.length > 0) await database.rows.bulkPut(upsertRecords);

        const maxRows = value.maxRows ?? collection.maxRows;
        const maxBytes = value.maxBytes ?? collection.maxBytes;
        const ordered = sortSyncRows(
          await database.rows.where("collectionKey").equals(key).sortBy("rank"),
          value.orderBy ?? collection.orderBy,
          value.orderDirection ?? collection.orderDirection,
        );
        const kept = boundedRowRecords(ordered, maxRows, maxBytes);
        const keptIds = new Set(kept.map((row) => row.rowId));
        const evicted = ordered.filter((row) => !keptIds.has(row.rowId));
        if (evicted.length > 0) {
          await database.rows.bulkDelete(evicted.map((row) => [key, row.rowId] as [string, string]));
        }
        const hashes = { ...(collection.hashes ?? {}) };
        for (const rowId of deleted) delete hashes[rowId];
        for (const rowId of evicted.map((row) => row.rowId)) delete hashes[rowId];
        for (const [rowId, hash] of Object.entries(value.hashes ?? {})) hashes[rowId] = hash;
        await database.collections.put({
          ...collection,
          cursor: value.cursor,
          keyField: value.keyField,
          mode: value.mode ?? collection.mode,
          orderBy: value.orderBy ?? collection.orderBy,
          orderDirection: value.orderDirection ?? collection.orderDirection,
          maxRows,
          maxBytes,
          hashes,
          rowCount: kept.length,
          sizeBytes: kept.reduce((sum, row) => sum + row.sizeBytes, 0),
          lastAccessedAt: Date.now(),
        });
      });
      this.scheduleCleanup();
    } catch {
      this.disabled = true;
    }
  }

  async clear(scope?: string): Promise<void> {
    if (this.disabled && !this.database) return;
    try {
      const database = await this.open();
      if (!scope) {
        await database.transaction("rw", database.collections, database.rows, database.identities, async () => {
          await database.rows.clear();
          await database.collections.clear();
          await database.identities.clear();
        });
        return;
      }
      const collections = await database.collections.where("scope").equals(scope).toArray();
      const keys = collections.map((collection) => collection.key);
      const identities = await database.identities.toArray();
      await database.transaction("rw", database.collections, database.rows, database.identities, async () => {
        for (const key of keys) await database.rows.where("collectionKey").equals(key).delete();
        await database.collections.bulkDelete(keys);
        await database.identities.bulkDelete(
          identities.filter((entry) => entry.directive.scope === scope).map((entry) => entry.identity),
        );
      });
    } catch {
      // Disposable local data must never break the application.
    }
  }

  async delete(scope: string, path: string, args: JsonValue): Promise<void> {
    if (this.disabled || !scope) return;
    try {
      const key = await collectionKey(scope, path, args);
      const database = await this.open();
      await database.transaction("rw", database.collections, database.rows, async () => {
        await database.rows.where("collectionKey").equals(key).delete();
        await database.collections.delete(key);
      });
    } catch {
      // Disposable local data must never break the application.
    }
  }

  async loadDirective(identity: string): Promise<QueryCacheDirective | undefined> {
    if (this.disabled || !identity) return undefined;
    try {
      const database = await this.open();
      const record = await database.identities.get(identity);
      if (!record) return undefined;
      void database.identities.update(identity, { lastAccessedAt: Date.now() }).catch(() => undefined);
      return record.directive;
    } catch {
      return undefined;
    }
  }

  async saveDirective(identity: string, directive: QueryCacheDirective): Promise<void> {
    if (this.disabled || !identity) return;
    try {
      const database = await this.open();
      await database.identities.put({ identity, directive, lastAccessedAt: Date.now() });
    } catch {
      // Warm-start metadata is an optimization, never a request prerequisite.
    }
  }

  close() {
    this.database?.close();
    this.database = undefined;
    this.databasePromise = undefined;
  }

  private async open(): Promise<SyncDatabase> {
    if (!this.databasePromise) {
      this.databasePromise = this.createDatabase().catch((error) => {
        this.databasePromise = undefined;
        throw error;
      });
    }
    return this.databasePromise;
  }

  private async createDatabase(): Promise<SyncDatabase> {
    const indexedDBValue = this.indexedDB ?? globalThis.indexedDB;
    const keyRangeValue = this.keyRange ?? globalThis.IDBKeyRange;
    if (!indexedDBValue || !keyRangeValue) throw new Error("indexeddb-unavailable");
    const { Dexie } = await import("dexie");
    const database = new Dexie(this.databaseName, {
      indexedDB: indexedDBValue,
      IDBKeyRange: keyRangeValue,
    }) as SyncDatabase;
    database.version(1).stores({
      collections: "&key, scope, [scope+lastAccessedAt], lastAccessedAt",
      rows: "[collectionKey+rowId], collectionKey, [collectionKey+rank]",
    });
    database.version(2).stores({
      collections: "&key, scope, [scope+lastAccessedAt], lastAccessedAt",
      rows: "[collectionKey+rowId], collectionKey, [collectionKey+rank]",
      identities: "&identity, lastAccessedAt",
    });
    database.on("versionchange", () => database.close());
    await database.open();
    this.database = database;
    return database;
  }

  private scheduleCleanup() {
    if (this.cleanupScheduled) return;
    this.cleanupScheduled = true;
    const run = () => {
      this.cleanupScheduled = false;
      void this.cleanup().catch(() => undefined);
    };
    if (typeof globalThis.requestIdleCallback === "function") {
      globalThis.requestIdleCallback(run, { timeout: 2_000 });
    } else {
      setTimeout(run, 0);
    }
  }

  private async cleanup() {
    const database = await this.open();
    const collections = await database.collections.orderBy("lastAccessedAt").reverse().toArray();
    let bytes = 0;
    const evict: string[] = [];
    for (const collection of collections) {
      if (bytes + collection.sizeBytes > this.maxBytes) {
        evict.push(collection.key);
      } else {
        bytes += collection.sizeBytes;
      }
    }
    if (evict.length === 0) return;
    await database.transaction("rw", database.collections, database.rows, async () => {
      for (const key of evict) await database.rows.where("collectionKey").equals(key).delete();
      await database.collections.bulkDelete(evict);
    });
  }
}

export function createSyncStore(options: SyncStoreOptions | false | undefined): SyncStore | undefined {
  if (options === false || options?.enabled === false) return undefined;
  if (options?.store) return options.store;
  return new DexieSyncStore(options);
}

function boundedRows(
  rows: JsonValue[],
  keyField: string,
  maxRows?: number,
  maxBytes?: number,
  orderBy?: string,
  orderDirection?: "asc" | "desc",
) {
  const seen = new Set<string>();
  const kept: JsonValue[] = [];
  let bytes = 0;
  for (const row of sortSyncRows(rows, orderBy, orderDirection)) {
    const key = rowKey(row, keyField);
    if (!key || seen.has(key)) continue;
    const size = jsonSize(row);
    if (maxRows && kept.length >= maxRows) break;
    if (maxBytes && bytes + size > maxBytes) break;
    seen.add(key);
    bytes += size;
    kept.push(row);
  }
  return kept;
}

function sortSyncRows<T extends JsonValue | RowRecord>(
  rows: T[],
  orderBy?: string,
  orderDirection?: "asc" | "desc",
): T[] {
  if (!orderBy) return rows;
  const direction = orderDirection === "asc" ? 1 : -1;
  return [...rows].sort((left, right) => {
    const leftValue = syncOrderValue(syncSortableValue(left), orderBy);
    const rightValue = syncOrderValue(syncSortableValue(right), orderBy);
    if (leftValue === rightValue) return 0;
    if (leftValue === null) return 1;
    if (rightValue === null) return -1;
    return leftValue < rightValue ? -direction : direction;
  });
}

function syncSortableValue(value: JsonValue | RowRecord): JsonValue {
  if (
    value
    && typeof value === "object"
    && !Array.isArray(value)
    && "collectionKey" in value
    && "rowId" in value
    && "rank" in value
    && "value" in value
  ) {
    return (value as RowRecord).value;
  }
  return value as JsonValue;
}

function syncOrderValue(value: JsonValue, orderBy: string): string | number | null {
  if (!value || Array.isArray(value) || typeof value !== "object") return null;
  const candidate = value[orderBy];
  return typeof candidate === "string" || typeof candidate === "number" ? candidate : null;
}

function boundedRowRecords(rows: RowRecord[], maxRows?: number, maxBytes?: number) {
  const kept: RowRecord[] = [];
  let bytes = 0;
  for (const row of rows) {
    if (maxRows && kept.length >= maxRows) break;
    if (maxBytes && bytes + row.sizeBytes > maxBytes) break;
    kept.push(row);
    bytes += row.sizeBytes;
  }
  return kept;
}

function rowKey(value: JsonValue, keyField: string) {
  if (!value || Array.isArray(value) || typeof value !== "object") return "";
  const key = value[keyField];
  return key === null || key === undefined ? "" : String(key);
}

async function collectionKey(scope: string, path: string, args: JsonValue) {
  return `${scope}:${await hashValue(`${path}\u0000${stableStringify(args)}`)}`;
}

async function hashValue(value: string) {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error("web-crypto-unavailable");
  const digest = await subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (part) => part.toString(16).padStart(2, "0")).join("");
}

export async function syncHashesDigest(hashes: Record<string, string>): Promise<string> {
  const pairs = Object.keys(hashes).sort(utf8KeyCompare).map((key) => [key, hashes[key]]);
  return hashValue(stableStringify(pairs as JsonValue));
}

/**
 * Hash the rows the client actually materialized. Persisted server-provided
 * hashes are only a resume optimization; they are not integrity evidence until
 * they have been recomputed from the IndexedDB values that the UI will read.
 */
export async function syncRowsHashes(
  rows: JsonValue[],
  keyField: string,
): Promise<Record<string, string>> {
  const seen = new Set<string>();
  const entries = await Promise.all(rows.map(async (row) => {
    const key = rowKey(row, keyField);
    if (!key) throw new Error(`sync row is missing key ${JSON.stringify(keyField)}`);
    if (seen.has(key)) throw new Error(`sync collection contains duplicate key ${JSON.stringify(key)}`);
    seen.add(key);
    return [key, await hashValue(stableStringify(row))] as const;
  }));
  return Object.fromEntries(entries);
}

function stableStringify(value: JsonValue): string {
  if (typeof value === "string") {
    // Go's JSON encoder always escapes the two JavaScript line-separator code
    // points. Make the cross-language row hash canonical even after a row has
    // made an IndexedDB structured-clone round trip.
    return JSON.stringify(value)
      .replace(/\u2028/g, "\\u2028")
      .replace(/\u2029/g, "\\u2029");
  }
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  return `{${Object.keys(value)
    .sort(utf8KeyCompare)
    .map((key) => `${stableStringify(key)}:${stableStringify(value[key]!)}`)
    .join(",")}}`;
}

function utf8KeyCompare(left: string, right: string) {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    if (leftBytes[index] !== rightBytes[index]) return leftBytes[index]! - rightBytes[index]!;
  }
  return leftBytes.length - rightBytes.length;
}

function jsonSize(value: JsonValue) {
  return new TextEncoder().encode(stableStringify(value)).byteLength;
}

function positive(value: number | undefined, fallback: number) {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : fallback;
}
