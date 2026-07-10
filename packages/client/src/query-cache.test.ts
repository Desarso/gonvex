import { describe, expect, it } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { DexieQueryCacheStore } from "./query-cache";

const scopeA = "a".repeat(64);
const scopeB = "b".repeat(64);

function store(options: ConstructorParameters<typeof DexieQueryCacheStore>[0] = {}) {
  return new DexieQueryCacheStore({
    databaseName: `gonvex-query-cache-test-${Math.random().toString(36).slice(2)}`,
    indexedDB: new IDBFactory(),
    IDBKeyRange,
    ...options,
  });
}

describe("DexieQueryCacheStore", () => {
  it("persists exact query results using canonical argument hashes", async () => {
    const cache = store();
    await expect(cache.write({
      scope: scopeA,
      path: "tasks.list",
      args: { status: "open", page: 1 },
      result: [{ id: "task-1" }],
      revision: "0001:0001",
      maxAgeMs: 60_000,
    })).resolves.toBe("written");

    await expect(cache.read(scopeA, "tasks.list", { page: 1, status: "open" }, 60_000)).resolves.toMatchObject({
      result: [{ id: "task-1" }],
      revision: "0001:0001",
    });
    await expect(cache.read(scopeA, "tasks.list", { page: 2, status: "open" }, 60_000)).resolves.toBeUndefined();
    await expect(cache.read(scopeB, "tasks.list", { page: 1, status: "open" }, 60_000)).resolves.toBeUndefined();
    cache.close();
  });

  it("does not let an older cross-tab result overwrite a newer revision", async () => {
    const indexedDB = new IDBFactory();
    const databaseName = `gonvex-query-cache-revision-${Math.random().toString(36).slice(2)}`;
    const first = new DexieQueryCacheStore({ databaseName, indexedDB, IDBKeyRange });
    const second = new DexieQueryCacheStore({ databaseName, indexedDB, IDBKeyRange });
    const base = {
      scope: scopeA,
      path: "tasks.list",
      args: {},
      maxAgeMs: 60_000,
    } as const;

    await expect(first.write({ ...base, result: "new", revision: "0001:0002" })).resolves.toBe("written");
    await expect(second.write({ ...base, result: "old", revision: "0001:0001" })).resolves.toBe("older");

    // Read through a new connection so neither store's memory front-cache can mask IndexedDB.
    const reader = new DexieQueryCacheStore({ databaseName, indexedDB, IDBKeyRange });
    await expect(reader.read(scopeA, "tasks.list", {}, 60_000)).resolves.toMatchObject({ result: "new" });
    first.close();
    second.close();
    reader.close();
  });

  it("ignores and removes expired records", async () => {
    const cache = store({ maxAgeMs: 1 });
    await cache.write({
      scope: scopeA,
      path: "tasks.list",
      args: {},
      result: "cached",
      revision: "0001:0001",
      maxAgeMs: 1,
    });

    await new Promise((resolve) => setTimeout(resolve, 5));
    await expect(cache.read(scopeA, "tasks.list", {}, 1)).resolves.toBeUndefined();
    cache.close();
  });

  it("skips oversized results without disabling ordinary reads", async () => {
    const cache = store({ maxEntryBytes: 16 });
    await expect(cache.write({
      scope: scopeA,
      path: "tasks.list",
      args: {},
      result: "this value is larger than sixteen bytes",
      revision: "0001:0001",
      maxAgeMs: 60_000,
    })).resolves.toBe("oversize");
    expect(cache.status()).toMatchObject({ enabled: true, readsEnabled: true, writesEnabled: true });
    cache.close();
  });

  it("clears one confirmed scope without deleting another", async () => {
    const cache = store();
    const write = (scope: string, result: string) => cache.write({
      scope,
      path: "tasks.list",
      args: {},
      result,
      revision: "0001:0001",
      maxAgeMs: 60_000,
    });
    await write(scopeA, "a");
    await write(scopeB, "b");
    await cache.clear(scopeA);

    await expect(cache.read(scopeA, "tasks.list", {}, 60_000)).resolves.toBeUndefined();
    await expect(cache.read(scopeB, "tasks.list", {}, 60_000)).resolves.toMatchObject({ result: "b" });
    cache.close();
  });
});
