import { describe, expect, it, vi } from "vitest";
import {
  createKvOutboxStore,
  createKvQueryCacheStore,
  createKvSyncStore,
  createMemoryGonvexKv,
  kvReducerOutboxTable,
} from "./kv-stores";
import { StoreReducerOutbox, createReducerOutbox, type OutboxStore } from "./outbox";

const scope = "project-a::tenant-a::user-a";
const otherScope = "project-a::tenant-a::user-b";

function throwingStore(overrides: Partial<OutboxStore> = {}): OutboxStore {
  const boom = async () => {
    throw new Error("storage-unavailable");
  };
  return { load: boom, put: boom, delete: boom, clear: boom, ...overrides };
}

describe("StoreReducerOutbox", () => {
  it("survives a restart with ids, states, idempotency keys, and order intact", async () => {
    const kv = createMemoryGonvexKv();
    const before = new StoreReducerOutbox(createKvOutboxStore(kv));
    const first = await before.enqueue({
      scope,
      path: "tasks.create",
      args: { title: "First" },
      idempotencyKey: "reducer-first",
      entityKeys: ["task:first"],
      patches: [{
        collection: "tasks.list",
        rowId: "first",
        op: "patch",
        fields: { title: "Optimistic" },
      }],
    });
    const second = await before.enqueue({
      scope,
      path: "tasks.update",
      args: { title: "Second" },
      idempotencyKey: "reducer-second",
      entityKeys: ["task:second"],
    });
    await before.fail(second.id, "offline");

    const after = new StoreReducerOutbox(createKvOutboxStore(kv));
    await expect(after.loadAll(scope)).resolves.toMatchObject([
      {
        id: first.id,
        path: "tasks.create",
        idempotencyKey: "reducer-first",
        state: "pending",
        patches: [{ rowId: "first", fields: { title: "Optimistic" } }],
      },
      {
        id: second.id,
        idempotencyKey: "reducer-second",
        state: "pending",
        attempts: 1,
        lastError: "offline",
      },
    ]);
    const next = await after.enqueue({ scope, path: "tasks.create", args: {} });
    expect(next.id).toBeGreaterThan(second.id);
  });

  it("recovers inflight entries as pending after a restart", async () => {
    const kv = createMemoryGonvexKv();
    const before = new StoreReducerOutbox(createKvOutboxStore(kv));
    const entry = await before.enqueue({ scope, path: "tasks.update", args: {} });
    await before.markInflight(entry.id);

    const after = new StoreReducerOutbox(createKvOutboxStore(kv));
    await expect(after.loadAll(scope)).resolves.toMatchObject([
      { id: entry.id, state: "pending" },
    ]);
    // The recovery itself is persisted: a further restart must not see
    // inflight either.
    const again = new StoreReducerOutbox(createKvOutboxStore(kv));
    await expect(again.loadAll(scope)).resolves.toMatchObject([
      { id: entry.id, state: "pending" },
    ]);
  });

  it("blocks later writes to the same entity but allows independent writes", async () => {
    const outbox = new StoreReducerOutbox(createKvOutboxStore(createMemoryGonvexKv()));
    const first = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      entityKeys: ["task:a"],
    });
    const blocked = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });
    const independent = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 3 },
      entityKeys: ["task:b"],
    });

    await outbox.markInflight(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: independent.id });
    await outbox.ack(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: blocked.id });
  });

  it("does not let an accepted committed row block a newer write", async () => {
    const outbox = new StoreReducerOutbox(createKvOutboxStore(createMemoryGonvexKv()));
    const committed = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      entityKeys: ["task:a"],
    });
    const pending = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });
    await outbox.markCommitted(committed.id);

    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: pending.id });
  });

  it("keeps causal barriers across a restart", async () => {
    const kv = createMemoryGonvexKv();
    const before = new StoreReducerOutbox(createKvOutboxStore(kv));
    const first = await before.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      entityKeys: ["task:a"],
    });
    await before.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });
    await before.markInflight(first.id);

    const after = new StoreReducerOutbox(createKvOutboxStore(kv));
    await after.loadAll(scope);
    await expect(after.nextReady(scope, Date.now())).resolves.toMatchObject({ id: first.id });
  });

  it("clears only the requested scope, in memory and in the store", async () => {
    const kv = createMemoryGonvexKv();
    const outbox = new StoreReducerOutbox(createKvOutboxStore(kv));
    await outbox.enqueue({ scope, path: "tasks.update", args: {} });
    const theirs = await outbox.enqueue({ scope: otherScope, path: "tasks.update", args: {} });

    await outbox.clear(scope);

    await expect(outbox.count(scope)).resolves.toBe(0);
    await expect(outbox.count(otherScope)).resolves.toBe(1);
    const restarted = new StoreReducerOutbox(createKvOutboxStore(kv));
    await expect(restarted.loadAll(scope)).resolves.toEqual([]);
    await expect(restarted.loadAll(otherScope)).resolves.toMatchObject([{ id: theirs.id }]);
  });

  it("keeps queue semantics when hydration fails", async () => {
    const outbox = new StoreReducerOutbox(throwingStore());
    const first = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      entityKeys: ["task:a"],
    });
    const second = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });

    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: first.id });
    await outbox.markInflight(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toBeUndefined();
    await outbox.ack(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: second.id });
  });

  it("degrades to memory when a write-through fails without failing the reducer", async () => {
    const kv = createMemoryGonvexKv();
    const working = createKvOutboxStore(kv);
    let failPuts = false;
    const flaky: OutboxStore = {
      ...working,
      put: (entry) => {
        if (failPuts) throw new Error("disk-full");
        return working.put(entry);
      },
    };
    const outbox = new StoreReducerOutbox(flaky);
    const durable = await outbox.enqueue({ scope, path: "tasks.update", args: { value: 1 } });
    failPuts = true;
    const memoryOnlyEntry = await outbox.enqueue({ scope, path: "tasks.update", args: { value: 2 } });

    expect(memoryOnlyEntry.id).toBeGreaterThan(durable.id);
    await expect(outbox.count(scope)).resolves.toBe(2);
    await expect(outbox.loadAll(scope)).resolves.toMatchObject([
      { id: durable.id },
      { id: memoryOnlyEntry.id },
    ]);
  });

  it("is selected by createReducerOutbox when a store is injected", async () => {
    const outbox = createReducerOutbox({ store: createKvOutboxStore(createMemoryGonvexKv()) });
    expect(outbox).toBeInstanceOf(StoreReducerOutbox);
  });

  it("skips corrupt persisted rows instead of stranding the queue", async () => {
    const kv = createMemoryGonvexKv();
    const before = new StoreReducerOutbox(createKvOutboxStore(kv));
    const entry = await before.enqueue({ scope, path: "tasks.update", args: {} });
    await kv.set(kvReducerOutboxTable, "corrupt", "{not json");

    const after = new StoreReducerOutbox(createKvOutboxStore(kv));
    await expect(after.loadAll(scope)).resolves.toMatchObject([{ id: entry.id }]);
  });
});

describe("createKvQueryCacheStore", () => {
  it("round-trips results and refuses older revisions", async () => {
    const store = createKvQueryCacheStore(createMemoryGonvexKv());
    await expect(store.write({
      scope,
      path: "tasks.list",
      args: { done: false },
      result: [{ id: "a" }],
      revision: "5",
      maxAgeMs: 60_000,
    })).resolves.toBe("written");

    await expect(store.read(scope, "tasks.list", { done: false })).resolves.toMatchObject({
      result: [{ id: "a" }],
      revision: "5",
    });
    await expect(store.write({
      scope,
      path: "tasks.list",
      args: { done: false },
      result: [],
      revision: "4",
      maxAgeMs: 60_000,
    })).resolves.toBe("older");
    await expect(store.read(scope, "tasks.list", { done: true })).resolves.toBeUndefined();
  });

  it("expires entries past maxAgeMs", async () => {
    const now = vi.spyOn(Date, "now").mockReturnValue(10_000);
    const store = createKvQueryCacheStore(createMemoryGonvexKv(), { maxAgeMs: 1_000 });
    await store.write({
      scope,
      path: "tasks.list",
      args: {},
      result: [],
      revision: "1",
      maxAgeMs: 1_000,
    });
    now.mockReturnValue(10_500);
    await expect(store.read(scope, "tasks.list", {})).resolves.toBeDefined();
    now.mockReturnValue(11_001);
    await expect(store.read(scope, "tasks.list", {})).resolves.toBeUndefined();
    now.mockRestore();
  });

  it("evicts the least recently used entries past the per-scope cap", async () => {
    let clock = 1_000;
    const now = vi.spyOn(Date, "now").mockImplementation(() => (clock += 1));
    const store = createKvQueryCacheStore(createMemoryGonvexKv(), { maxEntriesPerScope: 2 });
    for (const name of ["a", "b", "c"]) {
      await store.write({
        scope,
        path: "tasks.get",
        args: { name },
        result: { name },
        revision: "1",
        maxAgeMs: 60_000,
      });
    }

    await expect(store.read(scope, "tasks.get", { name: "a" })).resolves.toBeUndefined();
    await expect(store.read(scope, "tasks.get", { name: "b" })).resolves.toBeDefined();
    await expect(store.read(scope, "tasks.get", { name: "c" })).resolves.toBeDefined();
    now.mockRestore();
  });

  it("disables itself when the backend throws and reports the reason", async () => {
    const kv = createMemoryGonvexKv();
    const store = createKvQueryCacheStore({
      ...kv,
      get: async () => {
        throw new Error("sqlite-locked");
      },
    });
    await expect(store.read(scope, "tasks.list", {})).resolves.toBeUndefined();
    expect(store.status()).toMatchObject({
      enabled: false,
      readsEnabled: false,
      writesEnabled: false,
      reason: "sqlite-locked",
    });
  });
});

describe("createKvSyncStore", () => {
  it("round-trips collections and applies deltas by key", async () => {
    const store = createKvSyncStore(createMemoryGonvexKv());
    await store.replace(scope, "tasks.sync", {}, {
      rows: [{ id: "a", title: "A" }, { id: "b", title: "B" }],
      cursor: { epoch: "sync-a", revision: 10 },
      keyField: "id",
    });

    await expect(store.load(scope, "tasks.sync", {})).resolves.toMatchObject({
      rows: [{ id: "a" }, { id: "b" }],
      cursor: { epoch: "sync-a", revision: 10 },
      keyField: "id",
    });

    await store.applyDelta(scope, "tasks.sync", {}, {
      cursor: { epoch: "sync-a", revision: 11 },
      keyField: "id",
      upserts: [{ id: "b", title: "B2" }, { id: "c", title: "C" }],
      deleted: ["a"],
    });

    const loaded = await store.load(scope, "tasks.sync", {});
    expect(loaded?.cursor).toEqual({ epoch: "sync-a", revision: 11 });
    expect(loaded?.rows).toEqual([{ id: "b", title: "B2" }, { id: "c", title: "C" }]);
  });

  it("advances only cursor metadata when rows are proven unchanged", async () => {
    const store = createKvSyncStore(createMemoryGonvexKv());
    await store.replace(scope, "tasks.sync", {}, {
      rows: [{ id: "a" }],
      cursor: { epoch: "sync-a", revision: 10 },
      keyField: "id",
    });
    await store.replace(scope, "tasks.sync", {}, {
      rows: [],
      cursor: { epoch: "sync-a", revision: 12 },
      keyField: "id",
      rowsUnchanged: true,
    });

    await expect(store.load(scope, "tasks.sync", {})).resolves.toMatchObject({
      rows: [{ id: "a" }],
      cursor: { epoch: "sync-a", revision: 12 },
    });
  });

  it("evicts the least recently used collections past maxBytes", async () => {
    let clock = 1_000;
    const now = vi.spyOn(Date, "now").mockImplementation(() => (clock += 1));
    const bigRow = { id: "row", payload: "x".repeat(200) };
    const store = createKvSyncStore(createMemoryGonvexKv(), { maxBytes: 300 });
    await store.replace(scope, "tasks.sync", { page: 1 }, {
      rows: [{ ...bigRow, id: "old" }],
      cursor: { epoch: "sync-a", revision: 1 },
      keyField: "id",
    });
    await store.replace(scope, "tasks.sync", { page: 2 }, {
      rows: [{ ...bigRow, id: "new" }],
      cursor: { epoch: "sync-a", revision: 1 },
      keyField: "id",
    });

    await expect(store.load(scope, "tasks.sync", { page: 1 })).resolves.toBeUndefined();
    await expect(store.load(scope, "tasks.sync", { page: 2 })).resolves.toMatchObject({
      rows: [{ id: "new" }],
    });
    now.mockRestore();
  });

  it("round-trips directives and clears only the requested scope", async () => {
    const store = createKvSyncStore(createMemoryGonvexKv());
    await store.saveDirective("identity-a", {
      protocolVersion: 1,
      scope,
      epoch: "epoch-a",
      maxAgeMs: 60_000,
    });
    await expect(store.loadDirective("identity-a")).resolves.toMatchObject({ scope });

    await store.replace(scope, "tasks.sync", {}, {
      rows: [{ id: "a" }],
      cursor: { epoch: "sync-a", revision: 1 },
      keyField: "id",
    });
    await store.replace(otherScope, "tasks.sync", {}, {
      rows: [{ id: "b" }],
      cursor: { epoch: "sync-b", revision: 1 },
      keyField: "id",
    });
    await store.clear(scope);

    await expect(store.load(scope, "tasks.sync", {})).resolves.toBeUndefined();
    await expect(store.load(otherScope, "tasks.sync", {})).resolves.toMatchObject({
      rows: [{ id: "b" }],
    });
  });
});
