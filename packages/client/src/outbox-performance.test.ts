import { describe, expect, it, vi } from "vitest";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import { DexieReducerOutbox, StoreReducerOutbox, readyIntents, intentsConflict, type IntentDependencies, type OutboxStore, type ReducerOutboxEntry } from "./outbox.js";

const scope = "tenant";
const deps = (id: string, scan = false): IntentDependencies => ({ reads: [{ entity: "tasks", rowId: id }], writes: [{ entity: "tasks", rowId: id }], scans: scan ? ["tasks"] : [] });
function memoryStore(shared = false) {
  const rows = new Map<number, ReducerOutboxEntry>();
  const invalidations = new Set<() => void>();
  let sequence = 0;
  const store: OutboxStore = {
    shared, cacheShared: shared, strictPersistence: true,
    subscribeInvalidation: listener => { invalidations.add(listener); return () => invalidations.delete(listener); },
    load: vi.fn(async owner => [...rows.values()].filter(entry => !owner || owner === entry.scope).map(entry => structuredClone(entry))),
    put: async entry => { rows.set(entry.id, structuredClone(entry)); },
    append: async draft => { const entry = { ...draft, id: ++sequence }; rows.set(entry.id, structuredClone(entry)); return entry; },
    replacePending: async (id, draft) => {
      const prior = rows.get(id);
      if (!prior || prior.state !== "pending" || prior.sentAt !== undefined) return undefined;
      const entry = { ...draft, id }; rows.set(id, structuredClone(entry)); return entry;
    },
    update: vi.fn(async (id, change) => { const entry = rows.get(id); if (!entry) return; const next = change(structuredClone(entry)); rows.set(id, next); return next; }),
    updateMany: vi.fn(async (ids, change) => {
      const next = ids.flatMap(id => rows.has(id) ? [change(structuredClone(rows.get(id)!))] : []);
      for (const entry of next) rows.set(entry.id, entry); return next;
    }),
    delete: vi.fn(async id => { rows.delete(id); }),
    deleteMany: vi.fn(async ids => { for (const id of ids) rows.delete(id); }),
    clear: async () => { rows.clear(); },
  };
  return { store, rows, invalidate: () => { for (const listener of invalidations) listener(); } };
}
const intent = (id: string) => ({ scope, path: "save", args: { id }, dependencies: deps(id), entityKeys: [] });

describe("causal outbox", () => {
  it("lets unrelated work pass a backed-off head, retaining read/write and scan barriers", async () => {
    const { store } = memoryStore(); const queue = new StoreReducerOutbox(store);
    const first = await queue.enqueue(intent("a"));
    const dependent = await queue.enqueue({ ...intent("b"), dependencies: { ...deps("b"), reads: deps("a").reads } });
    const transitive = await queue.enqueue(intent("b"));
    const independent = await queue.enqueue(intent("c"));
    await queue.markInflight(first.id);
    await queue.fail(first.id, "retry", { delayMs: 60000 });
    expect((await queue.readyBatch(scope, Date.now(), 8)).map(entry => entry.id)).toEqual([independent.id]);
    await queue.ack(first.id);
    expect((await queue.readyBatch(scope, Date.now(), 8)).map(entry => entry.id)).toEqual([dependent.id, independent.id]);
    await queue.ack(dependent.id);
    expect((await queue.nextReady(scope, Date.now()))?.id).toBe(transitive.id);
    const scan = await queue.enqueue({ ...intent("z"), dependencies: { reads: [], writes: [], scans: ["tasks"] } });
    const laterWrite = await queue.enqueue(intent("d"));
    const ready = readyIntents(await queue.list(scope), Date.now(), 8).map(entry => entry.id);
    expect(ready).not.toContain(scan.id); expect(ready).not.toContain(laterWrite.id);
  });

  it("preserves read/read independence and legacy global barriers", async () => {
    const { store } = memoryStore(); const queue = new StoreReducerOutbox(store);
    await queue.enqueue({ ...intent("a"), dependencies: { ...deps("a"), writes: [] } });
    await queue.enqueue({ ...intent("a"), dependencies: { ...deps("a"), writes: [] } });
    expect(await queue.readyBatch(scope, Date.now(), 8)).toHaveLength(2);
    const legacy = await queue.enqueue({ scope, path: "old", args: {}, entityKeys: ["__gonvex_local_intents"] });
    const later = await queue.enqueue(intent("x"));
    expect((await queue.readyBatch(scope, Date.now(), 8)).map(entry => entry.id)).not.toContain(later.id);
    expect((await queue.readyBatch(scope, Date.now(), 8)).map(entry => entry.id)).not.toContain(legacy.id);
  });

  it("admits only pending entries when durable states changed after selection", async () => {
    const { store } = memoryStore(); const queue = new StoreReducerOutbox(store);
    const entries = await Promise.all(['a', 'b', 'c', 'd'].map(id => queue.enqueue(intent(id))));
    await queue.markInflight(entries[0]!.id);
    await queue.markInflight(entries[1]!.id); await queue.markCommitted(entries[1]!.id);
    await queue.reject(entries[2]!.id, 'discarded');
    expect((await queue.markInflightMany(entries.map(entry => entry.id))).map(entry => entry.id)).toEqual([entries[3]!.id]);
    expect((await queue.list(scope)).map(entry => entry.state)).toEqual(['inflight', 'committed', 'rejected', 'inflight']);
  });

  it("coalesces only declared unsent overwrites with no dependents, including after recovery", async () => {
    const { store } = memoryStore(); const queue = new StoreReducerOutbox(store);
    await queue.enqueue(intent("undeclared")); await queue.enqueue(intent("undeclared"));
    expect(await queue.list(scope)).toHaveLength(2);
    const first = await queue.enqueue({ ...intent("a"), coalesceKey: "a" });
    const latest = await queue.enqueue({ ...intent("a"), args: { id: "a", value: "latest" }, coalesceKey: "a" });
    expect(latest.id).toBe(first.id); expect(latest.idempotencyKey).not.toBe(first.idempotencyKey);
    await queue.markInflight(latest.id);
    const recovered = new StoreReducerOutbox(store); await recovered.loadAll(scope);
    const afterCrash = await recovered.enqueue({ ...intent("a"), coalesceKey: "a" });
    expect(afterCrash.id).not.toBe(latest.id);
    await queue.enqueue({ ...intent("b"), coalesceKey: "b" });
    await queue.enqueue({ ...intent("c"), dependencies: { ...deps("c"), reads: deps("b").reads } });
    const next = await queue.enqueue({ ...intent("b"), coalesceKey: "b" });
    expect((await queue.list(scope)).filter(entry => entry.coalesceKey === "b")).toHaveLength(2);
    expect(next.state).toBe("pending");
  });

  it("coalesces atomically in IndexedDB and reloads the latest complete entry", async () => {
    const databaseName = `coalesce-${crypto.randomUUID()}`;
    const queue = new DexieReducerOutbox({ databaseName, indexedDB, IDBKeyRange });
    const first = await queue.enqueue({ ...intent("a"), coalesceKey: "a" });
    const latest = await queue.enqueue({ ...intent("a"), args: { value: 2 }, coalesceKey: "a" });
    expect(latest.id).toBe(first.id);
    const reload = new DexieReducerOutbox({ databaseName, indexedDB, IDBKeyRange });
    expect(await reload.loadAll(scope)).toMatchObject([{ args: { value: 2 }, idempotencyKey: latest.idempotencyKey }]);
  });

  it("drains 500 shared intents with one hydration and 63 admission transactions", async () => {
    const { store, rows, invalidate } = memoryStore(true); const queue = new StoreReducerOutbox(store);
    // Admission is measured separately from draining; hydrate the initial empty scope once.
    for (let n = 0; n < 500; n++) await queue.enqueue(intent(String(n)));
    vi.mocked(store.load).mockClear(); invalidate();
    let delivered = 0;
    while (true) {
      const ready = await queue.readyBatch(scope, Date.now(), 8); if (!ready.length) break;
      const admitted = await queue.markInflightMany(ready.map(entry => entry.id));
      expect([...rows.values()].filter(entry => entry.state === "inflight")).toHaveLength(admitted.length);
      await queue.markCommittedMany(admitted.map(entry => entry.id));
      await queue.ackMany(admitted.map(entry => entry.id)); delivered += admitted.length;
    }
    expect(vi.mocked(store.load).mock.calls.length).toBe(1);
    const legacy = memoryStore(true); legacy.store.cacheShared = false;
    const oldQueue = new StoreReducerOutbox(legacy.store);
    for (let n = 0; n < 500; n++) await oldQueue.enqueue(intent(String(n)));
    vi.mocked(legacy.store.load).mockClear();
    let oldDelivered = 0;
    while (true) {
      const entry = await oldQueue.nextReady(scope, Date.now()); if (!entry) break;
      await oldQueue.markInflight(entry.id); await oldQueue.markInflight(entry.id);
      await oldQueue.markCommitted(entry.id); await oldQueue.ack(entry.id); oldDelivered++;
    }
    expect(oldDelivered).toBe(500);
    expect(vi.mocked(legacy.store.load)).toHaveBeenCalledTimes(501);
    expect(vi.mocked(legacy.store.update!)).toHaveBeenCalledTimes(1500);
    expect(vi.mocked(legacy.store.delete)).toHaveBeenCalledTimes(500);
    expect(delivered).toBe(500);
    expect(vi.mocked(store.updateMany).mock.calls.length).toBe(126);
    expect(vi.mocked(store.deleteMany).mock.calls.length).toBe(63);
  }, 15000);

  it("refreshes only invalidated scopes and retains other cached owners", async () => {
    const { store, rows, invalidate } = memoryStore(true); const queue = new StoreReducerOutbox(store);
    const a = await queue.enqueue(intent("a"));
    await queue.enqueue({ ...intent("b"), scope: "other" });
    expect(await queue.count(scope)).toBe(1);
    rows.delete(a.id); invalidate();
    expect(await queue.count(scope)).toBe(0); expect(await queue.count("other")).toBe(1);
  });
});


it('matches pairwise dependency ordering when selecting from a mixed-version queue', async () => {
  const { store } = memoryStore(); const queue = new StoreReducerOutbox(store);
  for (let n = 0; n < 60; n++) {
    const entry = await queue.enqueue(n % 11 === 0 ? { scope, path: 'legacy', args: {}, entityKeys: n % 22 === 0 ? ['__gonvex_local_intents'] : ['tasks:1'] } : {
      ...intent(String(n % 5)), dependencies: { reads: n % 3 ? deps(String(n % 5)).reads : [], writes: n % 4 ? deps(String((n + 1) % 5)).writes : [], scans: n % 7 === 0 ? ['tasks'] : [] },
    });
    if (n % 3 === 0) await queue.markInflight(entry.id);
    if (n % 9 === 0) await queue.fail(entry.id, 'retry', { delayMs: 60000 });
    if (n % 13 === 0) await queue.markCommitted(entry.id);
    if (n % 17 === 0) await queue.reject(entry.id, 'denied');
  }
  const entries = await queue.list(scope);
  const active = entries.filter(entry => entry.state === 'pending' || entry.state === 'inflight');
  const expected = active.filter((entry, index) => entry.state === 'pending' && entry.nextAttemptAt <= Date.now() && !active.slice(0, index).some(prior => intentsConflict(prior, entry))).slice(0, 8);
  expect(readyIntents(entries, Date.now(), 8).map(entry => entry.id)).toEqual(expected.map(entry => entry.id));
});
