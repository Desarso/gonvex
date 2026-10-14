import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { reducer, schema } from "@gonvex/module-sdk";
import { createPortableReducer } from "@gonvex/local-runtime/portable-client";
import { GonvexClient, GonvexClientError, IndexedDBLocalReplicaStorage, type FunctionReference } from "../index.js";
import { EngineRpc, EngineRpcTimeout, sendEngineMessage, type EngineEndpoint } from "./rpc.js";
import { diffEngineValue, applyEnginePatches } from "./patch.js";
import { EngineReceipts } from "./receipts.js";
import { serveEngineHost, type EngineInit } from "./host.js";
import { EngineClient, createEngineClient } from "./client.js";

class Endpoint implements EngineEndpoint {
  peer!: Endpoint;
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  sent: any[] = [];
  postMessage(data: unknown) { this.sent.push(data); queueMicrotask(() => this.peer.emit("message", { data: structuredClone(data) })); }
  addEventListener(type: string, listener: (event: any) => void) { let set = this.listeners.get(type); if (!set) { set = new Set(); this.listeners.set(type, set); } set.add(listener); }
  removeEventListener(type: string, listener: (event: any) => void) { this.listeners.get(type)?.delete(listener); }
  emit(type: string, event: unknown) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
}
function pair() { const ui = new Endpoint(), worker = new Endpoint(); ui.peer = worker; worker.peer = ui; return { ui, worker }; }
class Socket {
  static OPEN = 1; static CONNECTING = 0; static CLOSED = 3; static CLOSING = 2;
  static all: Socket[] = [];
  readyState = 0; sent: any[] = []; listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.all.push(this); }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners.set(type, [...this.listeners.get(type) ?? [], listener]); }
  emit(type: string, event: any = {}) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
  open() { this.readyState = 1; this.emit("open"); }
  send(data: string) { this.sent.push(JSON.parse(data)); }
  receive(data: unknown) { this.emit("message", { data: JSON.stringify(data) }); }
  close() { this.readyState = 3; this.emit("close"); }
}
const cleanups: Array<() => void | Promise<void>> = [];
beforeEach(() => {
  const indexedDB = new IDBFactory(); vi.stubGlobal("indexedDB", indexedDB); vi.stubGlobal("IDBKeyRange", IDBKeyRange);
  Dexie.dependencies.indexedDB = indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
  vi.stubGlobal("navigator", { locks: { request: (_name: string, options: any, callback?: any) => Promise.resolve((callback ?? options)({ name: _name })) } });
  vi.stubGlobal("WebSocket", Socket); Socket.all = [];
});
afterEach(async () => { for (const close of cleanups.splice(0)) await close(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it("correlates out-of-order replies, rejects old generations, and reuses request identity after timeout", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "generation", "scope"); cleanups.push(() => rpc.close());
  const first = rpc.request("reducer", [{ engineCommand: { id: "stable", now: 100, entropy: "fixed" } }], "first");
  const second = rpc.request("query", [], "second");
  const a = rpc.call(first, 5); const b = rpc.call(second);
  worker.postMessage({ generation: "old", scope: "scope", id: "first", result: "wrong" });
  worker.postMessage({ generation: "generation", scope: "scope", id: "second", result: 2 });
  expect(await b).toBe(2); await expect(a).rejects.toBeInstanceOf(EngineRpcTimeout);
  const retried = rpc.call(first); worker.postMessage({ generation: "generation", scope: "scope", id: "first", result: 1 });
  expect(await retried).toBe(1); expect(ui.sent.filter(frame => frame.id === "first")).toEqual([first, first]);
});

it("bounds chunk delivery and reassembles a large reply atomically", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const result = "😀".repeat(80_000); const pending = rpc.call(rpc.request("query", [], "large"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "large", result });
  expect(await pending).toBe(result);
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
  expect(ui.sent.filter(frame => frame.deliveryAck).length).toBe(worker.sent.length);
});

it.each(["\u0000", "\\\"", "\u0800"])("bounds serialized packets for escaped and multibyte strings, %j", async character => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const shared = { text: character.repeat(16_000), missing: undefined };
  const result = { first: shared, second: shared, rows: [shared, undefined] };
  const pending = rpc.call<typeof result>(rpc.request("query", [], "escaped"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "escaped", result });
  const actual = await pending;
  expect(actual).toEqual(result); expect(actual.first).toBe(actual.second); expect(actual.rows[0]).toBe(actual.first);
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
  expect(ui.sent.filter(frame => frame.deliveryAck).length).toBe(worker.sent.length);
});

it("rejects outstanding RPCs on a worker crash", async () => {
  const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("reducer", [])); ui.emit("error", {});
  await expect(pending).rejects.toThrow("retry the same command");
});

const url = "ws://engine.test/ws";
const auth = { project: "p", tenant: "t", identity: { sub: "a", iss: "issuer" } };
const owner = ["identity", url, "p", "t", "issuer", "a"].join("\u0000");
const ref: FunctionReference = { kind: "reducer", path: "insert", localExecution: 1, offline: { mode: "allowed" } };
const insert = reducer({ args: schema.object({}), result: schema.number(), run: async ctx => { await ctx.db.insert("tasks", { value: 1 }); return 7; } });
const binding = { mode: "portable" as const, artifactHash: "artifact", tables: ["tasks"], create: () => createPortableReducer({ artifactHash: "artifact", reducers: { insert }, schema: { tasks: { key: "id", columns: { id: { type: "text" as const, nullable: false }, value: { type: "bigint" as const, nullable: false } } } } }) };
async function setup() {
  const name = crypto.randomUUID(); const storage = new IndexedDBLocalReplicaStorage(name);
  await storage.saveSession(owner, { directive: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, artifactHash: "artifact",
    identity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } });
  storage.close();
  const init: EngineInit = { protocolVersion: 1, url, options: auth, storage: { replicaName: name, outboxName: `${name}-outbox`, contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 }, artifactHash: "artifact", fetchToken: false };
  const create = async () => {
    const endpoints = pair(); const stop = serveEngineHost(endpoints.worker, binding);
    const rpc = new EngineRpc(endpoints.ui, crypto.randomUUID(), "private");
    await rpc.call(rpc.request("init", [init]));
    const close = async () => { await rpc.call(rpc.request("close", [])); rpc.close(); stop(); };
    return { rpc, endpoints, close };
  };
  return { init, create };
}

it("recovers an admitted local reducer from the canonical journal with its original outcome and entropy", async () => {
  const { create, init } = await setup(); let host = await create();
  const command = { id: "original", now: 123, artifactHash: "artifact", scope: owner, entropy: "aa".repeat(32) };
  const request = host.rpc.request("reducer", [ref, {}, { engineCommand: command }]);
  expect(await host.rpc.call(request)).toBe(7);
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  const entries = await host.rpc.call<any[]>(host.rpc.request("listIntents", []));
  expect(entries).toHaveLength(1); expect(entries[0].id).toBe("original");
  const database = new Dexie(init.storage.outboxName); await database.open();
  const [entry] = await database.table("entries").toArray(); database.close();
  expect(entry.localExecution).toMatchObject({ commandId: "original", now: 123, intentEntropy: "aa".repeat(32) });
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  expect((await receipts.get(owner, "original"))?.result).toBe(7);
  await expect(host.rpc.call(host.rpc.request("reducer", [ref, { different: true }, { engineCommand: command }]))).rejects.toThrow("different arguments");
});

it("proxies auth fetch over the private endpoint before sending the private token", async () => {
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  const received: any[] = []; host.rpc.onEvent.add((method, args) => {
    received.push([method, args]);
    if (method === "fetchToken") void host.rpc.call(host.rpc.request("fetchTokenResult", [args[0], "private-token"]));
  });
  await host.rpc.call(host.rpc.request("setAuth", [{}, true, url]));
  await host.rpc.call(host.rpc.request("connect", [])); const socket = Socket.all[0]; socket.open();
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth" && frame.token === "private-token")).toBe(true));
  expect(received.some(([name]) => name === "fetchToken")).toBe(true);
});

it("falls back without coordination and keeps entity absence distinct from a pending cold read", async () => {
  const { init } = await setup();
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact" }, auth, storage: init.storage, fallbackBinding: binding });
  cleanups.push(() => client.close()); expect(client.engineMode).toBe("legacy");
  const watch = client.watchEntities("tasks", ["missing"]); expect(watch.getSnapshot()).toEqual({ status: "pending" });
  const stop = watch.onUpdate(() => undefined); cleanups.push(stop);
  await vi.waitFor(() => expect(watch.getSnapshot()).toEqual({ status: "ready", rows: [undefined] }));
});

it("the default client uses the original auth handshake and creates no worker", async () => {
  const worker = vi.fn(); vi.stubGlobal("Worker", worker);
  const client = new GonvexClient(url, auth); cleanups.push(() => client.close()); client.connect(); Socket.all[0].open();
  await vi.waitFor(() => expect(Socket.all[0].sent.some(frame => frame.type === "auth")).toBe(true));
  expect(worker).not.toHaveBeenCalled(); expect(Socket.all[0].sent.find(frame => frame.type === "auth").capabilities).not.toHaveProperty("engine");
});

it("preserves undefined cold rows in chunked results", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("readEntities", [], "rows"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "rows", result: [{ payload: "x".repeat(40_000) }, undefined] });
  expect(await pending).toEqual([{ payload: "x".repeat(40_000) }, undefined]);
});

it("patch batches preserve unchanged rows/nested fields and do not alter the prior snapshot", () => {
  const nested = Object.freeze({ label: "same" });
  const unchanged = Object.freeze({ id: "2", count: 0 });
  const previous = Object.freeze({ rows: Object.freeze([Object.freeze({ id: "1", count: 0, nested }), unchanged]) });
  const next = { rows: [{ id: "1", count: 1, nested }, unchanged] };
  const patches = diffEngineValue(previous, next)!;
  expect(patches).toEqual([{ path: ["rows", "0", "count"], value: 1 }]);
  const result = applyEnginePatches(previous, patches) as typeof next;
  expect(result.rows[0].nested).toBe(nested); expect(result.rows[1]).toBe(unchanged);
  expect(previous.rows[0].count).toBe(0); expect(Object.isFrozen(result.rows[0])).toBe(true);
});

it("shares mounted row objects, evicts all unmounted row metadata, and clears account caches immediately", async () => {
  const { init } = await setup();
  const storage = new IndexedDBLocalReplicaStorage(init.storage.replicaName);
  await storage.replaceSnapshot({ entities: { tasks: { one: { id: "one", value: 1, nested: { label: "same" } } } }, liveQueries: {} }, "visible"); storage.close();
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact" }, auth, storage: init.storage, fallbackBinding: binding }); cleanups.push(() => client.close());
  const a = client.watchEntities("tasks", ["one"]), b = client.watchEntities("tasks", ["one"]);
  const stopA = a.onUpdate(() => undefined), stopB = b.onUpdate(() => undefined);
  await vi.waitFor(() => { expect(a.getSnapshot().status).toBe("ready"); expect(b.getSnapshot().status).toBe("ready"); });
  const before = a.getSnapshot() as Extract<ReturnType<typeof a.getSnapshot>, { status: "ready" }>;
  expect((b.getSnapshot() as typeof before).rows[0]).toBe(before.rows[0]); expect(client.cacheStats().rows).toBe(1);
  client.setAuth({ identity: { sub: "other", iss: "issuer" } });
  expect(a.getSnapshot()).toEqual({ status: "pending" }); expect(b.getSnapshot()).toEqual({ status: "pending" }); expect(client.cacheStats().rows).toBe(0);
  await new Promise(resolve => setTimeout(resolve, 20)); expect(a.getSnapshot()).toEqual({ status: "pending" });
  stopA(); stopB(); expect(client.cacheStats().rows).toBe(0);
});

it("retains the canonical journal entry if a retirement receipt cannot persist", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  const remove = vi.fn(async () => undefined);
  const entry: any = { id: 1, scope: "s", path: "p", args: {}, idempotencyKey: "c", engineOutcome: { result: 1 } };
  const store = receipts.wrap({ load: async () => [entry], put: async () => undefined, delete: remove, clear: async () => undefined });
  vi.spyOn(receipts, "put").mockRejectedValueOnce(new Error("Disk full"));
  await expect(store.delete(1)).rejects.toThrow("Disk full"); expect(remove).not.toHaveBeenCalled();
});

it("preserves typed superseded errors across RPC so hooks can ignore old-scope requests", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("query", [], "query"));
  worker.postMessage({ generation: "g", scope: "s", id: "query", error: { name: "GonvexClientError", message: "Account changed", code: "superseded", operation: "query", path: "tasks" } });
  await expect(pending).rejects.toBeInstanceOf(GonvexClientError);
});

it("streams nested arrays and object maps without losing shared large subtrees or JSON key semantics", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const rows = Array.from({ length: 100 }, (_, index) => ({ id: String(index), nested: { value: "x".repeat(1000) } }));
  const result = { first: rows, second: rows, dictionary: Object.fromEntries(rows.map(row => [row.id, row])), special: JSON.parse('{"__proto__":{"value":1}}') };
  const pending = rpc.call<typeof result>(rpc.request("query", [], "graph"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "graph", result });
  const actual = await pending; expect(actual).toEqual(result); expect(actual.first).toBe(actual.second); expect(Object.hasOwn(actual.special, "__proto__")).toBe(true);
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
});

it("publishes a whole committed watch batch before notifying either mounted view", async () => {
  const endpoints = pair();
  const watchIds: string[] = [];
  let envelope: any;
  endpoints.worker.addEventListener("message", ({ data }) => {
    if (!data.method) return;
    envelope = data;
    if (data.method === "watch") watchIds.push(data.args[0]);
    endpoints.worker.postMessage({ generation: data.generation, scope: data.scope, id: data.id, result: data.method === "init" ? {
      connection: { isWebSocketConnected: false, hasEverConnected: false, connectionCount: 0, connectionRetries: 0, hasInflightRequests: false, inflightReducers: 0, inflightActions: 0, inflightOneShotQueries: 0 }, session: "restored", commandScope: owner,
    } : undefined });
  });
  vi.stubGlobal("Worker", class {});
  vi.stubGlobal("navigator", { locks: { request: (_name: string, _options: any, callback: any) => Promise.resolve(callback({ name: _name })), query: async () => ({ held: [] }) } });
  const worker = Object.assign(endpoints.ui, { terminate: vi.fn() }) as unknown as Worker;
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact", workerFactory: () => worker }, auth,
    storage: { replicaName: "atomic", outboxName: "atomic-outbox", contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 }, fallbackBinding: binding }); cleanups.push(() => client.close());
  const a = client.watchEntities("tasks", ["1"]), b = client.watchEntities("tasks", ["2"]);
  const observed: unknown[] = [];
  const read = () => observed.push([a.getSnapshot(), b.getSnapshot()]);
  const stopA = a.onUpdate(read), stopB = b.onUpdate(read); cleanups.push(stopA, stopB);
  await vi.waitFor(() => expect(watchIds).toHaveLength(2));
  endpoints.worker.postMessage({ generation: envelope.generation, scope: envelope.scope, id: "commit", event: "watchBatch", args: [1, watchIds.map((id, index) => ({ id, value: { status: "ready", rows: [{ id: String(index + 1), count: 1 }] } }))] });
  await vi.waitFor(() => expect(observed).toHaveLength(2));
  for (const states of observed as any[]) expect(states.map((state: any) => state.rows[0].count)).toEqual([1, 1]);
});

it("clears a watch error when its next successful host read has the same value", async () => {
  let failed = false, publish = () => {};
  vi.spyOn(GonvexClient.prototype, "watchControlQuery").mockImplementation(() => ({
    onUpdate: (notify: () => void) => { publish = notify; return () => {}; },
    getSnapshot: () => { if (failed) throw new Error("Temporary read failure"); return { result: 1, version: 1 }; },
  }) as any);
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  const updates: any[] = [];
  host.rpc.onEvent.add((method, args) => { if (method === "watchBatch") updates.push(...args[1]); });
  await host.rpc.call(host.rpc.request("watch", ["control", "control", [{ kind: "query", path: "control.test" }, {}]]));
  await vi.waitFor(() => expect(updates.at(-1)?.value).toEqual({ result: 1, version: 1 }));
  failed = true; publish();
  await vi.waitFor(() => expect(updates.at(-1)?.error?.message).toBe("Temporary read failure"));
  failed = false; publish();
  await vi.waitFor(() => expect(updates.at(-1)).toEqual({ id: "control", value: { result: 1, version: 1 } }));
});

it("uses the declared Live Query key for offline membership", async () => {
  const { init, create } = await setup();
  const storage = new IndexedDBLocalReplicaStorage(init.storage.replicaName);
  await storage.replaceSnapshot({ entities: { tasks: { one: { slug: "one", title: "Task", id: "other-id" } } }, liveQueries: {} }, "visible"); storage.close();
  const host = await create(); cleanups.push(host.close);
  const liveRef: FunctionReference = { kind: "query", path: "tasks.bySlug", delivery: "live", live: {
    entity: "tasks", key: "slug", plan: { table: "tasks", key: "slug", columns: ["slug", "title", "id"] },
  } };
  let snapshot: any;
  host.rpc.onEvent.add((method, args) => { if (method === "watchBatch") for (const update of args[1]) if (update.id === "live") snapshot = update.patches ? applyEnginePatches(snapshot, update.patches) : update.value; });
  await host.rpc.call(host.rpc.request("watch", ["live", "liveState", [liveRef, {}]]));
  await vi.waitFor(() => expect(snapshot?.ids).toEqual(["one"]));
  expect(snapshot.rows).toEqual([{ slug: "one", title: "Task", id: "other-id" }]);
  expect(snapshot.freshness).toBe("offline");
});

async function mockEngine(intercept?: (request: any, endpoint: Endpoint) => boolean) {
  const hosts: Array<{ endpoints: ReturnType<typeof pair>; envelope: any; watchIds: string[] }> = [];
  vi.stubGlobal("Worker", class {});
  vi.stubGlobal("navigator", { locks: { request: (_name: string, _options: any, callback: any) => Promise.resolve(callback({ name: _name })), query: async () => ({ held: [] }) } });
  const workerFactory = () => {
    const host = { endpoints: pair(), envelope: undefined as any, watchIds: [] as string[] }; hosts.push(host);
    host.endpoints.worker.addEventListener("message", ({ data }) => {
      if (!data.method) return;
      host.envelope = data;
      if (data.method === "watch") host.watchIds.push(data.args[0]);
      if (intercept?.(data, host.endpoints.worker)) return;
      host.endpoints.worker.postMessage({ generation: data.generation, scope: data.scope, id: data.id, result: data.method === "init" ? {
        connection: { isWebSocketConnected: false, hasEverConnected: false, connectionCount: 0, connectionRetries: 0, hasInflightRequests: false, inflightReducers: 0, inflightActions: 0, inflightOneShotQueries: 0 }, session: "restored", commandScope: owner,
      } : undefined });
    });
    return Object.assign(host.endpoints.ui, { terminate: vi.fn() }) as unknown as Worker;
  };
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact", workerFactory }, auth,
    storage: { replicaName: "mock", outboxName: "mock-outbox", contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 }, fallbackBinding: binding });
  cleanups.push(() => client.close());
  const publish = (sequence: number, value: unknown) => {
    const host = hosts.at(-1)!;
    host.endpoints.worker.postMessage({ generation: host.envelope.generation, scope: host.envelope.scope, id: crypto.randomUUID(), event: "watchBatch",
      args: [sequence, [{ id: host.watchIds.at(-1), value }]] });
  };
  return { client, hosts, publish };
}

it.each(["query", "action"] as const)("honors longer and disabled per-call %s timeouts", async method => {
  const calls: Array<{ request: any; endpoint: Endpoint }> = [];
  const { client } = await mockEngine((request, endpoint) => {
    if (request.method !== method) return false;
    calls.push({ request, endpoint }); return true;
  });
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    for (const timeoutMs of [30_000, 0]) {
      let settled = false;
      const pending = client[method]({ kind: method, path: "slow.test" }, {}, { timeoutMs }).then(
        result => { settled = true; return { result }; }, error => { settled = true; return { error }; });
      await vi.advanceTimersByTimeAsync(21_000);
      const timedOut = settled;
      const { request, endpoint } = calls.at(-1)!;
      endpoint.postMessage({ generation: request.generation, scope: request.scope, id: request.id, result: 42 });
      const outcome = await pending;
      expect(timedOut).toBe(false);
      expect(outcome).toEqual({ result: 42 });
      expect(request.args[2]).toEqual({ timeoutMs });
    }
  } finally { vi.useRealTimers(); }
});

it.each([false, true])("resubscribes mounted watches after replica reset, rejected=%s", async rejected => {
  const { client, hosts, publish } = await mockEngine((request, endpoint) => {
    if (request.method !== "resetLocalReplica") return false;
    endpoint.postMessage({ generation: request.generation, scope: request.scope, id: request.id,
      ...(rejected ? { error: { name: "GonvexClientError", code: "storage", message: "Reset failed" } }
        : { result: { discardedIntents: 0, resubscribed: 1 } }) }); return true;
  });
  const watch = client.watchEntities("tasks", ["one"]);
  cleanups.push(watch.onUpdate(() => {}));
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  const previousId = hosts[0].watchIds[0];
  publish(42, { status: "ready", rows: [{ id: "one", value: 1 }] });
  await vi.waitFor(() => expect(watch.getSnapshot().status).toBe("ready"));
  const reset = client.resetLocalReplica();
  if (rejected) await expect(reset).rejects.toMatchObject({ code: "storage", message: "Reset failed" });
  else expect(await reset).toEqual({ discardedIntents: 0, resubscribed: 1 });
  expect(watch.getSnapshot()).toEqual({ status: "pending" });
  expect(hosts[0].watchIds).toHaveLength(2);
  expect(hosts[0].watchIds[1]).not.toBe(previousId);
  publish(1, { status: "ready", rows: [{ id: "one", value: 2 }] });
  await vi.waitFor(() => expect(watch.getSnapshot()).toEqual({ status: "ready", rows: [{ id: "one", value: 2 }] }));
});

it("accepts object results after truthy scalars and keeps new undefined own fields", async () => {
  const { client, hosts, publish } = await mockEngine();
  const watch = client.watchControlQuery({ kind: "query", path: "control.test" });
  cleanups.push(watch.onUpdate(() => {}));
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  publish(1, { result: 1, version: 1 });
  await vi.waitFor(() => expect(watch.getSnapshot()?.result).toBe(1));
  publish(2, { result: {}, version: 2 });
  await vi.waitFor(() => expect(watch.getSnapshot()?.result).toEqual({}));
  publish(3, { result: { a: undefined }, version: 3 });
  await vi.waitFor(() => expect(watch.getSnapshot()?.result).toEqual({ a: undefined }));
  publish(4, { result: { b: undefined }, version: 4 });
  await vi.waitFor(() => expect(Object.keys(watch.getSnapshot()!.result as object)).toEqual(["b"]));
});

it("accepts a new host's sequence when a detached watch is subscribed again", async () => {
  const { client, hosts, publish } = await mockEngine();
  const watch = client.watchEntities("tasks", ["one"]);
  const stop = watch.onUpdate(() => {});
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  publish(42, { status: "ready", rows: [{ id: "one", value: 1 }] });
  await vi.waitFor(() => expect(watch.getSnapshot().status).toBe("ready"));
  stop(); await client.recover();
  cleanups.push(watch.onUpdate(() => {}));
  await vi.waitFor(() => expect(hosts[1].watchIds).toHaveLength(1));
  publish(1, { status: "ready", rows: [{ id: "one", value: 2 }] });
  await vi.waitFor(() => expect(watch.getSnapshot()).toEqual({ status: "ready", rows: [{ id: "one", value: 2 }] }));
});

it("patches own __proto__ fields without changing prototypes or earlier snapshots", () => {
  const previous = Object.freeze({ nested: Object.freeze({}) });
  const added = { nested: JSON.parse('{"__proto__":{"value":1},"missing":null}') };
  added.nested.missing = undefined;
  const first = applyEnginePatches(previous, diffEngineValue(previous, added)!) as typeof added;
  expect(Object.hasOwn(first.nested, "__proto__")).toBe(true);
  expect(Object.hasOwn(first.nested, "missing")).toBe(true);
  expect(Object.getPrototypeOf(first.nested)).toBe(Object.prototype);
  const changed = { nested: JSON.parse('{"__proto__":{"value":2}}') };
  const second = applyEnginePatches(first, diffEngineValue(first, changed)!) as typeof changed;
  expect(Object.getPrototypeOf(second.nested)).toBe(Object.prototype);
  expect(second.nested.__proto__.value).toBe(2);
  expect(first.nested.__proto__.value).toBe(1);
  expect(previous.nested).toEqual({});
});

it("preserves aliases and empty-name undefined fields in the long-key text codec", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const shared = { "": undefined, nested: { value: "x".repeat(40_000) } };
  const result = { ["k".repeat(1100)]: shared, first: shared, second: shared, "": undefined };
  const pending = rpc.call<typeof result>(rpc.request("query", [], "text"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "text", result });
  const actual = await pending;
  expect(actual).toEqual(result); expect(actual.first).toBe(actual.second); expect(actual.first).toBe(actual["k".repeat(1100)]);
  expect(Object.hasOwn(actual, "")).toBe(true); expect(actual[""]).toBeUndefined();
  expect(Object.hasOwn(actual.first, "")).toBe(true); expect(actual.first[""]).toBeUndefined();
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
});

it.each([false, true])("finishes late chunked replies after timeout, text codec=%s", async text => {
  const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("query", [], "late"), 5);
  const envelope = { generation: "g", scope: "s", id: "late" };
  const receive = (packet: unknown) => ui.emit("message", { data: { ...envelope, packet } });
  receive(text ? { kind: "textBegin" } : { kind: "begin", value: { ...envelope, result: "" } });
  await expect(pending).rejects.toBeInstanceOf(EngineRpcTimeout);
  expect(() => {
    receive(text ? { kind: "text", value: JSON.stringify({ payload: { ...envelope, result: "late" }, undefinedPaths: [], aliasPaths: [] }) }
      : { kind: "string", path: ["result"], value: "late" });
    receive({ kind: text ? "textEnd" : "end" });
  }).not.toThrow();
  const next = rpc.call(rpc.request("query", [], "next"));
  ui.emit("message", { data: { ...envelope, id: "next", result: "fresh" } });
  expect(await next).toBe("fresh");
});
