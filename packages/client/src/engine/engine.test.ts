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
  expect(await retried).toBe(1); expect(ui.sent.flatMap(frame => frame.admissions ?? [frame]).filter(frame => frame.id === "first")).toEqual([first, first]);
});

it("bounds chunk delivery and reassembles a large reply atomically", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const result = "😀".repeat(80_000); const pending = rpc.call(rpc.request("query", [], "large"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "large", result });
  expect(await pending).toBe(result);
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
    const state = await rpc.call<{ capabilities?: { admissionBatch?: number } }>(rpc.request("init", [init]));
    rpc.setAdmissionBatching(state.capabilities?.admissionBatch === 1);
    const close = async () => { await rpc.call(rpc.request("close", [])); rpc.close(); stop(); };
    return { rpc, endpoints, close };
  };
  return { init, create };
}

it("recovers an admitted local reducer from the canonical journal with its original outcome and entropy", async () => {
  const { create, init } = await setup(); let host = await create();
  const command = { id: "original", now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "aa".repeat(32) };
  const request = host.rpc.request("reducer", [ref, {}, { engineCommand: command }]);
  expect(await host.rpc.call(request)).toBe(7);
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  const entries = await host.rpc.call<any[]>(host.rpc.request("listIntents", []));
  expect(entries).toHaveLength(1); expect(entries[0].id).toBe("original");
  const database = new Dexie(init.storage.outboxName); await database.open();
  const [entry] = await database.table("entries").toArray(); database.close();
  expect(entry.localExecution).toMatchObject({ commandId: "original", now: command.now, intentEntropy: "aa".repeat(32) });
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  expect(await receipts.get(owner, "original")).toBeUndefined();
  expect(entry.engineResult).toBe(7);
  expect(entry.engineOutcome).toBeUndefined();
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

it("coalesces admissions without waiting for replies and preserves control ordering", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  rpc.setAdmissionBatching(true);
  const requests = Array.from({ length: 3 }, (_, index) => rpc.request("reducer", [{ index }], String(index)));
  const pending = requests.map(request => rpc.call(request));
  const control = rpc.call(rpc.request("query", [], "control"));
  expect(ui.sent[0]).toEqual({ admissions: requests });
  expect(ui.sent[1].id).toBe("control");
  for (const request of requests.toReversed()) worker.postMessage({ ...request, result: Number(request.id) });
  worker.postMessage({ generation: "g", scope: "s", id: "control", result: 9 });
  expect(await Promise.all(pending)).toEqual([0, 1, 2]); expect(await control).toBe(9);
});

it("recovers every command whose batch replies were lost without duplicating durable entries", async () => {
  const { create } = await setup(); let host = await create();
  const commands = Array.from({ length: 3 }, (_, index) => ({ id: `batch-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const requests = commands.map(command => host.rpc.request("reducer", [ref, {}, { engineCommand: command }]));
  const reply = host.endpoints.worker.postMessage.bind(host.endpoints.worker);
  host.endpoints.worker.postMessage = data => { if (!requests.some(request => request.id === (data as any).id)) reply(data); };
  const results = await Promise.allSettled(requests.map(request => host.rpc.call(request, 100)));
  expect(results.every(result => result.status === "rejected" && result.reason instanceof EngineRpcTimeout)).toBe(true);
  expect(host.endpoints.ui.sent.filter(frame => frame.admissions).map(frame => frame.admissions.length)).toEqual([3]);
  const before = await host.rpc.call<any[]>(host.rpc.request("listIntents", []));
  expect(before.map(entry => entry.id)).toEqual(commands.map(command => command.id));
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await Promise.all(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))))).toEqual([7, 7, 7]);
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual(before);
});

it("batches receipt retirement into one transaction and bounds recovery with a durable retry floor", async () => {
  const name = crypto.randomUUID(); let receipts = new EngineReceipts(name, 2, 1000);
  const database = (receipts as any).database as Dexie;
  const transaction = vi.spyOn(database, "transaction");
  const now = Date.now();
  await Promise.all([0, 1, 2].map(index => receipts.put("s", String(index), "p", {}, index, now - 10 + index)));
  expect(transaction).toHaveBeenCalledTimes(1);
  expect(await database.table("receipts").count()).toBe(2);
  const remaining = await database.table("receipts").toArray();
  const removed = [0, 1, 2].find(index => !remaining.some(receipt => receipt.key === JSON.stringify(["s", String(index)])))!;
  receipts.close(); receipts = new EngineReceipts(name, 2, 1000); cleanups.push(() => receipts.close());
  await expect(receipts.assertCanAdmit(now - 10 + removed)).rejects.toThrow("receipt expired");
  await expect(receipts.assertCanAdmit(now + 1)).resolves.toBeUndefined();
  const spy = vi.spyOn(Date, "now").mockReturnValue(now + 2000);
  expect(await receipts.get("s", String(remaining[0].result))).toBeUndefined();
  await expect(receipts.assertCanAdmit(now)).rejects.toThrow("receipt expired");
  await receipts.put("s", "new", "p", {}, 9);
  spy.mockRestore();
  expect(await (receipts as any).database.table("receipts").count()).toBe(1);
});

it("keeps all canonical entries when a batched receipt transaction fails", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  const entries: any[] = [1, 2].map(id => ({ id, scope: "s", path: "p", args: {}, idempotencyKey: String(id), engineOutcome: { result: id }, createdAt: Date.now() }));
  const remove = vi.fn(async () => undefined);
  const store = receipts.wrap({ load: async () => entries, put: async () => undefined, delete: remove, deleteMany: remove, clear: async () => undefined });
  vi.spyOn((receipts as any).database, "transaction").mockRejectedValueOnce(new Error("Crash before commit"));
  await expect(store.deleteMany!([1, 2])).rejects.toThrow("Crash before commit");
  expect(remove).not.toHaveBeenCalled();
  expect(await receipts.get("s", "1")).toBeUndefined(); expect(await receipts.get("s", "2")).toBeUndefined();
});

it("preserves sibling admissions when storage fails mid-batch and retries the original missing command", async () => {
  const { create } = await setup(); let host = await create();
  const commands = [0, 1, 2].map(index => ({ id: `partial-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "bc".repeat(32) }));
  const wrap = EngineReceipts.prototype.wrap;
  let calls = 0;
  const failure = vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    const append = wrapped.append!;
    return { ...wrapped, append: async entry => {
      if (++calls === 2) throw new Error("Crash before durable admission");
      return append(entry);
    } };
  });
  await host.close(); host = await create();
  const results = await Promise.allSettled(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }]))));
  expect(results.map(result => result.status)).toEqual(["fulfilled", "rejected", "fulfilled"]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(["partial-0", "partial-2"]);
  failure.mockRestore(); await host.close(); host = await create(); cleanups.push(host.close);
  expect(await Promise.all(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))))).toEqual([7, 7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id).sort()).toEqual(commands.map(command => command.id));
});

it("migrates old receipts before pruning and refuses ambiguous retries after eviction", async () => {
  const name = crypto.randomUUID();
  const old = new Dexie(`${name}-engine-receipts`);
  old.version(1).stores({ receipts: "&key" });
  await old.table("receipts").bulkPut([0, 1].map(index => ({ key: JSON.stringify(["s", String(index)]), path: "p", args: "{}", result: index })));
  old.close();
  const receipts = new EngineReceipts(name, 1); cleanups.push(() => receipts.close());
  const existing = await receipts.get("s", "0");
  const migrated = await (receipts as any).database.table("receipts").toArray();
  expect(migrated).toHaveLength(1); expect([0, 1]).toContain(migrated[0].result);
  if (existing) expect(existing.result).toBe(0);
  await expect(receipts.assertCanAdmit(Date.now() - 1000)).rejects.toThrow("receipt expired");
  await receipts.put("s", "new", "p", {}, 9);
  expect(await (receipts as any).database.table("receipts").count()).toBe(1);
  await expect(receipts.assertCanAdmit(Date.now() - 1000)).rejects.toThrow("receipt expired");
});

it("keeps individual reducer envelopes until batching is negotiated", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const requests = [rpc.request("reducer", [], "old-a"), rpc.request("reducer", [], "old-b")];
  const pending = requests.map(request => rpc.call(request));
  expect(ui.sent).toEqual(requests);
  for (const request of requests) worker.postMessage({ ...request, result: 1 });
  expect(await Promise.all(pending)).toEqual([1, 1]);
});

it("orders lookup starts without making later calls wait for the first durable acknowledgement", async () => {
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const starts: string[] = [];
  const lookup = vi.spyOn(EngineReceipts.prototype, "findReceipt").mockImplementation(async (_scope, id) => { starts.push(id); return undefined; });
  const admit = EngineReceipts.prototype.assertCanAdmit;
  let checks = 0;
  const check = vi.spyOn(EngineReceipts.prototype, "assertCanAdmit").mockImplementation(async function(now) {
    if (++checks === 1) await blocked;
    return admit.call(this, now);
  });
  const commands = [0, 1].map(index => ({ id: `pipeline-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "cd".repeat(32) }));
  const pending = commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }])));
  await vi.waitFor(() => expect(starts.slice(0, 2)).toEqual(["pipeline-0", "pipeline-1"]));
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  release(); expect(await Promise.all(pending)).toEqual([7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
  lookup.mockRestore(); check.mockRestore();
});

it("delivers preceding watch batches before resolving a small reducer reply", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const events: string[] = []; rpc.onEvent.add(method => events.push(method));
  const pending = rpc.call(rpc.request("reducer", [], "result"));
  const watch = sendEngineMessage(worker, { generation: "g", scope: "s", id: "watch", event: "watchBatch", args: ["x".repeat(40_000)] });
  const reply = sendEngineMessage(worker, { generation: "g", scope: "s", id: "result", result: 7 });
  expect(await pending).toBe(7); expect(events).toEqual(["watchBatch"]);
  await Promise.all([watch, reply]);
});

it("amortizes pruning while never exceeding the hard receipt cap", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  await Promise.all(Array.from({ length: 1024 }, (_, index) => receipts.put("s", String(index), "p", {}, index)));
  const database = (receipts as any).database;
  const readFloor = vi.spyOn(database.metadata, "get");
  for (let index = 1024; index < 1088; index++) {
    await receipts.put("s", String(index), "p", {}, index);
    expect(await database.receipts.count()).toBeLessThanOrEqual(1024);
  }
  expect(readFloor).toHaveBeenCalledTimes(1);
  expect(await database.receipts.count()).toBe(1024);
});

it("preserves an online queued-capable reducer's actual server result after retirement and reopen", async () => {
  const { create } = await setup(); let host = await create();
  await host.rpc.call(host.rpc.request("connect", [])); const socket = Socket.all[0]; socket.open();
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
  const frame = socket.sent.find(frame => frame.type === "auth");
  socket.receive({ type: "auth.result", id: frame.id, result: { artifactHash: "artifact", replica: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, localIdentity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } } });
  await vi.waitFor(() => expect(host.endpoints.worker.sent.some(frame => frame.event === "session" && frame.args[0] === "authenticated")).toBe(true));
  const remote: FunctionReference = { kind: "reducer", path: "online", offline: { mode: "allowed" } };
  const command = { id: "server-result", now: Date.now(), scope: owner, entropy: "de".repeat(32) };
  const first = host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command }]));
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "reducer.call" && frame.path === "online")).toBe(true));
  const call = socket.sent.find(frame => frame.type === "reducer.call" && frame.path === "online");
  socket.receive({ type: "reducer.result", id: call.id, result: { value: 42 } });
  expect(await first).toEqual({ value: 42 });
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command }]))).toEqual({ value: 42 });
  expect(socket.sent.filter(frame => frame.type === "reducer.call" && frame.path === "online")).toHaveLength(1);
});

it("rechecks receipts after the canonical read so retirement cannot race an explicit retry", async () => {
  const { create, init } = await setup(); const host = await create(); cleanups.push(host.close);
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  const command = { id: "retired-race", now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ef".repeat(32) };
  await receipts.put(owner, command.id, ref.path, {}, 7, command.now);
  const lookup = vi.spyOn(EngineReceipts.prototype, "findReceipt").mockResolvedValueOnce(undefined);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  expect(lookup).toHaveBeenCalledTimes(2); lookup.mockRestore();
});

it("keeps loaded entity reads ready across same-account offline session transitions", async () => {
  const { init } = await setup();
  const storage = new IndexedDBLocalReplicaStorage(init.storage.replicaName);
  await storage.replaceSnapshot({ entities: { tasks: { one: { id: "one", value: 1 } } }, liveQueries: {} }, "visible"); storage.close();
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact" }, auth, storage: init.storage, fallbackBinding: binding }); cleanups.push(() => client.close());
  const watch = client.watchEntities("tasks", ["one"]); const states: string[] = [];
  const stop = watch.onUpdate(() => states.push(watch.getSnapshot().status)); cleanups.push(stop);
  await vi.waitFor(() => expect(watch.getSnapshot().status).toBe("ready"));
  client.connect(); await vi.waitFor(() => expect(Socket.all).toHaveLength(1)); const socket = Socket.all[0]; socket.open();
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
  const frame = socket.sent.find(frame => frame.type === "auth");
  socket.receive({ type: "auth.result", id: frame.id, result: { artifactHash: "artifact", replica: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, localIdentity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } } });
  await vi.waitFor(() => expect(client.localSessionState()).toBe("authenticated"));
  states.length = 0; const before = watch.getSnapshot(); socket.close();
  await vi.waitFor(() => expect(client.localSessionState()).toBe("restored"));
  await new Promise(resolve => setTimeout(resolve, 20));
  expect(states).not.toContain("pending"); expect(watch.getSnapshot()).toBe(before);
});

it("coalesces a 500-call burst into one negotiated packet and retains the older 128-call limit", async () => {
  for (const limit of [undefined, 1024]) {
    const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); rpc.setAdmissionBatching(true, limit);
    const pending = Array.from({ length: 500 }, (_, index) => rpc.call(rpc.request("reducer", [], String(index)), 0));
    const settled = Promise.allSettled(pending); await Promise.resolve();
    expect(ui.sent.map(frame => frame.admissions.length)).toEqual(limit ? [500] : [128, 128, 128, 116]);
    rpc.close(); await settled;
  }
});

it("recovers the original outcome from a journal written in the older wrapper format", async () => {
  const { create, init } = await setup(); let host = await create();
  const command = { id: "older-format", now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) };
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  await host.close();
  const database = new Dexie(init.storage.outboxName); await database.open();
  const [entry] = await database.table("entries").toArray();
  const { engineResult, ...older } = entry;
  await database.table("entries").put({ ...older, engineOutcome: { result: engineResult } }); database.close();
  host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  expect(await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).toHaveLength(1);
});

it("shares one intent lease for a fresh burst while acknowledging each durable entry in order", async () => {
  const { create } = await setup();
  const locks: string[] = [];
  const wrap = EngineReceipts.prototype.wrap;
  vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    return { ...wrapped, coordinate: (scope, lane, run) => {
      locks.push(lane); return wrapped.coordinate!(scope, lane, run);
    } };
  });
  const host = await create(); cleanups.push(host.close); locks.length = 0;
  const commands = Array.from({ length: 8 }, (_, index) => ({ id: `cohort-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const acknowledgements: string[] = [];
  expect(await Promise.all(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }])).then(result => { acknowledgements.push(command.id); return result; })))).toEqual(Array(8).fill(7));
  expect(acknowledgements).toEqual(commands.map(command => command.id));
  expect(locks.filter(name => name.includes("intent"))).toHaveLength(1);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(acknowledgements);
});

it("does not let a fresh cohort overtake an explicit retry awaiting lookup", async () => {
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const lookup = EngineReceipts.prototype.findReceipt;
  const waiting = vi.spyOn(EngineReceipts.prototype, "findReceipt").mockImplementation(async function(scope, id, path, args) {
    if (id === "ordered-0") await blocked;
    return lookup.call(this, scope, id, path, args);
  });
  const commands = Array.from({ length: 3 }, (_, index) => ({ id: `ordered-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const first = host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: commands[0] }]));
  await vi.waitFor(() => expect(waiting).toHaveBeenCalled());
  const rest = commands.slice(1).map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }])));
  await new Promise(resolve => setTimeout(resolve, 0));
  release(); expect(await Promise.all([first, ...rest])).toEqual([7, 7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
});

it("keeps a later packet behind a cohort waiting for its delivery lease", async () => {
  const { create } = await setup();
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const wrap = EngineReceipts.prototype.wrap; let waiting = false;
  vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    return { ...wrapped, coordinate: async (scope, lane, run) => {
      if (lane === "delivery" && !waiting) { waiting = true; await blocked; }
      return wrapped.coordinate!(scope, lane, run);
    } };
  });
  const host = await create(); cleanups.push(host.close);
  const commands = Array.from({ length: 3 }, (_, index) => ({ id: `packet-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const pending = commands.slice(0, 2).map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }])));
  await vi.waitFor(() => expect(waiting).toBe(true));
  pending.push(host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: commands[2], engineFresh: true }])));
  await new Promise(resolve => setTimeout(resolve, 0));
  release(); expect(await Promise.all(pending)).toEqual([7, 7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
});
