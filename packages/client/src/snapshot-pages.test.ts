import { afterEach, expect, it, vi } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { Dexie } from "dexie";
import { IndexedDBLocalReplicaStorage } from "./indexeddb-replica";
import type { LocalReplicaStorage } from "./local-replica";
import type { JsonValue } from "@gonvex/protocol";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference } from "./index";
import { replicaAggregateDigest, replicaHashesDigest, replicaRowsHashes } from "./replica-integrity";
import { SnapshotPageStager, type SnapshotPage } from "./snapshot-pages";

class Socket {
  static CONNECTING = 0;
  static OPEN = 1;
  static instances: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.instances.push(this); }
  addEventListener(type: string, handler: (event: any) => void) {
    this.listeners.set(type, [...this.listeners.get(type) ?? [], handler]);
  }
  send(message: string) { this.sent.push(JSON.parse(message)); }
  emit(type: string, event = {}) { for (const handler of this.listeners.get(type) ?? []) handler(event); }
  open() { this.readyState = 1; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(message: unknown) { this.emit("message", { data: JSON.stringify(message) }); }
}

const clients: GonvexClient[] = [];
afterEach(() => { for (const client of clients.splice(0)) client.close(); vi.restoreAllMocks(); vi.unstubAllGlobals(); Socket.instances = []; });
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "e" };
const reference: FunctionReference = {
  kind: "query", path: "tasks.all", delivery: "replica",
  replica: { table: "tasks", key: "id", columns: ["id", "value"], maxRows: 20_000 },
};
const base: SnapshotPage = {
  type: "replica.snapshotPage", id: "open", snapshotId: "snapshot", scope: "tenant", visibilityScope: "visible",
  sequence: 0, last: false, key: "id", cursor: { epoch: "e", revision: 10 }, result: [],
  mode: "eager", path: "tasks.all", maxRows: 20_000, maxBytes: 100_000_000, truncated: false,
};
async function pages(rows: JsonValue[], id = "open", aggregate = false) {
  const hashes = await replicaRowsHashes(rows, "id");
  const digest = aggregate ? replicaAggregateDigest(hashes) : await replicaHashesDigest(hashes);
  return [
    { ...base, id, result: rows.slice(0, 1) },
    { ...base, id, sequence: 1, last: true, result: rows.slice(1), digest },
  ];
}
async function settled(client: GonvexClient) { await (client as any).replicaFrames; }
async function setup(capabilities: Record<string, number> = { replicaSnapshotPages: 1 }, storage: LocalReplicaStorage = new MemoryLocalReplicaStorage()) {
  vi.stubGlobal("WebSocket", Socket);
  const client = new GonvexClient("ws://runtime.test/ws", { token: "session", tenant: "tenant", project: "project", localReplica: { storage }, replicaSubscriptionRetentionMs: 0 });
  clients.push(client);
  const observer = vi.fn();
  client.subscribeReplica(reference, {}, observer);
  const socket = Socket.instances.at(-1)!;
  socket.open();
  socket.receive({ type: "session.ready", replica: directive, capabilities });
  await new Promise(resolve => setTimeout(resolve, 0));
  const auth = socket.sent.find(message => message.type === "auth");
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "replica.open")).toBe(true));
  const open = socket.sent.find(message => message.type === "replica.open");
  return { client, socket, observer, open, storage };
}

it("stages pages without publishing rows or cursor and commits once after final validation", async () => {
  const { client, socket, observer, open, storage } = await setup();
  expect(socket.sent.find(message => message.type === "auth").capabilities.replicaSnapshotPages).toBe(1);
  const replace = vi.spyOn(storage, "replaceWindow");
  const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: null }], open.id);
  socket.receive(sequence[0]); await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toBeUndefined();
  expect(client.localReplica.snapshot().cursor).toBeUndefined();
  expect(replace).not.toHaveBeenCalled();
  expect(observer).not.toHaveBeenCalled();
  socket.receive(sequence[1]); await settled(client);
  expect(client.localReplica.entityRows("tasks")).toEqual([{ id: "a", value: 1 }, { id: "b", value: null }]);
  expect(replace).toHaveBeenCalledTimes(1);
  expect(observer).toHaveBeenCalledTimes(1);
  expect(observer.mock.calls[0]![0].cursor).toEqual(base.cursor);
  expect(observer.mock.calls[0]![0].type).toBe("replica.snapshot");
  const windows = Object.values((await storage.load("visible"))!.liveQueries!);
  expect(windows[0]!.cursor).toEqual(base.cursor);
  expect(windows[0]!.completeness).toBe("complete");
});

it("buffers a cross-entity authoritative transaction, delta and watermark until activation", async () => {
  const { client, socket, open } = await setup({ replicaSnapshotPages: 1, replicaWatermark: 1 });
  const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: 1 }], open.id);
  socket.receive(sequence[0]); await settled(client);
  socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 11 }, changes: [
    { entity: "other", id: "x", operation: "insert", newValue: { id: "x", value: 2 } },
    { entity: "tasks", id: "a", operation: "update", newValue: { id: "a", value: 2 } },
  ] });
  socket.receive({ type: "replica.delta", id: open.id, cursor: { epoch: "e", revision: 11 }, upserts: [{ id: "a", value: 2 }], deleted: [] });
  socket.receive({ type: "replica.watermark", revision: 12 });
  await settled(client);
  expect(client.localReplica.entity("other", "x")).toBeUndefined();
  expect(client.localReplica.entity("tasks", "a")).toBeUndefined();
  socket.receive(sequence[1]); await settled(client);
  expect(client.localReplica.entity("other", "x")).toEqual({ id: "x", value: 2 });
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 2 });
  expect(client.localReplica.entity("tasks", "b")).toEqual({ id: "b", value: 1 });
});

it("discards interrupted pages and reopens against an old runtime using only committed data", async () => {
  const { client, socket, open, storage } = await setup();
  const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: 2 }], open.id);
  socket.receive(sequence[0]); await settled(client); socket.close();
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
  expect((await storage.load("visible"))?.entities ?? {}).toEqual({});
  // Reconnect with no paging support, as after a runtime rollback.
  await vi.waitFor(() => expect(Socket.instances.length).toBe(2));
  const resumed = Socket.instances.at(-1)!;
  resumed.open(); resumed.receive({ type: "session.ready", replica: directive });
  const auth = resumed.sent.find(message => message.type === "auth");
  resumed.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
  await vi.waitFor(() => expect(resumed.sent.some(message => message.type === "replica.open")).toBe(true));
  const reopened = resumed.sent.find(message => message.type === "replica.open");
  expect(reopened.cursor).toBeUndefined();
  socket.receive(sequence[1]); await settled(client);
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
  resumed.receive({ type: "replica.snapshot", id: reopened.id, key: "id", cursor: base.cursor, result: [{ id: "legacy", value: 3 }] });
  await settled(client);
  expect(client.localReplica.entity("tasks", "legacy")).toEqual({ id: "legacy", value: 3 });
});

it("preserves active data and durable checkpoint on invalid digest or failed activation", async () => {
  for (const failStorage of [false, true]) {
    const { client, socket, open, storage } = await setup();
    socket.receive({ type: "replica.snapshot", id: open.id, key: "id", cursor: { epoch: "e", revision: 1 }, result: [{ id: "old", value: 0 }] });
    await settled(client);
    // Simulate the next full open without resetting committed state.
    const subscription = [...(client as any).replicaSubscriptions.values()][0] as any;
    subscription.opening = true;
    const sequence = await pages([{ id: "new", value: 1 }], open.id);
    if (failStorage) vi.spyOn(storage, "replaceWindow").mockRejectedValueOnce(new Error("disk-full"));
    else sequence[1]!.digest = "invalid";
    socket.receive(sequence[0]); socket.receive(sequence[1]); await settled(client);
    expect(client.localReplica.entityRows("tasks")).toEqual([{ id: "old", value: 0 }]);
    expect(Object.values((await storage.load("visible"))!.liveQueries!)[0]!.cursor?.revision).toBe(1);
    expect(socket.readyState).toBe(3);
    client.close();
  }
});

it("fences scope changes and disconnects during asynchronous hashing", async () => {
  const { client, socket, open } = await setup();
  const sequence = await pages([{ id: "a", value: 1 }], open.id);
  const original = crypto.subtle.digest.bind(crypto.subtle);
  let release!: () => void;
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  vi.spyOn(crypto.subtle, "digest").mockImplementationOnce(async (...args) => {
    entered(); await new Promise<void>(resolve => { release = resolve; }); return original(...args);
  });
  socket.receive(sequence[0]); await started;
  socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 11 }, changes: [
    { entity: "tasks", id: "late", operation: "insert", newValue: { id: "late", value: 2 } },
  ] });
  socket.close(); release(); await settled(client);
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
  expect([...(client as any).replicaSubscriptions.values()][0].snapshotPages).toBeUndefined();
});

it("rejects sequence gaps, duplicate keys, changed generation and budgets; supports empty/truncated snapshots", async () => {
  const valid = await pages([{ id: "__proto__", value: 1 }, { id: "b" }]);
  const stager = new SnapshotPageStager(valid[0]!);
  await stager.append(valid[0]!);
  const complete = await stager.append(valid[1]!);
  expect(complete!.result).toEqual([{ id: "__proto__", value: 1 }, { id: "b" }]);
  expect(Object.keys(complete!.hashes!)).toEqual(["__proto__", "b"]);
  for (const changed of [
    { sequence: 2 }, { snapshotId: "other" }, { visibilityScope: "revoked" },
    { cursor: { epoch: "e", revision: 11 } }, { key: "other" }, { result: [{ id: "__proto__" }] },
  ]) {
    const staging = new SnapshotPageStager(valid[0]!); await staging.append(valid[0]!);
    await expect(staging.append({ ...valid[1]!, ...changed })).rejects.toThrow();
  }
  await expect(new SnapshotPageStager({ ...valid[0]!, maxBytes: 1 }).append({ ...valid[0]!, maxBytes: 1 })).rejects.toThrow("budget");
  const empty = { ...base, result: [], last: true, truncated: true, digest: await replicaHashesDigest({}) };
  expect((await new SnapshotPageStager(empty).append(empty))!.truncated).toBe(true);
  const aggregate = await pages([{ id: "a", value: 1 }], "open", true);
  const staging = new SnapshotPageStager(aggregate[0]!); await staging.append(aggregate[0]!);
  expect((await staging.append(aggregate[1]!))!.digest).toMatch(/^xor1:/);
});

it("reconnects instead of retaining an unbounded feed during snapshot staging", async () => {
  const { client, socket, open } = await setup();
  const sequence = await pages([{ id: "a", value: 1 }], open.id);
  socket.receive(sequence[0]); await settled(client);
  socket.receive({ type: "replica.delta", id: open.id, cursor: { epoch: "e", revision: 11 }, upserts: [{ id: "huge", value: "x".repeat(2 * 1024 * 1024) }] });
  await settled(client);
  expect(socket.readyState).toBe(3);
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
  expect((client as any).snapshotDeferredFrames).toEqual([]);
});


it("keeps a progressive truncated collection partial and ignores pages without server acceptance", async () => {
  const { client, socket, open, observer } = await setup();
  const sequence = (await pages([{ id: "a", value: 1 }], open.id)).map(page => ({ ...page, mode: "progressive" as const, truncated: true }));
  socket.receive(sequence[0]); await settled(client);
  expect(observer).not.toHaveBeenCalled();
  socket.receive(sequence[1]); await settled(client);
  const window = Object.values(client.localReplica.snapshot().liveQueries!)[0]!;
  expect(window.completeness).toBe("partial");
  expect(window.mode).toBe("progressive");
  const old = await setup({});
  const unsolicited = await pages([{ id: "forbidden", value: 1 }], old.open.id);
  old.socket.receive(unsolicited[0]); old.socket.receive(unsolicited[1]); await settled(old.client);
  expect(old.client.localReplica.entityRows("tasks")).toEqual([]);
});

it("discards staged rows on visibility scope rotation and rejects late pages from the old scope", async () => {
  const { client, socket, open, storage } = await setup();
  const sequence = await pages([{ id: "foreign", value: 1 }], open.id);
  socket.receive(sequence[0]); await settled(client);
  await (client as any).activateReplicaDirective({ ...directive, visibilityScope: "next" });
  socket.receive(sequence[1]); await settled(client);
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
  expect((await storage.load("next"))?.entities ?? {}).toEqual({});
  expect([...(client as any).replicaSubscriptions.values()][0].snapshotPages).toBeUndefined();
});


it("can disable paging capability for rollout rollback", async () => {
  vi.stubGlobal("WebSocket", Socket);
  const client = new GonvexClient("ws://runtime.test/ws", { token: "session", replicaSnapshotPages: false });
  clients.push(client); client.connect();
  const socket = Socket.instances.at(-1)!; socket.open();
  expect(socket.sent.find(message => message.type === "auth").capabilities.replicaSnapshotPages).toBeUndefined();
});


it("reopens on missing pages, early ready, or malformed cursor without advancing checkpoints", async () => {
  for (const kind of ["gap", "ready", "cursor"]) {
    const { client, socket, open } = await setup();
    const sequence = await pages([{ id: "a", value: 1 }], open.id);
    socket.receive(sequence[0]); await settled(client);
    if (kind === "gap") socket.receive({ ...sequence[1], sequence: 2 });
    if (kind === "ready") socket.receive({ type: "replica.ready", id: open.id, cursor: base.cursor, digest: sequence[1]!.digest });
    if (kind === "cursor") socket.receive({ ...sequence[1], cursor: null });
    await settled(client);
    expect(socket.readyState).toBe(3);
    expect(client.localReplica.snapshot().cursor).toBeUndefined();
    expect(client.localReplica.entityRows("tasks")).toEqual([]);
    client.close();
  }
});


it("keeps optimistic reducer layers visible over atomically activated confirmed rows", async () => {
  const { client, socket, open } = await setup();
  socket.receive({ type: "replica.snapshot", id: open.id, key: "id", cursor: { epoch: "e", revision: 1 }, result: [{ id: "a", value: 0 }] });
  await settled(client);
  const replica = (client as any).replica;
  replica.applyOptimistic("offline-command", [{ entity: "tasks", rowId: "a", op: "patch", fields: { value: 5 } }]);
  const subscription = [...(client as any).replicaSubscriptions.values()][0] as any;
  subscription.opening = true;
  const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: 2 }], open.id);
  socket.receive(sequence[0]); await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 5 });
  expect(client.localReplica.entity("tasks", "b")).toBeUndefined();
  socket.receive(sequence[1]); await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 5 });
  expect(replica.committedWindowRows(subscription.key)).toEqual([{ id: "a", value: 1 }, { id: "b", value: 2 }]);
  expect(replica.hasPendingCommand("offline-command")).toBe(true);
});


it("rolls back rows, membership and checkpoint together if final IndexedDB activation aborts", async () => {
  const previousIDB = Dexie.dependencies.indexedDB;
  const previousRange = Dexie.dependencies.IDBKeyRange;
  vi.stubGlobal("indexedDB", new IDBFactory()); vi.stubGlobal("IDBKeyRange", IDBKeyRange);
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = IDBKeyRange;
  const name = `snapshot-pages-${Math.random()}`;
  const storage = new IndexedDBLocalReplicaStorage(name);
  const reader = new IndexedDBLocalReplicaStorage(name);
  let client: GonvexClient | undefined;
  try {
    const setupResult = await setup({ replicaSnapshotPages: 1 }, storage);
    client = setupResult.client;
    const { socket, open } = setupResult;
    socket.receive({ type: "replica.snapshot", id: open.id, key: "id", cursor: { epoch: "e", revision: 1 }, result: [{ id: "old", value: 0 }] });
    await settled(client);
    const subscription = [...(client as any).replicaSubscriptions.values()][0] as any;
    subscription.opening = true;
    const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: 2 }], open.id);
    socket.receive(sequence[0]); await settled(client);
    const before = await reader.load("visible");
    expect(before!.entities.tasks).toEqual({ old: { id: "old", value: 0 } });
    vi.spyOn((storage as any).database.windows, "bulkPut").mockRejectedValueOnce(new Error("abort-membership-write"));
    socket.receive(sequence[1]); await settled(client);
    expect(socket.readyState).toBe(3);
    const after = await reader.load("visible");
    expect(after!.entities).toEqual(before!.entities);
    expect(after!.liveQueries).toEqual(before!.liveQueries);
    expect(after!.cursor).toEqual(before!.cursor);
    expect(client.localReplica.entityRows("tasks")).toEqual([{ id: "old", value: 0 }]);
  } finally {
    client?.close(); storage.close(); reader.close();
    await Dexie.delete(name);
    Dexie.dependencies.indexedDB = previousIDB;
    Dexie.dependencies.IDBKeyRange = previousRange;
  }
});


it("retains validated server byte budgets and ordering without truncating Unicode rows again", async () => {
  const { client, socket, open, storage } = await setup();
  const rows = [{ id: "b", value: "\u2028" }, { id: "a", value: null }];
  const sequence = await pages(rows, open.id);
  const maxBytes = rows.reduce((sum, row) => sum + new TextEncoder().encode(JSON.stringify(row)).byteLength, 0);
  for (const page of sequence) socket.receive({ ...page, maxBytes, orderBy: "id", orderDirection: "desc" });
  await settled(client);
  expect(client.localReplica.entityRows("tasks")).toHaveLength(2);
  expect(Object.values((await storage.load("visible"))!.liveQueries!)[0]!.ids).toEqual(["b", "a"]);
  expect(Object.values((await storage.load("visible"))!.liveQueries!)[0]!.completeness).toBe("complete");
});

it("reconnects when replaying the buffered feed fails after a staging collection closes", async () => {
  vi.stubGlobal("WebSocket", Socket);
  const client = new GonvexClient("ws://runtime.test/ws", { token: "session", tenant: "tenant", project: "project", localReplica: { storage: new MemoryLocalReplicaStorage() }, replicaSubscriptionRetentionMs: 0 });
  clients.push(client);
  const unsubscribe = client.subscribeReplica(reference, {}, vi.fn());
  const socket = Socket.instances.at(-1)!;
  socket.open();
  socket.receive({ type: "session.ready", replica: directive, capabilities: { replicaSnapshotPages: 1, replicaWatermark: 1 } });
  await new Promise(resolve => setTimeout(resolve, 0));
  const auth = socket.sent.find(message => message.type === "auth");
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "replica.open")).toBe(true));
  const open = socket.sent.find(message => message.type === "replica.open");
  const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: 1 }], open.id);
  socket.receive(sequence[0]); await settled(client);
  socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 11 }, changes: [
    { entity: "other", id: "x", operation: "insert", newValue: { id: "x", value: 2 } },
  ] });
  await settled(client);
  vi.spyOn((client as any).replica, "applyTransaction").mockRejectedValueOnce(new Error("storage failed"));
  (unsubscribe as () => void)();
  await vi.waitFor(() => expect(socket.readyState).toBe(3));
  expect(client.localReplica.entity("other", "x")).toBeUndefined();
});

it("drops frames buffered during staging when the collection is reset", async () => {
  const { client, socket, open } = await setup();
  const sequence = await pages([{ id: "a", value: 1 }, { id: "b", value: 1 }], open.id);
  socket.receive(sequence[0]); await settled(client);
  socket.receive({ type: "replica.delta", id: open.id, cursor: { epoch: "e", revision: 11 }, upserts: [{ id: "stale", value: 2 }], deleted: [] });
  await settled(client);
  socket.receive({ type: "replica.reset", id: open.id, path: "tasks.all", reason: "integrity-mismatch" });
  await settled(client);
  await new Promise(resolve => setTimeout(resolve, 0));
  await settled(client);
  expect(client.localReplica.entity("tasks", "stale")).toBeUndefined();
  expect((client as any).replica.getWindow(open.key ?? "")).toBeUndefined();
  expect((client as any).snapshotDeferredFrames).toEqual([]);
});
