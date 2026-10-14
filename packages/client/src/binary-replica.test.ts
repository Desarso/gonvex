import { afterEach, expect, it, vi } from "vitest";
import { BinaryReplicaCodec, BINARY_REPLICA_MAX_BYTES, type ServerMessage } from "@gonvex/protocol";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference } from "./index";
import { replicaAggregateDigest, replicaRowsHashes } from "./replica-integrity";

const cursor = { epoch: "epoch-é😀", revision: Number.MAX_SAFE_INTEGER };
const delta: ServerMessage = { type: "replica.delta", id: "open", cursor, upserts: [
  { id: "a", nullable: null, nested: { unicode: "你好", empty: [], bool: false, number: 1.25 } },
  { id: "b", absent: "present", __authority: { nullable: { revision: 1 } } },
], deleted: [], originCommandIds: ["origin"] };

it("round trips nested JSON, absent/null, authority, ordering, origin IDs and exact revisions", () => {
  const encoder = new BinaryReplicaCodec();
  const decoder = new BinaryReplicaCodec();
  for (const frame of [delta, { type: "replica.transaction", cursor, originCommandId: "command", changes: [
    { entity: "tasks", id: "a", operation: "update", oldValue: null, newValue: { id: "a" }, changedColumns: [] },
  ] } as ServerMessage, delta]) {
    expect(decoder.decode(encoder.encode(frame))).toEqual(frame);
  }
  const decoded = new BinaryReplicaCodec().decode(new BinaryReplicaCodec().encode(delta)) as typeof delta;
  expect(Object.hasOwn(decoded.upserts![0]!, "absent")).toBe(false);
  expect(Object.hasOwn(decoded.upserts![0]!, "nullable")).toBe(true);
});

it("rejects truncation, oversized frames, invalid dictionary state, unsafe revisions and excessive depth", () => {
  const bytes = new BinaryReplicaCodec().encode(delta);
  const decoder = new BinaryReplicaCodec();
  expect(() => decoder.decode(bytes.slice(0, -1))).toThrow();
  expect(decoder.decode(bytes)).toEqual(delta);
  expect(() => decoder.decode(bytes)).toThrow();
  expect(() => new BinaryReplicaCodec().decode(Uint8Array.from([...Array(200).fill(0x91), 0xc0]))).toThrow();
  expect(() => new BinaryReplicaCodec().decode(Uint8Array.from([0xdd, 0xff, 0xff, 0xff, 0xff]))).toThrow();
  expect(() => decoder.decode(new Uint8Array(BINARY_REPLICA_MAX_BYTES + 1))).toThrow();
  expect(() => new BinaryReplicaCodec().decode(new BinaryReplicaCodec().encode({ ...delta, cursor: { ...cursor, revision: 2 ** 53 } }))).toThrow();
  let value: any = null;
  for (let i = 0; i < 70; i++) value = [value];
  expect(() => new BinaryReplicaCodec().encode({ ...delta, upserts: [value] })).toThrow();
});

class Socket {
  static CONNECTING = 0;
  static OPEN = 1;
  static instances: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.instances.push(this); }
  addEventListener(type: string, handler: (event: any) => void) { this.listeners.set(type, [...this.listeners.get(type) ?? [], handler]); }
  send(message: string) { this.sent.push(JSON.parse(message)); }
  emit(type: string, event = {}) { for (const handler of this.listeners.get(type) ?? []) handler(event); }
  open() { this.readyState = 1; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(message: unknown) { this.emit("message", { data: JSON.stringify(message) }); }
  binary(bytes: Uint8Array) { this.emit("message", { data: bytes.slice().buffer }); }
}
const clients: GonvexClient[] = [];
afterEach(() => { for (const client of clients.splice(0)) client.close(); vi.unstubAllGlobals(); Socket.instances = []; });
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "e" };
const reference: FunctionReference = { kind: "query", path: "tasks.all", delivery: "replica", replica: { table: "tasks", key: "id", columns: ["id", "value"], maxRows: 20_000 } };
async function setup(binaryReplica: boolean, accepted: boolean) {
  vi.stubGlobal("WebSocket", Socket);
  const client = new GonvexClient("ws://runtime.test/ws", { binaryReplica, token: "session", tenant: "tenant", project: "project", localReplica: { storage: new MemoryLocalReplicaStorage() }, replicaSubscriptionRetentionMs: 0 });
  clients.push(client);
  client.subscribeReplica(reference, {}, () => {});
  const socket = Socket.instances.at(-1)!;
  socket.open(); socket.receive({ type: "session.ready", replica: directive, capabilities: { replicaSnapshotPages: 1, replicaWatermark: 1 } });
  await new Promise(resolve => setTimeout(resolve, 0));
  const auth = socket.sent.find(message => message.type === "auth");
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive, ...(accepted ? { binaryReplica: 1 } : {}) } });
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "replica.open")).toBe(true));
  return { client, socket, auth, id: socket.sent.find(message => message.type === "replica.open").id };
}
async function settled(client: GonvexClient) { await (client as any).replicaFrames; }

it.each([[false, false], [false, true], [true, false], [true, true]])("negotiates client=%s runtime=%s while retaining JSON", async (enabled, accepted) => {
  const { client, socket, auth, id } = await setup(enabled, accepted);
  expect(auth.capabilities.binaryReplica).toBe(enabled ? 1 : undefined);
  const frame: ServerMessage = { type: "replica.snapshot", id, key: "id", cursor: { epoch: "e", revision: 1 }, result: [{ id: "a", value: 1 }] };
  if (enabled && accepted) socket.binary(new BinaryReplicaCodec().encode(frame));
  else socket.receive(frame);
  await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 1 });
});

it("checks snapshot and delta integrity across an ordered mixed stream", async () => {
  const { client, socket, id } = await setup(true, true);
  const encoder = new BinaryReplicaCodec();
  const rows = [{ id: "a", value: 1 }];
  const hashes = await replicaRowsHashes(rows, "id");
  socket.binary(encoder.encode({ type: "replica.snapshot", id, key: "id", cursor: { epoch: "e", revision: 1 }, result: rows, hashes, digest: replicaAggregateDigest(hashes) }));
  socket.receive({ type: "replica.watermark", revision: 1 });
  await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toEqual(rows[0]);
  socket.binary(encoder.encode({ type: "replica.transaction", cursor: { epoch: "e", revision: 2 }, originCommandId: "command", changes: [
    { entity: "tasks", id: "a", operation: "update", newValue: { id: "a", value: 2 } },
    { entity: "other", id: "x", operation: "insert", newValue: { id: "x", value: 2 } },
  ] }));
  socket.receive({ type: "replica.watermark", revision: 3 });
  socket.binary(encoder.encode({ type: "replica.delta", id, cursor: { epoch: "e", revision: 3 }, upserts: [{ id: "a", value: 3 }], deleted: [] }));
  const currentHashes = await replicaRowsHashes([{ id: "a", value: 3 }], "id");
  socket.receive({ type: "replica.ready", id, cursor: { epoch: "e", revision: 3 }, digest: replicaAggregateDigest(currentHashes) });
  await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 3 });
  expect(client.localReplica.entity("other", "x")).toEqual({ id: "x", value: 2 });
  socket.binary(encoder.encode({ type: "replica.delta", id, cursor: { epoch: "e", revision: 4 }, upserts: [{ id: "a", value: 4 }], hashes: { a: "wrong" }, digest: "wrong" }));
  await settled(client);
  socket.receive({ type: "replica.ready", id, cursor: { epoch: "e", revision: 4 }, digest: "wrong" });
  await settled(client);
  await vi.waitFor(() => expect(socket.sent.filter(frame => frame.type === "replica.open")).toHaveLength(2));
});

it("closes unexpected or malformed binary before applying it", async () => {
  for (const accepted of [false, true]) {
    const { client, socket } = await setup(true, accepted);
    socket.binary(new Uint8Array([0xff]));
    await settled(client);
    expect(socket.readyState).toBe(3);
    expect(client.localReplica.entityRows("tasks")).toEqual([]);
    socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 1 }, changes: [{ entity: "tasks", id: "late", operation: "insert", newValue: { id: "late", value: 1 } }] });
    await settled(client);
    expect(client.localReplica.entityRows("tasks")).toEqual([]);
    client.close();
  }
});

it("preserves prototype-named JSON keys without changing the decoded object prototype", () => {
  const row = JSON.parse('{"id":"a","__proto__":{"polluted":true},"constructor":null}');
  const frame: ServerMessage = { ...delta, upserts: [row] };
  const decoded = new BinaryReplicaCodec().decode(new BinaryReplicaCodec().encode(frame)) as typeof delta;
  expect(decoded).toEqual(frame);
  expect(Object.getPrototypeOf(decoded.upserts![0])).toBe(Object.prototype);
  expect(({} as any).polluted).toBeUndefined();
});

it("checks paged snapshot integrity after binary decoding and discards staging on corruption", async () => {
  const { client, socket, id } = await setup(true, true);
  const encoder = new BinaryReplicaCodec();
  socket.binary(encoder.encode({ type: "replica.snapshotPage", snapshotId: "s", scope: "tenant", visibilityScope: "visible", sequence: 0, last: true, id, key: "id", cursor: { epoch: "e", revision: 1 }, result: [{ id: "a", value: 1 }], digest: "wrong" }));
  await settled(client);
  expect(socket.readyState).toBe(3);
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
});

it("atomically activates valid binary snapshot pages only after their final digest", async () => {
  const { client, socket, id } = await setup(true, true);
  const encoder = new BinaryReplicaCodec();
  const rows = [{ id: "a", value: 1 }, { id: "b", value: null }];
  const page = { type: "replica.snapshotPage" as const, snapshotId: "s", scope: "tenant", visibilityScope: "visible", sequence: 0, last: false, id, key: "id", cursor: { epoch: "e", revision: 1 }, result: rows.slice(0, 1) };
  socket.binary(encoder.encode(page));
  await settled(client);
  expect(client.localReplica.entityRows("tasks")).toEqual([]);
  const digest = replicaAggregateDigest(await replicaRowsHashes(rows, "id"));
  socket.binary(encoder.encode({ ...page, sequence: 1, last: true, result: rows.slice(1), digest }));
  await settled(client);
  expect(client.localReplica.entityRows("tasks")).toEqual(rows);
});
