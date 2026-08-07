import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import {
  GonvexClient,
  GonvexClientError,
  type FunctionReference,
  type QueuedMutationOutcome,
} from "./index";

type Listener = (event: { data?: string }) => void;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readonly sent: string[] = [];
  readyState = FakeWebSocket.CONNECTING;
  private readonly listeners = new Map<string, Array<{ listener: Listener; once: boolean }>>();

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: Listener, options?: { once?: boolean }) {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push({ listener, once: Boolean(options?.once) });
    this.listeners.set(type, listeners);
  }

  send(message: string) {
    this.sent.push(message);
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  disconnect() {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  receive(message: unknown) {
    this.emit("message", { data: JSON.stringify(message) });
  }

  private emit(type: string, event: { data?: string }) {
    const listeners = this.listeners.get(type) ?? [];
    this.listeners.set(type, listeners.filter((entry) => !entry.once));
    for (const entry of listeners) entry.listener(event);
  }
}

const syncRef: FunctionReference = { kind: "sync", path: "tasks.list" };
const mutationRef: FunctionReference = { kind: "mutation", path: "tasks.update" };
const directive = {
  protocolVersion: 1 as const,
  scope: "scope-user-a-0000000000000000000000000000000000000000000000000000",
  epoch: "epoch-a-00000000000000000000000000000000000000000000000000000",
  maxAgeMs: 86_400_000,
};

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
  vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
  vi.stubGlobal("indexedDB", indexedDB);
  vi.stubGlobal("IDBKeyRange", IDBKeyRange);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

function latestSocket() {
  const socket = FakeWebSocket.instances.at(-1);
  if (!socket) throw new Error("expected WebSocket instance");
  return socket;
}

function sentMessages(socket = latestSocket()) {
  return socket.sent.map((message) => JSON.parse(message));
}

async function flushAsyncWork() {
  for (let index = 0; index < 10; index += 1) await Promise.resolve();
  await vi.advanceTimersByTimeAsync(0);
}

function openSync(client: GonvexClient, handler: ReturnType<typeof vi.fn>) {
  client.subscribeSync(syncRef, {}, handler);
  const socket = latestSocket();
  socket.open();
  socket.receive({ type: "session.ready", queryCache: directive });
  const open = sentMessages(socket).find((message) => message.type === "sync.open");
  if (!open) throw new Error("expected sync.open frame");
  socket.receive({
    type: "sync.snapshot",
    id: open.id,
    path: syncRef.path,
    result: [{ id: "task-a", title: "Server title" }],
    cursor: { epoch: "sync-a", revision: 1 },
    key: "id",
  });
  return socket;
}

function optimisticPatch(title: string) {
  return [{
    collection: syncRef.path,
    rowId: "task-a",
    op: "patch" as const,
    fields: { title },
  }];
}

describe("optimistic mutation integration", () => {
  it("emits optimistic rows before ack and keeps authoritative rows after settling", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    const socket = openSync(client, handler);

    const mutation = client.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Optimistic title"),
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "sync.snapshot",
      result: [{ id: "task-a", title: "Optimistic title" }],
    });

    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    socket.receive({
      type: "sync.delta",
      id: sentMessages(socket).find((message) => message.type === "sync.open").id,
      path: syncRef.path,
      cursor: { epoch: "sync-a", revision: 2 },
      upserts: [{ id: "task-a", title: "Server confirmed" }],
    });
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });

    await expect(mutation).resolves.toBe("ok");
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server confirmed" }],
    });
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
  });

  it("reverts optimistic rows when the server rejects the mutation", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    const socket = openSync(client, handler);
    const mutation = client.mutation(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Rejected title"),
    });
    const call = sentMessages(socket).find((message) => message.type === "mutation.call");

    socket.receive({ type: "mutation.error", id: call.id, path: mutationRef.path, error: "not allowed" });

    await expect(mutation).rejects.toMatchObject<GonvexClientError>({ code: "server" });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server title" }],
    });
  });

  it("queues a disconnected mutation, drains it on reconnect, and settles the overlay", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    const firstSocket = openSync(client, handler);
    const mutation = client.mutation(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Queued title"),
      offline: "queue",
    });
    const firstCall = sentMessages(firstSocket).find((message) => message.type === "mutation.call");

    firstSocket.disconnect();
    await expect(mutation).resolves.toEqual<QueuedMutationOutcome>({
      status: "queued",
      mutationId: firstCall.id,
    });
    await expect(client.outboxCount()).resolves.toBe(1);
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(true);

    await vi.advanceTimersByTimeAsync(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    await flushAsyncWork();
    const replay = sentMessages(secondSocket).find((message) => message.type === "mutation.call");
    expect(replay).toMatchObject({ id: firstCall.id, path: mutationRef.path, args: { id: "task-a" } });

    secondSocket.receive({ type: "mutation.result", id: replay.id, path: mutationRef.path, result: "ok" });
    await flushAsyncWork();
    await expect(client.outboxCount()).resolves.toBe(0);
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server title" }],
    });
  });

  it("rebuilds pending optimistic state from a persisted outbox after reload", async () => {
    vi.useRealTimers();
    vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
    const databaseName = `gonvex-optimistic-reload-${crypto.randomUUID()}`;
    const firstClient = new GonvexClient("ws://runtime.test/ws", {
      sync: false,
      outbox: { databaseName },
    });
    const firstSocket = openSync(firstClient, vi.fn());
    const queued = firstClient.mutation(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Restored title"),
      offline: "queue",
    });
    firstSocket.disconnect();
    await expect(queued).resolves.toMatchObject({ status: "queued" });
    await expect(firstClient.outboxCount()).resolves.toBe(1);
    firstClient.close();

    const secondClient = new GonvexClient("ws://runtime.test/ws", {
      sync: false,
      outbox: { databaseName },
    });
    await expect(secondClient.outboxCount()).resolves.toBe(1);
    for (let index = 0; index < 10; index += 1) await Promise.resolve();
    expect(secondClient.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(true);

    const handler = vi.fn();
    openSync(secondClient, handler);
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Restored title" }],
    });
  });
});
