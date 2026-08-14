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
const entitySyncRef: FunctionReference = {
  kind: "sync",
  path: "tasks.byWorkspaceSync",
  optimistic: { projection: { entity: "tasks", key: "id", resultPath: [] } },
};
const pageQueryRef: FunctionReference = {
  kind: "query",
  path: "tasks.byWorkspace",
  optimistic: { projection: { entity: "tasks", key: "id", resultPath: ["page"] } },
};
const objectQueryRef: FunctionReference = {
  kind: "query",
  path: "tasks.get",
  optimistic: { projection: { entity: "tasks", key: "id", resultPath: [] } },
};
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

async function waitForSentMessage(socket: FakeWebSocket, type: string) {
  for (let index = 0; index < 100; index += 1) {
    const message = sentMessages(socket).find((candidate) => candidate.type === type);
    if (message) return message;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error(`expected ${type} frame`);
}

async function waitForOutboxCount(client: GonvexClient, expected: number) {
  for (let index = 0; index < 100; index += 1) {
    if (await client.outboxCount() === expected) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  expect(await client.outboxCount()).toBe(expected);
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

function optimisticEntityPatch(title: string) {
  return [{
    entity: "tasks",
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
    await flushAsyncWork();
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "sync.snapshot",
      result: [{ id: "task-a", title: "Optimistic title" }],
    });

    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    const callsBeforeDelta = handler.mock.calls.length;
    socket.receive({
      type: "sync.delta",
      id: sentMessages(socket).find((message) => message.type === "sync.open").id,
      path: syncRef.path,
      cursor: { epoch: "sync-a", revision: 2 },
      upserts: [{ id: "task-a", title: "Server confirmed" }],
      mutationIds: [call.id],
    });
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });

    await expect(mutation).resolves.toBe("ok");
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server confirmed" }],
    });
    const deltaTitles = handler.mock.calls.slice(callsBeforeDelta).map(([message]) => (
      message.result?.[0]?.title
    ));
    expect(deltaTitles).not.toContain("Server title");
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
  });

  it("does not reveal stale sync rows when the mutation result arrives before its delta", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    const socket = openSync(client, handler);

    const mutation = client.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Optimistic title"),
    });
    await flushAsyncWork();
    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });

    await expect(mutation).resolves.toBe("ok");
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      type: "sync.snapshot",
      result: [{ id: "task-a", title: "Optimistic title" }],
    });

    const callsBeforeAcceptedDelta = handler.mock.calls.length;
    socket.receive({
      type: "sync.delta",
      id: sentMessages(socket).find((message) => message.type === "sync.open").id,
      path: syncRef.path,
      cursor: { epoch: "sync-a", revision: 2 },
      upserts: [{ id: "task-a", title: "Server confirmed" }],
      mutationIds: [call.id],
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server confirmed" }],
    });
    const acceptedDeltaTitles = handler.mock.calls.slice(callsBeforeAcceptedDelta).map(([message]) => (
      message.result?.[0]?.title
    ));
    expect(acceptedDeltaTitles).not.toContain("Server title");
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
  });

  it("materializes one entity patch into paginated query subscriptions until reconciliation", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    client.subscribeQuery(pageQueryRef, { workspaceId: "workspace-a" }, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", queryCache: directive });
    const subscribe = sentMessages(socket).find((message) => message.type === "query.subscribe");
    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: pageQueryRef.path,
      result: { page: [{ id: "task-a", title: "Server title" }], isDone: true },
      subscriptionRevision: { epoch: "query-a", sequence: 1 },
    });

    const mutation = client.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticEntityPatch("Optimistic title"),
    });
    await flushAsyncWork();
    expect(client.optimisticOverlay.pendingFor("tasks", "task-a")).toBe(true);
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { page: [{ id: "task-a", title: "Optimistic title" }] },
    });

    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });
    await expect(mutation).resolves.toBe("ok");
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { page: [{ id: "task-a", title: "Optimistic title" }] },
    });

    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: pageQueryRef.path,
      result: { page: [{ id: "task-a", title: "Server confirmed" }], isDone: true },
      subscriptionRevision: { epoch: "query-a", sequence: 2 },
      mutationIds: [call.id],
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { page: [{ id: "task-a", title: "Server confirmed" }] },
    });
    expect(client.optimisticOverlay.pendingFor("tasks", "task-a")).toBe(false);
  });

  it("materializes an entity patch into a single-object query", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    client.subscribeQuery(objectQueryRef, { taskId: "task-a" }, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", queryCache: directive });
    const subscribe = sentMessages(socket).find((message) => message.type === "query.subscribe");
    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: objectQueryRef.path,
      result: { id: "task-a", title: "Server title" },
      subscriptionRevision: { epoch: "query-object", sequence: 1 },
    });

    const mutation = client.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticEntityPatch("Optimistic title"),
    });
    await flushAsyncWork();
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { id: "task-a", title: "Optimistic title" },
    });

    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });
    await expect(mutation).resolves.toBe("ok");
    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: objectQueryRef.path,
      result: { id: "task-a", title: "Server confirmed" },
      subscriptionRevision: { epoch: "query-object", sequence: 2 },
      mutationIds: [call.id],
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { id: "task-a", title: "Server confirmed" },
    });
  });

  it("keeps an accepted overlay when the first query snapshot arrives after the RPC result", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    client.subscribeQuery(pageQueryRef, { workspaceId: "workspace-a" }, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", queryCache: directive });
    const subscribe = sentMessages(socket).find((message) => message.type === "query.subscribe");

    const mutation = client.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticEntityPatch("Optimistic title"),
    });
    await flushAsyncWork();
    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });
    await expect(mutation).resolves.toBe("ok");

    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: pageQueryRef.path,
      result: { page: [{ id: "task-a", title: "Stale initial title" }], isDone: true },
      subscriptionRevision: { epoch: "query-late", sequence: 1 },
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { page: [{ id: "task-a", title: "Optimistic title" }] },
    });

    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: pageQueryRef.path,
      result: { page: [{ id: "task-a", title: "Server confirmed" }], isDone: true },
      subscriptionRevision: { epoch: "query-late", sequence: 2 },
      mutationIds: [call.id],
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: { page: [{ id: "task-a", title: "Server confirmed" }] },
    });
  });

  it("keeps an accepted overlay when the first sync snapshot arrives after the RPC result", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    client.subscribeSync(entitySyncRef, { workspaceId: "workspace-a" }, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", queryCache: directive });
    const open = sentMessages(socket).find((message) => message.type === "sync.open");

    const mutation = client.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticEntityPatch("Optimistic title"),
    });
    await flushAsyncWork();
    const call = sentMessages(socket).find((message) => message.type === "mutation.call");
    socket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });
    await expect(mutation).resolves.toBe("ok");

    socket.receive({
      type: "sync.snapshot",
      id: open.id,
      path: entitySyncRef.path,
      result: [{ id: "task-a", title: "Stale initial title" }],
      cursor: { epoch: "sync-late", revision: 1 },
      key: "id",
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Optimistic title" }],
    });

    socket.receive({
      type: "sync.delta",
      id: open.id,
      path: entitySyncRef.path,
      cursor: { epoch: "sync-late", revision: 2 },
      upserts: [{ id: "task-a", title: "Server confirmed" }],
      mutationIds: [call.id],
    });
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server confirmed" }],
    });
  });

  it("reverts optimistic rows when the server rejects the mutation", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const handler = vi.fn();
    const socket = openSync(client, handler);
    const mutation = client.mutation(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Rejected title"),
    });
    await flushAsyncWork();
    const call = sentMessages(socket).find((message) => message.type === "mutation.call");

    socket.receive({ type: "mutation.error", id: call.id, path: mutationRef.path, error: "not allowed" });

    await expect(mutation).rejects.toMatchObject<GonvexClientError>({ code: "server" });
    await expect(client.outboxCount()).resolves.toBe(0);
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
    await flushAsyncWork();
    const firstCall = sentMessages(firstSocket).find((message) => message.type === "mutation.call");

    firstSocket.disconnect();
    await expect(mutation).resolves.toEqual<QueuedMutationOutcome>({
      status: "queued",
      mutationId: firstCall.id,
    });
    await expect(client.outboxCount()).resolves.toBe(1);
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(true);

    await vi.advanceTimersByTimeAsync(2_500);
    const secondSocket = latestSocket();
    secondSocket.open();
    await flushAsyncWork();
    const replay = sentMessages(secondSocket).find((message) => message.type === "mutation.call");
    expect(replay).toMatchObject({ id: firstCall.id, path: mutationRef.path, args: { id: "task-a" } });

    secondSocket.receive({ type: "mutation.result", id: replay.id, path: mutationRef.path, result: "ok" });
    secondSocket.receive({
      type: "sync.delta",
      id: sentMessages(firstSocket).find((message) => message.type === "sync.open").id,
      path: syncRef.path,
      cursor: { epoch: "sync-a", revision: 2 },
      upserts: [{ id: "task-a", title: "Server confirmed" }],
      mutationIds: [replay.id],
    });
    await flushAsyncWork();
    await expect(client.outboxCount()).resolves.toBe(0);
    expect(client.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Server confirmed" }],
    });
  });

  it("queues and acknowledges offline mutations without optimistic UI metadata", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false, outbox: { enabled: false } });
    const mutation = client.mutation(mutationRef, { id: "task-a" }, { offline: "queue" });
    await flushAsyncWork();
    const firstSocket = latestSocket();
    firstSocket.disconnect();
    await expect(mutation).resolves.toMatchObject({ status: "queued" });
    await expect(client.outboxCount()).resolves.toBe(1);

    await vi.advanceTimersByTimeAsync(2_500);
    const secondSocket = latestSocket();
    secondSocket.open();
    await flushAsyncWork();
    const replay = sentMessages(secondSocket).find((message) => message.type === "mutation.call");
    secondSocket.receive({ type: "mutation.result", id: replay.id, path: mutationRef.path, result: "ok" });
    await flushAsyncWork();

    await expect(client.outboxCount()).resolves.toBe(0);
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
    await waitForSentMessage(firstSocket, "mutation.call");
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

  it("restores durable optimistic writes only for their authenticated identity", async () => {
    vi.useRealTimers();
    vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
    const databaseName = `gonvex-optimistic-scope-${crypto.randomUUID()}`;
    const clientOptions = (sub: string) => ({
      project: "project-a",
      tenant: "tenant-a",
      identity: { sub, iss: "https://identity.test" },
      sync: false as const,
      outbox: { databaseName },
    });

    const firstClient = new GonvexClient("ws://runtime.test/ws", clientOptions("user-a"));
    const queued = firstClient.mutation(mutationRef, { id: "task-a" }, {
      optimistic: optimisticEntityPatch("User A title"),
      offline: "queue",
    });
    await waitForOutboxCount(firstClient, 1);
    latestSocket().disconnect();
    await expect(queued).resolves.toMatchObject({ status: "queued" });
    firstClient.close();

    const otherDeploymentClient = new GonvexClient(
      "ws://other-runtime.test/ws",
      clientOptions("user-a"),
    );
    await expect(otherDeploymentClient.outboxCount()).resolves.toBe(0);
    otherDeploymentClient.close();

    const otherClient = new GonvexClient("ws://runtime.test/ws", clientOptions("user-b"));
    await expect(otherClient.outboxCount()).resolves.toBe(0);
    expect(otherClient.optimisticOverlay.pendingFor("tasks", "task-a")).toBe(false);
    otherClient.close();

    const restoredClient = new GonvexClient("ws://runtime.test/ws", clientOptions("user-a"));
    await expect(restoredClient.outboxCount()).resolves.toBe(1);
    expect(restoredClient.optimisticOverlay.pendingFor("tasks", "task-a")).toBe(true);
    restoredClient.close();
  });

  it("persists an online optimistic mutation before transport and restores it until reconciliation", async () => {
    vi.useRealTimers();
    vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
    const databaseName = `gonvex-optimistic-online-${crypto.randomUUID()}`;
    const firstClient = new GonvexClient("ws://runtime.test/ws", {
      sync: false,
      outbox: { databaseName },
    });
    const firstSocket = openSync(firstClient, vi.fn());
    const mutation = firstClient.mutation<string>(mutationRef, { id: "task-a" }, {
      optimistic: optimisticPatch("Durable online title"),
    });
    const call = await waitForSentMessage(firstSocket, "mutation.call");
    firstSocket.receive({ type: "mutation.result", id: call.id, path: mutationRef.path, result: "ok" });
    await expect(mutation).resolves.toBe("ok");
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
    const secondSocket = openSync(secondClient, handler);
    expect(handler.mock.calls.at(-1)?.[0]).toMatchObject({
      result: [{ id: "task-a", title: "Durable online title" }],
    });
    secondSocket.receive({
      type: "sync.delta",
      id: sentMessages(secondSocket).find((message) => message.type === "sync.open").id,
      path: syncRef.path,
      cursor: { epoch: "sync-a", revision: 2 },
      upserts: [{ id: "task-a", title: "Server confirmed" }],
      mutationIds: [call.id],
    });
    expect(secondClient.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
    await waitForOutboxCount(secondClient, 0);
    expect(secondClient.optimisticOverlay.pendingFor(syncRef.path, "task-a")).toBe(false);
  });
});
