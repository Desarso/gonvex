import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConvexReactClient, GonvexClient, type FunctionReference } from "./index";

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

  close() {
    this.readyState = FakeWebSocket.CLOSED;
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  receive(message: unknown) {
    this.emit("message", { data: typeof message === "string" ? message : JSON.stringify(message) });
  }

  private emit(type: string, event: { data?: string }) {
    const listeners = this.listeners.get(type) ?? [];
    this.listeners.set(type, listeners.filter((entry) => !entry.once));
    for (const entry of listeners) entry.listener(event);
  }
}

const ref: FunctionReference = { kind: "query", path: "tasks.list" };

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
  vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
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

describe("GonvexClient", () => {
  it("converts http runtime URLs to websocket URLs for ConvexReactClient compatibility", () => {
    const client = new ConvexReactClient("https://runtime.example.com/");
    client.connect();

    expect(latestSocket().url).toBe("wss://runtime.example.com/ws");
  });

  it("keeps explicit websocket URLs unchanged", () => {
    const client = new ConvexReactClient("ws://localhost:8080/ws");
    client.connect();

    expect(latestSocket().url).toBe("ws://localhost:8080/ws");
  });

  it("reuses an existing connecting socket instead of opening duplicates", () => {
    const client = new GonvexClient("ws://runtime.test/ws");

    client.connect();
    client.connect();

    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it("queues subscription messages until the socket opens", () => {
    const client = new GonvexClient("ws://runtime.test/ws");

    client.subscribeQuery(ref, { status: "open" }, () => undefined);
    const socket = latestSocket();
    expect(socket.sent).toHaveLength(0);

    socket.open();

    expect(sentMessages(socket)).toMatchObject([
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("sends auth before queued messages when the socket opens", () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token", tenant: "tenant-a" });

    client.subscribeQuery(ref, { status: "open" }, () => undefined);
    const socket = latestSocket();
    socket.open();

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "session-token", tenant: "tenant-a" },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: { userId: "user-a", tenantId: "tenant-a" } });

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "session-token", tenant: "tenant-a" },
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("queues subscription messages while an auth update is in flight", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.setAuth({ token: "next-token", tenant: "tenant-b" });
    client.subscribeQuery(ref, { status: "open" }, () => undefined);

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "next-token", tenant: "tenant-b" },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: { userId: "user-b", tenantId: "tenant-b" } });

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "next-token", tenant: "tenant-b" },
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("sends auth updates on an open socket", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.setAuth({ token: "next-token", tenant: "tenant-b" });

    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "auth", token: "next-token", tenant: "tenant-b" });
  });

  it("routes query subscription results to the matching handler", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(handler).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"] });
  });

  it("coalesces identical query subscriptions and fans out results", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, first);
    client.subscribeQuery(ref, { status: "open" }, second);
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    expect(messages.filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    const [{ id }] = messages;
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(first).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"] });
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"] });
  });

  it("replays the latest result to a late joiner without resubscribing", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"], reason: "initial" });
    expect(first).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"], reason: "initial" });

    // A component mounting after the initial result must still receive the cached value;
    // the coalesced subscription only gets `initial` once from the server.
    client.subscribeQuery(ref, { status: "open" }, second);
    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    expect(second).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"], reason: "initial" });
  });

  it("does not replay a stale result to a late joiner that unsubscribes synchronously", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    const unsubscribeSecond = client.subscribeQuery(ref, { status: "open" }, second);
    unsubscribeSecond();
    await Promise.resolve();
    expect(second).not.toHaveBeenCalled();
  });

  it("keeps a coalesced query subscribed until the last listener leaves", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    const unsubscribeFirst = client.subscribeQuery(ref, { status: "open" }, first);
    const unsubscribeSecond = client.subscribeQuery(ref, { status: "open" }, second);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    unsubscribeFirst();
    vi.advanceTimersByTime(300);
    expect(sentMessages(socket).filter((message) => message.type === "query.unsubscribe")).toHaveLength(0);

    socket.receive({ type: "query.result", id, result: ["task"] });
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);

    unsubscribeSecond();
    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("keeps network telemetry disabled unless explicitly enabled", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(sentMessages(socket).some((message) => message.type === "telemetry.event")).toBe(false);
  });

  it("supports Convex-compatible watchQuery updates", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const watch = client.watchQuery<string[]>(ref, { status: "open" });
    const onUpdate = vi.fn();
    const offUpdate = watch.onUpdate(onUpdate);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(onUpdate).toHaveBeenCalledTimes(1);
    expect(watch.localQueryResult()).toEqual(["task"]);

    offUpdate();
    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("throws the latest watchQuery error from localQueryResult", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const watch = client.watchQuery(ref);
    const onUpdate = vi.fn();
    watch.onUpdate(onUpdate);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "watch failed" });

    expect(onUpdate).toHaveBeenCalledTimes(1);
    expect(() => watch.localQueryResult()).toThrow("watch failed");
  });

  it("ignores invalid JSON messages from the socket", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive("{not json");

    expect(handler).not.toHaveBeenCalled();
  });

  it("sends unsubscribe after a short grace period and removes the handler after in-flight results", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    const unsubscribe = client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    unsubscribe();
    expect(sentMessages(socket).at(-1)).not.toMatchObject({ type: "query.unsubscribe", id });

    socket.receive({ type: "query.result", id, result: "in-flight" });
    expect(handler).toHaveBeenCalledTimes(0);

    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });

    vi.advanceTimersByTime(500);
    socket.receive({ type: "query.result", id, result: "late" });
    expect(handler).toHaveBeenCalledTimes(0);
  });

  it("resolves one-shot queries and unsubscribes after the first result", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref, { status: "open" });
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.result", id, result: { count: 2 } });

    await expect(promise).resolves.toEqual({ count: 2 });
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("rejects one-shot queries on query errors", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "boom" });

    await expect(promise).rejects.toThrow("boom");
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("resolves mutations and actions from matching response types", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "Ship" });
    const action = client.action({ kind: "action", path: "jobs.run" }, { id: "job_1" });
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    expect(messages[0]).toMatchObject({ type: "mutation.call", path: "tasks.create", args: { title: "Ship" } });
    expect(messages[1]).toMatchObject({ type: "action.call", path: "jobs.run", args: { id: "job_1" } });

    socket.receive({ type: "mutation.result", id: messages[0].id, result: { id: "task_1" } });
    socket.receive({ type: "action.result", id: messages[1].id, result: "queued" });

    await expect(mutation).resolves.toEqual({ id: "task_1" });
    await expect(action).resolves.toBe("queued");
  });

  it("reports browser and device telemetry for received mutation results", async () => {
    vi.stubGlobal("performance", { timeOrigin: 1_000, now: vi.fn(() => 25.5) });
    vi.stubGlobal("navigator", {
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
      platform: "Win32",
      language: "en-US",
      hardwareConcurrency: 12,
      maxTouchPoints: 0,
    });
    vi.stubGlobal("innerWidth", 1440);
    vi.stubGlobal("innerHeight", 900);
    const client = new GonvexClient("ws://runtime.test/ws", { telemetry: true });
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "Ship" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    socket.receive({
      type: "mutation.result",
      id: call.id,
      result: { id: "task_1" },
      trace: {
        clientSentAtMs: call.trace.clientSentAtMs,
        serverMutationCommittedAtMs: 1_010.25,
        serverCompletedAtMs: 1_012.5,
      },
    });

    await expect(mutation).resolves.toEqual({ id: "task_1" });
    const telemetry = sentMessages(socket).find((message) => message.type === "telemetry.event");
    expect(telemetry).toMatchObject({
      kind: "mutation",
      path: "tasks.create",
      outcome: "ok",
      clientReceivedAtMs: 1_025.5,
      device: {
        browserName: "Chrome",
        browserVersion: "126.0.0.0",
        deviceType: "desktop",
        platform: "Win32",
        viewportWidth: 1440,
        viewportHeight: 900,
      },
    });
  });

  it("rejects mutations and actions from matching error response types", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const action = client.action({ kind: "action", path: "jobs.run" });
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    socket.receive({ type: "mutation.error", id: messages[0].id, error: "mutation failed" });
    socket.receive({ type: "action.error", id: messages[1].id, error: "action failed" });

    await expect(mutation).rejects.toThrow("mutation failed");
    await expect(action).rejects.toThrow("action failed");
  });

  it("drops handlers and closes the socket when closed", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    client.close();
    socket.receive({ type: "query.result", id, result: "ignored" });

    expect(socket.readyState).toBe(FakeWebSocket.CLOSED);
    expect(handler).not.toHaveBeenCalled();
  });
});
