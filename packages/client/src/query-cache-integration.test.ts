import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  GonvexClient,
  type FunctionReference,
  type QueryCacheDirective,
  type QueryCacheLookup,
  type QueryCacheStore,
  type QueryCacheWrite,
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

  close() {
    this.readyState = FakeWebSocket.CLOSED;
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
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

class FakeQueryCacheStore implements QueryCacheStore {
  lookup: QueryCacheLookup | undefined;
  readGate: Promise<void> | undefined;
  readonly reads: Array<{ scope: string; path: string; args: unknown }> = [];
  readonly writes: QueryCacheWrite[] = [];
  readonly deletes: Array<{ scope: string; path: string; args: unknown }> = [];

  async read(scope: string, path: string, args: unknown) {
    this.reads.push({ scope, path, args });
    await this.readGate;
    return this.lookup;
  }

  async write(value: QueryCacheWrite) {
    this.writes.push(value);
    return "written" as const;
  }

  async delete(scope: string, path: string, args: unknown) {
    this.deletes.push({ scope, path, args });
  }

  async clear() {}

  status() {
    return { enabled: true, readsEnabled: true, writesEnabled: true };
  }

  close() {}
}

const ref: FunctionReference = { kind: "query", path: "tasks.list" };
const directive: QueryCacheDirective = {
  protocolVersion: 1,
  scope: "scope-user-a-0000000000000000000000000000000000000000000000000000",
  epoch: "epoch-a-00000000000000000000000000000000000000000000000000000",
  maxAgeMs: 86_400_000,
};

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.stubGlobal("WebSocket", FakeWebSocket);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function socket() {
  const value = FakeWebSocket.instances.at(-1);
  if (!value) throw new Error("expected WebSocket instance");
  return value;
}

function sentMessages(value = socket()) {
  return value.sent.map((message) => JSON.parse(message));
}

async function flushAsyncWork() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("persistent query cache integration", () => {
  it("replays a scoped snapshot and then replaces and persists it from the server", async () => {
    const store = new FakeQueryCacheStore();
    store.lookup = {
      result: [{ id: "cached" }],
      revision: "0000000000001:00000000000000000001",
      writtenAt: Date.now() - 500,
      ageMs: 500,
    };
    const client = new GonvexClient("ws://runtime.test/ws", { queryCache: { store } });
    const handler = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, handler);
    socket().open();
    socket().receive({ type: "session.ready", project: "project-a", tenant: "tenant-a", queryCache: directive });
    await flushAsyncWork();

    expect(handler).toHaveBeenCalledWith(expect.objectContaining({
      type: "query.result",
      result: [{ id: "cached" }],
      reason: "initial",
    }));
    expect(sentMessages()).toContainEqual(expect.objectContaining({
      type: "query.subscribe",
      path: "tasks.list",
      args: { status: "open" },
    }));

    const subscribe = sentMessages().find((message) => message.type === "query.subscribe");
    socket().receive({
      type: "query.result",
      id: subscribe.id,
      path: "tasks.list",
      result: [{ id: "fresh" }],
      reason: "initial",
      cacheScope: directive.scope,
      cacheRevision: "0000000000001:00000000000000000002",
    });
    await flushAsyncWork();

    expect(handler).toHaveBeenLastCalledWith(expect.objectContaining({ result: [{ id: "fresh" }] }));
    expect(store.writes).toContainEqual(expect.objectContaining({
      scope: directive.scope,
      path: "tasks.list",
      args: { status: "open" },
      result: [{ id: "fresh" }],
      revision: "0000000000001:00000000000000000002",
    }));
  });

  it("does not emit a late cache read after the server has already won", async () => {
    let releaseRead!: () => void;
    const store = new FakeQueryCacheStore();
    store.lookup = {
      result: [{ id: "stale" }],
      revision: "0000000000001:00000000000000000001",
      writtenAt: Date.now() - 500,
      ageMs: 500,
    };
    store.readGate = new Promise<void>((resolve) => {
      releaseRead = resolve;
    });
    const client = new GonvexClient("ws://runtime.test/ws", { queryCache: { store } });
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await Promise.resolve();
    const subscribe = sentMessages().find((message) => message.type === "query.subscribe");
    socket().receive({
      type: "query.result",
      id: subscribe.id,
      result: [{ id: "fresh" }],
      cacheScope: directive.scope,
      cacheRevision: "0000000000001:00000000000000000002",
    });
    releaseRead();
    await flushAsyncWork();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ result: [{ id: "fresh" }] }));
  });

  it("stays server-only when the runtime does not advertise cache support", async () => {
    const store = new FakeQueryCacheStore();
    store.lookup = {
      result: [{ id: "cached" }],
      revision: "1",
      writtenAt: Date.now(),
      ageMs: 0,
    };
    const client = new GonvexClient("ws://old-runtime.test/ws", { queryCache: { store } });

    client.subscribeQuery(ref, {}, () => undefined);
    socket().open();
    await flushAsyncWork();

    expect(store.reads).toHaveLength(0);
    expect(sentMessages()).toContainEqual(expect.objectContaining({ type: "query.subscribe" }));
  });

  it("waits for the authenticated scope and ignores the anonymous session scope", async () => {
    const store = new FakeQueryCacheStore();
    store.lookup = {
      result: "authenticated-cache",
      revision: "0001:0001",
      writtenAt: Date.now(),
      ageMs: 0,
    };
    const client = new GonvexClient("ws://runtime.test/ws", {
      token: "session-token",
      tenant: "tenant-a",
      queryCache: { store },
    });
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: { ...directive, scope: "f".repeat(64) } });
    await flushAsyncWork();
    expect(store.reads).toHaveLength(0);
    expect(sentMessages()).toEqual([
      expect.objectContaining({ type: "auth", token: "session-token", tenant: "tenant-a" }),
    ]);

    const auth = sentMessages()[0];
    socket().receive({
      type: "auth.result",
      id: auth.id,
      result: { userId: "user-a", tenantId: "tenant-a", queryCache: directive },
    });
    await flushAsyncWork();

    expect(store.reads).toContainEqual(expect.objectContaining({ scope: directive.scope }));
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ result: "authenticated-cache" }));
    expect(sentMessages()).toContainEqual(expect.objectContaining({ type: "query.subscribe" }));
  });

  it("drops results from an obsolete server scope during an auth change", async () => {
    const store = new FakeQueryCacheStore();
    const client = new GonvexClient("ws://runtime.test/ws", { queryCache: { store } });
    const handler = vi.fn();
    const scopeChange = vi.fn();
    client.onSessionScopeChange(scopeChange);

    client.subscribeQuery(ref, {}, handler);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const subscribe = sentMessages().find((message) => message.type === "query.subscribe");

    client.setAuth({ token: "next-user-token", tenant: "tenant-b" });
    socket().receive({
      type: "query.result",
      id: subscribe.id,
      result: "old-user-result",
      cacheScope: directive.scope,
      cacheRevision: "0001:0002",
    });

    expect(scopeChange).toHaveBeenCalledTimes(1);
    expect(handler).not.toHaveBeenCalled();
    expect(store.writes).toHaveLength(0);
  });

  it("clears the confirmed scope and sends an empty auth frame on logout", async () => {
    const store = new FakeQueryCacheStore();
    const client = new GonvexClient("ws://runtime.test/ws", {
      token: "session-token",
      tenant: "tenant-a",
      queryCache: { store },
    });
    const scopeChange = vi.fn();
    client.onSessionScopeChange(scopeChange);
    client.connect();
    socket().open();
    const initialAuth = sentMessages()[0];
    socket().receive({
      type: "auth.result",
      id: initialAuth.id,
      result: { userId: "user-a", tenantId: "tenant-a", queryCache: directive },
    });

    client.setAuth({ token: undefined, tenant: undefined });

    expect(scopeChange).toHaveBeenCalledTimes(1);
    expect(sentMessages().at(-1)).toEqual(expect.objectContaining({ type: "auth" }));
    expect(sentMessages().at(-1)).not.toHaveProperty("token");
    expect(sentMessages().at(-1)).not.toHaveProperty("tenant");
  });
});
