import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  GonvexClient,
  type FunctionReference,
  type StoredSyncCollection,
  type SyncStore,
} from "./index";
import type { JsonValue, SyncCursor } from "@gonvex/protocol";
import type { QueryCacheDirective } from "@gonvex/protocol";

type Listener = (event: { data?: string }) => void;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readonly sent: string[] = [];
  readyState = FakeWebSocket.CONNECTING;
  private readonly listeners = new Map<string, Listener[]>();

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: Listener) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
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
  }

  receive(message: unknown) {
    this.emit("message", { data: JSON.stringify(message) });
  }

  private emit(type: string, event: { data?: string }) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

class FakeSyncStore implements SyncStore {
  stored: StoredSyncCollection | undefined;
  readonly loads: Array<{ scope: string; path: string; args: JsonValue }> = [];
  readonly replacements: StoredSyncCollection[] = [];
  readonly deltas: Array<{ cursor: SyncCursor; upserts: JsonValue[]; deleted: string[] }> = [];
  readonly deletes: Array<{ scope: string; path: string; args: JsonValue }> = [];
  directive: QueryCacheDirective | undefined;

  async load(scope: string, path: string, args: JsonValue) {
    this.loads.push({ scope, path, args });
    return this.stored;
  }

  async replace(_scope: string, _path: string, _args: JsonValue, value: StoredSyncCollection) {
    this.replacements.push(value);
  }

  async applyDelta(
    _scope: string,
    _path: string,
    _args: JsonValue,
    value: {
      cursor: SyncCursor;
      keyField: string;
      upserts: JsonValue[];
      deleted: string[];
    },
  ) {
    this.deltas.push(value);
  }

  async delete(scope: string, path: string, args: JsonValue) {
    this.deletes.push({ scope, path, args });
  }

  async loadDirective() {
    return this.directive;
  }

  async saveDirective(_identity: string, directive: QueryCacheDirective) {
    this.directive = directive;
  }

  async clear() {}
  close() {}
}

const ref: FunctionReference = { kind: "sync", path: "tasks.recentSync" };
const scope = "scope-user-a-0000000000000000000000000000000000000000000000000000";
const directive = {
  protocolVersion: 1 as const,
  scope,
  epoch: "epoch-a-00000000000000000000000000000000000000000000000000000",
  maxAgeMs: 86_400_000,
};

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
}

describe("durable sync integration", () => {
  it("replays the prior identity scope before background server auth completes", async () => {
    const store = new FakeSyncStore();
    store.directive = directive;
    store.stored = {
      rows: [{ id: "cached", title: "instant" }],
      cursor: { epoch: "sync-a", revision: 7 },
      keyField: "id",
    };
    const token = [
      btoa(JSON.stringify({ alg: "none" })),
      btoa(JSON.stringify({ sub: "user-a", iss: "issuer-a" })),
      "signature",
    ].join(".");
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "project-a",
      tenant: "tenant-a",
      token,
      sync: { store },
    });
    const handler = vi.fn();

    client.subscribeSync(ref, { workspaceId: "workspace-a" }, handler);
    await flushAsyncWork();

    expect(handler).toHaveBeenCalledWith(expect.objectContaining({
      type: "sync.snapshot",
      result: [{ id: "cached", title: "instant" }],
    }));

    socket().open();
    expect(sentMessages()[0]).toMatchObject({ type: "auth", token, tenant: "tenant-a" });
    expect(sentMessages().some((message) => message.type === "sync.open")).toBe(false);
  });

  it("renders the IndexedDB snapshot first and resumes from its cursor", async () => {
    const store = new FakeSyncStore();
    store.stored = {
      rows: [{ id: "cached", title: "cached" }],
      cursor: { epoch: "sync-a", revision: 41 },
      keyField: "id",
      maxRows: 100,
      maxBytes: 1_000_000,
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const handler = vi.fn();

    client.subscribeSync(ref, { workspaceId: "workspace-a" }, handler);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();

    expect(handler).toHaveBeenCalledWith(expect.objectContaining({
      type: "sync.snapshot",
      result: [{ id: "cached", title: "cached" }],
      cursor: { epoch: "sync-a", revision: 41 },
    }));
    expect(sentMessages()).toContainEqual(expect.objectContaining({
      type: "sync.open",
      path: "tasks.recentSync",
      args: { workspaceId: "workspace-a" },
      cursor: { epoch: "sync-a", revision: 41 },
    }));
  });

  it("materializes only changed rows and persists the delta cursor", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const handler = vi.fn();

    client.subscribeSync(ref, { workspaceId: "workspace-a" }, handler);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");

    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "a", title: "old" }, { id: "b", title: "delete" }],
      cursor: { epoch: "sync-a", revision: 10 },
      key: "id",
      maxRows: 100,
      maxBytes: 1_000_000,
    });
    socket().receive({
      type: "sync.delta",
      id: open.id,
      path: ref.path,
      upserts: [{ id: "a", title: "new" }, { id: "c", title: "added" }],
      deleted: ["b"],
      mutationIds: ["mutation-a"],
      cursor: { epoch: "sync-a", revision: 11 },
    });
    await flushAsyncWork();

    expect(handler).toHaveBeenLastCalledWith(expect.objectContaining({
      type: "sync.snapshot",
      result: [
        { id: "a", title: "new" },
        { id: "c", title: "added" },
      ],
      cursor: { epoch: "sync-a", revision: 11 },
    }));
    expect(store.deltas).toEqual([expect.objectContaining({
      cursor: { epoch: "sync-a", revision: 11 },
      upserts: [{ id: "a", title: "new" }, { id: "c", title: "added" }],
      deleted: ["b"],
    })]);
  });

  it("resumes the latest cursor after reconnect and clears only a reset collection", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    const first = socket();
    first.open();
    first.receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages(first).find((message) => message.type === "sync.open");
    first.receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [],
      cursor: { epoch: "sync-a", revision: 20 },
      key: "id",
    });

    first.disconnect();
    vi.advanceTimersByTime(250);
    const second = socket();
    second.open();
    second.receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();

    expect(sentMessages(second)).toContainEqual(expect.objectContaining({
      type: "sync.open",
      id: open.id,
      cursor: { epoch: "sync-a", revision: 20 },
    }));

    second.receive({ type: "sync.reset", id: open.id, path: ref.path, reason: "cursor-expired" });
    await flushAsyncWork();

    expect(store.deletes).toEqual([{
      scope,
      path: ref.path,
      args: { workspaceId: "workspace-a" },
    }]);
    expect(sentMessages(second).at(-1)).toMatchObject({
      type: "sync.open",
      id: open.id,
    });
    expect(sentMessages(second).at(-1).cursor).toBeUndefined();
  });
});
