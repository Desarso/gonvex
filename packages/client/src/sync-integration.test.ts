import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  GonvexClient,
  syncHashesDigest,
  syncRowsHashes,
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

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

class DelayedSyncStore extends FakeSyncStore {
  readonly replaceGate = deferred();
  readonly deleteGate = deferred();
  readonly operationOrder: string[] = [];

  override async replace(
    scope: string,
    path: string,
    args: JsonValue,
    value: StoredSyncCollection,
  ) {
    this.operationOrder.push("replace:start");
    await this.replaceGate.promise;
    this.operationOrder.push("replace:finish");
    await super.replace(scope, path, args, value);
  }

  override async applyDelta(
    scope: string,
    path: string,
    args: JsonValue,
    value: {
      cursor: SyncCursor;
      keyField: string;
      upserts: JsonValue[];
      deleted: string[];
    },
  ) {
    this.operationOrder.push("delta");
    await super.applyDelta(scope, path, args, value);
  }

  override async delete(scope: string, path: string, args: JsonValue) {
    this.operationOrder.push("delete:start");
    await this.deleteGate.promise;
    this.operationOrder.push("delete:finish");
    await super.delete(scope, path, args);
  }
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
  for (let index = 0; index < 5; index += 1) {
    await Promise.resolve();
  }
  // WebCrypto completion is an event-loop task, not a plain promise
  // microtask. Await one digest so integrity preparation can settle while the
  // rest of the test keeps deterministic fake timers.
  for (let barrier = 0; barrier < 3; barrier += 1) {
    await syncHashesDigest({});
    for (let index = 0; index < 5; index += 1) await Promise.resolve();
  }
}

async function digestRows(rows: JsonValue[], keyField = "id") {
  return syncHashesDigest(await syncRowsHashes(rows, keyField));
}

function seededRandom(seed: number) {
  let state = seed >>> 0;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
}

function orderedRows(rows: Map<string, { id: string; value: number }>) {
  return [...rows.values()].sort((left, right) => left.id.localeCompare(right.id));
}

function normalizedRows(rows: JsonValue[] | undefined) {
  return [...(rows ?? [])].sort((left, right) => {
    const leftID = String((left as { id?: unknown }).id ?? "");
    const rightID = String((right as { id?: unknown }).id ?? "");
    return leftID.localeCompare(rightID);
  });
}

describe("durable sync integration", () => {
  it("retains a listenerless sync across a transient React remount", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const first = vi.fn();
    const second = vi.fn();

    const unsubscribe = client.subscribeSync(ref, { workspaceId: "workspace-a" }, first);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");
    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "task-a", title: "kept warm" }],
      cursor: { epoch: "sync-a", revision: 1 },
      key: "id",
    });

    unsubscribe();
    expect(sentMessages().filter((message) => message.type === "sync.close")).toHaveLength(0);
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, second);
    await flushAsyncWork();

    expect(sentMessages().filter((message) => message.type === "sync.open")).toHaveLength(1);
    expect(sentMessages().filter((message) => message.type === "sync.close")).toHaveLength(0);
    expect(second).toHaveBeenCalledWith(expect.objectContaining({
      type: "sync.snapshot",
      result: [{ id: "task-a", title: "kept warm" }],
    }));
  });

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

  it("keeps live sync state when a deploy rotates the query scope but not the sync scope", async () => {
    const syncScope = "visibility-scope-a-0000000000000000000000000000000000000000";
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const handler = vi.fn();

    client.subscribeSync(ref, { workspaceId: "workspace-a" }, handler);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: { ...directive, syncScope } });
    await flushAsyncWork();

    // Warm reads and persistence key by the visibility scope, not the
    // bundle-epoch query scope.
    expect(store.loads).toHaveLength(1);
    expect(store.loads[0]!.scope).toBe(syncScope);

    const open = sentMessages().find((message) => message.type === "sync.open");
    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "task-a", title: "live" }],
      cursor: { epoch: "sync-a", revision: 5 },
      key: "id",
    });
    await flushAsyncWork();
    const opensBefore = sentMessages().filter((message) => message.type === "sync.open").length;
    handler.mockClear();

    // A deploy: new bundle epoch and query scope, same visibility.
    socket().receive({
      type: "session.scope",
      queryCache: {
        ...directive,
        scope: "scope-after-deploy-000000000000000000000000000000000000000000",
        epoch: "epoch-b-00000000000000000000000000000000000000000000000000000",
        syncScope,
      },
    });
    await flushAsyncWork();

    expect(store.loads).toHaveLength(1);
    expect(sentMessages().filter((message) => message.type === "sync.open")).toHaveLength(opensBefore);
    expect(handler).not.toHaveBeenCalledWith(expect.objectContaining({ type: "sync.reset" }));
    expect(handler).not.toHaveBeenCalledWith(expect.objectContaining({ result: [] }));

    // A real visibility change (different user/permissions) still resets.
    socket().receive({
      type: "session.scope",
      queryCache: {
        ...directive,
        scope: "scope-other-user-00000000000000000000000000000000000000000000",
        syncScope: "visibility-scope-b-0000000000000000000000000000000000000000",
      },
    });
    await flushAsyncWork();

    expect(store.loads).toHaveLength(2);
    expect(store.loads[1]!.scope).toBe("visibility-scope-b-0000000000000000000000000000000000000000");
    expect(sentMessages().filter((message) => message.type === "sync.open").length).toBeGreaterThan(opensBefore);
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

  it("batches warm sync resumes with integrity keys for every persisted mode", async () => {
    const store = new FakeSyncStore();
    store.stored = {
      rows: [{ id: "cached", title: "cached" }],
      cursor: { epoch: "sync-a", revision: 41 },
      keyField: "id",
      mode: "eager",
    } as StoredSyncCollection & { mode: "eager" };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });

    client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    client.subscribeSync(
      { kind: "sync", path: "statuses.sync" },
      { workspaceId: "workspace-a" },
      () => undefined,
    );
    socket().open();
    socket().receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncBatch: 1 },
    });
    await flushAsyncWork();
    await vi.runOnlyPendingTimersAsync();
    await flushAsyncWork();

    const messages = sentMessages();
    expect(messages.some((message) => message.type === "sync.open")).toBe(false);
    const batch = messages.find((message) => message.type === "sync.openMany");
    expect(batch?.opens).toHaveLength(2);
    expect(batch?.opens).toEqual(expect.arrayContaining([
      expect.objectContaining({
        path: "tasks.recentSync",
        cursor: { epoch: "sync-a", revision: 41 },
      }),
      expect.objectContaining({
        path: "statuses.sync",
        cursor: { epoch: "sync-a", revision: 41 },
      }),
    ]));
    expect(batch?.opens.every((open: { keys?: string[] }) => (
      JSON.stringify(open.keys) === JSON.stringify(["cached"])
    ))).toBe(true);
    expect(batch?.opens.every((open: { digest?: string; hashes?: Record<string, string>; fullIntegrity?: boolean }) => (
      typeof open.digest === "string"
      && Object.keys(open.hashes ?? {}).length === 1
      && open.fullIntegrity === true
    ))).toBe(true);
  });

  it("resumes large unchanged collections with one compact digest and expands hashes only on mismatch", async () => {
    const rows = Array.from({ length: 300 }, (_, index) => ({ id: `row-${index}`, value: index }));
    const store = new FakeSyncStore();
    store.stored = {
      rows,
      cursor: { epoch: "sync-a", revision: 41 },
      keyField: "id",
      mode: "eager",
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();

    const first = sentMessages().find((message) => message.type === "sync.open");
    expect(first).toMatchObject({
      cursor: { epoch: "sync-a", revision: 41 },
      digest: await digestRows(rows),
    });
    expect(first.keys).toBeUndefined();
    expect(first.hashes).toBeUndefined();
    expect(first.fullIntegrity).toBeUndefined();

    socket().receive({ type: "sync.needHashes", id: first.id, path: ref.path });
    await flushAsyncWork();
    const opens = sentMessages().filter((message) => message.type === "sync.open");
    expect(opens).toHaveLength(2);
    expect(opens[1]).toMatchObject({
      id: first.id,
      fullIntegrity: true,
      digest: first.digest,
    });
    expect(opens[1].keys).toHaveLength(300);
    expect(Object.keys(opens[1].hashes)).toHaveLength(300);
  });

  it("resumes the maximum supported warm batch in one frame without dropping a collection", async () => {
    const store = new FakeSyncStore();
    store.stored = {
      rows: [{ id: "cached" }],
      cursor: { epoch: "sync-a", revision: 41 },
      keyField: "id",
      mode: "eager",
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const handlers = Array.from({ length: 256 }, () => vi.fn());

    handlers.forEach((handler, index) => {
      client.subscribeSync(
        { kind: "sync", path: `references.collection${index}` },
        { tenantId: "tenant-a" },
        handler,
      );
    });
    socket().open();
    socket().receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncBatch: 1 },
    });
    await flushAsyncWork();
    await vi.runOnlyPendingTimersAsync();
    await flushAsyncWork();

    const batches = sentMessages().filter((message) => message.type === "sync.openMany");
    expect(batches).toHaveLength(1);
    expect(batches[0].opens).toHaveLength(256);
    expect(new Set(batches[0].opens.map((open: { id: string }) => open.id))).toHaveProperty("size", 256);

    const digest = await digestRows([{ id: "cached" }]);
    socket().receive({
      type: "sync.readyMany",
      ready: batches[0].opens.map((open: { id: string; path: string }) => ({
        id: open.id,
        path: open.path,
        cursor: { epoch: "sync-a", revision: 41 },
        mode: "eager",
        digest,
      })),
    });
    vi.useRealTimers();
    await vi.waitFor(() => expect(
      handlers.every((handler) => handler.mock.calls.at(-1)?.[0]?.type === "sync.ready"),
    ).toBe(true));
  });

  it("splits oversized warm resumes into bounded frames without stranding a collection", async () => {
    const store = new FakeSyncStore();
    store.stored = {
      rows: [{ id: "cached" }],
      cursor: { epoch: "sync-a", revision: 41 },
      keyField: "id",
      mode: "eager",
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });

    for (let index = 0; index < 513; index += 1) {
      client.subscribeSync(
        { kind: "sync", path: `references.oversizedCollection${index}` },
        { tenantId: "tenant-a" },
        () => undefined,
      );
    }
    socket().open();
    socket().receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncBatch: 1 },
    });
    await flushAsyncWork();
    await vi.runOnlyPendingTimersAsync();
    await flushAsyncWork();

    const batches = sentMessages().filter((message) => message.type === "sync.openMany");
    expect(batches.map((batch) => batch.opens.length)).toEqual([256, 256, 1]);
    const opens = batches.flatMap((batch) => batch.opens);
    expect(opens).toHaveLength(513);
    expect(new Set(opens.map((open: { id: string }) => open.id)).size).toBe(513);
  });

  it("keeps progressive visibility keys and persists modes learned from batched ready messages", async () => {
    const store = new FakeSyncStore();
    store.stored = {
      rows: [{ id: "cached", title: "cached" }],
      cursor: { epoch: "sync-a", revision: 41 },
      keyField: "id",
      mode: "progressive",
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const handler = vi.fn();

    client.subscribeSync(ref, { workspaceId: "workspace-a" }, handler);
    socket().open();
    socket().receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncBatch: 1 },
    });
    await flushAsyncWork();
    await vi.runOnlyPendingTimersAsync();
    await flushAsyncWork();

    const batch = sentMessages().find((message) => message.type === "sync.openMany");
    expect(batch?.opens).toEqual([
      expect.objectContaining({ keys: ["cached"] }),
    ]);

    socket().receive({
      type: "sync.readyMany",
      ready: [{
        id: batch.opens[0].id,
        path: ref.path,
        cursor: { epoch: "sync-a", revision: 41 },
        mode: "progressive",
        digest: await digestRows([{ id: "cached", title: "cached" }]),
      }],
    });
    vi.useRealTimers();
    await vi.waitFor(() => expect(handler).toHaveBeenLastCalledWith(expect.objectContaining({
      type: "sync.ready",
      mode: "progressive",
    })));
    expect(store.replacements.at(-1)).toEqual(expect.objectContaining({
      mode: "progressive",
      rows: [{ id: "cached", title: "cached" }],
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

  it("never lets delayed snapshot or ready frames regress an authoritative cursor", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync<{ id: string; title: string }>(ref, { workspaceId: "workspace-a" });
    watch.onUpdate(() => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");
    const currentRows = [{ id: "task-a", title: "current" }];
    const currentDigest = await digestRows(currentRows);

    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: currentRows,
      cursor: { epoch: "sync-a", revision: 20 },
      key: "id",
      mode: "eager",
    });
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 20 },
      mode: "eager",
      digest: currentDigest,
    });
    await flushAsyncWork();
    expect(watch.status().isUpToDate).toBe(true);

    const delayedRows = [{ id: "task-a", title: "delayed-old-value" }];
    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: delayedRows,
      cursor: { epoch: "sync-a", revision: 19 },
      key: "id",
      mode: "eager",
    });
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 19 },
      mode: "eager",
      digest: await digestRows(delayedRows),
    });
    await flushAsyncWork();

    expect(watch.status().isUpToDate).toBe(true);
    expect(watch.localSyncResult()).toEqual(currentRows);
    expect(store.replacements.at(-1)?.cursor).toEqual({ epoch: "sync-a", revision: 20 });
  });

  it("never reports stale rows as current during bounded randomized protocol chaos", async () => {
    const requestedMultiplier = Number.parseInt(process.env.GONVEX_SYNC_CHAOS_MULTIPLIER ?? "1", 10);
    const multiplier = Math.min(4, Math.max(1, Number.isFinite(requestedMultiplier) ? requestedMultiplier : 1));
    const seeds = Array.from({ length: 12 * multiplier }, (_, index) => 0x51a7e + index * 7919);

    for (const seed of seeds) {
      const random = seededRandom(seed);
      const store = new FakeSyncStore();
      const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
      const watch = client.watchSync<{ id: string; value: number }>(
        ref,
        { workspaceId: `workspace-${seed}` },
      );
      watch.onUpdate(() => undefined);
      let activeSocket = socket();
      activeSocket.open();
      activeSocket.receive({ type: "session.ready", queryCache: directive });
      await flushAsyncWork();
      const syncID = sentMessages(activeSocket).find((message) => message.type === "sync.open")?.id;
      expect(syncID).toBeTypeOf("string");

      const authoritative = new Map<string, { id: string; value: number }>([
        ["row-0", { id: "row-0", value: seed }],
        ["row-1", { id: "row-1", value: seed + 1 }],
      ]);
      let revision = 1;
      const sendReady = async () => {
        const rows = orderedRows(authoritative);
        activeSocket.receive({
          type: "sync.ready",
          id: syncID,
          path: ref.path,
          cursor: { epoch: "sync-chaos", revision },
          mode: "eager",
          digest: await digestRows(rows),
        });
        await flushAsyncWork();
      };
      const sendFreshSnapshot = async () => {
        const rows = orderedRows(authoritative);
        activeSocket.receive({
          type: "sync.snapshot",
          id: syncID,
          path: ref.path,
          result: rows,
          cursor: { epoch: "sync-chaos", revision },
          key: "id",
          mode: "eager",
        });
        await sendReady();
      };
      const assertInvariant = (step: number) => {
        if (!watch.status().isUpToDate) return;
        expect(
          normalizedRows(watch.localSyncResult()),
          `seed=${seed} step=${step} exposed stale rows while current`,
        ).toEqual(orderedRows(authoritative));
      };

      await sendFreshSnapshot();
      assertInvariant(-1);

      for (let step = 0; step < 80 * multiplier; step += 1) {
        const operation = random() % 7;
        if (operation <= 1) {
          activeSocket.receive({
            type: "sync.syncing",
            id: syncID,
            path: ref.path,
            reason: "chaos-mutation",
          });
          const id = `row-${random() % 12}`;
          const deleted = authoritative.has(id) && random() % 4 === 0;
          revision += 1;
          let upserts: Array<{ id: string; value: number }> = [];
          let deletedIDs: string[] = [];
          if (deleted) {
            authoritative.delete(id);
            deletedIDs = [id];
          } else {
            const row = { id, value: random() % 1_000_000 };
            authoritative.set(id, row);
            upserts = [row];
          }
          activeSocket.receive({
            type: "sync.delta",
            id: syncID,
            path: ref.path,
            upserts,
            deleted: deletedIDs,
            cursor: { epoch: "sync-chaos", revision },
            digest: await digestRows(orderedRows(authoritative)),
          });
          await sendReady();
        } else if (operation === 2) {
          activeSocket.receive({
            type: "sync.syncing",
            id: syncID,
            path: ref.path,
            reason: "chaos-dependency-change",
          });
          revision += 1;
          activeSocket.receive({
            type: "sync.delta",
            id: syncID,
            path: ref.path,
            upserts: [],
            deleted: [],
            cursor: { epoch: "sync-chaos", revision },
            digest: await digestRows(orderedRows(authoritative)),
          });
          await sendReady();
        } else if (operation === 3) {
          const delayedRows = [{ id: "delayed-corruption", value: seed + step }];
          const delayedRevision = Math.max(0, revision - 1);
          activeSocket.receive({
            type: "sync.snapshot",
            id: syncID,
            path: ref.path,
            result: delayedRows,
            cursor: { epoch: "sync-chaos", revision: delayedRevision },
            key: "id",
            mode: "eager",
          });
          activeSocket.receive({
            type: "sync.ready",
            id: syncID,
            path: ref.path,
            cursor: { epoch: "sync-chaos", revision: delayedRevision },
            mode: "eager",
            digest: await digestRows(delayedRows),
          });
          await flushAsyncWork();
        } else if (operation === 4) {
          activeSocket.receive({
            type: "sync.syncing",
            id: syncID,
            path: ref.path,
            reason: "chaos-corruption",
          });
          revision += 1;
          activeSocket.receive({
            type: "sync.delta",
            id: syncID,
            path: ref.path,
            upserts: [{ id: "injected-corruption", value: step }],
            deleted: [],
            cursor: { epoch: "sync-chaos", revision },
            digest: "intentionally-wrong",
          });
          await sendReady();
          expect(watch.status().isUpToDate, `seed=${seed} step=${step} trusted corrupt delta`).toBe(false);
          await flushAsyncWork();
          await sendFreshSnapshot();
        } else if (operation === 5) {
          activeSocket.disconnect();
          expect(watch.status().isUpToDate).toBe(false);
          await vi.advanceTimersByTimeAsync(250);
          activeSocket = socket();
          activeSocket.open();
          activeSocket.receive({ type: "session.ready", queryCache: directive });
          await flushAsyncWork();
          await sendReady();
        } else {
          activeSocket.receive({ type: "sync.needHashes", id: syncID, path: ref.path });
          expect(watch.status().isUpToDate).toBe(false);
          await flushAsyncWork();
          await sendReady();
        }
        assertInvariant(step);
      }
      client.close();
    }
  }, 30_000);

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
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
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

  it("never reports a cached sync as current while its WebSocket is reconciling", async () => {
    const store = new FakeSyncStore();
    store.stored = {
      rows: [{ id: "cached", title: "old" }],
      cursor: { epoch: "sync-a", revision: 20 },
      keyField: "id",
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync<{ id: string; title: string }>(ref, { workspaceId: "workspace-a" });
    const updates: Array<{ rows: unknown; isUpToDate: boolean }> = [];
    watch.onUpdate(() => {
      updates.push({
        rows: watch.localSyncResult(),
        isUpToDate: watch.status().isUpToDate,
      });
    });

    const first = socket();
    first.open();
    first.receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages(first).find((message) => message.type === "sync.open");

    expect(watch.localSyncResult()).toEqual([{ id: "cached", title: "old" }]);
    expect(watch.status().isUpToDate).toBe(false);

    first.receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 20 },
      digest: await digestRows([{ id: "cached", title: "old" }]),
    });
    vi.useRealTimers();
    await vi.waitFor(() => expect(watch.status().isUpToDate).toBe(true));

    first.receive({
      type: "sync.syncing",
      id: open.id,
      path: ref.path,
      reason: "reconciling",
    });
    expect(watch.status().isUpToDate).toBe(false);
    first.receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 20 },
      digest: await digestRows([{ id: "cached", title: "old" }]),
    });
    await vi.waitFor(() => expect(watch.status().isUpToDate).toBe(true));

    first.disconnect();
    expect(watch.status().isUpToDate).toBe(false);

    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const second = socket();
    second.open();
    second.receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    expect(watch.status().isUpToDate).toBe(false);

    second.receive({
      type: "sync.reset",
      id: open.id,
      path: ref.path,
      reason: "visibility-changed",
    });
    await flushAsyncWork();
    expect(watch.status().isUpToDate).toBe(false);

    const reopened = sentMessages(second).at(-1);
    second.receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "cached", title: "current" }],
      cursor: { epoch: "sync-a", revision: 21 },
      key: "id",
    });
    expect(watch.status().isUpToDate).toBe(false);

    second.receive({
      type: "sync.ready",
      id: reopened.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 21 },
      digest: await digestRows([{ id: "cached", title: "current" }]),
    });
    await vi.waitFor(() => expect(watch.status().isUpToDate).toBe(true));
    expect(watch.localSyncResult()).toEqual([{ id: "cached", title: "current" }]);
    expect(updates.some((update) => update.isUpToDate === false)).toBe(true);
  });

  it("fails closed and rebuilds when the authoritative collection digest disagrees", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync<{ id: string; title: string }>(ref, { workspaceId: "workspace-a" });
    watch.onUpdate(() => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");

    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "task-a", title: "stale" }],
      cursor: { epoch: "sync-a", revision: 20 },
      key: "id",
      mode: "progressive",
      hashes: { "task-a": "server-row-hash" },
    });
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 20 },
      mode: "progressive",
      digest: await syncHashesDigest({ "task-a": "different-row-hash" }),
    });
    // WebCrypto resolves outside Vitest's fake-timer microtask queue.
    vi.useRealTimers();
    await vi.waitFor(() => expect(store.deletes).toHaveLength(1));

    expect(watch.status().isUpToDate).toBe(false);
    expect(store.deletes).toEqual([{
      scope,
      path: ref.path,
      args: { workspaceId: "workspace-a" },
    }]);
    expect(sentMessages().at(-1)).toMatchObject({ type: "sync.open", id: open.id });
    expect(sentMessages().at(-1).cursor).toBeUndefined();
  });

  it("hashes the materialized rows instead of trusting matching persisted metadata", async () => {
    const store = new FakeSyncStore();
    const correctRows = [{ id: "task-a", title: "correct" }];
    const correctHashes = await syncRowsHashes(correctRows, "id");
    store.stored = {
      rows: [{ id: "task-a", title: "corrupted-in-indexeddb" }],
      cursor: { epoch: "sync-a", revision: 20 },
      keyField: "id",
      mode: "progressive",
      // Simulate row corruption that did not update the collection metadata.
      hashes: correctHashes,
    };
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync<{ id: string; title: string }>(ref, { workspaceId: "workspace-a" });
    watch.onUpdate(() => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");
    const corruptHashes = await syncRowsHashes(store.stored!.rows, "id");
    expect(open.hashes).toEqual(corruptHashes);
    expect(open.hashes).not.toEqual(correctHashes);

    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 20 },
      mode: "progressive",
      digest: await syncHashesDigest(correctHashes),
    });

    vi.useRealTimers();
    await vi.waitFor(() => expect(store.deletes).toHaveLength(1));
    expect(watch.status().isUpToDate).toBe(false);
    expect(sentMessages().at(-1)).toMatchObject({ type: "sync.open", id: open.id });
    expect(sentMessages().at(-1).cursor).toBeUndefined();
  });

  it("rejects a digest-less ready frame when the runtime advertises sync integrity", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync(ref, { workspaceId: "workspace-a" });
    watch.onUpdate(() => undefined);
    socket().open();
    socket().receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { protocolVersion: 2, runtimeVersion: "test-sha", syncIntegrity: 1 },
    });
    expect(client.serverInfo()).toEqual({
      protocolVersion: 2,
      runtimeVersion: "test-sha",
      syncIntegrity: 1,
    });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");
    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [],
      cursor: { epoch: "sync-a", revision: 1 },
      key: "id",
      mode: "eager",
    });
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 1 },
      mode: "eager",
    });
    await flushAsyncWork();

    expect(watch.status().isUpToDate).toBe(false);
    expect(store.deletes).toHaveLength(1);
    expect(sentMessages().at(-1).cursor).toBeUndefined();
  });

  it("accepts a digest-less ready frame from a legacy runtime without reopening", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync(ref, { workspaceId: "workspace-a" });
    watch.onUpdate(() => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");
    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [],
      cursor: { epoch: "sync-a", revision: 1 },
      key: "id",
      mode: "eager",
    });
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 1 },
      mode: "eager",
    });

    vi.useRealTimers();
    await vi.waitFor(() => expect(watch.status().isUpToDate).toBe(true));
    expect(store.deletes).toEqual([]);
    expect(sentMessages().filter((message) => message.type === "sync.open")).toHaveLength(1);
  });

  it("repairs a legacy or corrupted progressive cache at the same cursor without a full snapshot", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const watch = client.watchSync<{ id: string; title: string }>(ref, { workspaceId: "workspace-a" });
    watch.onUpdate(() => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");
    const currentRows = [{ id: "task-a", title: "current" }];
    const currentHashes = await syncRowsHashes(currentRows, "id");

    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "task-a", title: "stale" }],
      cursor: { epoch: "sync-a", revision: 20 },
      key: "id",
      mode: "progressive",
      hashes: { "task-a": "stale-row-hash" },
    });
    socket().receive({
      type: "sync.delta",
      id: open.id,
      path: ref.path,
      upserts: [{ id: "task-a", title: "current" }],
      deleted: [],
      cursor: { epoch: "sync-a", revision: 20 },
      hashes: currentHashes,
      digest: await syncHashesDigest(currentHashes),
    });
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 20 },
      mode: "progressive",
      digest: await syncHashesDigest(currentHashes),
    });

    vi.useRealTimers();
    await vi.waitFor(() => expect(watch.status().isUpToDate).toBe(true));
    expect(watch.localSyncResult()).toEqual([{ id: "task-a", title: "current" }]);
    expect(store.deletes).toEqual([]);
  });

  it("serializes snapshot, delta, and reset persistence under slow IndexedDB writes", async () => {
    const store = new DelayedSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const open = sentMessages().find((message) => message.type === "sync.open");

    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: [{ id: "a", title: "old" }],
      cursor: { epoch: "sync-a", revision: 10 },
      key: "id",
    });
    socket().receive({
      type: "sync.delta",
      id: open.id,
      path: ref.path,
      upserts: [{ id: "a", title: "new" }],
      deleted: [],
      cursor: { epoch: "sync-a", revision: 11 },
    });
    socket().receive({
      type: "sync.reset",
      id: open.id,
      path: ref.path,
      reason: "cursor-expired",
    });
    await flushAsyncWork();

    expect(store.operationOrder).toEqual(["replace:start"]);

    store.replaceGate.resolve();
    await flushAsyncWork();
    expect(store.operationOrder).toEqual([
      "replace:start",
      "replace:finish",
      "delta",
      "delete:start",
    ]);

    store.deleteGate.resolve();
    await flushAsyncWork();
    expect(store.operationOrder).toEqual([
      "replace:start",
      "replace:finish",
      "delta",
      "delete:start",
      "delete:finish",
    ]);
  });

  it("serializes persistence across unsubscribe and resubscribe incarnations", async () => {
    const store = new DelayedSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const unsubscribe = client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const first = sentMessages().find((message) => message.type === "sync.open");
    socket().receive({
      type: "sync.snapshot",
      id: first.id,
      path: ref.path,
      result: [{ id: "a", title: "old" }],
      cursor: { epoch: "sync-a", revision: 10 },
      key: "id",
    });
    await flushAsyncWork();
    expect(store.operationOrder).toEqual(["replace:start"]);

    unsubscribe();
    vi.advanceTimersByTime(250);
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    await flushAsyncWork();
    const opens = sentMessages().filter((message) => message.type === "sync.open");
    const second = opens.at(-1);
    socket().receive({
      type: "sync.snapshot",
      id: second.id,
      path: ref.path,
      result: [{ id: "a", title: "new" }],
      cursor: { epoch: "sync-a", revision: 11 },
      key: "id",
    });
    await flushAsyncWork();
    expect(store.operationOrder).toEqual(["replace:start"]);

    store.replaceGate.resolve();
    await flushAsyncWork();
    expect(store.operationOrder).toEqual([
      "replace:start",
      "replace:finish",
      "replace:start",
      "replace:finish",
    ]);
    expect(store.replacements.at(-1)?.cursor).toEqual({ epoch: "sync-a", revision: 11 });
  });

  it("retries a transient sync error without waiting for a socket reconnect", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, () => undefined);
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();
    const firstOpen = sentMessages().find((message) => message.type === "sync.open");

    socket().receive({
      type: "sync.error",
      id: firstOpen.id,
      path: ref.path,
      error: "temporary database failover",
    });
    vi.advanceTimersByTime(250);
    await flushAsyncWork();

    const opens = sentMessages().filter((message) => message.type === "sync.open");
    expect(opens).toHaveLength(2);
    expect(opens[1]).toMatchObject({ id: firstOpen.id });
    expect(opens[1]).not.toHaveProperty("cursor");
  });
});
