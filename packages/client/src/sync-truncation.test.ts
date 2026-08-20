import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import type { JsonValue, QueryCacheDirective, ReplicaCursor } from "@gonvex/protocol";
import {
  DexieSyncStore,
  GonvexClient,
  syncHashesDigest,
  syncRowsHashes,
  type FunctionReference,
  type StoredSyncCollection,
  type SyncStore,
} from "./index";

type Listener = (event: { data?: string }) => void;

class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
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
  readonly replacements: StoredSyncCollection[] = [];
  directive: QueryCacheDirective | undefined;

  async load() {
    return this.stored;
  }

  async replace(_scope: string, _path: string, _args: JsonValue, value: StoredSyncCollection) {
    this.replacements.push(value);
    this.stored = value;
  }

  async applyDelta(
    _scope: string,
    _path: string,
    _args: JsonValue,
    _value: {
      cursor: ReplicaCursor;
      keyField: string;
      upserts: JsonValue[];
      deleted: string[];
    },
  ) {}

  async delete() {}

  async loadDirective() {
    return this.directive;
  }

  async saveDirective(_identity: string, directive: QueryCacheDirective) {
    this.directive = directive;
  }

  async clear() {}
  close() {}
}

const clients: GonvexClient[] = [];
const dexieStores: DexieSyncStore[] = [];
const ref: FunctionReference = { kind: "query", delivery: "replica", path: "tasks.recentSync" };
const directive: QueryCacheDirective = {
  protocolVersion: 1,
  scope: "scope-user-a-0000000000000000000000000000000000000000000000000000",
  epoch: "epoch-a-00000000000000000000000000000000000000000000000000000",
  maxAgeMs: 86_400_000,
};

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.stubGlobal("WebSocket", FakeWebSocket);
  vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
});

afterEach(async () => {
  for (const client of clients.splice(0)) client.close();
  for (const store of dexieStores.splice(0)) {
    await store.clear();
    store.close();
  }
  vi.unstubAllGlobals();
});

function socket() {
  const value = FakeWebSocket.instances.at(-1);
  if (!value) throw new Error("expected WebSocket instance");
  return value;
}

function sentMessages() {
  return socket().sent.map((message) => JSON.parse(message));
}

async function flushAsyncWork() {
  for (let barrier = 0; barrier < 3; barrier += 1) {
    await syncHashesDigest({});
    for (let index = 0; index < 5; index += 1) await Promise.resolve();
  }
}

describe("sync truncation", () => {
  it("passes direct ready truncation to listeners and persists later false", async () => {
    const store = new FakeSyncStore();
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    clients.push(client);
    const ready: Array<boolean | undefined> = [];
    client.subscribeSync(ref, { workspaceId: "workspace-a" }, (message) => {
      if (message.type === "sync.ready") ready.push(message.truncated);
    });
    socket().open();
    socket().receive({ type: "session.ready", queryCache: directive });
    await flushAsyncWork();

    const open = sentMessages().find((message) => message.type === "sync.open");
    const rows = [{ id: "task-a", title: "budget edge" }];
    socket().receive({
      type: "sync.snapshot",
      id: open.id,
      path: ref.path,
      result: rows,
      cursor: { epoch: "sync-a", revision: 1 },
      key: "id",
    });
    const digest = await syncHashesDigest(await syncRowsHashes(rows, "id"));
    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 1 },
      digest,
      truncated: true,
    });
    await flushAsyncWork();

    socket().receive({
      type: "sync.ready",
      id: open.id,
      path: ref.path,
      cursor: { epoch: "sync-a", revision: 2 },
      digest,
      truncated: false,
    });
    await flushAsyncWork();

    expect(ready).toEqual([true, false]);
    expect(store.replacements.some((value) => value.truncated === true)).toBe(true);
    expect(store.replacements.at(-1)?.truncated).toBe(false);
  });

  it("passes truncation through batched ready messages and leaves absence undefined", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { sync: false });
    clients.push(client);
    const received: Array<boolean | undefined> = [];
    client.subscribeSync(ref, {}, (message) => {
      if (message.type === "sync.ready") received.push(message.truncated);
    });
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
    });
    const digest = await syncHashesDigest({});
    socket().receive({
      type: "sync.readyMany",
      ready: [{
        id: open.id,
        path: ref.path,
        cursor: { epoch: "sync-a", revision: 1 },
        digest,
        truncated: true,
      }],
    });
    await flushAsyncWork();
    socket().receive({
      type: "sync.readyMany",
      ready: [{
        id: open.id,
        path: ref.path,
        cursor: { epoch: "sync-a", revision: 2 },
        digest,
      }],
    });
    await flushAsyncWork();

    expect(received).toEqual([true, undefined]);
  });

  it("restores persisted truncation and updates it with metadata-only replacement", async () => {
    const store = new DexieSyncStore({
      databaseName: `gonvex-sync-truncation-${crypto.randomUUID()}`,
      indexedDB,
      IDBKeyRange,
      maxBytes: 1_000_000,
    });
    dexieStores.push(store);
    const args = { workspaceId: "workspace-a" };
    const rows = [{ id: "task-a" }];

    await store.replace(directive.scope, ref.path, args, {
      rows,
      cursor: { epoch: "sync-a", revision: 1 },
      keyField: "id",
      mode: "eager",
      truncated: true,
    });
    await expect(store.load(directive.scope, ref.path, args)).resolves.toMatchObject({
      rows,
      truncated: true,
    });

    await store.replace(directive.scope, ref.path, args, {
      rows,
      cursor: { epoch: "sync-a", revision: 2 },
      keyField: "id",
      mode: "eager",
      truncated: false,
      rowsUnchanged: true,
    });
    await expect(store.load(directive.scope, ref.path, args)).resolves.toMatchObject({
      cursor: { epoch: "sync-a", revision: 2 },
      truncated: false,
    });
  });
});
