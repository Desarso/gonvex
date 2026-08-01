import { afterEach, describe, expect, it } from "vitest";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import { DexieSyncStore, syncHashesDigest, syncRowsHashes } from "./sync-store";
import type { QueryCacheDirective } from "@gonvex/protocol";

const stores: DexieSyncStore[] = [];

afterEach(async () => {
  for (const store of stores.splice(0)) {
    await store.clear();
    store.close();
  }
});

function createStore() {
  const store = new DexieSyncStore({
    databaseName: `gonvex-sync-test-${crypto.randomUUID()}`,
    indexedDB,
    IDBKeyRange,
    maxBytes: 1_000_000,
  });
  stores.push(store);
  return store;
}

describe("DexieSyncStore", () => {
  it("uses the same canonical row hash as the Go runtime for nested JSON edge cases", async () => {
    const row = {
      nested: {
        z: 1,
        a: "\u2028",
        "key\u2028": "value",
        "\ue000": "bmp",
        "\u{10000}": "supplementary",
      },
      minusZero: -0,
      id: "row-a",
      amp: "<&>",
    };
    await expect(syncRowsHashes([row], "id")).resolves.toEqual({
      "row-a": "ba54453d2ecfb596b1cedeb64d8753675128ae801cf18da01a6cdb1005c5d71b",
    });
  });

  it("uses the same collection digest as Go for Unicode and escaped row keys", async () => {
    await expect(syncHashesDigest({ "<\u2028": "one", "\ue000": "two", "\u{10000}": "three" }))
      .resolves.toBe("829a3fd7e2d0b8a181fc05f90729311dee80aa61e3a94723a1cc4553dd4dd7c9");
  });

  it("canonicalizes scalar row keys identically to the Go runtime", async () => {
    const hashes = await syncRowsHashes([
      { id: 100000000000000000000, value: "large" },
      { id: 1e-7, value: "small" },
      { id: -0, value: "zero" },
      { id: true, value: "boolean" },
    ], "id");
    expect(Object.keys(hashes).sort()).toEqual([
      "0",
      "100000000000000000000",
      "1e-7",
      "true",
    ]);
  });

  it("persists identity scope metadata for authenticated warm reloads", async () => {
    const store = createStore();
    const directive: QueryCacheDirective = {
      protocolVersion: 1,
      scope: "scope-user-a-0000000000000000000000000000000000000000000000000000",
      epoch: "epoch-a-00000000000000000000000000000000000000000000000000000",
      maxAgeMs: 86_400_000,
    };

    await store.saveDirective("project-a\u0000tenant-a\u0000issuer-a\u0000user-a", directive);

    await expect(store.loadDirective("project-a\u0000tenant-a\u0000issuer-a\u0000user-a"))
      .resolves.toEqual(directive);
  });

  it("orders and bounds snapshots, then updates only delta rows", async () => {
    const store = createStore();
    const scope = "scope-a";
    const path = "tasks.recent";
    const args = { workspaceId: "workspace-a" };

    await store.replace(scope, path, args, {
      rows: [
        { id: "old", updatedAt: 1 },
        { id: "new", updatedAt: 3 },
        { id: "middle", updatedAt: 2 },
      ],
      cursor: { epoch: "sync-a", revision: 10 },
      keyField: "id",
      orderBy: "updatedAt",
      orderDirection: "desc",
      maxRows: 2,
    });

    await expect(store.load(scope, path, args)).resolves.toMatchObject({
      rows: [
        { id: "new", updatedAt: 3 },
        { id: "middle", updatedAt: 2 },
      ],
      cursor: { epoch: "sync-a", revision: 10 },
    });

    await store.applyDelta(scope, path, args, {
      cursor: { epoch: "sync-a", revision: 11 },
      keyField: "id",
      orderBy: "updatedAt",
      orderDirection: "desc",
      upserts: [
        { id: "middle", updatedAt: 5 },
        { id: "latest", updatedAt: 4 },
      ],
      deleted: ["new"],
      maxRows: 2,
    });

    await expect(store.load(scope, path, args)).resolves.toMatchObject({
      rows: [
        { id: "middle", updatedAt: 5 },
        { id: "latest", updatedAt: 4 },
      ],
      cursor: { epoch: "sync-a", revision: 11 },
    });
  });

  it("deletes only the requested collection", async () => {
    const store = createStore();
    const value = {
      rows: [{ id: "a" }],
      cursor: { epoch: "sync-a", revision: 1 },
      keyField: "id",
    };
    await store.replace("scope-a", "tasks.a", {}, value);
    await store.replace("scope-a", "tasks.b", {}, value);

    await store.delete("scope-a", "tasks.a", {});

    await expect(store.load("scope-a", "tasks.a", {})).resolves.toBeUndefined();
    await expect(store.load("scope-a", "tasks.b", {})).resolves.toMatchObject({ rows: [{ id: "a" }] });
  });

  it("never lets stale same-epoch or wrong-epoch deltas regress a persisted collection", async () => {
    const store = createStore();
    const args = { workspaceId: "workspace-a" };
    await store.replace("scope-a", "tasks.cached", args, {
      rows: [{ id: "current", value: 10 }],
      cursor: { epoch: "sync-a", revision: 10 },
      keyField: "id",
    });
    await store.replace("scope-a", "tasks.cached", args, {
      rows: [{ id: "stale", value: 9 }],
      cursor: { epoch: "sync-a", revision: 9 },
      keyField: "id",
    });
    await store.applyDelta("scope-a", "tasks.cached", args, {
      cursor: { epoch: "sync-a", revision: 8 },
      keyField: "id",
      upserts: [{ id: "stale-delta", value: 8 }],
      deleted: ["current"],
    });
    await store.applyDelta("scope-a", "tasks.cached", args, {
      cursor: { epoch: "wrong-definition", revision: 99 },
      keyField: "id",
      upserts: [{ id: "wrong-epoch", value: 99 }],
      deleted: ["current"],
    });

    await expect(store.load("scope-a", "tasks.cached", args)).resolves.toMatchObject({
      rows: [{ id: "current", value: 10 }],
      cursor: { epoch: "sync-a", revision: 10 },
    });
  });

  it("keeps 10k tasks bounded across 100 workspace windows", async () => {
    const store = new DexieSyncStore({
      databaseName: `gonvex-sync-stress-${crypto.randomUUID()}`,
      indexedDB,
      IDBKeyRange,
      maxBytes: 64 * 1024 * 1024,
    });
    stores.push(store);
    for (let workspace = 0; workspace < 100; workspace += 1) {
      const args = { workspaceId: `workspace-${workspace}` };
      await store.replace("scope-a", "tasks.cached", args, {
        rows: Array.from({ length: 100 }, (_, index) => ({
          id: `task-${workspace}-${index}`,
          updatedAt: index,
          title: `Task ${workspace}-${index}`,
          workspaceId: args.workspaceId,
        })),
        cursor: { epoch: "sync-a", revision: 1 },
        keyField: "id",
        orderBy: "updatedAt",
        orderDirection: "desc",
        maxRows: 100,
        maxBytes: 1024 * 1024,
      });
      await store.applyDelta("scope-a", "tasks.cached", args, {
        cursor: { epoch: "sync-a", revision: 2 },
        keyField: "id",
        orderBy: "updatedAt",
        orderDirection: "desc",
        upserts: Array.from({ length: 10 }, (_, index) => ({
          id: `burst-${workspace}-${index}`,
          updatedAt: 200 + index,
          title: `Burst ${workspace}-${index}`,
          workspaceId: args.workspaceId,
        })),
        deleted: Array.from({ length: 5 }, (_, index) => `task-${workspace}-${index}`),
        maxRows: 100,
        maxBytes: 1024 * 1024,
      });
    }

    const first = await store.load("scope-a", "tasks.cached", { workspaceId: "workspace-0" });
    const last = await store.load("scope-a", "tasks.cached", { workspaceId: "workspace-99" });
    expect(first?.rows).toHaveLength(100);
    expect(last?.rows).toHaveLength(100);
    expect(last?.rows[0]).toMatchObject({ id: "burst-99-9", updatedAt: 209 });
    expect(first?.rows.some((row: any) => row.id === "task-0-0")).toBe(false);
    expect(last?.cursor).toEqual({ epoch: "sync-a", revision: 2 });
  }, 60_000);
});
