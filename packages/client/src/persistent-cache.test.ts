import { describe, expect, it } from "vitest";
import { defineVisibleShape, type CacheQueryPlan } from "./cache";
import { BrowserPersistentCache } from "./persistent-cache";

const definition = defineVisibleShape({
  id: "tasks.visible",
  tables: ["tasks"],
  visibilityScope: "tenant",
  maxRows: 100,
  schemaVersion: "schema-1",
  permissionVersion: "perm-1",
});

const scope = {
  projectId: "project-a",
  tenantId: "tenant-a",
  userId: "user-a",
  authScope: "role:member",
};

const query: CacheQueryPlan = {
  queryKey: "tasks.list",
  queryPath: "tasks.list",
  requiredShapeId: definition.id,
  hasLocalPlan: true,
  coveredByShapeIds: [definition.id],
  schemaVersion: definition.schemaVersion,
  permissionVersion: definition.permissionVersion,
  ...scope,
};

describe("BrowserPersistentCache", () => {
  it("does not wait for background shape hydration before server routing", async () => {
    let resolveHydration: (() => void) | undefined;
    const hydrated = new Promise<void>((resolve) => {
      resolveHydration = resolve;
    });
    const cache = new BrowserPersistentCache();
    cache.metadata.setDBHealth("healthy");
    cache.coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    cache.coordinator.markActiveOwnerReady();

    cache.hydrateShapeInBackground(definition, scope, async () => {
      await hydrated;
      return {
        ...scope,
        id: definition.id,
        checkpoint: "checkpoint-1",
        rowCount: 10,
      };
    });

    expect(cache.decideQuerySource(query)).toMatchObject({
      source: "server",
      reason: "shape-not-ready",
    });

    resolveHydration?.();
    await hydrated;
    await Promise.resolve();

    expect(cache.decideQuerySource(query)).toMatchObject({
      source: "local",
      reason: "local-ready",
    });
  });

  it("keeps server routing when the coordinator has no verified active owner", () => {
    const cache = new BrowserPersistentCache();
    cache.metadata.setDBHealth("healthy");
    cache.metadata.updateShape({
      id: definition.id,
      ...scope,
      status: "ready",
      schemaVersion: definition.schemaVersion,
      permissionVersion: definition.permissionVersion,
      rowCount: 10,
      maxRows: 100,
    });
    cache.metadata.registerShape(definition);
    cache.coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });

    expect(cache.decideQuerySource(query)).toMatchObject({
      source: "server",
      reason: "db-owner-unsafe",
    });
  });

  it("disables local routing when browser ownership primitives are unavailable", () => {
    const cache = new BrowserPersistentCache();
    const capabilities = cache.disableIfUnsupported({
      sharedWorker: false,
      webLocks: true,
      dedicatedWorker: true,
      opfs: true,
      supported: false,
      disableReason: "shared-worker-unavailable",
    });

    expect(capabilities.disableReason).toBe("shared-worker-unavailable");
    expect(cache.coordinator.routeRequest()).toEqual({
      route: "server",
      reason: "unsafe-fallback-disabled",
    });
  });

  it("marks old scoped shapes stale when auth or tenant scope changes", () => {
    const cache = new BrowserPersistentCache();
    cache.metadata.registerShape(definition);
    cache.metadata.updateShape({
      id: definition.id,
      ...scope,
      status: "ready",
      schemaVersion: definition.schemaVersion,
      permissionVersion: definition.permissionVersion,
    });

    cache.quarantineScope({ ...scope, userId: "user-b" });

    expect(cache.metadata.environment(false).shapes[definition.id]).toMatchObject({
      status: "stale",
      userId: "user-a",
    });
  });
});
