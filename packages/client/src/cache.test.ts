import { describe, expect, it } from "vitest";
import {
  cacheKey,
  cacheFailureAction,
  chooseQuerySource,
  defineVisibleShape,
  shouldResetLocalCache,
  type CacheEnvironment,
  type CacheQueryPlan,
  type CacheShapeState,
} from "./cache";

const query: CacheQueryPlan = {
  queryKey: "tasks.grid/open",
  projectId: "whagons-5",
  tenantId: "calaluna",
  userId: "user-1",
  authScope: "roles:a",
  requiredShapeId: "tasks.visible",
  hasLocalPlan: true,
  schemaVersion: "schema-1",
  permissionVersion: "perm-1",
};

const readyShape: CacheShapeState = {
  id: "tasks.visible",
  projectId: query.projectId,
  tenantId: query.tenantId,
  userId: query.userId,
  authScope: query.authScope,
  status: "ready",
  schemaVersion: query.schemaVersion,
  permissionVersion: query.permissionVersion,
  checkpoint: "checkpoint-1",
  rowCount: 20_000,
  maxRows: 50_000,
};

const healthyEnvironment: CacheEnvironment = {
  dbHealth: "healthy",
  singleWriter: true,
  shapes: { [readyShape.id]: readyShape },
  definitions: {
    [readyShape.id]: defineVisibleShape({
      id: readyShape.id,
      tables: ["tasks"],
      visibilityScope: "tenant",
      maxRows: readyShape.maxRows,
      schemaVersion: query.schemaVersion,
      permissionVersion: query.permissionVersion,
    }),
  },
};

describe("chooseQuerySource", () => {
  it("routes local only when the shape is ready and scope-safe", () => {
    expect(chooseQuerySource(query, healthyEnvironment)).toMatchObject({
      source: "local",
      reason: "local-ready",
    });
  });

  it("routes server when the query has no local plan", () => {
    expect(chooseQuerySource({ ...query, hasLocalPlan: false }, healthyEnvironment)).toMatchObject({
      source: "server",
      reason: "query-has-no-local-plan",
    });
  });

  it("routes server when the query has no backing shape", () => {
    expect(chooseQuerySource({ ...query, requiredShapeId: undefined }, healthyEnvironment)).toMatchObject({
      source: "server",
      reason: "query-has-no-shape",
    });
  });

  it("routes server when the declared shape does not fully cover the query", () => {
    expect(chooseQuerySource({ ...query, coveredByShapeIds: ["tasks.other"] }, healthyEnvironment)).toMatchObject({
      source: "server",
      reason: "shape-does-not-cover-query",
    });
  });

  it("routes server when the local database is still initializing", () => {
    expect(chooseQuerySource(query, { ...healthyEnvironment, dbHealth: "initializing" })).toMatchObject({
      source: "server",
      reason: "db-not-healthy",
    });
  });

  it("routes server when corruption is detected", () => {
    expect(chooseQuerySource(query, { ...healthyEnvironment, dbHealth: "corrupt" })).toMatchObject({
      source: "server",
      reason: "db-not-healthy",
    });
  });

  it("routes server when there is no single safe writer", () => {
    expect(chooseQuerySource(query, { ...healthyEnvironment, singleWriter: false })).toMatchObject({
      source: "server",
      reason: "db-owner-unsafe",
    });
  });

  it("routes server when the shape has not been synced yet", () => {
    expect(chooseQuerySource(query, { ...healthyEnvironment, shapes: {} })).toMatchObject({
      source: "server",
      reason: "shape-missing",
    });
  });

  it("routes server while shape sync is in progress", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, status: "syncing" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-not-ready",
    });
  });

  it("routes server when a shape is stale", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, status: "stale" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-not-ready",
    });
  });

  it("routes server when the shape is too large", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, status: "too_large" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-too-large",
    });
  });

  it("routes server when row count exceeds the declared max", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, rowCount: 50_001 } },
    })).toMatchObject({
      source: "server",
      reason: "shape-too-large",
    });
  });

  it("routes server when the query limit exceeds the safe shape limit", () => {
    expect(chooseQuerySource({ ...query, limit: 50_001 }, healthyEnvironment)).toMatchObject({
      source: "server",
      reason: "query-row-limit-unsafe",
    });
  });

  it("routes server on tenant mismatch so cache cannot leak across tenants", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, tenantId: "other-tenant" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-auth-scope-mismatch",
    });
  });

  it("routes server on project mismatch so cache cannot leak across projects", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, projectId: "other-project" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-auth-scope-mismatch",
    });
  });

  it("routes server on user mismatch so cache cannot leak across users", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, userId: "user-2" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-auth-scope-mismatch",
    });
  });

  it("routes server on auth scope mismatch so role changes cannot reuse old rows", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, authScope: "roles:b" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-auth-scope-mismatch",
    });
  });

  it("routes server when permission version changes", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, permissionVersion: "perm-2" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-epoch-mismatch",
    });
  });

  it("routes server when schema version changes", () => {
    expect(chooseQuerySource(query, {
      ...healthyEnvironment,
      shapes: { [readyShape.id]: { ...readyShape, schemaVersion: "schema-2" } },
    })).toMatchObject({
      source: "server",
      reason: "shape-epoch-mismatch",
    });
  });
});

describe("defineVisibleShape", () => {
  it("normalizes backend-defined shape metadata", () => {
    const shape = defineVisibleShape({
      id: "tasks.visible",
      tables: ["tasks"],
      visibilityScope: "tenant",
      maxRows: 10,
      schemaVersion: "schema-1",
      permissionVersion: "perm-1",
    });
    expect(shape.tables).toEqual(["tasks"]);
  });

  it("rejects shapes without table coverage", () => {
    expect(() => defineVisibleShape({
      id: "empty",
      tables: [],
      visibilityScope: "tenant",
      schemaVersion: "schema-1",
      permissionVersion: "perm-1",
    })).toThrow("at least one table");
  });
});

describe("cacheKey", () => {
  it("includes project, tenant, user, auth, query, args, schema, and permission scope", () => {
    const first = cacheKey({
      projectId: "p",
      tenantId: "t",
      userId: "u",
      authScope: "role:a",
      queryPath: "tasks.grid",
      argsHash: "args",
      schemaVersion: "schema",
      permissionVersion: "perm",
    });
    const second = cacheKey({
      projectId: "p",
      tenantId: "t2",
      userId: "u",
      authScope: "role:a",
      queryPath: "tasks.grid",
      argsHash: "args",
      schemaVersion: "schema",
      permissionVersion: "perm",
    });
    expect(first).not.toBe(second);
    expect(first).toContain("tasks.grid");
  });

  it("escapes separators so key parts cannot collide", () => {
    expect(cacheKey({
      projectId: "p:a",
      tenantId: "t/b",
      userId: "u",
      authScope: "role:a",
      queryPath: "tasks.grid",
      argsHash: "args",
      schemaVersion: "schema",
      permissionVersion: "perm",
    })).toContain("p%3Aa:t%2Fb");
  });
});

describe("shouldResetLocalCache", () => {
  it("resets cache on corruption-like errors", () => {
    expect(shouldResetLocalCache(new Error("database disk image is corrupt"))).toBe(true);
    expect(shouldResetLocalCache(new Error("checkpoint mismatch"))).toBe(true);
    expect(shouldResetLocalCache(new Error("schema mismatch"))).toBe(true);
    expect(shouldResetLocalCache(new Error("integrity check failed"))).toBe(true);
  });

  it("does not reset cache for ordinary network failures", () => {
    expect(shouldResetLocalCache(new Error("websocket disconnected"))).toBe(false);
    expect(shouldResetLocalCache("corrupt")).toBe(false);
  });
});

describe("cacheFailureAction", () => {
  it("turns corruption signals into reset plus server fallback", () => {
    expect(cacheFailureAction(new Error("sqlite integrity check failed"))).toEqual({
      action: "reset-and-server-fallback",
      reason: "cache-corruption-suspected",
    });
  });

  it("keeps ordinary failures on server fallback without clearing scoped cache", () => {
    expect(cacheFailureAction(new Error("server timeout"))).toEqual({
      action: "server-fallback",
      reason: "ordinary-failure",
    });
  });
});
