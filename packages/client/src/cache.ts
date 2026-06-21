export type ShapeSyncStatus = "not_started" | "syncing" | "ready" | "stale" | "too_large" | "error";

export type CacheDBHealth = "healthy" | "initializing" | "unavailable" | "corrupt";

export type CacheRoutingSource = "local" | "server";

export type CacheRoutingReason =
  | "local-ready"
  | "query-has-no-local-plan"
  | "query-has-no-shape"
  | "shape-missing"
  | "shape-does-not-cover-query"
  | "shape-not-ready"
  | "shape-too-large"
  | "query-row-limit-unsafe"
  | "shape-auth-scope-mismatch"
  | "shape-epoch-mismatch"
  | "db-not-healthy"
  | "db-owner-unsafe";

export type CacheShapeVisibilityScope = "project" | "tenant" | "user" | "auth";

export type CacheShapeDefinition = {
  id: string;
  tables: string[];
  visibilityScope: CacheShapeVisibilityScope;
  maxRows?: number;
  maxBytes?: number;
  schemaVersion: string;
  permissionVersion: string;
};

export type CacheShapeState = {
  id: string;
  projectId: string;
  tenantId: string;
  userId: string;
  authScope: string;
  status: ShapeSyncStatus;
  schemaVersion: string;
  permissionVersion: string;
  checkpoint?: string;
  rowCount?: number;
  maxRows?: number;
};

export type CacheQueryPlan = {
  queryKey: string;
  queryPath?: string;
  projectId: string;
  tenantId: string;
  userId: string;
  authScope: string;
  requiredShapeId?: string;
  hasLocalPlan: boolean;
  schemaVersion: string;
  permissionVersion: string;
  coveredByShapeIds?: string[];
  estimatedRows?: number;
  limit?: number;
};

export type CacheEnvironment = {
  dbHealth: CacheDBHealth;
  singleWriter: boolean;
  shapes: Record<string, CacheShapeState | undefined>;
  definitions?: Record<string, CacheShapeDefinition | undefined>;
};

export type CacheRoutingDecision = {
  source: CacheRoutingSource;
  reason: CacheRoutingReason;
  shape?: CacheShapeState;
};

export type CacheFailureAction =
  | { action: "reset-and-server-fallback"; reason: "cache-corruption-suspected" }
  | { action: "server-fallback"; reason: "ordinary-failure" };

export function chooseQuerySource(query: CacheQueryPlan, environment: CacheEnvironment): CacheRoutingDecision {
  if (!query.hasLocalPlan) {
    return server("query-has-no-local-plan");
  }
  if (!query.requiredShapeId) {
    return server("query-has-no-shape");
  }
  if (environment.dbHealth !== "healthy") {
    return server("db-not-healthy");
  }
  if (!environment.singleWriter) {
    return server("db-owner-unsafe");
  }

  const shape = environment.shapes[query.requiredShapeId];
  if (!shape) {
    return server("shape-missing");
  }
  const definition = environment.definitions?.[query.requiredShapeId];
  if (!shapeCoversQuery(query, definition)) {
    return server("shape-does-not-cover-query", shape);
  }
  if (shape.status === "too_large") {
    return server("shape-too-large", shape);
  }
  if (shape.status !== "ready") {
    return server("shape-not-ready", shape);
  }
  if (!sameScope(query, shape)) {
    return server("shape-auth-scope-mismatch", shape);
  }
  if (shape.schemaVersion !== query.schemaVersion || shape.permissionVersion !== query.permissionVersion) {
    return server("shape-epoch-mismatch", shape);
  }
  if (shape.maxRows !== undefined && shape.rowCount !== undefined && shape.rowCount > shape.maxRows) {
    return server("shape-too-large", shape);
  }
  if (!queryLimitIsSafe(query, shape, definition)) {
    return server("query-row-limit-unsafe", shape);
  }

  return { source: "local", reason: "local-ready", shape };
}

export function defineVisibleShape(definition: CacheShapeDefinition): CacheShapeDefinition {
  if (!definition.id.trim()) {
    throw new Error("cache shape id is required");
  }
  if (definition.tables.length === 0 || definition.tables.some((table) => !table.trim())) {
    throw new Error(`cache shape ${definition.id} must include at least one table`);
  }
  if (definition.maxRows !== undefined && definition.maxRows <= 0) {
    throw new Error(`cache shape ${definition.id} maxRows must be positive`);
  }
  if (definition.maxBytes !== undefined && definition.maxBytes <= 0) {
    throw new Error(`cache shape ${definition.id} maxBytes must be positive`);
  }
  return {
    ...definition,
    tables: [...definition.tables],
  };
}

export function cacheKey(parts: {
  projectId: string;
  tenantId: string;
  userId: string;
  authScope: string;
  queryPath: string;
  argsHash: string;
  schemaVersion: string;
  permissionVersion: string;
}) {
  return [
    "gonvex",
    parts.projectId,
    parts.tenantId,
    parts.userId,
    parts.authScope,
    parts.queryPath,
    parts.argsHash,
    parts.schemaVersion,
    parts.permissionVersion,
  ].map(escapeKeyPart).join(":");
}

export function shouldResetLocalCache(error: unknown) {
  if (!(error instanceof Error)) {
    return false;
  }
  const message = error.message.toLowerCase();
  return message.includes("corrupt")
    || message.includes("integrity")
    || message.includes("checkpoint")
    || message.includes("schema mismatch");
}

export function cacheFailureAction(error: unknown): CacheFailureAction {
  if (shouldResetLocalCache(error)) {
    return { action: "reset-and-server-fallback", reason: "cache-corruption-suspected" };
  }
  return { action: "server-fallback", reason: "ordinary-failure" };
}

function sameScope(query: CacheQueryPlan, shape: CacheShapeState) {
  return shape.projectId === query.projectId
    && shape.tenantId === query.tenantId
    && shape.userId === query.userId
    && shape.authScope === query.authScope;
}

function shapeCoversQuery(query: CacheQueryPlan, definition: CacheShapeDefinition | undefined) {
  if (query.coveredByShapeIds) {
    return query.coveredByShapeIds.includes(query.requiredShapeId ?? "");
  }
  return definition !== undefined;
}

function queryLimitIsSafe(
  query: CacheQueryPlan,
  shape: CacheShapeState,
  definition: CacheShapeDefinition | undefined,
) {
  const maxRows = shape.maxRows ?? definition?.maxRows;
  if (maxRows === undefined) {
    return true;
  }
  if (query.limit !== undefined) {
    return query.limit <= maxRows;
  }
  if (query.estimatedRows !== undefined) {
    return query.estimatedRows <= maxRows;
  }
  return shape.rowCount === undefined || shape.rowCount <= maxRows;
}

function server(reason: CacheRoutingReason, shape?: CacheShapeState): CacheRoutingDecision {
  return { source: "server", reason, shape };
}

function escapeKeyPart(part: string) {
  return encodeURIComponent(part);
}
