import {
  chooseQuerySource,
  type CacheDBHealth,
  type CacheEnvironment,
  type CacheQueryPlan,
  type CacheRoutingDecision,
  type CacheShapeDefinition,
  type CacheShapeState,
} from "./cache.js";
import { CacheCoordinatorModel } from "./cache-coordinator.js";
import { detectBrowserCacheCapabilities, type BrowserCacheCapabilities } from "./browser-capabilities.js";

export type ShapeHydrationResult = Omit<CacheShapeState, "status"> & {
  status?: CacheShapeState["status"];
};

export type ShapeHydrator = (definition: CacheShapeDefinition) => Promise<ShapeHydrationResult>;

export class CacheMetadataStore {
  private dbHealth: CacheDBHealth = "initializing";
  private readonly definitions = new Map<string, CacheShapeDefinition>();
  private readonly shapes = new Map<string, CacheShapeState>();

  setDBHealth(health: CacheDBHealth) {
    this.dbHealth = health;
  }

  getDBHealth() {
    return this.dbHealth;
  }

  registerShape(definition: CacheShapeDefinition) {
    this.definitions.set(definition.id, definition);
  }

  markShapeSyncing(definition: CacheShapeDefinition, scope: Pick<CacheShapeState, "projectId" | "tenantId" | "userId" | "authScope">) {
    this.registerShape(definition);
    this.shapes.set(definition.id, {
      id: definition.id,
      ...scope,
      status: "syncing",
      schemaVersion: definition.schemaVersion,
      permissionVersion: definition.permissionVersion,
      maxRows: definition.maxRows,
    });
  }

  updateShape(shape: CacheShapeState) {
    this.shapes.set(shape.id, { ...shape });
  }

  markAllShapesStale() {
    for (const [id, shape] of this.shapes) {
      this.shapes.set(id, { ...shape, status: "stale" });
    }
  }

  environment(singleWriter: boolean): CacheEnvironment {
    return {
      dbHealth: this.dbHealth,
      singleWriter,
      shapes: Object.fromEntries(this.shapes),
      definitions: Object.fromEntries(this.definitions),
    };
  }

  shapeStates() {
    return [...this.shapes.values()].map((shape) => ({ ...shape }));
  }
}

export class BrowserPersistentCache {
  constructor(
    readonly metadata: CacheMetadataStore = new CacheMetadataStore(),
    readonly coordinator: CacheCoordinatorModel = new CacheCoordinatorModel(),
  ) {}

  decideQuerySource(query: CacheQueryPlan): CacheRoutingDecision {
    const route = this.coordinator.routeRequest();
    return chooseQuerySource(query, this.metadata.environment(route.route === "active-worker"));
  }

  disableIfUnsupported(capabilities: BrowserCacheCapabilities = detectBrowserCacheCapabilities()) {
    if (!capabilities.supported) {
      this.coordinator.disableUnsafeFallback();
    }
    return capabilities;
  }

  hydrateShapeInBackground(
    definition: CacheShapeDefinition,
    scope: Pick<CacheShapeState, "projectId" | "tenantId" | "userId" | "authScope">,
    hydrate: ShapeHydrator,
  ) {
    this.metadata.markShapeSyncing(definition, scope);
    void hydrate(definition).then((shape) => {
      this.metadata.updateShape({
        ...shape,
        id: definition.id,
        projectId: shape.projectId,
        tenantId: shape.tenantId,
        userId: shape.userId,
        authScope: shape.authScope,
        status: shape.status ?? "ready",
        schemaVersion: definition.schemaVersion,
        permissionVersion: definition.permissionVersion,
        maxRows: definition.maxRows,
      });
    }).catch(() => {
      this.metadata.updateShape({
        id: definition.id,
        ...scope,
        status: "error",
        schemaVersion: definition.schemaVersion,
        permissionVersion: definition.permissionVersion,
        maxRows: definition.maxRows,
      });
    });
  }

  quarantineScope(nextScope: Pick<CacheShapeState, "projectId" | "tenantId" | "userId" | "authScope">) {
    for (const shape of this.metadata.shapeStates()) {
      const scopeMatches = shape.projectId === nextScope.projectId
        && shape.tenantId === nextScope.tenantId
        && shape.userId === nextScope.userId
        && shape.authScope === nextScope.authScope;
      if (!scopeMatches) {
        this.metadata.updateShape({ ...shape, status: "stale" });
      }
    }
  }
}
