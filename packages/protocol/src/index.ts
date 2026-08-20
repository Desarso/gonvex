export type FunctionKind = "query" | "reducer" | "action";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

/** Opaque, language-neutral function argument/result metadata. */
export type ModuleSchema = {
  placeholder: true;
  type?: string;
};

export type FunctionManifestEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
  args?: ModuleSchema;
  result?: ModuleSchema;
  internal?: boolean;
  delivery?: "oneShot" | "live" | "replica";
  dependencies?: FunctionDependencies;
  replica?: ReplicaCollectionDefinition;
  /** Reducer delivery policy declared by a TypeScript module. */
  offline?: JsonValue;
  /** Ordered atomic optimistic transaction declared by a TypeScript module. */
  optimistic?: JsonValue;
};

export type ReplicaCollectionDefinition = {
  table: string;
  key: string;
  columns: string[];
  equalFilters?: Record<string, string>;
  excludeWhenSet?: string[];
  visibilityTables?: string[];
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  mode?: "eager" | "progressive";
  maxRows?: number;
  maxBytes?: number;
  retentionMs?: number;
};

export type ReplicaCursor = {
  epoch: string;
  revision: number;
};

export type ReplicaChange = {
  entity: string;
  id: string;
  operation: "insert" | "update" | "delete";
  oldValue?: JsonValue;
  newValue?: JsonValue;
  changedColumns?: string[];
};

export type FunctionDependencies = {
	reads?: Array<{ table: string; columns?: string[]; filters?: string[]; ordersBy?: string[]; windowed?: boolean }>;
	shareByPermissions?: boolean;
	liveQueryPlan?: LiveQueryPlan;
	nonOptimisticReason?: string;
	optimisticReducer?: OptimisticReducerDefinition;
	optimisticProjection?: OptimisticProjectionDefinition;
	shareResultFrom?: string;
	shareResultField?: string;
};

/** Legacy single-patch optimistic reducer contract retained for v1 clients. */
export type OptimisticReducerDefinition = {
  entity: string;
  rowIdPath: string[];
  fieldsPath: string[];
};

export type OptimisticProjectionDefinition = {
  entity: string;
  key: string;
  resultPath: string[];
};

export type LiveQueryPlan = {
  table: string;
  key: string;
  columns?: string[];
  resultPath?: string[];
  where?: LiveExpression;
  search?: { argument: string; columns: string[] };
  sort?: { columnArgument: string; directionArgument: string; defaultColumn: string; defaultDirection: "asc" | "desc"; allowedColumns: string[] };
  window?: { offsetArgument: string; limitArgument: string; defaultLimit: number; maxLimit: number };
  serverOnly?: boolean;
};

export type LiveExpression = {
  operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "contains" | "containsInsensitive" | "range" | "and" | "or" | "not" | "server";
  column?: string;
  value?: LiveValue;
  valueTo?: LiveValue;
  children?: LiveExpression[];
};

export type LiveValue = { argument?: string; literal?: JsonValue };

export type SubscriptionRevision = { epoch: string; sequence: number };

export type MessageTrace = {
  clientSentAtMs?: number;
  serverReceivedAtMs?: number;
  serverReducerStartedAtMs?: number;
  serverReducerCommittedAtMs?: number;
  serverCompletedAtMs?: number;
  serverBroadcastScheduledAtMs?: number;
  serverChangeCommittedAtMs?: number;
  serverSubscriptionStartedAtMs?: number;
  serverSubscriptionSentAtMs?: number;
  serverDurationMs?: number;
  /** Non-semantic top-level query performance metadata from result.perf. */
  queryPerf?: JsonValue;
};

export type BrowserTelemetryInfo = {
  userAgent?: string;
  browserName?: string;
  browserVersion?: string;
  deviceType?: string;
  platform?: string;
  language?: string;
  timezone?: string;
  viewportWidth?: number;
  viewportHeight?: number;
  hardwareConcurrency?: number;
  deviceMemory?: number;
  touchPoints?: number;
  connectionType?: string;
  effectiveConnectionType?: string;
};

export type QueryCacheDirective = {
  protocolVersion: 1;
  scope: string;
  /**
   * Visibility-only scope (project/tenant/user/permissions, no code epoch)
   * under which sync collections are persisted and resumed. Unlike `scope`,
   * it survives deploys; resume safety comes from the server's authoritative
   * reconcile. Absent on runtimes that predate deploy-stable sync scopes.
   */
  syncScope?: string;
  epoch: string;
  maxAgeMs: number;
};

export type ServerCapabilities = {
  /** WebSocket protocol generation implemented by this runtime. */
  protocolVersion?: number;
  /** Exact runtime build identifier, normally the deployed Git commit SHA. */
  runtimeVersion?: string;
  syncBatch?: 1;
  /** sync.ready frames always carry a collection integrity digest. */
  syncIntegrity?: 1;
  /** Server accepts `query.subscribeMany` batched subscription frames. */
  queryBatch?: 1;
	/** Server emits several independent query updates in one WebSocket frame. */
	queryResultBatch?: 1;
  /** Server accepts `reducer.callMany` batched command-outbox flushes. */
  reducerBatch?: 1;
  /** Server emits connection-level sync revision watermarks. */
  syncWatermark?: 1;
};

export type QuerySubscribeRequest = {
  id: string;
  path: string;
  args: JsonValue;
  cacheRevision?: string;
};

export type ReducerCallRequest = {
  id: string;
  path: string;
  args: JsonValue;
  trace?: MessageTrace;
  /** Stable key for a replayable command; see the `reducer.call` message. */
  idempotencyKey?: string;
};

export type SyncOpenRequest = {
  id: string;
  path: string;
  args: JsonValue;
  cursor?: ReplicaCursor;
  keys?: string[];
  hashes?: Record<string, string>;
  digest?: string;
  fullIntegrity?: boolean;
};

export type SyncReady = {
  id: string;
  path?: string;
  cursor: ReplicaCursor;
  mode?: "eager" | "progressive";
  digest?: string;
  truncated?: boolean;
};

export type ClientCapabilities = {
  /** Client accepts coalesced `sync.readyMany` server frames. */
  syncReadyMany?: 1;
  /** Client accepts connection-level `sync.watermark` server frames. */
  syncWatermark?: 1;
	/** Client accepts keyed patches for object results with a `page` row array. */
	queryPagePatch?: 1;
	/** Client atomically applies keyed patches to named arrays in object results. */
	queryObjectPatch?: 1;
	/** Client applies compact front/back order deltas on keyed patches. */
	queryOrderDelta?: 1;
	/** Client accepts one query payload fanned out to multiple subscription IDs. */
	queryFanout?: 1;
	/** Client accepts several independent query updates in one WebSocket frame. */
	queryResultBatch?: 1;
};

export type KeyedCollectionPatch = {
	inserted?: JsonValue[];
	updated?: JsonValue[];
	deleted?: string[];
	order?: string[];
	prepend?: string[];
	append?: string[];
};

export type GonvexManifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionManifestEntry>;
  schema: Record<string, JsonValue>;
};

export type ClientMessage =
  | { type: "auth"; id: string; token?: string; project?: string; tenant?: string; device?: BrowserTelemetryInfo; capabilities?: ClientCapabilities }
  | { type: "query.call"; id: string; path: string; args: JsonValue }
  | { type: "query.subscribe"; id: string; path: string; args: JsonValue; cacheRevision?: string }
  | { type: "query.unsubscribe"; id: string }
  | {
    type: "sync.open";
    id: string;
    path: string;
    args: JsonValue;
    cursor?: ReplicaCursor;
    keys?: string[];
    hashes?: Record<string, string>;
    digest?: string;
    fullIntegrity?: boolean;
  }
  | { type: "sync.openMany"; opens: SyncOpenRequest[] }
  | { type: "query.subscribeMany"; subscribes: QuerySubscribeRequest[] }
  | { type: "sync.close"; id: string }
  | {
    type: "reducer.call";
    id: string;
    path: string;
    args: JsonValue;
    trace?: MessageTrace;
    /**
     * Stable key for a replayable command from the client outbox. The runtime
     * executes the reducer once per key and serves the stored result to
     * every duplicate delivery.
     */
    idempotencyKey?: string;
  }
  | { type: "reducer.callMany"; calls: ReducerCallRequest[] }
  | { type: "action.call"; id: string; path: string; args: JsonValue; trace?: MessageTrace }
  | {
    type: "telemetry.event";
    id: string;
    kind: "query" | "reducer" | "action";
    path: string;
    reason?: "initial" | "invalidate" | "recover";
    outcome: "ok" | "error";
    error?: string;
    clientSentAtMs?: number;
    clientReceivedAtMs: number;
    clientDurationMs?: number;
    trace?: MessageTrace;
    device?: BrowserTelemetryInfo;
  };

export type ServerMessage =
	| {
		type: "query.batch";
		messages: ServerMessage[];
	}
	| {
		type: "query.fanout";
		queryType: "query.result" | "query.progress" | "query.patch" | "query.pagePatch" | "query.objectPatch";
		ids: string[];
		path?: string;
		result?: JsonValue;
		reason?: "initial" | "invalidate" | "recover";
		trace?: MessageTrace;
		cacheScope?: string;
		cacheRevision?: string;
		subscriptionRevision?: SubscriptionRevision;
		baseRevision?: SubscriptionRevision;
		throughRevision?: SubscriptionRevision;
		inserted?: JsonValue[];
		updated?: JsonValue[];
		deleted?: string[];
		order?: string[];
		prepend?: string[];
		append?: string[];
		collections?: Record<string, KeyedCollectionPatch>;
		originCommandIds?: string[];
	}
  | {
    type: "replica.transaction";
    cursor: ReplicaCursor;
    originCommandId?: string;
    changes: ReplicaChange[];
  }
  | {
    type: "session.ready";
    project?: string;
    tenant?: string;
    queryCache?: QueryCacheDirective;
    capabilities?: ServerCapabilities;
  }
  | { type: "session.scope"; queryCache?: QueryCacheDirective }
  | { type: "auth.result"; id: string; result: JsonValue }
  | { type: "auth.error"; id: string; error: string }
  | {
    type: "query.result";
    id: string;
    path?: string;
    result: JsonValue;
    reason?: "initial" | "invalidate" | "recover";
    trace?: MessageTrace;
    cacheScope?: string;
    cacheRevision?: string;
    subscriptionRevision?: SubscriptionRevision;
    originCommandIds?: string[];
  }
  | {
    type: "query.progress";
    id: string;
    path?: string;
    reason?: "initial" | "invalidate" | "recover";
    throughRevision: SubscriptionRevision;
    trace?: MessageTrace;
    originCommandIds?: string[];
  }
  | {
    type: "query.patch";
    id: string;
    path?: string;
    reason?: "initial" | "invalidate" | "recover";
    baseRevision: SubscriptionRevision;
    subscriptionRevision: SubscriptionRevision;
    inserted?: JsonValue[];
    updated?: JsonValue[];
    deleted?: string[];
    order?: string[];
		prepend?: string[];
		append?: string[];
    cacheScope?: string;
    cacheRevision?: string;
    trace?: MessageTrace;
    originCommandIds?: string[];
  }
	| {
		type: "query.pagePatch";
		id: string;
		path?: string;
		reason?: "initial" | "invalidate" | "recover";
		baseRevision: SubscriptionRevision;
		subscriptionRevision: SubscriptionRevision;
		result?: JsonValue;
		inserted?: JsonValue[];
		updated?: JsonValue[];
		deleted?: string[];
		order?: string[];
		prepend?: string[];
		append?: string[];
		cacheScope?: string;
		cacheRevision?: string;
		trace?: MessageTrace;
		originCommandIds?: string[];
	}
	| {
		type: "query.objectPatch";
		id: string;
		path?: string;
		reason?: "initial" | "invalidate" | "recover";
		baseRevision: SubscriptionRevision;
		subscriptionRevision: SubscriptionRevision;
		collections: Record<string, KeyedCollectionPatch>;
		cacheScope?: string;
		cacheRevision?: string;
		trace?: MessageTrace;
		originCommandIds?: string[];
	}
  | {
    type: "sync.snapshot";
    id: string;
    path?: string;
    result: JsonValue[];
    cursor: ReplicaCursor;
    key: string;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    mode?: "eager" | "progressive";
    maxRows?: number;
    maxBytes?: number;
    hashes?: Record<string, string>;
  }
  | {
    type: "sync.delta";
    id: string;
    path?: string;
    cursor: ReplicaCursor;
    upserts?: JsonValue[];
    deleted?: string[];
    originCommandIds?: string[];
    hashes?: Record<string, string>;
    digest?: string;
  }
  | ({ type: "sync.ready"; digest?: string } & SyncReady)
  | { type: "sync.readyMany"; ready: SyncReady[] }
  | { type: "sync.watermark"; revision: number }
  | { type: "sync.needHashes"; id: string; path?: string }
  // Client-local status frame emitted when a formerly authoritative materialized
  // collection must be reconciled before it can be trusted again.
  | {
    type: "sync.syncing";
    id: string;
    path?: string;
    reason: "disconnected" | "reconciling" | "listener-reconnecting" | "integrity-reconciling";
  }
  | {
    type: "sync.reset";
    id: string;
    path?: string;
    reason: "cursor-expired" | "definition-changed" | "visibility-changed" | "integrity-mismatch" | "integrity-missing" | "recover";
  }
  | { type: "sync.error"; id: string; path?: string; error: string }
  | { type: "query.error"; id: string; path?: string; error: string }
  | { type: "reducer.result"; id: string; path?: string; result: JsonValue; originCommandId: string; committedRevision?: number; trace?: MessageTrace }
  | { type: "reducer.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "action.result"; id: string; path?: string; result: JsonValue; trace?: MessageTrace }
  | { type: "action.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "system.reload"; reason: string };
