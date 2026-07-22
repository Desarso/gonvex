export type FunctionKind =
  | "query"
  | "mutation"
  | "action"
  | "http"
  | "internalMutation"
  | "liveGrid";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type FunctionManifestEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
  dependencies?: FunctionDependencies;
};

export type FunctionDependencies = {
  reads?: Array<{ table: string; columns?: string[]; filters?: string[]; ordersBy?: string[]; windowed?: boolean; predicate?: string }>;
  writes?: Array<{ table: string; columns?: string[] }>;
  shareByPermissions?: boolean;
};

export type SubscriptionRevision = { epoch: string; sequence: number };

export type MessageTrace = {
  clientSentAtMs?: number;
  serverReceivedAtMs?: number;
  serverMutationStartedAtMs?: number;
  serverMutationCommittedAtMs?: number;
  serverCompletedAtMs?: number;
  serverBroadcastScheduledAtMs?: number;
  serverChangeCommittedAtMs?: number;
  serverSubscriptionStartedAtMs?: number;
  serverSubscriptionSentAtMs?: number;
  serverDurationMs?: number;
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
  epoch: string;
  maxAgeMs: number;
};

export type GonvexManifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionManifestEntry>;
  schema: Record<string, JsonValue>;
};

export type ClientMessage =
  | { type: "auth"; id: string; token?: string; project?: string; tenant?: string }
  | { type: "query.subscribe"; id: string; path: string; args: JsonValue }
  | { type: "query.unsubscribe"; id: string }
  | { type: "mutation.call"; id: string; path: string; args: JsonValue; trace?: MessageTrace }
  | { type: "action.call"; id: string; path: string; args: JsonValue; trace?: MessageTrace }
  | {
    type: "telemetry.event";
    id: string;
    kind: "query" | "mutation" | "action";
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
  | { type: "session.ready"; project?: string; tenant?: string; queryCache?: QueryCacheDirective }
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
  }
  | {
    type: "query.progress";
    id: string;
    path?: string;
    reason?: "initial" | "invalidate" | "recover";
    throughRevision: SubscriptionRevision;
    trace?: MessageTrace;
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
    cacheScope?: string;
    cacheRevision?: string;
    trace?: MessageTrace;
  }
  | { type: "query.error"; id: string; path?: string; error: string }
  | { type: "mutation.result"; id: string; path?: string; result: JsonValue; trace?: MessageTrace }
  | { type: "mutation.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "action.result"; id: string; path?: string; result: JsonValue; trace?: MessageTrace }
  | { type: "action.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "system.reload"; reason: string };
