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
};

export type GonvexManifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionManifestEntry>;
  schema: Record<string, JsonValue>;
};

export type ClientMessage =
  | { type: "auth"; id: string; token?: string; tenant?: string }
  | { type: "query.subscribe"; id: string; path: string; args: JsonValue }
  | { type: "query.unsubscribe"; id: string }
  | { type: "mutation.call"; id: string; path: string; args: JsonValue }
  | { type: "action.call"; id: string; path: string; args: JsonValue };

export type ServerMessage =
  | { type: "auth.result"; id: string; result: JsonValue }
  | { type: "auth.error"; id: string; error: string }
  | { type: "query.result"; id: string; result: JsonValue; reason?: "initial" | "invalidate" }
  | { type: "query.error"; id: string; error: string }
  | { type: "mutation.result"; id: string; result: JsonValue }
  | { type: "mutation.error"; id: string; error: string }
  | { type: "action.result"; id: string; result: JsonValue }
  | { type: "action.error"; id: string; error: string }
  | { type: "system.reload"; reason: string };
