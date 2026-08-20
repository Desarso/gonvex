// Manifest shapes shared by the Go source-bundle pipeline and the
// language-neutral module artifact pipeline. The runtime consumes the manifest
// as JSON, so every field here mirrors `pkg/manifest` on the Go side.

export type FunctionKind = "query" | "reducer" | "action" | "http";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type FunctionEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
  /** Language-neutral argument metadata from a TypeScript module artifact. */
  args?: ModuleSchema;
  /** Language-neutral result metadata from a TypeScript module artifact. */
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
};

export type ReadDependency = {
  table: string;
  columns?: string[];
  filters?: string[];
  ordersBy?: string[];
  windowed?: boolean;
};

export type FunctionDependencies = {
  reads?: ReadDependency[];
  shareByPermissions?: boolean;
  shareByVisibility?: string;
  shareResultFrom?: string;
  shareResultField?: string;
  liveQueryPlan?: LiveQueryPlan;
  nonOptimisticReason?: string;
  optimisticReducer?: OptimisticReducerDefinition;
  optimisticProjection?: OptimisticProjectionDefinition;
};

export type LiveQueryPlan = {
  table: string;
  key: string;
  columns?: string[];
  resultPath?: string[];
  where?: LiveExpression;
  search?: { argument: string; columns: string[] };
  sort?: { columnArgument: string; directionArgument: string; defaultColumn: string; defaultDirection: string; allowedColumns: string[] };
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

export type LiveValue = { argument?: string; literal?: unknown };

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

export type Column = {
  type: string;
  nullable: boolean;
  primaryKey: boolean;
};

export type Table = {
  columns: Record<string, Column>;
  indexes: Record<string, { columns: string[]; unique: boolean; kind?: string }>;
};

export type SchemaDefinition = {
  tables: Record<string, Table>;
  /** Preferred terminology; emitted alongside landlordTables during cutover. */
  controlPlaneTables?: Record<string, Table>;
  /** Legacy wire key retained until every deployed runtime is v2-aware. */
  landlordTables: Record<string, Table>;
  tenantTables: Record<string, Table>;
};

export type Manifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionEntry>;
  schema: SchemaDefinition;
  bundle?: SourceBundle;
  // Emitted for module-artifact projects (TypeScript today). Go projects keep
  // shipping `bundle` alone so the runtime sees the exact payload it does now.
  module?: ModuleArtifact;
};

export type SourceBundle = {
  hash: string;
  modulePath: string;
  packageName: string;
  goVersion?: string;
  files: Record<string, string>;
};

/** Languages a server module can be authored in. */
export type ModuleLanguage = "go" | "typescript";

/**
 * Argument and result shapes are placeholders until codegen can lower real
 * TypeScript types into JSON Schema. `type` carries the declared type text so
 * the runtime (and later generations of this pipeline) can resolve it.
 */
export type ModuleSchema = {
  placeholder: true;
  type?: string;
};

export type ModuleFunction = {
  kind: FunctionKind;
  /** Symbol the runtime invokes inside the module. */
  handler: string;
  /** Project-relative POSIX path, matching a key in `ModuleArtifact.files`. */
  file: string;
  /** Exported binding when the function is defined as an export. */
  export?: string;
  args: ModuleSchema;
  result: ModuleSchema;
  dependencies?: FunctionDependencies;
  internal?: boolean;
  delivery?: "oneShot" | "live" | "replica";
  replica?: ReplicaCollectionDefinition;
  // Declarative metadata is passed through verbatim: the CLI does not
  // interpret delivery, offline, or optimistic policies, the runtime does.
  offline?: JsonValue;
  optimistic?: JsonValue;
};

/** Self-contained JavaScript generated by the CLI for the module runtime. */
export type ModuleJavaScript = {
  /** Project-relative POSIX path written by the module bundler. */
  path: string;
  hash: string;
  /** base64-encoded bundle contents. */
  code: string;
  /** base64-encoded source map when bundle generation enables one. */
  sourceMap?: string;
};

export type ModuleArtifact = {
  language: ModuleLanguage;
  /** Artifact format generation; bumped when the layout changes. */
  generation: number;
  /** Deterministic content hash over the generation, language, and payload. */
  hash: string;
  /** Project-relative POSIX path of the module entrypoint. */
  entrypoint: string;
  functions: Record<string, ModuleFunction>;
  /** Project-relative POSIX path to base64 contents, in sorted key order. */
  files: Record<string, string>;
  javascript?: ModuleJavaScript;
};
