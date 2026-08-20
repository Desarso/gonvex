/** JSON values accepted by the module ABI. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type JsonObject = { [key: string]: JsonValue };

export type ModuleLanguage = "typescript";
export type ModuleEngine = "v8";

export type PortableSchema =
  | StringSchema
  | NumberSchema
  | BooleanSchema
  | NullSchema
  | AnySchema
  | IdSchema
  | LiteralSchema
  | ArraySchema
  | ObjectSchema
  | RecordSchema
  | OptionalSchema;

export type StringSchema = {
  readonly kind: "string";
  readonly format?: "email" | "uri" | "uuid" | "datetime";
  readonly minLength?: number;
  readonly maxLength?: number;
};

export type NumberSchema = {
  readonly kind: "number";
  readonly integer?: boolean;
  readonly minimum?: number;
  readonly maximum?: number;
};

export type BooleanSchema = { readonly kind: "boolean" };
export type NullSchema = { readonly kind: "null" };
export type AnySchema = { readonly kind: "any" };
export type IdSchema = { readonly kind: "id"; readonly entity: string };
export type LiteralSchema = { readonly kind: "literal"; readonly value: JsonValue };
export type ArraySchema = { readonly kind: "array"; readonly items: PortableSchema };
export type ObjectSchema = {
  readonly kind: "object";
  readonly fields: Readonly<Record<string, PortableSchema>>;
  readonly allowUnknown?: boolean;
};
export type RecordSchema = { readonly kind: "record"; readonly values: PortableSchema };
export type OptionalSchema = { readonly kind: "optional"; readonly value: PortableSchema };

export type InferSchema<S extends PortableSchema> =
  S extends StringSchema ? string
    : S extends NumberSchema ? number
      : S extends BooleanSchema ? boolean
        : S extends NullSchema ? null
          : S extends AnySchema ? JsonValue
            : S extends IdSchema ? string
              : S extends LiteralSchema ? S["value"]
                : S extends ArraySchema ? InferSchema<S["items"]>[]
                  : S extends ObjectSchema ? {
                    [K in keyof S["fields"]]: S["fields"][K] extends OptionalSchema
                      ? InferSchema<S["fields"][K]["value"]> | undefined
                      : S["fields"][K] extends PortableSchema ? InferSchema<S["fields"][K]> : never;
                  }
                    : S extends RecordSchema ? Record<string, InferSchema<S["values"]>>
                      : S extends OptionalSchema ? InferSchema<S["value"]> | undefined
                        : never;

const freeze = <T>(value: T): T => Object.freeze(value);

/** Constructors for the language-neutral schema subset. */
export const schema = {
  string(options: Omit<StringSchema, "kind"> = {}): StringSchema {
    return freeze({ kind: "string", ...options });
  },
  email(): StringSchema {
    return freeze({ kind: "string", format: "email" });
  },
  uri(): StringSchema {
    return freeze({ kind: "string", format: "uri" });
  },
  uuid(): StringSchema {
    return freeze({ kind: "string", format: "uuid" });
  },
  datetime(): StringSchema {
    return freeze({ kind: "string", format: "datetime" });
  },
  number(options: Omit<NumberSchema, "kind"> = {}): NumberSchema {
    return freeze({ kind: "number", ...options });
  },
  integer(options: Omit<NumberSchema, "kind" | "integer"> = {}): NumberSchema {
    return freeze({ kind: "number", integer: true, ...options });
  },
  boolean(): BooleanSchema {
    return freeze({ kind: "boolean" });
  },
  null(): NullSchema {
    return freeze({ kind: "null" });
  },
  any(): AnySchema {
    return freeze({ kind: "any" });
  },
  id(entity: string): IdSchema {
    if (!entity.trim()) throw new Error("schema.id requires an entity name");
    return freeze({ kind: "id", entity });
  },
  literal(value: JsonValue): LiteralSchema {
    return freeze({ kind: "literal", value });
  },
  array(items: PortableSchema): ArraySchema {
    return freeze({ kind: "array", items });
  },
  object(fields: Record<string, PortableSchema>, options: Omit<ObjectSchema, "kind" | "fields"> = {}): ObjectSchema {
    return freeze({ kind: "object", fields: freeze({ ...fields }), ...options });
  },
  record(values: PortableSchema): RecordSchema {
    return freeze({ kind: "record", values });
  },
  optional(value: PortableSchema): OptionalSchema {
    return freeze({ kind: "optional", value });
  },
};

export type Account = {
  readonly id: string;
  readonly email?: string;
  readonly name?: string;
  readonly avatarUrl?: string;
};

export type Tenant = { readonly id: string; readonly name?: string };

export type Member = {
  readonly id: string;
  readonly accountId: string;
  readonly status?: "active" | "revoked" | "disabled" | (string & {});
  readonly role?: string;
  readonly displayName?: string;
  readonly permissions: Readonly<Record<string, JsonValue>> | null;
};

/**
 * Authentication identity exposed to a module.
 *
 * `auth.account` is the v2 spelling. The top-level `account` field is retained
 * as a compatibility alias for early module examples and adapters. Both are
 * nullable because the V8 host always mirrors the ABI's optional identity
 * values into the context, including for anonymous/system invocations.
 */
export type AuthContext = {
  readonly auth: { readonly account: Account | null };
  readonly account: Account | null;
};

/** Tenant and tenant-local member identity, both nullable at the ABI boundary. */
export type TenantContext = { readonly tenant: Tenant | null; readonly member: Member | null };

export type ReadDB = {
  readonly query: <T = JsonValue>(statement: string, parameters?: readonly JsonValue[]) => Promise<readonly T[]>;
};

export type WriteDB = ReadDB & {
  readonly insert: <T = JsonValue>(table: string, row: JsonObject) => Promise<T>;
  readonly update: <T = JsonValue>(table: string, id: string, patch: JsonObject) => Promise<T>;
  readonly delete: (table: string, id: string) => Promise<void>;
};

/** Durable external work recorded in the Reducer's current transaction. */
export type ReducerActions = {
  readonly enqueue: (path: string, args: JsonValue) => Promise<string>;
};

export type QueryContext = AuthContext & TenantContext & { readonly db: ReadDB; readonly now: number };

export type ReducerContext = AuthContext & TenantContext & {
  readonly db: WriteDB;
  readonly actions: ReducerActions;
  readonly now: number;
};

export type ActionStorage = {
  readonly generateUploadUrl: (options?: JsonObject) => Promise<JsonValue>;
  readonly getUrl: (fileId: string) => Promise<JsonValue>;
  readonly generateDownloadUrl: (fileId: string, ttlMs?: number) => Promise<JsonValue>;
  readonly getMetadata: (fileId: string) => Promise<JsonValue>;
  readonly delete: (fileId: string) => Promise<JsonValue>;
  readonly store: (contentBase64: string, options?: JsonObject) => Promise<JsonValue>;
  readonly call: (operation: string, payload?: JsonValue) => Promise<JsonValue>;
};

export type ActionContext = AuthContext & TenantContext & {
  readonly now: number;
  readonly fetch: (input: string | URL, init?: RequestInit) => Promise<Response>;
  readonly runReducer: <T = JsonValue>(path: string, args: JsonValue) => Promise<T>;
  readonly storage?: ActionStorage;
};

export type Handler<Context, Args, Result> = (context: Context, args: Args) => Result | Promise<Result>;

export type OfflinePolicy =
  | { readonly mode: "forbidden" }
  | { readonly mode: "allowed"; readonly conflict?: "reject" | "expectedVersion" | "merge" }
  | { readonly mode: "onlineOnly"; readonly reason: string };

export type OptimisticID = string | readonly string[];
/** Resolve this value from Reducer arguments in the client Local Replica. */
export type OptimisticArgument = { readonly $arg: string | readonly string[] };
export type OptimisticValue = JsonValue | OptimisticArgument | readonly OptimisticValue[] | { readonly [key: string]: OptimisticValue };
export type OptimisticObject = { readonly [key: string]: OptimisticValue };
export type OptimisticEffect =
  | { readonly operation: "patch"; readonly entity: string; readonly id: OptimisticID; readonly fields: OptimisticObject }
  | { readonly operation: "upsert"; readonly entity: string; readonly id: OptimisticID; readonly value: OptimisticObject }
  | { readonly operation: "delete"; readonly entity: string; readonly id: OptimisticID };

export type OptimisticTransaction = {
  readonly effects: readonly OptimisticEffect[];
  readonly expectedRevision?: number;
};

export type QueryOptions<Args, Result> = {
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly delivery?: "oneShot" | "live" | "replica";
  readonly livePlan?: LiveQueryPlan;
  /** Preferred v2 spelling; `livePlan` remains supported for compatibility. */
  readonly liveQueryPlan?: LiveQueryPlan;
  readonly replica?: ReplicaCollectionDefinition;
  /** Alias accepted by the first-class replicaCollection builder. */
  readonly replicaCollection?: ReplicaCollectionDefinition;
  readonly run?: Handler<QueryContext, Args, Result>;
};

/** Declarative contract for a bounded, locally materialized query collection. */
export type ReplicaCollectionDefinition = {
  readonly table: string;
  readonly key: string;
  readonly columns: readonly string[];
  readonly equalFilters?: Readonly<Record<string, string>>;
  readonly excludeWhenSet?: readonly string[];
  readonly visibilityTables?: readonly string[];
  readonly orderBy?: string;
  readonly orderDirection?: "asc" | "desc";
  /** `eager` is complete at initial delivery; `progressive` may fill incrementally. */
  readonly mode?: "eager" | "progressive";
  readonly maxRows?: number;
  readonly maxBytes?: number;
  readonly retentionMs?: number;
};

export type ReplicaCollectionOptions<Args, Result> = Omit<QueryOptions<Args, Result>, "delivery" | "replica" | "replicaCollection"> & {
  readonly replica: ReplicaCollectionDefinition;
};

export type ReducerOptions<Args, Result> = {
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly offline: OfflinePolicy;
  /** Set false for reducers that are not invoked directly by an interactive client. */
  readonly interactive?: boolean;
  readonly optimistic?: OptimisticTransaction;
  /** Required exception for a public interactive reducer that cannot predict a safe local transaction. */
  readonly nonOptimisticReason?: string;
  readonly internal?: boolean;
  readonly run?: Handler<ReducerContext, Args, Result>;
};

export type InternalReducerOptions<Args, Result> = Omit<ReducerOptions<Args, Result>, "offline" | "interactive" | "internal"> & {
  readonly offline?: OfflinePolicy;
};

export type ActionOptions<Args, Result> = {
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly run?: Handler<ActionContext, Args, Result>;
};

export type LiveQueryPlan = {
  readonly table: string;
  readonly key: string;
  readonly columns?: readonly string[];
  readonly where?: JsonValue;
  readonly search?: { readonly argument: string; readonly columns: readonly string[] };
  readonly sort?: { readonly columnArgument?: string; readonly directionArgument?: string; readonly defaultColumn: string; readonly defaultDirection: "asc" | "desc"; readonly allowedColumns: readonly string[] };
  readonly window?: { readonly offsetArgument: string; readonly limitArgument: string; readonly defaultLimit: number; readonly maxLimit: number };
  readonly serverOnly?: boolean;
};

export type ModuleFunctionKind = "query" | "reducer" | "action";

export type ModuleFunctionManifest = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly internal?: boolean;
  readonly delivery?: "oneShot" | "live" | "replica";
  readonly livePlan?: LiveQueryPlan;
  readonly replica?: ReplicaCollectionDefinition;
  readonly offline?: OfflinePolicy;
  readonly interactive?: boolean;
  readonly optimistic?: OptimisticTransaction;
  readonly nonOptimisticReason?: string;
};

export type ModuleManifest = {
  readonly format: "gonvex.module.v1";
  readonly name: string;
  readonly version: string;
  readonly language: ModuleLanguage;
  readonly engine: ModuleEngine;
  readonly functions: Readonly<Record<string, ModuleFunctionManifest>>;
  readonly schema?: PortableSchema;
  readonly artifact?: { readonly hash: string; readonly mediaType: string; readonly entrypoint: string };
};

export type ModuleArtifact = {
  readonly manifest: ModuleManifest;
  readonly bytes: Uint8Array;
};

export type ModuleFunctionHandler =
  | Handler<QueryContext, unknown, unknown>
  | Handler<ReducerContext, unknown, unknown>
  | Handler<ActionContext, unknown, unknown>;

/** A manifest entry together with the executable function retained by the host. */
export type RuntimeFunctionRegistration = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly definition: ModuleFunctionManifest;
  readonly handler?: ModuleFunctionHandler;
};

export type ModuleRuntimeRegistration = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly definition: ModuleFunctionManifest;
};

/** Deterministic, handler-free payload consumed by a module host during loading. */
export type ModuleRuntimeRegistrationPayload = {
  readonly format: "gonvex.module.runtime.v1";
  readonly manifest: ModuleManifest;
  readonly registrations: readonly ModuleRuntimeRegistration[];
};

export type QueryInvocation<Args = unknown> = {
  readonly path: string;
  readonly kind: "query";
  readonly context: QueryContext;
  readonly args: Args;
};

export type ReducerInvocation<Args = unknown> = {
  readonly path: string;
  readonly kind: "reducer";
  readonly context: ReducerContext;
  readonly args: Args;
};

export type ActionInvocation<Args = unknown> = {
  readonly path: string;
  readonly kind: "action";
  readonly context: ActionContext;
  readonly args: Args;
};

export type ModuleInvocation<Args = unknown> =
  | QueryInvocation<Args>
  | ReducerInvocation<Args>
  | ActionInvocation<Args>;

type AnyFunctionOptions = QueryOptions<unknown, unknown> | ReducerOptions<unknown, unknown> | ActionOptions<unknown, unknown>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const normalizePath = (path: string): string => {
  const normalized = path.trim();
  if (!normalized) throw new Error("module function path is required");
  return normalized;
};

const validateOfflinePolicy: (value: unknown, path: string) => asserts value is OfflinePolicy = (value, path) => {
  if (!isRecord(value) || (value.mode !== "forbidden" && value.mode !== "allowed" && value.mode !== "onlineOnly")) {
    throw new Error(`reducer ${path} must declare a valid offline policy`);
  }
  if (value.mode === "onlineOnly" && (typeof value.reason !== "string" || !value.reason.trim())) {
    throw new Error(`reducer ${path} onlineOnly policy requires a reason`);
  }
  if (value.mode === "allowed" && value.conflict !== undefined &&
    value.conflict !== "reject" && value.conflict !== "expectedVersion" && value.conflict !== "merge") {
    throw new Error(`reducer ${path} has an invalid offline conflict policy`);
  }
};

const validateOptimisticTransaction: (value: unknown, path: string) => asserts value is OptimisticTransaction = (value, path) => {
  if (!isRecord(value) || !Array.isArray(value.effects) || value.effects.length === 0) {
    throw new Error(`reducer ${path} optimistic metadata must contain a non-empty effects array`);
  }
  if (value.expectedRevision !== undefined &&
    (typeof value.expectedRevision !== "number" || !Number.isSafeInteger(value.expectedRevision) || value.expectedRevision < 0)) {
    throw new Error(`reducer ${path} optimistic expectedRevision must be a non-negative integer`);
  }
  for (const effect of value.effects) {
    if (!isRecord(effect) || (effect.operation !== "patch" && effect.operation !== "upsert" && effect.operation !== "delete")) {
      throw new Error(`reducer ${path} has an invalid optimistic effect`);
    }
    if (typeof effect.entity !== "string" || !effect.entity.trim()) {
      throw new Error(`reducer ${path} optimistic effects require an entity`);
    }
    if ((typeof effect.id === "string" && !effect.id.trim()) ||
      (typeof effect.id !== "string" &&
      (!Array.isArray(effect.id) || effect.id.length === 0 || effect.id.some((part) => typeof part !== "string" || !part.trim())))) {
      throw new Error(`reducer ${path} optimistic effects require a string id or id references`);
    }
    if ((effect.operation === "patch" || effect.operation === "upsert") && !isRecord(effect.operation === "patch" ? effect.fields : effect.value)) {
      throw new Error(`reducer ${path} optimistic ${effect.operation} effects require an object value`);
    }
  }
};

const validateReplicaCollection: (value: unknown, path: string) => asserts value is ReplicaCollectionDefinition = (value, path) => {
  if (!isRecord(value) || typeof value.table !== "string" || !value.table.trim() ||
    typeof value.key !== "string" || !value.key.trim() || !Array.isArray(value.columns) ||
    value.columns.some((column) => typeof column !== "string" || !column.trim())) {
    throw new Error(`replica collection ${path} requires a table, key, and columns`);
  }
  for (const field of ["maxRows", "maxBytes", "retentionMs"] as const) {
    const budget = value[field];
    if (budget !== undefined && (typeof budget !== "number" || !Number.isSafeInteger(budget) || budget <= 0)) {
      throw new Error(`replica collection ${path} ${field} must be a positive integer`);
    }
  }
  if (value.mode !== undefined && value.mode !== "eager" && value.mode !== "progressive") {
    throw new Error(`replica collection ${path} has an invalid completeness mode`);
  }
  if (value.orderDirection !== undefined && value.orderDirection !== "asc" && value.orderDirection !== "desc") {
    throw new Error(`replica collection ${path} has an invalid order direction`);
  }
};

const stableValue = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(stableValue);
  if (isRecord(value)) {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) sorted[key] = stableValue(value[key]);
    return sorted;
  }
  return value;
};

/** JSON serialization with recursively sorted object keys for reproducible artifacts. */
export const stableJsonStringify = (value: unknown, space?: number): string =>
  JSON.stringify(stableValue(value), null, space);

export class ModuleManifestCollector {
  private readonly entries = new Map<string, ModuleFunctionManifest>();

  constructor(private readonly metadata: Omit<ModuleManifest, "functions">) {}

  register(path: string, entry: Omit<ModuleFunctionManifest, "path">): ModuleFunctionManifest {
    const normalized = normalizePath(path);
    if (this.entries.has(normalized)) throw new Error(`duplicate module function: ${normalized}`);
    const result = freeze({ path: normalized, ...entry });
    this.entries.set(normalized, result);
    return result;
  }

  manifest(): ModuleManifest {
    const functions: Record<string, ModuleFunctionManifest> = {};
    for (const path of [...this.entries.keys()].sort()) functions[path] = this.entries.get(path)!;
    return freeze({ ...this.metadata, functions: freeze(functions) });
  }

  serialize(space?: number): string {
    return stableJsonStringify(this.manifest(), space);
  }
}

export type RegisteredFunction<Args, Result> = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly definition: ModuleFunctionManifest;
  readonly handler?: Handler<QueryContext, Args, Result> | Handler<ReducerContext, Args, Result> | Handler<ActionContext, Args, Result>;
};

/**
 * Executable definition returned by the top-level declaration helpers.
 *
 * The module loader uses the exported binding itself as the declaration and
 * invokes `handler` when a V8 request arrives. `options` retains the
 * declarative input so hosts can project it into a manifest without needing
 * to evaluate TypeScript source again.
 */
export type ModuleDefinition<Kind extends ModuleFunctionKind, Options> = {
  readonly kind: Kind;
  readonly internal?: boolean;
  readonly delivery?: "oneShot" | "live" | "replica";
  readonly livePlan?: LiveQueryPlan;
  readonly replica?: ReplicaCollectionDefinition;
  readonly options: Readonly<Options>;
  readonly handler?: Options extends { readonly run?: infer HandlerType } ? HandlerType : never;
};

export type QueryDefinition<Args, Result> = ModuleDefinition<"query", QueryOptions<Args, Result>>;
export type ReducerDefinition<Args, Result> = ModuleDefinition<"reducer", ReducerOptions<Args, Result>>;
export type ActionDefinition<Args, Result> = ModuleDefinition<"action", ActionOptions<Args, Result>>;

const executableOptions = <T extends object>(options: T): Readonly<T> => freeze({ ...options });

const queryDefinition = <Args, Result>(
  options: QueryOptions<Args, Result>,
  deliveryOverride?: "live" | "replica",
): QueryDefinition<Args, Result> => {
  const livePlan = options.liveQueryPlan ?? options.livePlan;
  const replica = options.replicaCollection ?? options.replica;
  const delivery = deliveryOverride ?? options.delivery ?? (replica ? "replica" : livePlan ? "live" : "oneShot");
  if (delivery === "live" && !livePlan) throw new Error("live query requires a live query plan");
  if (delivery === "replica") {
    if (!replica) throw new Error("replica collection requires a replica definition");
    validateReplicaCollection(replica, "<export>");
  }
  return freeze({
    kind: "query",
    delivery,
    livePlan,
    replica,
    options: executableOptions(options),
    handler: options.run,
  });
};

/** Declare an executable one-shot, live, or replica query export. */
export function query<Args = JsonValue, Result = JsonValue>(options: QueryOptions<Args, Result> = {}): QueryDefinition<Args, Result> {
  return queryDefinition(options);
}

/** Declare an executable live query export with a structured live plan. */
export function liveQuery<Args = JsonValue, Result = JsonValue>(options: Omit<QueryOptions<Args, Result>, "delivery"> = {}): QueryDefinition<Args, Result> {
  return queryDefinition({ ...options, delivery: "live" }, "live");
}

/** Declare an executable bounded replica collection export. */
export function replicaCollection<Args = JsonValue, Result = JsonValue>(options: ReplicaCollectionOptions<Args, Result>): QueryDefinition<Args, Result> {
  return queryDefinition({ ...options, delivery: "replica", replica: options.replica }, "replica");
}

const reducerDefinition = <Args, Result>(
  options: ReducerOptions<Args, Result>,
  internal = false,
): ReducerDefinition<Args, Result> => {
  validateOfflinePolicy(options.offline, "<export>");
  if (options.optimistic !== undefined) validateOptimisticTransaction(options.optimistic, "<export>");
  if (options.interactive === false && options.optimistic !== undefined) {
    throw new Error("non-interactive reducer <export> cannot declare optimistic metadata");
  }
  if (options.interactive !== false && options.optimistic === undefined && !options.nonOptimisticReason?.trim()) {
    throw new Error("interactive reducer <export> requires an optimistic transaction or nonOptimisticReason");
  }
  return freeze({
    kind: "reducer",
    internal: internal || options.internal,
    options: executableOptions(options),
    handler: options.run,
  });
};

/** Declare an executable public reducer export. */
export function reducer<Args = JsonValue, Result = JsonValue>(options: ReducerOptions<Args, Result>): ReducerDefinition<Args, Result> {
  return reducerDefinition(options);
}

/** Declare an executable non-interactive internal reducer export. */
export function internalReducer<Args = JsonValue, Result = JsonValue>(options: InternalReducerOptions<Args, Result>): ReducerDefinition<Args, Result> {
  return reducerDefinition({
    ...options,
    offline: options.offline ?? { mode: "forbidden" },
    interactive: false,
    internal: true,
  }, true);
}

/** Declare an executable action export. */
export function action<Args = JsonValue, Result = JsonValue>(options: ActionOptions<Args, Result> = {}): ActionDefinition<Args, Result> {
  return freeze({ kind: "action", options: executableOptions(options), handler: options.run });
}

export class ModuleBuilder {
  readonly manifestCollector: ModuleManifestCollector;
  private readonly runtimeEntries = new Map<string, RuntimeFunctionRegistration>();

  constructor(metadata: { name: string; version: string; language?: ModuleLanguage; engine?: ModuleEngine; schema?: PortableSchema; artifact?: ModuleManifest["artifact"] }) {
    this.manifestCollector = new ModuleManifestCollector({
      format: "gonvex.module.v1",
      name: metadata.name,
      version: metadata.version,
      language: metadata.language ?? "typescript",
      engine: metadata.engine ?? "v8",
      schema: metadata.schema,
      artifact: metadata.artifact,
    });
  }

  query<Args = JsonValue, Result = JsonValue>(path: string, options: QueryOptions<Args, Result> = {}): RegisteredFunction<Args, Result> {
    const livePlan = options.liveQueryPlan ?? options.livePlan;
    const replica = options.replicaCollection ?? options.replica;
    const delivery = options.delivery ?? (replica ? "replica" : livePlan ? "live" : "oneShot");
    if (delivery === "live" && !livePlan) throw new Error(`live query ${normalizePath(path)} requires a live query plan`);
    if (delivery === "replica") {
      if (!replica) throw new Error(`replica collection ${normalizePath(path)} requires a replica definition`);
      validateReplicaCollection(replica, normalizePath(path));
    }
    const definition = this.manifestCollector.register(path, {
      kind: "query",
      args: options.args,
      result: options.result,
      delivery,
      livePlan,
      replica,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
  }

  /** Register a Query delivered as a live, structured query stream. */
  liveQuery<Args = JsonValue, Result = JsonValue>(path: string, options: Omit<QueryOptions<Args, Result>, "delivery"> = {}): RegisteredFunction<Args, Result> {
    return this.query(path, { ...options, delivery: "live" });
  }

  /** Register a Query delivered as a bounded local replica collection. */
  replicaCollection<Args = JsonValue, Result = JsonValue>(path: string, options: ReplicaCollectionOptions<Args, Result>): RegisteredFunction<Args, Result> {
    return this.query(path, { ...options, delivery: "replica", replica: options.replica });
  }

  reducer<Args = JsonValue, Result = JsonValue>(path: string, options: ReducerOptions<Args, Result>): RegisteredFunction<Args, Result> {
    const normalized = normalizePath(path);
    validateOfflinePolicy(options.offline, normalized);
    if (options.optimistic !== undefined) validateOptimisticTransaction(options.optimistic, normalized);
    if (options.interactive === false && options.optimistic !== undefined) {
      throw new Error(`non-interactive reducer ${normalized} cannot declare optimistic metadata`);
    }
    if (options.interactive !== false && options.optimistic === undefined && !options.nonOptimisticReason?.trim()) {
      throw new Error(`interactive reducer ${normalized} requires an optimistic transaction or nonOptimisticReason`);
    }
    const definition = this.manifestCollector.register(path, {
      kind: "reducer",
      args: options.args,
      result: options.result,
      offline: options.offline,
      interactive: options.interactive ?? true,
      internal: options.internal,
      optimistic: options.optimistic,
      nonOptimisticReason: options.nonOptimisticReason?.trim() || undefined,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
  }

  /** Register a non-public Reducer while retaining kind `reducer` in the manifest. */
  internalReducer<Args = JsonValue, Result = JsonValue>(path: string, options: InternalReducerOptions<Args, Result>): RegisteredFunction<Args, Result> {
    return this.reducer(path, {
      ...options,
      offline: options.offline ?? { mode: "forbidden" },
      interactive: false,
      internal: true,
    });
  }

  action<Args = JsonValue, Result = JsonValue>(path: string, options: ActionOptions<Args, Result> = {}): RegisteredFunction<Args, Result> {
    const definition = this.manifestCollector.register(path, {
      kind: "action",
      args: options.args,
      result: options.result,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
  }

  manifest(): ModuleManifest {
    return this.manifestCollector.manifest();
  }

  serialize(space?: number): string {
    return this.manifestCollector.serialize(space);
  }

  /** Executable registrations sorted by path for deterministic host loading. */
  runtimeRegistrations(): readonly RuntimeFunctionRegistration[] {
    return Object.freeze([...this.runtimeEntries.values()].sort((a, b) => a.path.localeCompare(b.path)));
  }

  runtimePayload(): ModuleRuntimeRegistrationPayload {
    const registrations = this.runtimeRegistrations().map(({ path, kind, definition }) => ({ path, kind, definition }));
    return freeze({
      format: "gonvex.module.runtime.v1",
      manifest: this.manifest(),
      registrations: freeze(registrations),
    });
  }

  serializeRuntimePayload(space?: number): string {
    return stableJsonStringify(this.runtimePayload(), space);
  }

  createRuntimeRegistry(): ModuleRuntimeRegistry {
    return new ModuleRuntimeRegistry(this);
  }
}

/**
 * Host-side executable registry. It is deliberately unaware of V8,
 * Postgres, or network transport; an engine supplies the capability-bearing
 * context and this registry only selects and invokes the registered handler.
 */
export class ModuleRuntimeRegistry {
  private readonly entries = new Map<string, RuntimeFunctionRegistration>();
  private readonly baseManifest: ModuleManifest;

  constructor(source: ModuleBuilder) {
    this.baseManifest = source.manifest();
    for (const registration of source.runtimeRegistrations()) this.register(registration);
  }

  register(registration: RuntimeFunctionRegistration): void {
    const path = normalizePath(registration.path);
    if (path !== registration.definition.path) {
      throw new Error(`runtime registration path does not match its manifest: ${path}`);
    }
    if (registration.kind !== registration.definition.kind) {
      throw new Error(`runtime registration kind does not match its manifest: ${path}`);
    }
    if (registration.kind === "reducer") {
      validateOfflinePolicy(registration.definition.offline, path);
      if (registration.definition.optimistic !== undefined) {
        validateOptimisticTransaction(registration.definition.optimistic, path);
      }
      if (registration.definition.interactive !== false && registration.definition.optimistic === undefined && !registration.definition.nonOptimisticReason?.trim()) {
        throw new Error(`interactive reducer ${path} requires an optimistic transaction or nonOptimisticReason`);
      }
    }
    if (registration.definition.delivery === "replica") {
      if (!registration.definition.replica) throw new Error(`replica query ${path} requires a replica definition`);
      validateReplicaCollection(registration.definition.replica, path);
    }
    if (this.entries.has(path)) throw new Error(`duplicate runtime registration: ${path}`);
    this.entries.set(path, freeze({ ...registration, path }));
  }

  has(path: string, kind?: ModuleFunctionKind): boolean {
    const registration = this.entries.get(normalizePath(path));
    return registration !== undefined && (kind === undefined || registration.kind === kind);
  }

  registration(path: string): RuntimeFunctionRegistration | undefined {
    return this.entries.get(normalizePath(path));
  }

  registrations(): readonly RuntimeFunctionRegistration[] {
    return Object.freeze([...this.entries.values()].sort((a, b) => a.path.localeCompare(b.path)));
  }

  manifest(): ModuleManifest {
    const functions: Record<string, ModuleFunctionManifest> = {};
    for (const registration of this.registrations()) functions[registration.path] = registration.definition;
    return freeze({
      format: this.baseManifest.format,
      name: this.baseManifest.name,
      version: this.baseManifest.version,
      language: this.baseManifest.language,
      engine: this.baseManifest.engine,
      schema: this.baseManifest.schema,
      artifact: this.baseManifest.artifact,
      functions: freeze(functions),
    });
  }

  registrationPayload(): ModuleRuntimeRegistrationPayload {
    return freeze({
      format: "gonvex.module.runtime.v1",
      manifest: this.manifest(),
      registrations: freeze(this.registrations().map(({ path, kind, definition }) => ({ path, kind, definition }))),
    });
  }

  serializeRegistrationPayload(space?: number): string {
    return stableJsonStringify(this.registrationPayload(), space);
  }

  async query<Args, Result>(path: string, context: QueryContext, args: Args): Promise<Result> {
    return this.dispatch({ path, kind: "query", context, args }) as Promise<Result>;
  }

  async reducer<Args, Result>(path: string, context: ReducerContext, args: Args): Promise<Result> {
    return this.dispatch({ path, kind: "reducer", context, args }) as Promise<Result>;
  }

  async action<Args, Result>(path: string, context: ActionContext, args: Args): Promise<Result> {
    return this.dispatch({ path, kind: "action", context, args }) as Promise<Result>;
  }

  async dispatch<Args = unknown>(invocation: ModuleInvocation<Args>): Promise<unknown> {
    const path = normalizePath(invocation.path);
    const registration = this.entries.get(path);
    if (!registration) throw new Error(`unknown module function: ${path}`);
    if (registration.kind !== invocation.kind) {
      throw new Error(`module function ${path} is ${registration.kind}, not ${invocation.kind}`);
    }
    if (!registration.handler) {
      throw new Error(`module function ${path} has no executable handler`);
    }

    switch (invocation.kind) {
      case "query":
        return (registration.handler as Handler<QueryContext, Args, unknown>)(invocation.context, invocation.args);
      case "reducer":
        return (registration.handler as Handler<ReducerContext, Args, unknown>)(invocation.context, invocation.args);
      case "action":
        return (registration.handler as Handler<ActionContext, Args, unknown>)(invocation.context, invocation.args);
    }
  }
}

export function createModule(metadata: ConstructorParameters<typeof ModuleBuilder>[0]): ModuleBuilder {
  return new ModuleBuilder(metadata);
}

/** Type-only helper for APIs that accept any builder options. */
export type ModuleFunctionOptions = AnyFunctionOptions;
