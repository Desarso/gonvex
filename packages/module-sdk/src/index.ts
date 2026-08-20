/** JSON values accepted by the module ABI. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type JsonObject = { [key: string]: JsonValue };

export type ModuleLanguage = "typescript" | "rust" | "wasm" | (string & {});
export type ModuleEngine = "v8" | "wasmtime" | (string & {});

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
  readonly status: "active" | "revoked" | "disabled" | (string & {});
  readonly roles: readonly string[];
  readonly permissions: readonly string[];
};

export type AuthContext = { readonly account: Account };
export type TenantContext = { readonly tenant: Tenant; readonly member: Member };

export type ReadDB = {
  readonly query: <T = JsonValue>(statement: string, parameters?: readonly JsonValue[]) => Promise<readonly T[]>;
};

export type WriteDB = ReadDB & {
  readonly insert: <T = JsonValue>(table: string, row: JsonObject) => Promise<T>;
  readonly update: <T = JsonValue>(table: string, id: string, patch: JsonObject) => Promise<T>;
  readonly delete: (table: string, id: string) => Promise<void>;
};

export type QueryContext = AuthContext & TenantContext & { readonly db: ReadDB; readonly now: number };

export type ReducerContext = AuthContext & TenantContext & {
  readonly db: WriteDB;
  readonly now: number;
  readonly runReducer: <T = JsonValue>(path: string, args: JsonValue) => Promise<T>;
};

export type ActionContext = AuthContext & TenantContext & {
  readonly now: number;
  readonly fetch: (input: string | URL, init?: RequestInit) => Promise<Response>;
  readonly runReducer: <T = JsonValue>(path: string, args: JsonValue) => Promise<T>;
};

export type Handler<Context, Args, Result> = (context: Context, args: Args) => Result | Promise<Result>;

export type OfflinePolicy =
  | { readonly mode: "forbidden" }
  | { readonly mode: "allowed"; readonly conflict?: "reject" | "expectedVersion" | "merge" }
  | { readonly mode: "onlineOnly"; readonly reason: string };

export type OptimisticID = string | readonly string[];
export type OptimisticEffect =
  | { readonly operation: "patch"; readonly entity: string; readonly id: OptimisticID; readonly fields: JsonObject }
  | { readonly operation: "upsert"; readonly entity: string; readonly id: OptimisticID; readonly value: JsonObject }
  | { readonly operation: "delete"; readonly entity: string; readonly id: OptimisticID };

export type OptimisticTransaction = {
  readonly effects: readonly OptimisticEffect[];
  readonly expectedRevision?: number;
};

export type QueryOptions<Args, Result> = {
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly delivery?: "oneShot" | "live";
  readonly livePlan?: LiveQueryPlan;
  readonly run?: Handler<QueryContext, Args, Result>;
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
  readonly run?: Handler<ReducerContext, Args, Result>;
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
  readonly delivery?: "oneShot" | "live";
  readonly livePlan?: LiveQueryPlan;
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

const validateOfflinePolicy = (value: unknown, path: string): asserts value is OfflinePolicy => {
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

const validateOptimisticTransaction = (value: unknown, path: string): asserts value is OptimisticTransaction => {
  if (!isRecord(value) || !Array.isArray(value.effects)) {
    throw new Error(`reducer ${path} optimistic metadata must contain effects`);
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
    if (typeof effect.id !== "string" &&
      (!Array.isArray(effect.id) || effect.id.some((part) => typeof part !== "string" || !part.trim()))) {
      throw new Error(`reducer ${path} optimistic effects require a string id or id references`);
    }
    if ((effect.operation === "patch" || effect.operation === "upsert") && !isRecord(effect.operation === "patch" ? effect.fields : effect.value)) {
      throw new Error(`reducer ${path} optimistic ${effect.operation} effects require an object value`);
    }
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
    const definition = this.manifestCollector.register(path, {
      kind: "query",
      args: options.args,
      result: options.result,
      delivery: options.delivery ?? (options.livePlan ? "live" : "oneShot"),
      livePlan: options.livePlan,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
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
      optimistic: options.optimistic,
      nonOptimisticReason: options.nonOptimisticReason?.trim() || undefined,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
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
 * Host-side executable registry. It is deliberately unaware of V8, Wasmtime,
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
