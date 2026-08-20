// TypeScript server modules ship as a language-neutral module artifact rather
// than the Go source bundle: the runtime receives declarative function metadata
// plus the module sources (and an optional prebuilt JavaScript bundle) instead
// of a package it has to compile with the Go toolchain.
//
// Parsing stays regex- and scanner-based, matching how the Go registrations are
// read in index.ts. Pulling the TypeScript compiler into the CLI at runtime
// would cost more than the declarative metadata this pipeline needs.
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

import type {
  FunctionDependencies,
  FunctionEntry,
  FunctionKind,
  JsonValue,
  LiveExpression,
  LiveQueryPlan,
  LiveValue,
  ModuleArtifact,
  ModuleFunction,
  ModuleJavaScript,
  ModuleLanguage,
  ModuleSchema,
  ReplicaCollectionDefinition,
  OptimisticProjectionDefinition,
  OptimisticReducerDefinition,
  ReadDependency,
} from "./manifest-types.js";

/** Bumped whenever the artifact layout changes; mixed into the hash. */
export const moduleArtifactGeneration = 1;

/** Conventional prebuilt bundle when gonvex.json does not name one. */
const defaultBundlePath = join("_build", "module.js");

const moduleSourceExtensions = [".ts", ".tsx", ".mts", ".cts"];
const skippedDirectories = new Set(["_generated", "node_modules", "dist", "build"]);
// `gonvex auth add google` writes gonvex/auth.tsx for the browser; it is not a
// server module, and it must not make a Go backend look like a TypeScript one.
const skippedSourceFiles = new Set(["auth.tsx", "auth.ts"]);

type ModuleFunctionRegistration = {
  kind: FunctionKind;
  internal?: boolean;
  delivery?: ModuleFunction["delivery"];
};

const moduleFunctionKinds = new Map<string, ModuleFunctionRegistration>([
  ["query", { kind: "query", delivery: "oneShot" }],
  ["livequery", { kind: "query", delivery: "live" }],
  ["replicacollection", { kind: "query", delivery: "replica" }],
  ["reducer", { kind: "reducer" }],
  ["internalreducer", { kind: "reducer", internal: true }],
  ["action", { kind: "action" }],
  ["http", { kind: "http" }],
  ["publichttp", { kind: "http" }],
]);

const kindAlternation = "query|liveQuery|replicaCollection|reducer|internalReducer|action|http|publicHttp";
const definitionPattern = new RegExp(
  `export\\s+(?:const|let|var)\\s+([A-Za-z_$][A-Za-z0-9_$]*)\\s*(?::[^=;]+)?=\\s*(?:await\\s+)?(${kindAlternation})\\s*(<[^(){};]*>)?\\s*\\(`,
  "gi",
);
const registrationPattern = new RegExp(
  `\\b(?:app|server|gonvex)\\s*\\.\\s*(${kindAlternation})\\s*(<[^(){};]*>)?\\s*\\(`,
  "gi",
);
const identifierPattern = /[A-Za-z_$][A-Za-z0-9_$]*/y;
const keywordPattern = /(?:true|false|null|undefined)\b/y;
const numberPattern = /-?(?:0[xX][0-9a-fA-F_]+|\d[\d_]*(?:\.[\d_]*)?(?:[eE][+-]?\d+)?)/y;

export type ProjectLanguage = ModuleLanguage;

export type ModuleArtifactOptions = {
  root: string;
  backendDir: string;
  /** Module sources, absolute paths. */
  files: string[];
  /** Versioned SQL migrations, absolute paths. */
  migrations: string[];
  /** gonvex.json `module.entrypoint`, project-relative. */
  entrypoint?: string;
  /** gonvex.json `module.bundle`, project-relative. */
  bundle?: string;
};

/**
 * Go remains the default so existing projects and empty backend directories
 * keep the bundle pipeline they have today. TypeScript is inferred only when a
 * backend has module sources and no Go sources at all; `language` in
 * gonvex.json overrides the inference either way.
 */
export async function detectProjectLanguage(backendDir: string, declared?: string): Promise<ProjectLanguage> {
  const normalized = declared?.trim().toLowerCase();
  if (normalized === "go" || normalized === "golang") return "go";
  if (normalized === "ts" || normalized === "typescript") return "typescript";
  if (normalized) throw new Error(`unknown gonvex.json language ${JSON.stringify(declared)}; expected "go" or "typescript"`);
  if (!existsSync(backendDir)) return "go";
  const [goSources, moduleSources] = await Promise.all([
    walkFiles(backendDir, (name) => name.endsWith(".go")),
    moduleSourceFiles(backendDir),
  ]);
  if (goSources.length > 0) return "go";
  return moduleSources.length > 0 ? "typescript" : "go";
}

export async function moduleSourceFiles(backendDir: string): Promise<string[]> {
  return walkFiles(backendDir, isModuleSourceFile);
}

export async function buildModuleArtifact(options: ModuleArtifactOptions): Promise<ModuleArtifact> {
  const sources = [...options.files].sort();
  const files: Record<string, string> = {};
  const functions: Record<string, ModuleFunction> = {};
  for (const file of sources) {
    const contents = await readFile(file);
    files[projectPath(options.root, file)] = contents.toString("base64");
    for (const [path, entry] of parseModuleFunctions(options.root, options.backendDir, file, contents.toString("utf8"))) {
      functions[path] = entry;
    }
  }
  // Versioned SQL migrations travel with the artifact for the same reason they
  // travel with the Go bundle: without them the runtime applies only the
  // declarative schema.
  for (const file of [...options.migrations].sort()) {
    files[projectPath(options.root, file)] = (await readFile(file)).toString("base64");
  }
  const javascript = await readBundledJavaScript(options.root, options.backendDir, options.bundle);
  const sortedFiles = sortedRecord(files);
  return {
    language: "typescript",
    generation: moduleArtifactGeneration,
    hash: artifactHash(sortedFiles, javascript),
    entrypoint: resolveEntrypoint(options.root, options.backendDir, sources, options.entrypoint),
    functions: sortedRecord(functions),
    files: sortedFiles,
    ...(javascript ? { javascript } : {}),
  };
}

/** Projects the artifact functions onto the language-neutral manifest shape. */
export function moduleManifestFunctions(artifact: ModuleArtifact): Record<string, FunctionEntry> {
  const functions: Record<string, FunctionEntry> = {};
  for (const [path, entry] of Object.entries(artifact.functions)) {
    functions[path] = {
      kind: entry.kind,
      handler: entry.handler,
      file: entry.file,
      ...(entry.internal ? { internal: true } : {}),
      ...(entry.delivery ? { delivery: entry.delivery } : {}),
      ...(entry.dependencies ? { dependencies: entry.dependencies } : {}),
      ...(entry.replica ? { replica: entry.replica } : {}),
    };
  }
  return functions;
}

function artifactHash(files: Record<string, string>, javascript?: ModuleJavaScript) {
  const hash = createHash("sha256");
  hash.update(`generation:${moduleArtifactGeneration};language:typescript;`);
  for (const path of Object.keys(files).sort()) {
    hash.update(`${path}:${files[path]};`);
  }
  if (javascript) hash.update(`javascript:${javascript.path}:${javascript.hash};`);
  return hash.digest("hex");
}

async function readBundledJavaScript(root: string, backendDir: string, configured?: string): Promise<ModuleJavaScript | undefined> {
  const declared = configured?.trim();
  const path = declared ? resolve(root, declared) : join(backendDir, defaultBundlePath);
  if (!existsSync(path)) {
    if (declared) {
      console.warn(`[gonvex] module bundle ${declared} does not exist; shipping module sources without prebuilt JavaScript`);
    }
    return undefined;
  }
  const code = await readFile(path);
  const sourceMapPath = `${path}.map`;
  const sourceMap = existsSync(sourceMapPath) ? await readFile(sourceMapPath) : undefined;
  return {
    path: projectPath(root, path),
    hash: createHash("sha256").update(code).digest("hex"),
    code: code.toString("base64"),
    ...(sourceMap ? { sourceMap: sourceMap.toString("base64") } : {}),
  };
}

function resolveEntrypoint(root: string, backendDir: string, sources: string[], configured?: string) {
  const declared = configured?.trim();
  if (declared) return projectPath(root, resolve(root, declared));
  for (const candidate of ["index.ts", "index.mts", "index.tsx", "main.ts", "module.ts"]) {
    const path = join(backendDir, candidate);
    if (sources.includes(path)) return projectPath(root, path);
  }
  const first = sources[0];
  return first ? projectPath(root, first) : "";
}

function parseModuleFunctions(root: string, backendDir: string, file: string, source: string): Array<[string, ModuleFunction]> {
  const relativeFile = projectPath(root, file);
  const prefix = functionPathPrefix(backendDir, file);
  const entries: Array<[string, ModuleFunction]> = [];

  // `export const list = query({ ... })` names the function after its module
  // path and exported binding, the way the generated api.ts addresses it.
  definitionPattern.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = definitionPattern.exec(source)) !== null) {
    const registration = moduleFunctionKinds.get((match[2] ?? "").toLowerCase());
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    definitionPattern.lastIndex = Math.max(call.end, openParen + 1);
    if (!registration) continue;
    const exportName = match[1]!;
    const options = call.args.find((argument) => argument.entries)?.entries;
    // `query(listMessages)` passes the handler directly instead of options.
    const firstArgument = call.args[0];
    const inlineHandler = firstArgument && !firstArgument.entries ? identifierText(firstArgument.text) : undefined;
    const handlerEntry = options?.get("handler");
    const declaredPath = stringEntry(options, "name");
    entries.push([
      declaredPath ?? (prefix ? `${prefix}.${exportName}` : exportName),
      moduleFunction({
        ...registration,
        file: relativeFile,
        handler: identifierText(handlerEntry?.text) ?? inlineHandler ?? exportName,
        exportName,
        generics: match[3],
        signature: handlerEntry?.text,
        options,
      }),
    ]);
  }

  // `app.query("messages.list", listMessages, { ... })` mirrors the Go
  // registration form, so a module can name its own paths.
  registrationPattern.lastIndex = 0;
  while ((match = registrationPattern.exec(source)) !== null) {
    const registration = moduleFunctionKinds.get((match[1] ?? "").toLowerCase());
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    registrationPattern.lastIndex = Math.max(call.end, openParen + 1);
    if (!registration) continue;
    const declaredPath = call.args[0]?.value;
    const path = typeof declaredPath === "string" ? declaredPath.trim() : "";
    if (!path) continue;
    const options = call.args.find((argument) => argument.entries)?.entries;
    entries.push([
      path,
      moduleFunction({
        ...registration,
        file: relativeFile,
        handler: identifierText(call.args[1]?.text) ?? path.split(".").pop() ?? path,
        generics: match[2],
        signature: options?.get("handler")?.text,
        options,
      }),
    ]);
  }

  return entries;
}

function moduleFunction(input: {
  kind: FunctionKind;
  internal?: boolean;
  delivery?: ModuleFunction["delivery"];
  file: string;
  handler: string;
  exportName?: string;
  generics?: string;
  signature?: string;
  options?: ObjectEntries;
}): ModuleFunction {
  const schemas = callSchemas(input.generics, input.signature);
  const configuredDelivery = input.options?.get("delivery")?.value;
  const delivery = normalizeDelivery(configuredDelivery) ?? input.delivery;
  const dependencies = dependenciesFromOptions(input.options, input.kind, delivery);
  const replica = delivery === "replica" ? replicaFromOptions(input.options) : undefined;
  const offline = input.options?.get("offline")?.value;
  const optimistic = input.options?.get("optimistic")?.value;
  return {
    kind: input.kind,
    handler: input.handler,
    file: input.file,
    ...(input.internal ? { internal: true } : {}),
    ...(input.exportName ? { export: input.exportName } : {}),
    args: schemas.args,
    result: schemas.result,
    ...(dependencies ? { dependencies } : {}),
    ...(delivery === undefined ? {} : { delivery }),
    ...(replica ? { replica } : {}),
    ...(offline === undefined ? {} : { offline }),
    ...(optimistic === undefined ? {} : { optimistic }),
  };
}

function callSchemas(generics?: string, signature?: string): { args: ModuleSchema; result: ModuleSchema } {
  const declared = generics ? splitTopLevel(generics.slice(1, -1)) : [];
  const fromSignature = signature ? signatureTypes(signature) : {};
  return {
    args: moduleSchema(declared.at(0) ?? fromSignature.args),
    result: moduleSchema(declared.at(1) ?? fromSignature.result),
  };
}

function moduleSchema(type?: string): ModuleSchema {
  const declared = type?.trim();
  // Schemas stay placeholders until codegen can lower TypeScript types into
  // JSON Schema; the declared type text is what the next generation resolves.
  return { placeholder: true, ...(declared ? { type: declared } : {}) };
}

function signatureTypes(text: string): { args?: string; result?: string } {
  const openParen = text.indexOf("(");
  if (openParen < 0) return {};
  const closeParen = findMatching(text, openParen);
  if (closeParen < 0) return {};
  // Handlers take the context first and the arguments second, like Go's
  // `func(ctx *gonvex.QueryCtx, args Args)`.
  const parameter = splitTopLevel(text.slice(openParen + 1, closeParen)).at(1) ?? "";
  const colon = parameter.indexOf(":");
  const args = colon < 0 ? undefined : parameter.slice(colon + 1).trim();
  let result: string | undefined;
  const afterParams = skipTrivia(text, closeParen + 1);
  if (text[afterParams] === ":") {
    const arrow = indexOfTopLevel(text, "=>", afterParams + 1);
    result = text.slice(afterParams + 1, arrow < 0 ? text.length : arrow);
  }
  return { args: args || undefined, result: unwrapPromise(result) };
}

function unwrapPromise(type?: string) {
  const declared = type?.trim();
  if (!declared) return undefined;
  const match = /^Promise\s*<([\s\S]*)>$/.exec(declared);
  return (match ? match[1]!.trim() : declared) || undefined;
}

function dependenciesFromOptions(
  options: ObjectEntries | undefined,
  kind: FunctionKind,
  delivery: ModuleFunction["delivery"],
): FunctionDependencies | undefined {
  if (!options) return undefined;
  const dependencies: FunctionDependencies = {};

  // Reads is retained only for legacy one-shot Query declarations. Reducers,
  // Actions, HTTP handlers and structured Live Queries derive their behavior
  // from the executable contract instead of hand-written dependency metadata.
  if (kind === "query" && delivery === "oneShot") {
    const reads = tableDependencies(options.get("reads")?.value);
    if (reads.length > 0) dependencies.reads = reads;
  }

  const liveQueryPlan = liveQueryPlanFromOptions(options);
  if (liveQueryPlan) dependencies.liveQueryPlan = liveQueryPlan;
  if (options.get("shareByPermissions")?.value === true) dependencies.shareByPermissions = true;
  const shareByVisibility = stringEntry(options, "shareByVisibility");
  if (shareByVisibility) dependencies.shareByVisibility = shareByVisibility;
  const shareResultFrom = stringEntry(options, "shareResultFrom");
  if (shareResultFrom) dependencies.shareResultFrom = shareResultFrom;
  const shareResultField = stringEntry(options, "shareResultField");
  if (shareResultField) dependencies.shareResultField = shareResultField;
  const optimistic = options.get("optimistic")?.value;
  const reducer = optimisticReducer(options.get("optimisticReducer")?.value ?? readMember(optimistic, "reducer"));
  if (reducer) dependencies.optimisticReducer = reducer;
  const projection = optimisticProjection(options.get("optimisticProjection")?.value ?? readMember(optimistic, "projection"));
  if (projection) dependencies.optimisticProjection = projection;
  const nonOptimisticReason = stringEntry(options, "nonOptimisticReason");
  if (nonOptimisticReason) dependencies.nonOptimisticReason = nonOptimisticReason;
  return Object.keys(dependencies).length > 0 ? dependencies : undefined;
}

function tableDependencies(value: JsonValue | undefined): ReadDependency[] {
  const items = Array.isArray(value) ? value : value === undefined || value === null ? [] : [value];
  const dependencies: ReadDependency[] = [];
  for (const item of items) {
    if (typeof item === "string") {
      if (item.trim()) dependencies.push({ table: item.trim() });
      continue;
    }
    const table = stringMember(item, "table");
    if (!table) continue;
    const dependency: ReadDependency = { table };
    const columns = stringArray(readMember(item, "columns"));
    if (columns) dependency.columns = columns;
    const filters = stringArray(readMember(item, "filters"));
    if (filters) dependency.filters = filters;
    const ordersBy = stringArray(readMember(item, "ordersBy"));
    if (ordersBy) dependency.ordersBy = ordersBy;
    if (readMember(item, "windowed") === true) dependency.windowed = true;
    dependencies.push(dependency);
  }
  return dependencies;
}

function optimisticReducer(value: JsonValue | undefined): OptimisticReducerDefinition | undefined {
  const entity = stringMember(value, "entity");
  if (!entity) return undefined;
  return {
    entity,
    rowIdPath: pathArray(readMember(value, "rowIdPath")),
    fieldsPath: pathArray(readMember(value, "fieldsPath")),
  };
}

function optimisticProjection(value: JsonValue | undefined): OptimisticProjectionDefinition | undefined {
  const entity = stringMember(value, "entity");
  if (!entity) return undefined;
  return {
    entity,
    key: stringMember(value, "key") ?? "id",
    resultPath: pathArray(readMember(value, "resultPath")),
  };
}

function normalizeDelivery(value: JsonValue | undefined): ModuleFunction["delivery"] | undefined {
  if (value === "oneShot" || value === "live" || value === "replica") return value;
  return undefined;
}

function liveQueryPlanFromOptions(options: ObjectEntries): LiveQueryPlan | undefined {
  const value = options.get("liveQueryPlan")?.value
    ?? options.get("livePlan")?.value
    ?? options.get("LiveQueryPlan")?.value
    ?? options.get("LivePlan")?.value;
  return parseLiveQueryPlan(value);
}

function parseLiveQueryPlan(value: JsonValue | undefined): LiveQueryPlan | undefined {
  const table = stringMember(value, "table");
  if (!table) return undefined;
  const plan: LiveQueryPlan = {
    table,
    key: stringMember(value, "key") ?? "id",
  };
  const columns = stringArray(readMember(value, "columns"));
  if (columns) plan.columns = columns;
  const resultPath = pathArray(readMember(value, "resultPath"));
  if (resultPath.length > 0) plan.resultPath = resultPath;
  const where = parseLiveExpression(readMember(value, "where"));
  if (where) plan.where = where;
  const searchValue = readMember(value, "search");
  const searchArgument = stringMember(searchValue, "argument");
  const searchColumns = stringArray(readMember(searchValue, "columns"));
  if (searchArgument && searchColumns) plan.search = { argument: searchArgument, columns: searchColumns };
  const sortValue = readMember(value, "sort");
  const sortDefaultColumn = stringMember(sortValue, "defaultColumn");
  const sortDefaultDirection = stringMember(sortValue, "defaultDirection");
  const sortAllowedColumns = stringArray(readMember(sortValue, "allowedColumns"));
  if (sortDefaultColumn && sortDefaultDirection && sortAllowedColumns) {
    plan.sort = {
      columnArgument: stringMember(sortValue, "columnArgument"),
      directionArgument: stringMember(sortValue, "directionArgument"),
      defaultColumn: sortDefaultColumn,
      defaultDirection: sortDefaultDirection,
      allowedColumns: sortAllowedColumns,
    };
  }
  const windowValue = readMember(value, "window");
  const offsetArgument = stringMember(windowValue, "offsetArgument");
  const limitArgument = stringMember(windowValue, "limitArgument");
  const defaultLimit = numberMember(windowValue, "defaultLimit");
  const maxLimit = numberMember(windowValue, "maxLimit");
  if (offsetArgument && limitArgument && defaultLimit !== undefined && maxLimit !== undefined) {
    plan.window = { offsetArgument, limitArgument, defaultLimit, maxLimit };
  }
  if (readMember(value, "serverOnly") === true) plan.serverOnly = true;
  return plan;
}

function parseLiveExpression(value: JsonValue | undefined): LiveExpression | undefined {
  const operator = stringMember(value, "operator");
  if (!operator || ![
    "eq", "neq", "gt", "gte", "lt", "lte", "in", "contains",
    "containsInsensitive", "range", "and", "or", "not", "server",
  ].includes(operator)) return undefined;
  const expression: LiveExpression = { operator: operator as LiveExpression["operator"] };
  const column = stringMember(value, "column");
  if (column) expression.column = column;
  const parsedValue = parseLiveValue(readMember(value, "value"));
  if (parsedValue) expression.value = parsedValue;
  const valueTo = parseLiveValue(readMember(value, "valueTo"));
  if (valueTo) expression.valueTo = valueTo;
  const childrenValue = readMember(value, "children");
  if (Array.isArray(childrenValue)) {
    const children = childrenValue.map(parseLiveExpression).filter((child): child is LiveExpression => child !== undefined);
    if (children.length > 0) expression.children = children;
  }
  return expression;
}

function parseLiveValue(value: JsonValue | undefined): LiveValue | undefined {
  const argument = stringMember(value, "argument");
  if (argument) return { argument };
  const literal = readMember(value, "literal");
  return literal === undefined ? undefined : { literal };
}

function literalObject(options?: ObjectEntries): JsonValue | undefined {
  if (!options) return undefined;
  const object: Record<string, JsonValue> = {};
  for (const [key, entry] of options) {
    if (entry.value !== undefined) object[key] = entry.value;
  }
  return Object.keys(object).length > 0 ? object : undefined;
}

function replicaFromOptions(options?: ObjectEntries): ReplicaCollectionDefinition | undefined {
  const value = options?.get("replica")?.value
    ?? options?.get("replicaCollection")?.value
    ?? literalObject(options);
  const table = stringMember(value, "table");
  if (!table) return undefined;
  const definition: ReplicaCollectionDefinition = {
    table,
    key: stringMember(value, "key") ?? "id",
    columns: stringArray(readMember(value, "columns")) ?? [],
  };
  const equalFilters = readMember(value, "equalFilters");
  if (equalFilters && typeof equalFilters === "object" && !Array.isArray(equalFilters)) {
    const filters: Record<string, string> = {};
    for (const [argument, column] of Object.entries(equalFilters)) {
      if (typeof column === "string") filters[argument] = column;
    }
    if (Object.keys(filters).length > 0) definition.equalFilters = filters;
  }
  const excludeWhenSet = stringArray(readMember(value, "excludeWhenSet"));
  if (excludeWhenSet) definition.excludeWhenSet = excludeWhenSet;
  const visibilityTables = stringArray(readMember(value, "visibilityTables"));
  if (visibilityTables) definition.visibilityTables = visibilityTables;
  const orderBy = stringMember(value, "orderBy");
  if (orderBy) {
    definition.orderBy = orderBy;
    definition.orderDirection = stringMember(value, "orderDirection")?.toLowerCase() === "asc" ? "asc" : "desc";
  }
  definition.mode = stringMember(value, "mode") === "progressive" ? "progressive" : "eager";
  const maxRows = numberMember(value, "maxRows");
  if (maxRows !== undefined && maxRows > 0) definition.maxRows = maxRows;
  const maxBytes = numberMember(value, "maxBytes");
  if (maxBytes !== undefined && maxBytes > 0) definition.maxBytes = maxBytes;
  if (!definition.columns.includes(definition.key)) definition.columns.push(definition.key);
  return definition;
}

type ObjectEntry = {
  /** Present only when the member is a pure literal. */
  value?: JsonValue;
  /** Raw source of the member, or the signature for method shorthands. */
  text: string;
};

type ObjectEntries = Map<string, ObjectEntry>;

type LiteralValue = {
  value?: JsonValue;
  entries?: ObjectEntries;
  text: string;
  end: number;
};

function readCallArguments(source: string, openParen: number): { args: LiteralValue[]; end: number } {
  const close = findMatching(source, openParen);
  if (close < 0) return { args: [], end: openParen + 1 };
  const args: LiteralValue[] = [];
  let cursor = openParen + 1;
  while (cursor < close) {
    cursor = skipTrivia(source, cursor);
    if (cursor >= close) break;
    const argument = readValue(source, cursor);
    if (argument.end <= cursor) break;
    args.push(argument);
    cursor = skipTrivia(source, argument.end);
    if (source[cursor] === ",") cursor += 1;
  }
  return { args, end: close + 1 };
}

function readValue(source: string, start: number): LiteralValue {
  const begin = skipTrivia(source, start);
  const char = source[begin];
  if (char === undefined) return { text: "", end: source.length };

  if (char === "{") {
    const close = findMatching(source, begin);
    if (close < 0) return { text: source.slice(begin), end: source.length };
    const entries = parseObjectEntries(source, begin, close);
    const object: Record<string, JsonValue> = {};
    let literal = true;
    for (const [key, entry] of entries) {
      if (entry.value === undefined) {
        literal = false;
        break;
      }
      object[key] = entry.value;
    }
    return { ...(literal ? { value: object } : {}), entries, text: source.slice(begin, close + 1), end: close + 1 };
  }

  if (char === "[") {
    const close = findMatching(source, begin);
    if (close < 0) return { text: source.slice(begin), end: source.length };
    const items: JsonValue[] = [];
    let literal = true;
    let cursor = begin + 1;
    while (cursor < close) {
      cursor = skipTrivia(source, cursor);
      if (cursor >= close) break;
      const item = readValue(source, cursor);
      if (item.end <= cursor) break;
      if (item.value === undefined) literal = false;
      else items.push(item.value);
      cursor = skipTrivia(source, item.end);
      if (source[cursor] === ",") cursor += 1;
    }
    return { ...(literal ? { value: items } : {}), text: source.slice(begin, close + 1), end: close + 1 };
  }

  if (char === '"' || char === "'" || char === "`") {
    const string = readStringLiteral(source, begin);
    const end = string?.end ?? source.length;
    return { ...(string?.value === undefined ? {} : { value: string.value }), text: source.slice(begin, end), end };
  }

  keywordPattern.lastIndex = begin;
  const keyword = keywordPattern.exec(source);
  if (keyword) {
    const end = begin + keyword[0].length;
    const value = keyword[0] === "true" ? true : keyword[0] === "false" ? false : keyword[0] === "null" ? null : undefined;
    return { ...(keyword[0] === "undefined" ? {} : { value }), text: keyword[0], end };
  }

  numberPattern.lastIndex = begin;
  const number = numberPattern.exec(source);
  if (number) {
    const parsed = Number(number[0].replace(/_/g, ""));
    const end = begin + number[0].length;
    return { ...(Number.isFinite(parsed) ? { value: parsed } : {}), text: number[0], end };
  }

  // Identifiers, calls, and arrow functions are kept as source text: the CLI
  // records what the module declared without pretending to evaluate it.
  let cursor = begin;
  while (cursor < source.length) {
    const current = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (current === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? source.length : newline + 1;
      continue;
    }
    if (current === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? source.length : closeComment + 2;
      continue;
    }
    if (current === '"' || current === "'" || current === "`") {
      const string = readStringLiteral(source, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (current === "{" || current === "[" || current === "(") {
      const closeBracket = findMatching(source, cursor);
      cursor = closeBracket < 0 ? source.length : closeBracket + 1;
      continue;
    }
    if (current === "," || current === "}" || current === "]" || current === ")") break;
    cursor += 1;
  }
  return { text: source.slice(begin, cursor).trim(), end: cursor };
}

function parseObjectEntries(source: string, open: number, close: number): ObjectEntries {
  const entries: ObjectEntries = new Map();
  let cursor = open + 1;
  while (cursor < close) {
    cursor = skipTrivia(source, cursor);
    if (cursor >= close) break;
    const char = source[cursor]!;
    if (char === "," || char === ";") {
      cursor += 1;
      continue;
    }
    if (char === ".") {
      // Spread members contribute nothing the CLI can resolve statically.
      const spread = readValue(source, cursor);
      cursor = spread.end > cursor ? spread.end : cursor + 1;
      continue;
    }
    const key = readMemberKey(source, cursor);
    if (!key) break;
    const afterKey = skipTrivia(source, key.end);

    if (source[afterKey] === ":") {
      const value = readValue(source, afterKey + 1);
      entries.set(key.key, { ...(value.value === undefined ? {} : { value: value.value }), text: value.text });
      cursor = value.end > afterKey ? value.end : afterKey + 1;
      continue;
    }

    if (source[afterKey] === "(") {
      // Method shorthand: keep the signature only, so the declared parameter
      // and return types stay readable without the body.
      const closeParen = findMatching(source, afterKey);
      if (closeParen < 0) break;
      const body = indexOfTopLevel(source, "{", closeParen + 1, close);
      const signatureEnd = body < 0 ? closeParen + 1 : body;
      entries.set(key.key, { text: source.slice(key.end, signatureEnd).trim() });
      const bodyEnd = body < 0 ? -1 : findMatching(source, body);
      cursor = bodyEnd < 0 ? signatureEnd : bodyEnd + 1;
      continue;
    }

    entries.set(key.key, { text: key.key });
    cursor = afterKey;
  }
  return entries;
}

function readMemberKey(source: string, start: number): { key: string; end: number } | undefined {
  let cursor = skipTrivia(source, start);
  // `async`, `get`, and `set` prefix method shorthands; the key follows them.
  for (let guard = 0; guard < 3; guard += 1) {
    const char = source[cursor];
    if (char === undefined) return undefined;
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(source, cursor);
      if (!string || string.value === undefined) return undefined;
      return { key: string.value, end: string.end };
    }
    identifierPattern.lastIndex = cursor;
    const identifier = identifierPattern.exec(source);
    if (!identifier) return undefined;
    const name = identifier[0];
    const after = skipTrivia(source, cursor + name.length);
    const modifier = (name === "async" || name === "get" || name === "set") && /[A-Za-z_$"'`]/.test(source[after] ?? "");
    if (!modifier) return { key: name, end: cursor + name.length };
    cursor = after;
  }
  return undefined;
}

function readStringLiteral(source: string, start: number): { value?: string; end: number } | undefined {
  const quote = source[start];
  if (quote !== '"' && quote !== "'" && quote !== "`") return undefined;
  let interpolated = false;
  let raw = "";
  let cursor = start + 1;
  while (cursor < source.length) {
    const char = source[cursor]!;
    if (char === "\\") {
      raw += source.slice(cursor, cursor + 2);
      cursor += 2;
      continue;
    }
    if (quote === "`" && char === "$" && source[cursor + 1] === "{") {
      const close = findMatching(source, cursor + 1);
      if (close < 0) return { end: source.length };
      interpolated = true;
      cursor = close + 1;
      continue;
    }
    if (char === quote) {
      return interpolated ? { end: cursor + 1 } : { value: decodeStringLiteral(raw), end: cursor + 1 };
    }
    raw += char;
    cursor += 1;
  }
  return { end: source.length };
}

function decodeStringLiteral(raw: string) {
  let decoded = "";
  for (let index = 0; index < raw.length; index += 1) {
    const char = raw[index]!;
    if (char !== "\\") {
      decoded += char;
      continue;
    }
    const escape = raw[index + 1] ?? "";
    index += 1;
    if (escape === "n") decoded += "\n";
    else if (escape === "r") decoded += "\r";
    else if (escape === "t") decoded += "\t";
    else if (escape === "b") decoded += "\b";
    else if (escape === "f") decoded += "\f";
    else if (escape === "v") decoded += "\v";
    else if (escape === "0") decoded += "\0";
    else if (escape === "x") {
      const code = Number.parseInt(raw.slice(index + 1, index + 3), 16);
      if (Number.isFinite(code)) {
        decoded += String.fromCharCode(code);
        index += 2;
      }
    } else if (escape === "u") {
      if (raw[index + 1] === "{") {
        const close = raw.indexOf("}", index + 2);
        const code = close < 0 ? Number.NaN : Number.parseInt(raw.slice(index + 2, close), 16);
        if (Number.isFinite(code) && code <= 0x10ffff) {
          decoded += String.fromCodePoint(code);
          index = close;
        }
      } else {
        const code = Number.parseInt(raw.slice(index + 1, index + 5), 16);
        if (Number.isFinite(code)) {
          decoded += String.fromCharCode(code);
          index += 4;
        }
      }
    } else {
      decoded += escape;
    }
  }
  return decoded;
}

/**
 * Balanced scanner over braces, brackets, and parentheses that skips strings
 * and comments. Regular-expression literals are not tracked; declarative module
 * metadata does not use them.
 */
function findMatching(source: string, open: number): number {
  const opener = source[open];
  if (opener !== "{" && opener !== "[" && opener !== "(") return -1;
  let depth = 0;
  let cursor = open;
  while (cursor < source.length) {
    const char = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (char === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? source.length : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? source.length : closeComment + 2;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(source, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      depth += 1;
      cursor += 1;
      continue;
    }
    if (char === "}" || char === "]" || char === ")") {
      depth -= 1;
      if (depth === 0) return cursor;
      cursor += 1;
      continue;
    }
    cursor += 1;
  }
  return -1;
}

function indexOfTopLevel(source: string, token: string, from: number, limit = source.length): number {
  let cursor = from;
  while (cursor < limit) {
    if (source.startsWith(token, cursor)) return cursor;
    const char = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (char === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? limit : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? limit : closeComment + 2;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(source, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      const closeBracket = findMatching(source, cursor);
      cursor = closeBracket < 0 ? limit : closeBracket + 1;
      continue;
    }
    if (char === "}" || char === "]" || char === ")") return -1;
    cursor += 1;
  }
  return -1;
}

function splitTopLevel(text: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let cursor = 0;
  while (cursor < text.length) {
    const char = text[cursor]!;
    const next = text[cursor + 1] ?? "";
    if (char === "/" && next === "/") {
      const newline = text.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? text.length : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = text.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? text.length : closeComment + 2;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(text, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      const closeBracket = findMatching(text, cursor);
      cursor = closeBracket < 0 ? text.length : closeBracket + 1;
      continue;
    }
    if (char === "<") {
      const closeAngle = findMatchingAngle(text, cursor);
      cursor = closeAngle < 0 ? cursor + 1 : closeAngle + 1;
      continue;
    }
    if (char === ",") {
      parts.push(text.slice(start, cursor).trim());
      start = cursor + 1;
    }
    cursor += 1;
  }
  parts.push(text.slice(start).trim());
  return parts.filter((part) => part.length > 0);
}

function findMatchingAngle(text: string, open: number): number {
  let depth = 0;
  let cursor = open;
  while (cursor < text.length) {
    const char = text[cursor]!;
    if (char === "=" && text[cursor + 1] === ">") {
      cursor += 2;
      continue;
    }
    if (char === "<") {
      depth += 1;
      cursor += 1;
      continue;
    }
    if (char === ">") {
      depth -= 1;
      if (depth === 0) return cursor;
      cursor += 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      const closeBracket = findMatching(text, cursor);
      if (closeBracket < 0) return -1;
      cursor = closeBracket + 1;
      continue;
    }
    cursor += 1;
  }
  return -1;
}

function skipTrivia(source: string, start: number): number {
  let cursor = start;
  while (cursor < source.length) {
    const char = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (/\s/.test(char)) {
      cursor += 1;
      continue;
    }
    if (char === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      if (newline < 0) return source.length;
      cursor = newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      if (closeComment < 0) return source.length;
      cursor = closeComment + 2;
      continue;
    }
    break;
  }
  return cursor;
}

function identifierText(text?: string) {
  const trimmed = text?.trim();
  return trimmed && /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(trimmed) ? trimmed : undefined;
}

function stringEntry(options: ObjectEntries | undefined, key: string) {
  const value = options?.get(key)?.value;
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readMember(value: JsonValue | undefined, key: string): JsonValue | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value[key];
}

function stringMember(value: JsonValue | undefined, key: string) {
  const member = readMember(value, key);
  return typeof member === "string" && member.trim() ? member.trim() : undefined;
}

function numberMember(value: JsonValue | undefined, key: string) {
  const member = readMember(value, key);
  return typeof member === "number" && Number.isFinite(member) ? member : undefined;
}

function stringArray(value: JsonValue | undefined): string[] | undefined {
  if (typeof value === "string") return value.trim() ? [value.trim()] : undefined;
  if (!Array.isArray(value)) return undefined;
  const values = value.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim());
  return values.length > 0 ? values : undefined;
}

function pathArray(value: JsonValue | undefined): string[] {
  if (typeof value === "string") return value.split(".").map((segment) => segment.trim()).filter(Boolean);
  return stringArray(value) ?? [];
}

function functionPathPrefix(backendDir: string, file: string) {
  const withoutExtension = relative(backendDir, file).replace(/\\/g, "/").replace(/\.(?:tsx|ts|mts|cts)$/, "");
  const segments = withoutExtension.split("/").filter((segment) => segment && segment !== ".");
  if (segments[segments.length - 1] === "index") segments.pop();
  return segments.join(".");
}

function projectPath(root: string, file: string) {
  return relative(root, file).replace(/\\/g, "/");
}

function sortedRecord<T>(record: Record<string, T>): Record<string, T> {
  const sorted: Record<string, T> = {};
  for (const key of Object.keys(record).sort()) sorted[key] = record[key]!;
  return sorted;
}

function isModuleSourceFile(name: string) {
  if (skippedSourceFiles.has(name)) return false;
  if (/\.d\.(?:ts|mts|cts)$/.test(name)) return false;
  if (/\.(?:test|spec)\.(?:ts|tsx|mts|cts)$/.test(name)) return false;
  return moduleSourceExtensions.some((extension) => name.endsWith(extension));
}

async function walkFiles(dir: string, accept: (name: string) => boolean): Promise<string[]> {
  if (!existsSync(dir)) return [];
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (skippedDirectories.has(entry.name) || entry.name.startsWith(".")) continue;
      files.push(...await walkFiles(path, accept));
    } else if (entry.isFile() && accept(entry.name)) {
      files.push(path);
    }
  }
  return files.sort();
}
