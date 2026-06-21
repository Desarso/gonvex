#!/usr/bin/env node
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { mkdir, readFile, readdir, stat, writeFile, copyFile } from "node:fs/promises";
import { createInterface } from "node:readline/promises";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type FunctionKind = "query" | "mutation" | "action" | "http" | "internalMutation" | "liveGrid";

type FunctionEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
};

type Column = {
  type: string;
  nullable: boolean;
  primaryKey: boolean;
};

type Table = {
  columns: Record<string, Column>;
  indexes: Record<string, { columns: string[]; unique: boolean }>;
};

type SchemaDefinition = {
  tables: Record<string, Table>;
  landlordTables: Record<string, Table>;
  tenantTables: Record<string, Table>;
};

type Manifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionEntry>;
  schema: SchemaDefinition;
  bundle?: SourceBundle;
};

type SourceBundle = {
  hash: string;
  modulePath: string;
  packageName: string;
  goVersion?: string;
  files: Record<string, string>;
};

type Settings = {
  projectID: string;
  runtimeURL: string;
  key: string;
};

type RuntimeProject = {
  id: string;
  name: string;
  environment?: string;
  database?: string;
};

type CreatedRuntimeProject = {
  project: RuntimeProject;
  projectKey: string;
};

type RuntimeEnvVariable = {
  name: string;
  value?: string;
  masked: string;
  source: string;
  sensitive: boolean;
};

type WatchState = {
  lastFingerprint: string;
  lastManifest: Manifest | null;
  lastSyncAttempt: number;
  lastSyncSucceeded: boolean;
  lastRuntimeCheck: number;
};

type BindingWriteResult = {
  changedFiles: number;
};

type FunctionDiff = {
  added: string[];
  removed: string[];
  edited: string[];
};

const defaultRuntimeURL = "http://localhost:8080";
const runtimeSyncRetryMs = 5000;
const runtimeStateCheckMs = 2500;

const color = {
  green: (value: string) => `\x1b[32m${value}\x1b[0m`,
  red: (value: string) => `\x1b[31m${value}\x1b[0m`,
  yellow: (value: string) => `\x1b[33m${value}\x1b[0m`,
};

export async function main(argv = process.argv.slice(2)) {
  const command = argv[0];
  if (!command || command === "help" || command === "--help") {
    printHelp();
    return;
  }

  if (command === "dev") {
    await runDev(argv.slice(1));
    return;
  }
  if (command === "init") {
    await runInit(argv.slice(1));
    return;
  }
  if (command === "create") {
    await runCreate(argv.slice(1));
    return;
  }
  if (command === "env") {
    await runEnv(argv.slice(1));
    return;
  }

  printHelp();
  throw new Error(`unknown command ${command}`);
}

export async function runCreate(argv: string[]) {
  const target = argv.find((arg) => !arg.startsWith("-")) ?? "my-gonvex-app";
  const appName = basename(target);
  const template = valueFor(argv, "--template") ?? "vite-react";
  const root = resolve(target);
  if (existsSync(root)) throw new Error(`${target} already exists`);

  await copyTemplate(template, root);
  await rewritePackageName(root, appName);
  await rewriteGonvexConfig(root, appName, defaultRuntimeURL);
  await writeEnvLocal(root, appName, defaultRuntimeURL);
  console.log(`[gonvex] created ${target} from ${template} template`);
  console.log(`[gonvex] next: cd ${target} && npm install && npm run dev`);
}

async function runInit(argv: string[]) {
  const template = valueFor(argv, "--template") ?? "vite-react";
  const project = valueFor(argv, "--project") ?? basename(process.cwd());
  const runtime = valueFor(argv, "--runtime") ?? defaultRuntimeURL;
  await copyTemplate(template, process.cwd(), { overwrite: false });
  await writeEnvLocal(process.cwd(), project, runtime);
  console.log(`[gonvex] initialized ${project}`);
}

async function runDev(argv: string[]) {
  const split = argv.indexOf("--");
  const flagArgs = split === -1 ? argv : argv.slice(0, split);
  const childCommand = split === -1 ? [] : argv.slice(split + 1);
  const projectRoot = resolve(valueFor(flagArgs, "--project") ?? ".");
  const once = flagArgs.includes("--once");
  if (once && childCommand.length > 0) throw new Error("--once cannot be used with a child command");

  let settings = await loadSettings(projectRoot, {
    runtimeURL: valueFor(flagArgs, "--runtime-url"),
    projectID: valueFor(flagArgs, "--project-id"),
    key: valueFor(flagArgs, "--key"),
  });
  settings = await ensureProjectSettings(projectRoot, settings, {
    keyWasExplicit: Boolean(valueFor(flagArgs, "--key")),
    runtimeWasExplicit: Boolean(valueFor(flagArgs, "--runtime-url")),
  });

  if (childCommand.length > 0) {
    const initialState = await watchProject(projectRoot, settings, true);
    const controller = new AbortController();
    const watcher = watchProject(projectRoot, settings, false, controller.signal, initialState);
    const child = spawn(childCommand[0]!, childCommand.slice(1), {
      cwd: projectRoot,
      stdio: "inherit",
      signal: controller.signal,
      shell: process.platform === "win32",
    });
    const code = await new Promise<number | null>((resolve) => child.on("exit", resolve));
    controller.abort();
    await watcher.catch((error) => {
      if (error?.name !== "AbortError") throw error;
    });
    process.exitCode = code ?? 0;
    return;
  }

  await watchProject(projectRoot, settings, once);
}

async function runEnv(argv: string[]) {
  const parsedArgs = parseEnvCommandArgs(argv);
  const action = parsedArgs.action;
  if (!action || action === "help" || action === "--help") {
    printEnvHelp();
    return;
  }
  const projectRoot = resolve(parsedArgs.options["--project"] ?? ".");
  const settings = await loadSettings(projectRoot, {
    runtimeURL: parsedArgs.options["--runtime-url"],
    projectID: parsedArgs.options["--project-id"],
    key: parsedArgs.options["--key"],
  });
  if (!settings.key) throw new Error("GONVEX_PROJECT_KEY is required for project env commands");

  if (action === "list" || action === "ls") {
    const variables = await fetchProjectEnv(settings);
    if (variables.length === 0) {
      console.log(`[gonvex] no env vars set for ${settings.projectID}`);
      return;
    }
    for (const variable of variables) {
      console.log(`${variable.name}=${variable.sensitive ? variable.masked : variable.value ?? variable.masked}`);
    }
    return;
  }

  if (action === "get") {
    const name = parsedArgs.positional[0];
    if (!name) throw new Error("usage: gonvex env get NAME");
    const variables = await fetchProjectEnv(settings);
    const variable = variables.find((item) => item.name === name);
    if (!variable) throw new Error(`${name} is not set for ${settings.projectID}`);
    console.log(variable.sensitive ? variable.masked : variable.value ?? variable.masked);
    return;
  }

  if (action === "set") {
    const parsed = parseEnvSetArgs(parsedArgs.positional);
    await saveProjectEnv(settings, parsed.name, parsed.value);
    console.log(`[gonvex] saved ${parsed.name} for ${settings.projectID}`);
    return;
  }

  if (action === "remove" || action === "rm" || action === "unset" || action === "delete") {
    const name = parsedArgs.positional[0];
    if (!name) throw new Error("usage: gonvex env remove NAME");
    await deleteProjectEnv(settings, name);
    console.log(`[gonvex] removed ${name} from ${settings.projectID}`);
    return;
  }

  printEnvHelp();
  throw new Error(`unknown env command ${action}`);
}

async function watchProject(root: string, settings: Settings, once: boolean, signal?: AbortSignal, initialState?: WatchState): Promise<WatchState> {
  const backendDir = join(root, "gonvex");
  await mkdir(backendDir, { recursive: true });
  let lastFingerprint = initialState?.lastFingerprint ?? "";
  let lastManifest: Manifest | null = initialState?.lastManifest ?? null;
  let lastSyncAttempt = initialState?.lastSyncAttempt ?? 0;
  let lastSyncSucceeded = initialState?.lastSyncSucceeded ?? false;
  let lastRuntimeCheck = initialState?.lastRuntimeCheck ?? 0;

  while (!signal?.aborted) {
    const files = await goFiles(backendDir);
    const fingerprint = await filesFingerprint(files);
    const now = Date.now();
    const shouldBuild = fingerprint !== lastFingerprint;
    const shouldRetryRuntimeSync = !once && !lastSyncSucceeded && lastManifest !== null && now - lastSyncAttempt > runtimeSyncRetryMs;
    const shouldVerifyRuntimeState = !once && lastSyncSucceeded && lastManifest !== null && now - lastRuntimeCheck > runtimeStateCheckMs;

    if (shouldBuild || shouldRetryRuntimeSync) {
      lastFingerprint = fingerprint;
      let manifest: Manifest;
      if (shouldBuild) {
        manifest = await buildManifest(root, files, settings.projectID);
      } else {
        manifest = lastManifest!;
      }
      const isInitialBuild = lastManifest === null;
      const previousManifest = lastManifest ?? await readGeneratedManifest(root);
      if (shouldBuild) {
        lastManifest = manifest;
        const writeResult = await writeBindings(root, manifest);
        logFunctionDiff(previousManifest, manifest, writeResult, isInitialBuild);
      }
      lastSyncAttempt = now;
      try {
        await syncRuntime(settings, manifest);
        lastSyncSucceeded = true;
        lastRuntimeCheck = now;
        console.log(`[gonvex] synced project ${settings.projectID || "(key-inferred)"} to ${settings.runtimeURL}`);
      } catch (error) {
        lastSyncSucceeded = false;
        console.error(`[gonvex] runtime sync failed: ${error instanceof Error ? error.message : String(error)}`);
      }
    } else if (shouldVerifyRuntimeState) {
      const manifest = lastManifest;
      if (manifest === null) continue;
      lastRuntimeCheck = now;
      try {
        const inSync = await runtimeHasManifest(settings, manifest);
        if (!inSync) {
          lastSyncAttempt = now;
          await syncRuntime(settings, manifest);
          lastSyncSucceeded = true;
          console.log(`[gonvex] runtime state was missing; re-synced project ${settings.projectID || "(key-inferred)"}`);
        }
      } catch {
        lastSyncSucceeded = false;
      }
    }

    if (once) {
      return { lastFingerprint, lastManifest, lastSyncAttempt, lastSyncSucceeded, lastRuntimeCheck };
    }
    await sleep(500, signal);
  }
  return { lastFingerprint, lastManifest, lastSyncAttempt, lastSyncSucceeded, lastRuntimeCheck };
}

async function buildManifest(root: string, files: string[], projectID: string): Promise<Manifest> {
  const functions: Record<string, FunctionEntry> = {};
  const schema = emptySchemaDefinition();
  let packageName = "app";
  for (const file of files) {
    Object.assign(functions, await parseRegistrations(root, file));
    mergeSchemaDefinition(schema, await parseSchema(file));
    if (packageName === "app") {
      packageName = await detectPackageName(file);
    }
  }
  const bundle = await buildSourceBundle(root, files, projectID, packageName);
  return {
    project: projectID,
    generatedAt: new Date().toISOString(),
    functions,
    schema,
    bundle,
  };
}

async function buildSourceBundle(root: string, files: string[], projectID: string, packageName: string): Promise<SourceBundle> {
  const backendDir = join(root, "gonvex");
  const encodedFiles: Record<string, string> = {};
  for (const file of files) {
    const source = await readFile(file);
    const rel = relative(backendDir, file).replace(/\\/g, "/");
    encodedFiles[`app/${rel}`] = Buffer.from(source).toString("base64");
  }
  const hash = createHash("sha256");
  for (const path of Object.keys(encodedFiles).sort()) {
    hash.update(`${path}:${encodedFiles[path]};`);
  }
  return {
    hash: hash.digest("hex"),
    modulePath: `gonvexapp/${sanitizeProjectID(projectID)}`,
    packageName,
    files: encodedFiles,
  };
}

async function detectPackageName(file: string) {
  const source = await readFile(file, "utf8");
  const match = source.match(/^package\s+([A-Za-z_][A-Za-z0-9_]*)/m);
  return match?.[1] ?? "app";
}

function sanitizeProjectID(projectID: string) {
  const trimmed = projectID.trim();
  if (!trimmed) return "project";
  return trimmed.replace(/[^a-zA-Z0-9._-]+/g, "-");
}

async function parseRegistrations(root: string, file: string) {
  const source = await readFile(file, "utf8");
  const pattern = /app\.(Query|Mutation|Action|HTTP|InternalMutation|LiveGrid)\(\s*"([^"]+)"\s*,\s*([A-Za-z_][A-Za-z0-9_]*)/g;
  const entries: Record<string, FunctionEntry> = {};
  for (const match of source.matchAll(pattern)) {
    entries[match[2]!] = {
      kind: functionKind(match[1]!),
      handler: match[3]!,
      file: relative(root, file),
    };
  }
  return entries;
}

async function parseSchema(file: string) {
  const source = await readFile(file, "utf8");
  const tablePattern = /s\.(Table|TenantTable|LandlordTable)\(\s*"([^"]+)"\s*,\s*func\([^)]*\)\s*\{([\s\S]*?)\n\s*\}\s*\)/g;
  const columnPattern = /t\.(ID|String|Text|Int|Int64|Float64|Bool|Time|JSON)\(\s*"([^"]+)"([^)]*)\)/g;
  const indexPattern = /t\.(Index|UniqueIndex)\(\s*"([^"]+)"([^)]*)\)/g;
  const schema = emptySchemaDefinition();
  for (const tableMatch of source.matchAll(tablePattern)) {
    const table: Table = { columns: {}, indexes: {} };
    const scope = tableMatch[1]!;
    const name = tableMatch[2]!;
    const body = tableMatch[3]!;
    for (const columnMatch of body.matchAll(columnPattern)) {
      const kind = columnMatch[1]!;
      table.columns[columnMatch[2]!] = {
        type: columnType(kind),
        nullable: columnMatch[3]!.includes("gonvex.Nullable"),
        primaryKey: kind === "ID",
      };
    }
    for (const indexMatch of body.matchAll(indexPattern)) {
      table.indexes[indexMatch[2]!] = {
        columns: stringArgs(indexMatch[3]!),
        unique: indexMatch[1] === "UniqueIndex",
      };
    }
    if (scope === "LandlordTable") {
      schema.landlordTables[name] = table;
    } else {
      schema.tenantTables[name] = table;
      schema.tables[name] = table;
    }
  }
  return schema;
}

async function writeBindings(root: string, manifest: Manifest): Promise<BindingWriteResult> {
  const dir = join(root, "gonvex", "_generated");
  await mkdir(dir, { recursive: true });
  let changedFiles = 0;
  if (await writeManifestIfChanged(join(dir, "manifest.json"), manifest)) changedFiles += 1;
  const outputs: Record<string, string> = {
    "api.ts": renderAPI(manifest),
    "client.ts": '// Generated by gonvex dev. Do not edit.\nexport { GonvexClient, ConvexReactClient } from "@gonvex/client";\n',
    "react.ts": '// Generated by gonvex dev. Do not edit.\nexport { ConvexProvider, ConvexProviderWithAuth, ConvexReactClient, GonvexProvider, useAction, useConvex, useConvexAuth, useConvexConnectionState, useMutation, usePaginatedQuery, useQuery } from "@gonvex/react";\n',
    "types.ts": "// Generated by gonvex dev. Do not edit.\nexport type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n",
    "schema.ts": renderSchemaIndex(manifest),
    "landlord/schema.ts": renderScopedSchemaModule("landlord", manifest.schema.landlordTables),
    "landlord/tables.ts": renderScopedTablesModule("landlord", manifest.schema.landlordTables),
    "tenant/schema.ts": renderScopedSchemaModule("tenant", manifest.schema.tenantTables),
    "tenant/tables.ts": renderScopedTablesModule("tenant", manifest.schema.tenantTables),
  };
  for (const [name, contents] of Object.entries(outputs)) {
    if (await writeFileIfChanged(join(dir, name), contents)) changedFiles += 1;
  }
  return { changedFiles };
}

async function writeManifestIfChanged(path: string, manifest: Manifest) {
  try {
    const existing = JSON.parse(await readFile(path, "utf8")) as Manifest;
    if (JSON.stringify(manifestWithoutGeneratedAt(existing)) === JSON.stringify(manifestWithoutGeneratedAt(manifest))) {
      return false;
    }
  } catch {
    // Missing, stale, or invalid generated manifests are repaired by writing them.
  }
  await writeFile(path, `${JSON.stringify(manifest, null, 2)}\n`);
  return true;
}

async function writeFileIfChanged(path: string, contents: string) {
  try {
    if ((await readFile(path, "utf8")) === contents) return false;
  } catch {
    // Missing or unreadable generated files are repaired by writing them.
  }
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
  return true;
}

async function readGeneratedManifest(root: string): Promise<Manifest | null> {
  try {
    return JSON.parse(await readFile(join(root, "gonvex", "_generated", "manifest.json"), "utf8")) as Manifest;
  } catch {
    return null;
  }
}

function manifestWithoutGeneratedAt(manifest: Manifest) {
  const { generatedAt: _generatedAt, ...rest } = manifest;
  return rest;
}

function logFunctionDiff(previous: Manifest | null, current: Manifest, writeResult: BindingWriteResult, isInitialBuild: boolean) {
  const functionCount = Object.keys(current.functions).length;
  if (!previous) {
    console.log(`[gonvex] uploaded ${functionCount} runtime function(s)`);
    return;
  }

  const diff = diffFunctions(previous, current);
  if (diff.added.length === 0 && diff.removed.length === 0 && diff.edited.length === 0) {
    if (isInitialBuild && writeResult.changedFiles === 0) {
      return;
    }
    if (isInitialBuild) {
      return;
    }
    console.log("[gonvex] ~ uploaded runtime source changes (no function API changes)");
    return;
  }

  for (const path of diff.added) console.log(color.green(`[gonvex] + added function ${path}`));
  for (const path of diff.edited) console.log(color.yellow(`[gonvex] ~ edited function ${path}`));
  for (const path of diff.removed) console.log(color.red(`[gonvex] - removed function ${path}`));
}

function diffFunctions(previous: Manifest, current: Manifest): FunctionDiff {
  const previousPaths = new Set(Object.keys(previous.functions));
  const currentPaths = new Set(Object.keys(current.functions));
  const added = [...currentPaths].filter((path) => !previousPaths.has(path)).sort();
  const removed = [...previousPaths].filter((path) => !currentPaths.has(path)).sort();
  const edited = [...currentPaths]
    .filter((path) => previousPaths.has(path))
    .filter((path) => functionEntryChanged(previous, current, path))
    .sort();
  return { added, removed, edited };
}

function functionEntryChanged(previous: Manifest, current: Manifest, path: string) {
  const previousEntry = previous.functions[path]!;
  const currentEntry = current.functions[path]!;
  if (JSON.stringify(previousEntry) !== JSON.stringify(currentEntry)) return true;
  const previousBundleFile = bundleFileForFunction(previousEntry);
  const currentBundleFile = bundleFileForFunction(currentEntry);
  return previous.bundle?.files[previousBundleFile] !== current.bundle?.files[currentBundleFile];
}

function bundleFileForFunction(entry: FunctionEntry) {
  const normalized = entry.file.replace(/\\/g, "/").replace(/^\.?\//, "");
  const withoutGonvexPrefix = normalized.startsWith("gonvex/") ? normalized.slice("gonvex/".length) : normalized;
  return `app/${withoutGonvexPrefix}`;
}

function renderAPI(manifest: Manifest) {
  const root: Record<string, any> = {};
  for (const [path, entry] of Object.entries(manifest.functions).sort(([a], [b]) => a.localeCompare(b))) {
    const parts = path.split(".").filter(Boolean);
    if (parts.length === 0) continue;
    let target = root;
    for (const part of parts.slice(0, -1)) {
      target = target[part] ??= {};
    }
    target[parts[parts.length - 1]!] = { kind: entry.kind, path };
  }
  const lines = [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const api = ${renderObject(root, 0)} as const;`,
    "",
    "export const internal = api;",
    "export type Api = typeof api;",
    "",
  ];
  return lines.join("\n");
}

function renderSchemaIndex(manifest: Manifest) {
  const landlord = renderSchemaObject("landlord", manifest.schema.landlordTables, 0);
  const tenant = renderSchemaObject("tenant", manifest.schema.tenantTables, 0);
  return [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const landlord = ${landlord} as const;`,
    "",
    `export const tenant = ${tenant} as const;`,
    "",
    "export const tables = tenant.tables;",
    "",
    "export const schema = {",
    "  landlord,",
    "  tenant,",
    "  tables,",
    "} as const;",
    "",
    "export type LandlordTableName = keyof typeof landlord.tables;",
    "export type TenantTableName = keyof typeof tenant.tables;",
    "export type TableName = TenantTableName;",
    "",
  ].join("\n");
}

function renderScopedSchemaModule(scope: "landlord" | "tenant", tables: Record<string, Table>) {
  return [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const schema = ${renderSchemaObject(scope, tables, 0)} as const;`,
    "",
    "export const tables = schema.tables;",
    "",
    "export type TableName = keyof typeof tables;",
    "",
  ].join("\n");
}

function renderScopedTablesModule(scope: "landlord" | "tenant", tables: Record<string, Table>) {
  return [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const scope = ${JSON.stringify(scope)} as const;`,
    "",
    `export const tables = ${renderObject(tables, 0)} as const;`,
    "",
    "export type TableName = keyof typeof tables;",
    "",
  ].join("\n");
}

function renderSchemaObject(scope: "landlord" | "tenant", tables: Record<string, Table>, depth: number) {
  return renderObject({ scope, tables }, depth);
}

function renderObject(value: any, depth: number): string {
  if (!value || typeof value !== "object" || Array.isArray(value)) return JSON.stringify(value);
  const entries = Object.entries(value).sort(([a], [b]) => a.localeCompare(b));
  if (isFunctionRef(value)) {
    return `{ kind: ${JSON.stringify(value.kind)}, path: ${JSON.stringify(value.path)} }`;
  }
  const indent = "  ".repeat(depth);
  const childIndent = "  ".repeat(depth + 1);
  const lines = ["{"];
  for (const [key, child] of entries) {
    lines.push(`${childIndent}${propertyKey(key)}: ${renderObject(child, depth + 1)},`);
  }
  lines.push(`${indent}}`);
  return lines.join("\n");
}

function isFunctionRef(value: any) {
  return typeof value.kind === "string" && typeof value.path === "string" && Object.keys(value).length === 2;
}

function propertyKey(key: string) {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);
}

function emptySchemaDefinition(): SchemaDefinition {
  return {
    tables: {},
    landlordTables: {},
    tenantTables: {},
  };
}

function mergeSchemaDefinition(target: SchemaDefinition, source: SchemaDefinition) {
  Object.assign(target.landlordTables, source.landlordTables);
  Object.assign(target.tenantTables, source.tenantTables);
  target.tables = target.tenantTables;
}

async function syncRuntime(settings: Settings, manifest: Manifest) {
  const response = await fetch(`${settings.runtimeURL.replace(/\/$/, "")}/dev/sync`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(settings.projectID ? { "x-gonvex-project-id": settings.projectID } : {}),
      ...(settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {}),
    },
    body: JSON.stringify(manifest),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
}

async function fetchProjectEnv(settings: Settings): Promise<RuntimeEnvVariable[]> {
  const response = await fetch(projectEnvURL(settings), {
    headers: projectAuthHeaders(settings),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
  const payload = await response.json() as { variables?: RuntimeEnvVariable[] };
  return payload.variables ?? [];
}

async function saveProjectEnv(settings: Settings, name: string, value: string) {
  const response = await fetch(projectEnvURL(settings), {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...projectAuthHeaders(settings),
    },
    body: JSON.stringify({ name, value }),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
}

async function deleteProjectEnv(settings: Settings, name: string) {
  const response = await fetch(projectEnvURL(settings), {
    method: "DELETE",
    headers: {
      "content-type": "application/json",
      ...projectAuthHeaders(settings),
    },
    body: JSON.stringify({ name }),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
}

function projectEnvURL(settings: Settings) {
  return `${settings.runtimeURL.replace(/\/$/, "")}/dev/projects/${encodeURIComponent(settings.projectID)}/env`;
}

function projectAuthHeaders(settings: Settings): Record<string, string> {
  return settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {};
}

function parseEnvSetArgs(positional: string[]) {
  const first = positional[0];
  if (!first) throw new Error("usage: gonvex env set NAME VALUE");
  const equalsIndex = first.indexOf("=");
  if (equalsIndex > 0 && positional.length === 1) {
    return { name: first.slice(0, equalsIndex), value: first.slice(equalsIndex + 1) };
  }
  const value = positional.slice(1).join(" ");
  if (!value) throw new Error("usage: gonvex env set NAME VALUE");
  return { name: first, value };
}

function parseEnvCommandArgs(argv: string[]) {
  const options: Record<string, string> = {};
  const positional: string[] = [];
  let action = "";
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!;
    if (["--project", "--runtime-url", "--project-id", "--key"].includes(arg)) {
      const value = argv[index + 1];
      if (value === undefined) throw new Error(`${arg} requires a value`);
      options[arg] = value;
      index += 1;
      continue;
    }
    if (!action && !arg.startsWith("-")) {
      action = arg;
      continue;
    }
    positional.push(arg);
  }
  return { action, options, positional };
}

async function runtimeHasManifest(settings: Settings, manifest: Manifest) {
  if (!manifest.project) return true;
  const url = new URL(`${settings.runtimeURL.replace(/\/$/, "")}/dev/manifest`);
  url.searchParams.set("project", manifest.project);
  const response = await fetch(url, {
    headers: {
      ...(settings.projectID ? { "x-gonvex-project-id": settings.projectID } : {}),
      ...(settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {}),
    },
  });
  if (!response.ok) return false;
  const current = await response.json() as Manifest;
  return current.project === manifest.project
    && current.bundle?.hash === manifest.bundle?.hash
    && Object.keys(current.functions ?? {}).length === Object.keys(manifest.functions ?? {}).length;
}

async function ensureProjectSettings(root: string, settings: Settings, options: { keyWasExplicit: boolean; runtimeWasExplicit: boolean }): Promise<Settings> {
  if (settings.key) return settings;
  if (options.keyWasExplicit) return settings;
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    console.warn("[gonvex] GONVEX_PROJECT_KEY is not configured; runtime sync will fail if the runtime requires project keys.");
    return settings;
  }

  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    console.log("[gonvex] No Gonvex project is configured for this app.");
    const runtimeURL = await promptDefault(rl, "Runtime URL", settings.runtimeURL || defaultRuntimeURL);
    settings = { ...settings, runtimeURL };

    const projects = await fetchRuntimeProjects(runtimeURL);
    let project: RuntimeProject;
    if (projects.length > 0) {
      console.log("[gonvex] Choose a project:");
      projects.forEach((item, index) => {
        console.log(`  ${index + 1}. ${item.name} (${item.id})`);
      });
      console.log(`  ${projects.length + 1}. Create a new project`);
      const choice = await promptDefault(rl, "Project", "1");
      const index = Number.parseInt(choice, 10);
      if (Number.isFinite(index) && index >= 1 && index <= projects.length) {
        project = projects[index - 1]!;
        const projectKey = await promptDefault(rl, "Project key from dashboard", "");
        if (!projectKey) throw new Error("Gonvex project key is required for existing projects");
        const next = { ...settings, projectID: project.id, key: projectKey };
        await upsertEnvLocal(root, {
          GONVEX_RUNTIME_URL: runtimeURL,
          VITE_GONVEX_URL: runtimeURL,
          GONVEX_PROJECT_KEY: projectKey,
          VITE_GONVEX_WS_URL: webSocketURL(runtimeURL),
        });
        console.log(`[gonvex] configured ${project.id} in .env.local`);
        return next;
      } else {
        const created = await createRuntimeProject(runtimeURL, await promptDefault(rl, "New project name", basename(root)));
        project = created.project;
        const next = { ...settings, projectID: project.id, key: created.projectKey };
        await upsertEnvLocal(root, {
          GONVEX_RUNTIME_URL: runtimeURL,
          VITE_GONVEX_URL: runtimeURL,
          GONVEX_PROJECT_KEY: created.projectKey,
          VITE_GONVEX_WS_URL: webSocketURL(runtimeURL),
        });
        console.log(`[gonvex] configured ${project.id} in .env.local`);
        return next;
      }
    } else {
      const created = await createRuntimeProject(runtimeURL, await promptDefault(rl, "New project name", basename(root)));
      project = created.project;
      const next = { ...settings, projectID: project.id, key: created.projectKey };
      await upsertEnvLocal(root, {
        GONVEX_RUNTIME_URL: runtimeURL,
        VITE_GONVEX_URL: runtimeURL,
        GONVEX_PROJECT_KEY: created.projectKey,
        VITE_GONVEX_WS_URL: webSocketURL(runtimeURL),
      });
      console.log(`[gonvex] configured ${project.id} in .env.local`);
      return next;
    }
  } finally {
    rl.close();
  }
}

async function fetchRuntimeProjects(runtimeURL: string): Promise<RuntimeProject[]> {
  const response = await fetch(`${runtimeURL.replace(/\/$/, "")}/dev/projects`);
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
  const payload = await response.json() as { projects?: RuntimeProject[] };
  return payload.projects ?? [];
}

async function createRuntimeProject(runtimeURL: string, name: string): Promise<CreatedRuntimeProject> {
  const response = await fetch(`${runtimeURL.replace(/\/$/, "")}/dev/projects`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
  const payload = await response.json() as { project: RuntimeProject; projectKey?: string };
  if (!payload.projectKey) throw new Error("runtime did not return a project key");
  return { project: payload.project, projectKey: payload.projectKey };
}

async function promptDefault(rl: ReturnType<typeof createInterface>, label: string, fallback: string) {
  const answer = (await rl.question(`${label} (${fallback}): `)).trim();
  return answer || fallback;
}

async function upsertEnvLocal(root: string, values: Record<string, string>) {
  const envPath = join(root, ".env.local");
  const existing = existsSync(envPath) ? await readFile(envPath, "utf8") : "";
  const seen = new Set<string>();
  const lines = existing.split(/\r?\n/).filter((line, index, array) => index < array.length - 1 || line !== "");
  const next = lines.flatMap((line) => {
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=/);
    if (!match) return [line];
    const key = match[1]!;
    if (!(key in values)) return [line];
    if (seen.has(key)) return [];
    seen.add(key);
    if (envLineValue(line) !== "") return [line];
    return [`${key}=${values[key]}`];
  });
  for (const [key, value] of Object.entries(values)) {
    if (!seen.has(key)) next.push(`${key}=${value}`);
  }
  await writeFile(envPath, `${next.join("\n")}\n`);
}

function envLineValue(line: string) {
  const index = line.indexOf("=");
  if (index === -1) return "";
  return line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
}

function webSocketURL(runtimeURL: string) {
  return runtimeURL.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/ws";
}

async function loadSettings(root: string, overrides: Partial<Settings>): Promise<Settings> {
  loadDotEnv(join(root, ".env.local"));
  loadDotEnv(join(root, ".env"));
  const config = await loadConfig(root);
  const key = overrides.key ?? process.env.GONVEX_PROJECT_KEY ?? process.env.GONVEX_DEPLOY_KEY ?? process.env.GONVEX_KEY ?? "";
  const explicitProjectID = overrides.projectID ?? process.env.GONVEX_PROJECT_ID ?? process.env.GONVEX_PROJECT ?? config.project;
  return {
    projectID: overrides.projectID ?? projectIDFromKey(key) ?? explicitProjectID ?? basename(root),
    runtimeURL: overrides.runtimeURL ?? process.env.GONVEX_RUNTIME_URL ?? config.runtime ?? defaultRuntimeURL,
    key,
  };
}

function projectIDFromKey(key: string) {
  const trimmed = key.trim();
  if (!trimmed.startsWith("gvx_")) return undefined;
  const payload = trimmed.slice("gvx_".length);
  let encodedProject = payload.split(".", 1)[0];
  if (encodedProject === payload) {
    const parts = trimmed.split("_");
    if (parts.length !== 3 || parts[0] !== "gvx" || !parts[1]) return undefined;
    encodedProject = parts[1]!;
  }
  try {
    return Buffer.from(encodedProject, "base64url").toString("utf8").trim() || undefined;
  } catch {
    return undefined;
  }
}

async function loadConfig(root: string): Promise<{ project?: string; runtime?: string }> {
  try {
    return JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
  } catch {
    return {};
  }
}

function loadDotEnv(path: string) {
  if (!existsSync(path)) return;
  const content = readFileSyncText(path);
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index === -1) continue;
    const key = line.slice(0, index).trim();
    if (!key || process.env[key] !== undefined) continue;
    process.env[key] = line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
  }
}

function readFileSyncText(path: string) {
  return existsSync(path) ? readFileSync(path, "utf8") : "";
}

async function goFiles(root: string): Promise<string[]> {
  if (!existsSync(root)) return [];
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "_generated") continue;
      files.push(...await goFiles(path));
    } else if (entry.isFile() && entry.name.endsWith(".go")) {
      files.push(path);
    }
  }
  return files.sort();
}

async function filesFingerprint(files: string[]) {
  const hash = createHash("sha256");
  for (const file of files) {
    const info = await stat(file);
    hash.update(`${file}:${info.mtimeMs}:${info.size};`);
  }
  return hash.digest("hex");
}

async function copyTemplate(template: string, target: string, options: { overwrite?: boolean } = {}) {
  const source = templateDir(template);
  if (!existsSync(source)) throw new Error(`unknown template ${template}`);
  await copyDir(source, target, options.overwrite ?? true);
}

async function copyDir(source: string, target: string, overwrite: boolean) {
  await mkdir(target, { recursive: true });
  for (const entry of await readdir(source, { withFileTypes: true })) {
    const sourcePath = join(source, entry.name);
    const targetPath = join(target, entry.name);
    if (entry.isDirectory()) {
      await copyDir(sourcePath, targetPath, overwrite);
    } else if (overwrite || !existsSync(targetPath)) {
      await mkdir(dirname(targetPath), { recursive: true });
      await copyFile(sourcePath, targetPath);
    }
  }
}

async function rewritePackageName(root: string, name: string) {
  const packagePath = join(root, "package.json");
  const packageJSON = JSON.parse(await readFile(packagePath, "utf8"));
  packageJSON.name = name;
  await writeFile(packagePath, `${JSON.stringify(packageJSON, null, 2)}\n`);
}

async function rewriteGonvexConfig(root: string, project: string, runtime: string) {
  const configPath = join(root, "gonvex.json");
  if (!existsSync(configPath)) return;
  const config = JSON.parse(await readFile(configPath, "utf8"));
  config.project = project;
  config.runtime = runtime;
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`);
}

async function writeEnvLocal(root: string, project: string, runtime: string) {
  const envPath = join(root, ".env.local");
  if (existsSync(envPath)) return;
  const wsURL = runtime.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/ws";
  await writeFile(envPath, `GONVEX_PROJECT_ID=${project}\nGONVEX_RUNTIME_URL=${runtime}\nGONVEX_PROJECT_KEY=\nVITE_GONVEX_WS_URL=${wsURL}\n`);
}

function templateDir(template: string) {
  const packageTemplate = resolve(dirname(fileURLToPath(import.meta.url)), "templates", template);
  if (existsSync(packageTemplate)) return packageTemplate;
  return resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "templates", template);
}

function functionKind(raw: string): FunctionKind {
  if (raw === "InternalMutation") return "internalMutation";
  if (raw === "LiveGrid") return "liveGrid";
  return raw.toLowerCase() as FunctionKind;
}

function columnType(kind: string) {
  if (kind === "ID") return "id";
  if (kind === "Int64") return "int64";
  if (kind === "Float64") return "float64";
  return kind.toLowerCase();
}

function stringArgs(input: string) {
  return [...input.matchAll(/"([^"]+)"/g)].map((match) => match[1]!);
}

function valueFor(args: string[], key: string) {
  const index = args.indexOf(key);
  if (index === -1) return undefined;
  return args[index + 1];
}

function basename(path: string) {
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts.at(-1) ?? "app";
}

function sleep(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(timeout);
      reject(new DOMException("aborted", "AbortError"));
    }, { once: true });
  });
}

function printHelp() {
  console.log("Usage: gonvex <dev|init|create|env> [options]");
  console.log("  gonvex dev [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>] [--once] [-- <command>]");
  console.log("  gonvex init [--template vite-react] [--project <id>] [--runtime <url>]");
  console.log("  gonvex create <app-name> [--template vite-react]");
  console.log("  gonvex env <list|get|set|remove> [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>]");
}

function printEnvHelp() {
  console.log("Usage: gonvex env <command> [options]");
  console.log("  gonvex env list");
  console.log("  gonvex env get NAME");
  console.log("  gonvex env set NAME VALUE");
  console.log("  gonvex env set NAME=VALUE");
  console.log("  gonvex env remove NAME");
}

function isCliEntrypoint() {
  const invokedPath = process.argv[1];
  if (!invokedPath) return false;
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(resolve(invokedPath));
  } catch {
    return fileURLToPath(import.meta.url) === resolve(invokedPath);
  }
}

if (isCliEntrypoint()) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
