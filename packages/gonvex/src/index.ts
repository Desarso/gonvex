#!/usr/bin/env node
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, readdir, stat, writeFile, copyFile } from "node:fs/promises";
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

type Manifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionEntry>;
  schema: { tables: Record<string, Table> };
};

type Settings = {
  projectID: string;
  runtimeURL: string;
  key: string;
};

const defaultRuntimeURL = "http://localhost:8080";

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

  const settings = await loadSettings(projectRoot, {
    runtimeURL: valueFor(flagArgs, "--runtime-url"),
    projectID: valueFor(flagArgs, "--project-id"),
    key: valueFor(flagArgs, "--key"),
  });

  if (childCommand.length > 0) {
    await watchProject(projectRoot, settings, true);
    const controller = new AbortController();
    const watcher = watchProject(projectRoot, settings, false, controller.signal);
    const child = spawn(childCommand[0]!, childCommand.slice(1), {
      cwd: projectRoot,
      stdio: "inherit",
      signal: controller.signal,
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

async function watchProject(root: string, settings: Settings, once: boolean, signal?: AbortSignal) {
  const backendDir = join(root, "gonvex");
  await mkdir(backendDir, { recursive: true });
  let lastFingerprint = "";
  let lastSyncAttempt = 0;

  while (!signal?.aborted) {
    const files = await goFiles(backendDir);
    const fingerprint = await filesFingerprint(files);
    const now = Date.now();
    const shouldRetryRuntimeSync = !once && fingerprint === lastFingerprint && now - lastSyncAttempt > 5000;

    if (fingerprint !== lastFingerprint || shouldRetryRuntimeSync) {
      lastFingerprint = fingerprint;
      lastSyncAttempt = now;
      const manifest = await buildManifest(root, files, settings.projectID);
      await writeBindings(root, manifest);
      try {
        await syncRuntime(settings, manifest);
        console.log(`[gonvex] synced project ${settings.projectID} to ${settings.runtimeURL}`);
      } catch (error) {
        console.error(`[gonvex] runtime sync failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      console.log(`[gonvex] generated ${Object.keys(manifest.functions).length} function binding(s)`);
    }

    if (once) return;
    await sleep(500, signal);
  }
}

async function buildManifest(root: string, files: string[], projectID: string): Promise<Manifest> {
  const functions: Record<string, FunctionEntry> = {};
  const tables: Record<string, Table> = {};
  for (const file of files) {
    Object.assign(functions, await parseRegistrations(root, file));
    Object.assign(tables, await parseSchema(file));
  }
  return {
    project: projectID,
    generatedAt: new Date().toISOString(),
    functions,
    schema: { tables },
  };
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
  const tablePattern = /s\.Table\(\s*"([^"]+)"\s*,\s*func\([^)]*\)\s*\{([\s\S]*?)\n\s*\}\s*\)/g;
  const columnPattern = /t\.(ID|String|Text|Int|Int64|Float64|Bool|Time|JSON)\(\s*"([^"]+)"([^)]*)\)/g;
  const indexPattern = /t\.(Index|UniqueIndex)\(\s*"([^"]+)"([^)]*)\)/g;
  const tables: Record<string, Table> = {};
  for (const tableMatch of source.matchAll(tablePattern)) {
    const table: Table = { columns: {}, indexes: {} };
    const body = tableMatch[2]!;
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
    tables[tableMatch[1]!] = table;
  }
  return tables;
}

async function writeBindings(root: string, manifest: Manifest) {
  const dir = join(root, "gonvex", "_generated");
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  await writeFile(join(dir, "api.ts"), renderAPI(manifest));
  await writeFile(join(dir, "client.ts"), '// Generated by gonvex dev. Do not edit.\nexport { GonvexClient, ConvexReactClient } from "@gonvex/client";\n');
  await writeFile(join(dir, "react.ts"), '// Generated by gonvex dev. Do not edit.\nexport { ConvexProvider, GonvexProvider, useAction, useConvex, useConvexAuth, useConvexConnectionState, useMutation, usePaginatedQuery, useQuery } from "@gonvex/react";\n');
  await writeFile(join(dir, "types.ts"), "// Generated by gonvex dev. Do not edit.\nexport type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n");
  await writeFile(join(dir, "schema.ts"), "// Generated by gonvex dev. Do not edit.\nexport const schema = {} as const;\n");
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
  const lines = ["// Generated by gonvex dev. Do not edit.", "", "export const api = ", renderObject(root, 0), " as const;", "", "export const internal = api;", "export type Api = typeof api;", ""];
  return lines.join("\n");
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

async function syncRuntime(settings: Settings, manifest: Manifest) {
  const response = await fetch(`${settings.runtimeURL.replace(/\/$/, "")}/dev/sync`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-gonvex-project-id": settings.projectID,
      ...(settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {}),
    },
    body: JSON.stringify(manifest),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
}

async function loadSettings(root: string, overrides: Partial<Settings>): Promise<Settings> {
  loadDotEnv(join(root, ".env.local"));
  loadDotEnv(join(root, ".env"));
  const config = await loadConfig(root);
  return {
    projectID: overrides.projectID ?? process.env.GONVEX_PROJECT_ID ?? process.env.GONVEX_PROJECT ?? config.project ?? basename(root),
    runtimeURL: overrides.runtimeURL ?? process.env.GONVEX_RUNTIME_URL ?? config.runtime ?? defaultRuntimeURL,
    key: overrides.key ?? process.env.GONVEX_PROJECT_KEY ?? process.env.GONVEX_DEPLOY_KEY ?? process.env.GONVEX_KEY ?? "",
  };
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
  console.log("Usage: gonvex <dev|init|create> [options]");
  console.log("  gonvex dev [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>] [--once] [-- <command>]");
  console.log("  gonvex init [--template vite-react] [--project <id>] [--runtime <url>]");
  console.log("  gonvex create <app-name> [--template vite-react]");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
