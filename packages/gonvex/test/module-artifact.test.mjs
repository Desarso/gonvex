import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { buildModuleArtifact, moduleManifestFunctions } from "../dist/module-artifact.js";

async function moduleProject(t, source, supportingFiles = {}) {
  const root = await mkdtemp(join(tmpdir(), "gonvex-module-artifact-"));
  const backendDir = join(root, "gonvex");
  await mkdir(backendDir);
  const entrypoint = join(backendDir, "index.ts");
  await writeFile(entrypoint, source);
  for (const [name, contents] of Object.entries(supportingFiles)) {
    await writeFile(join(backendDir, name), contents);
  }
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, backendDir, entrypoint };
}

test("TypeScript artifacts are self-contained ESM and preserve reducer and Live Query contracts", async (t) => {
  const project = await moduleProject(t, `
import { suffix } from "./shared.ts";

const liveQuery = (definition: unknown) => definition;
const reducer = (definition: unknown) => definition;
type GridArgs = { workspaceId: string };
type GridRow = { id: string };
type RenameArgs = { taskId: string; title: string };
type RenameResult = { ok: boolean };

export const grid = liveQuery<GridArgs, GridRow[]>({
  liveQueryPlan: {
    table: "tasks",
    key: "id",
    columns: ["id", "title", "workspaceId"],
    where: { operator: "eq", column: "workspaceId", value: { argument: "workspaceId" } },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 200 },
  },
  run: async (_ctx: unknown, args: { workspaceId: string }) => [{ id: args.workspaceId + suffix }],
});

export const rename = reducer<RenameArgs, RenameResult>({
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [{ operation: "patch", entity: "tasks", id: ["taskId"], fields: { title: "pending" } }],
  },
  run: async () => ({ ok: true }),
});
`, { "shared.ts": "export const suffix = '-bundled';\n" });

  const artifact = await buildModuleArtifact({
    root: project.root,
    backendDir: project.backendDir,
    files: [project.entrypoint, join(project.backendDir, "shared.ts")],
    migrations: [],
  });

  assert.equal(artifact.javascript?.path, "gonvex/_build/module.js");
  const bundled = Buffer.from(artifact.javascript.code, "base64").toString("utf8");
  assert.match(bundled, /-bundled/);
  assert.doesNotMatch(bundled, /from\s+["']\.\/shared/);
  assert.equal(await readFile(join(project.backendDir, "_build", "module.js"), "utf8"), bundled);

  const functions = moduleManifestFunctions(artifact);
  assert.equal(functions.grid.kind, "query");
  assert.equal(functions.grid.delivery, "live");
  assert.equal(functions.grid.dependencies.liveQueryPlan.table, "tasks");
  assert.deepEqual(functions.rename.offline, { mode: "allowed", conflict: "expectedVersion" });
  assert.deepEqual(functions.rename.optimistic.effects[0], {
    operation: "patch",
    entity: "tasks",
    id: ["taskId"],
    fields: { title: "pending" },
  });
  assert.equal(functions.rename.args.type, "RenameArgs");
  assert.equal(functions.rename.result.type, "RenameResult");
});

test("TypeScript artifacts reject Node built-ins", async (t) => {
  const project = await moduleProject(t, `
import { readFile } from "node:fs/promises";
export const unsafe = async () => readFile("secret");
`);

  await assert.rejects(
    buildModuleArtifact({
      root: project.root,
      backendDir: project.backendDir,
      files: [project.entrypoint],
      migrations: [],
    }),
    /Node runtime module.*node:fs\/promises.*unavailable/,
  );
});
