import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

function generateBindings(source) {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-optimistic-writes-"));
  mkdirSync(join(project, "gonvex"));
  writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "optimistic-writes-test" }));
  writeFileSync(join(project, "gonvex", "functions.go"), source);

  const environment = Object.fromEntries(
    Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
  );
  spawnSync(
    process.execPath,
    [cli, "dev", "--project", project, "--runtime-url", "http://127.0.0.1:65534", "--once"],
    { env: environment, encoding: "utf8" },
  );
  return project;
}

test("generated API derives optimistic patches only from an explicit reducer contract", async () => {
  const project = generateBindings(`package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
  app.Reducer(
    "tasks.update",
    UpdateTask,
    gonvex.OptimisticReducer(` + "` tasks `" + `).RowIDArg(` + "` taskId `" + `).FieldsArg(` + "` updates `" + `),
  )
  app.Query(
    "tasks.preview",
    PreviewTasks,
    gonvex.OptimisticProjection(` + "` tasks `" + `).Key(` + "` _id `" + `).ResultPath(` + "` page `" + `),
  )
  app.Action("tasks.reindex", ReindexTasks)
  app.ReplicaCollection(
    "tasks.sync",
    SyncTasks,
    gonvex.ReplicaTable("tasks").Key("_id").Columns("_id", "title"),
  )
}
`);

  try {
    const apiPath = join(project, "gonvex", "_generated", "api.ts");
    const generated = readFileSync(apiPath, "utf8");
    assert.match(generated, /export const optimisticReducers:/);

    const bindings = await import(`${pathToFileURL(apiPath).href}?test=${Date.now()}`);
    assert.deepEqual(bindings.api.tasks.update.optimistic, {
      reducer: { entity: "tasks", rowIdPath: ["taskId"], fieldsPath: ["updates"] },
    });
    assert.deepEqual(bindings.api.tasks.preview.optimistic, {
      projection: { entity: "tasks", key: "_id", resultPath: ["page"] },
    });
    assert.deepEqual(bindings.api.tasks.sync.optimistic, {
      projection: { entity: "tasks", key: "_id", resultPath: [] },
    });
    assert.deepEqual(bindings.optimisticReducers, {
      "tasks.update": { entity: "tasks", rowIdPath: ["taskId"], fieldsPath: ["updates"] },
    });
    assert.deepEqual(bindings.optimisticPatchesFor("tasks.update", {
      taskId: "task-3",
      updates: { title: "Nested", done: true },
    }), [{
      entity: "tasks",
      rowId: "task-3",
      op: "patch",
      fields: { title: "Nested", done: true },
    }]);
    assert.deepEqual(bindings.optimisticPatchesFor("tasks.update", { title: "No identifier" }), []);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});

test("generated API preserves an explicitly reviewed non-optimistic Reducer", async () => {
  const project = generateBindings(`package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
  app.Query("tasks.list", ListTasks)
  app.Reducer("tasks.create", CreateTask, gonvex.OnlineOnlyNonOptimistic("test fixture"))
}
`);

  try {
    const apiPath = join(project, "gonvex", "_generated", "api.ts");
    const bindings = await import(`${pathToFileURL(apiPath).href}?test=${Date.now()}`);
    assert.deepEqual(bindings.optimisticReducers, {});
    assert.deepEqual(bindings.optimisticPatchesFor("tasks.create", { id: "task-1", title: "No writes" }), []);
    assert.deepEqual(bindings.optimisticPatchesFor("constructor", { id: "task-1" }), []);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
