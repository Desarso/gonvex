import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("dev manifest preserves sync chains separated by Go comments", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-sync-chain-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "sync-chain-test" }));
    writeFileSync(
      join(project, "gonvex", "functions.go"),
      `package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
  app.Sync(
    "sync.tasks",
    Tasks,
    gonvex.SyncTable("tasks").
      Key("_id").
      Columns("_id", "tenantId", "workspaceId").
      EqualArg("tenantId").
      EqualArg("workspaceId").
      ExcludeWhenSet("deletedAt").
      // Comments, punctuation (), and fake .Eager() calls are trivia here.
      VisibilityDependsOn("taskAcks", "taskApprovalInstances").
      OrderBy("id", "desc").
      Progressive().
      Budget(100, 4194304),
    gonvex.Reads("tasks").
      // Dependency chain comments must be harmless too.
      Columns("_id", "tenantId").Filters("tenantId").Windowed(),
  )
}
`,
    );

    const environment = Object.fromEntries(
      Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
    );
    spawnSync(
      process.execPath,
      [cli, "dev", "--project", project, "--runtime-url", "http://127.0.0.1:65534", "--once"],
      { env: environment, encoding: "utf8" },
    );

    const manifest = JSON.parse(readFileSync(join(project, "gonvex", "_generated", "manifest.json"), "utf8"));
    assert.deepEqual(manifest.functions["sync.tasks"].sync, {
      table: "tasks",
      key: "_id",
      columns: ["_id", "tenantId", "workspaceId"],
      mode: "progressive",
      equalFilters: { tenantId: "tenantId", workspaceId: "workspaceId" },
      excludeWhenSet: ["deletedAt"],
      visibilityTables: ["taskAcks", "taskApprovalInstances"],
      orderBy: "id",
      orderDirection: "desc",
      maxRows: 100,
      maxBytes: 4194304,
    });
    assert.deepEqual(manifest.functions["sync.tasks"].dependencies, {
      reads: [{
        table: "tasks",
        columns: ["_id", "tenantId"],
        filters: ["tenantId"],
        windowed: true,
      }],
    });
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
