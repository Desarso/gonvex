import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("dev manifest preserves function dependency options", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-dependencies-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "dependency-test" }));
    writeFileSync(
      join(project, "gonvex", "functions.go"),
      `package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
  app.Query(
    "locations.list",
    ListLocations,
    gonvex.Reads("locations").Columns("tenantId", "updatedAt").Filters("tenantId").OrdersBy("updatedAt").Windowed().Predicate("active"),
    gonvex.Reads("users"),
    gonvex.ShareByPermissions(),
  )
  app.Mutation("locations.upsert", UpsertLocation, gonvex.Writes("locations").Columns("tenantId", "userId"))
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
    assert.deepEqual(manifest.functions["locations.list"].dependencies, {
      reads: [
        {
          table: "locations",
          columns: ["tenantId", "updatedAt"],
          filters: ["tenantId"],
          ordersBy: ["updatedAt"],
          windowed: true,
          predicate: "active",
        },
        { table: "users" },
      ],
      shareByPermissions: true,
    });
    assert.deepEqual(manifest.functions["locations.upsert"].dependencies, {
      writes: [{ table: "locations", columns: ["tenantId", "userId"] }],
    });
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
