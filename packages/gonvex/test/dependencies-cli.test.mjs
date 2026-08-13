import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("codegen manifest preserves function dependency options without runtime sync", () => {
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
    gonvex.ReadsEphemeral(),
    gonvex.ShareByPermissions(),
  )
  app.Mutation("locations.upsert", UpsertLocation, gonvex.Writes("locations").Columns("tenantId", "userId"))
  app.Mutation("presence.beat", Beat, gonvex.WritesEphemeral())
}
`,
    );

    const environment = Object.fromEntries(
      Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
    );
    const generated = spawnSync(
      process.execPath,
      [cli, "codegen", "--project", project],
      { env: environment, encoding: "utf8" },
    );
    assert.equal(generated.status, 0, generated.stderr);
    assert.match(generated.stdout, /without runtime sync/);

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
      readsEphemeral: true,
      shareByPermissions: true,
    });
    assert.deepEqual(manifest.functions["locations.upsert"].dependencies, {
      writes: [{ table: "locations", columns: ["tenantId", "userId"] }],
    });
    assert.deepEqual(manifest.functions["presence.beat"].dependencies, {
      writesEphemeral: true,
    });
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
