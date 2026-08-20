import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("codegen derives Live Query dependencies without runtime sync", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-dependencies-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "dependency-test" }));
    writeFileSync(
      join(project, "gonvex", "functions.go"),
      `package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
  app.LiveQuery(
    "locations.list",
    ListLocations,
    gonvex.LivePlan(gonvex.LiveTable("locations").Select("tenantId", "updatedAt").Filter(gonvex.Eq("tenantId", gonvex.Arg("tenantId"))).SortArgs("sort", "direction", "updatedAt", "desc", "updatedAt").WindowArgs("offset", "limit", 100, 200)),
    gonvex.ShareByPermissions(),
  )
  app.Reducer("locations.upsert", UpsertLocation, gonvex.OnlineOnlyNonOptimistic("test fixture"))
  app.Reducer("presence.beat", Beat, gonvex.OnlineOnlyNonOptimistic("test fixture"))
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
      reads: [{ table: "locations", columns: ["tenantId", "updatedAt"], filters: ["tenantId"], ordersBy: ["updatedAt"], windowed: true }],
      liveQueryPlan: {
        table: "locations", key: "id", columns: ["tenantId", "updatedAt"],
        where: { operator: "eq", column: "tenantId", value: { argument: "tenantId" } },
        sort: { columnArgument: "sort", directionArgument: "direction", allowedColumns: ["updatedAt"], defaultColumn: "updatedAt", defaultDirection: "desc" },
        window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 200 },
      },
      shareByPermissions: true,
    });
    assert.equal(manifest.functions["locations.upsert"].kind, "reducer");
    assert.deepEqual(manifest.functions["locations.upsert"].dependencies, {
      nonOptimisticReason: "test fixture",
    });
    assert.deepEqual(manifest.functions["presence.beat"].dependencies, {
      nonOptimisticReason: "test fixture",
    });
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
