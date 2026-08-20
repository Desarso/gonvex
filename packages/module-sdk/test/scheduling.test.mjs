import assert from "node:assert/strict";
import test from "node:test";

import { createModule, cron, tenantCron } from "../dist/index.js";

const nonInteractive = {
  interactive: false,
  offline: { mode: "forbidden" },
  run: async () => null,
};

test("cron declarations are validated and collected deterministically", () => {
  const module = createModule({ name: "whagons", version: "1" });
  module.action("reports.daily", { run: async () => null });
  module.reducer("tasks.expire", nonInteractive);
  module.tenantCron({
    name: "expire-tasks",
    expression: "0 1 * * *",
    function: "tasks.expire",
    args: { source: "cron" },
  });
  module.cron({ name: "daily-report", intervalMs: 86_400_000, function: "reports.daily" });

  assert.deepEqual(module.manifest().crons, [
    { name: "daily-report", function: "reports.daily", scope: "project", intervalMs: 86_400_000 },
    {
      name: "expire-tasks",
      function: "tasks.expire",
      args: { source: "cron" },
      scope: "tenant",
      expression: "0 1 * * *",
    },
  ]);
});

test("runtime registration payload preserves recurring declarations", () => {
  const module = createModule({ name: "whagons", version: "1" });
  module.action("reports.daily", { run: async () => null });
  module.action("tasks.expire", { run: async () => null });
  module.cron({ name: "daily-report", intervalMs: 60_000, function: "reports.daily" });
  module.tenantCron({ name: "tenant-expiry", expression: "*/5 * * * *", function: "tasks.expire" });

  const runtime = module.createRuntimeRegistry();
  assert.deepEqual(runtime.manifest().crons, module.manifest().crons);
  assert.deepEqual(runtime.registrationPayload().manifest.crons, module.manifest().crons);
});

test("standalone cron helpers assign scope and reject ambiguous schedules", () => {
  const declaration = cron({ name: "project", intervalMs: 1_000, function: "jobs.run" });
  assert.equal(declaration.scope, "project");
  assert.equal(tenantCron({ name: "tenant", expression: "*/5 * * * *", function: "jobs.run" }).scope, "tenant");

  const module = createModule({ name: "whagons", version: "1", crons: [declaration] });
  module.action("jobs.run", { run: async () => null });
  assert.deepEqual(module.manifest().crons, [declaration]);

  assert.throws(
    () => cron({ name: "ambiguous", intervalMs: 1_000, expression: "* * * * *", function: "jobs.run" }),
    /exactly one/,
  );
  assert.throws(() => cron({ name: "invalid", intervalMs: 0, function: "jobs.run" }), /positive safe integer/);
});

test("manifest collection rejects duplicate names and invalid targets", () => {
  const duplicate = createModule({ name: "whagons", version: "1" });
  duplicate.action("jobs.run", { run: async () => null });
  duplicate.cron({ name: "jobs", intervalMs: 1_000, function: "jobs.run" });
  assert.throws(() => duplicate.cron({ name: "jobs", intervalMs: 2_000, function: "jobs.run" }), /duplicate cron/);

  const queryTarget = createModule({ name: "whagons", version: "1" });
  queryTarget.query("jobs.read", {
    liveQueryPlan: { table: "jobs", key: "id", columns: ["id"] },
    run: async () => null,
  });
  queryTarget.cron({ name: "jobs", intervalMs: 1_000, function: "jobs.read" });
  assert.throws(() => queryTarget.manifest(), /must target a reducer or action/);

  const missingTarget = createModule({ name: "whagons", version: "1" });
  missingTarget.cron({ name: "jobs", intervalMs: 1_000, function: "jobs.missing" });
  assert.throws(() => missingTarget.manifest(), /targets unknown function/);
});

test("one-shot Queries require the structured visibility source plan", () => {
  const module = createModule({ name: "whagons", version: "1" });
  assert.throws(
    () => module.query("jobs.read", { run: async () => null }),
    /one-shot query jobs\.read requires a structured live query plan/,
  );
  assert.throws(
    () => module.query("jobs.read", { delivery: "oneShot", liveQueryPlan: { table: "jobs", key: "id", columns: ["name"] }, run: async () => null }),
    /columns must include its key/,
  );
});

test("query plans do not implicitly select live delivery", () => {
  const module = createModule({ name: "whagons", version: "1" });
  module.query("jobs.read", {
    liveQueryPlan: { table: "jobs", key: "id", columns: ["id"] },
    run: async () => null,
  });
  assert.equal(module.manifest().functions["jobs.read"].delivery, "oneShot");
  module.liveQuery("jobs.live", {
    liveQueryPlan: { table: "jobs", key: "id", columns: ["id"] },
    run: async () => null,
  });
  assert.equal(module.manifest().functions["jobs.live"].delivery, "live");
});
