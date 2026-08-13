import assert from "node:assert/strict";
import test from "node:test";

import {
  deployCoolifyServices,
  stampRuntimeCompose,
  verifyRuntimeCompose,
} from "./deploy-coolify.mjs";

const oldSha = "a".repeat(40);
const newSha = "b".repeat(40);
const compose = `services:
  gonvex-runtime:
    build:
      context: 'https://github.com/Whagons-International/gonvex.git#${oldSha}'
      dockerfile: Dockerfile.runtime
    working_dir: /var/lib/gonvex
    user: '0:0'
    cap_drop:
      - ALL
    environment:
      GONVEX_ADDR: '0.0.0.0:8080'
      GONVEX_REQUIRE_AUTH: 'true'
  gonvex-dashboard:
    build:
      context: 'https://github.com/Whagons-International/gonvex.git#${oldSha}'
      dockerfile: Dockerfile.dashboard
`;

const canonicalCompose = compose;

test("pins both Gonvex builds and stamps the runtime version", () => {
  const stamped = stampRuntimeCompose(compose, newSha);

  assert.equal(stamped.match(new RegExp(`#${newSha}`, "g"))?.length, 2);
  assert.match(stamped, new RegExp(`GONVEX_RUNTIME_VERSION: '${newSha}'`));
});

test("updates an existing runtime version without duplicating it", () => {
  const stamped = stampRuntimeCompose(
    compose.replace(
      "      GONVEX_ADDR: '0.0.0.0:8080'",
      `      GONVEX_ADDR: '0.0.0.0:8080'\n      GONVEX_RUNTIME_VERSION: '${oldSha}'`,
    ),
    newSha,
  );

  assert.equal(stamped.match(/GONVEX_RUNTIME_VERSION:/g)?.length, 1);
  assert.match(stamped, new RegExp(`GONVEX_RUNTIME_VERSION: '${newSha}'`));
});

test("rejects an unexpected Compose shape instead of deploying partially", () => {
  assert.throws(
    () => stampRuntimeCompose(compose.replace(/  gonvex-dashboard:[\s\S]*/, ""), newSha),
    /exactly 2 Gonvex Git build contexts/,
  );
});

test("verifies Coolify's quote-normalized Compose semantically", () => {
  const normalized = stampRuntimeCompose(canonicalCompose, newSha).replace(
    `GONVEX_RUNTIME_VERSION: '${newSha}'`,
    `GONVEX_RUNTIME_VERSION: ${newSha}`,
  );

  assert.doesNotThrow(() => verifyRuntimeCompose(normalized, newSha));
  assert.throws(() => verifyRuntimeCompose(normalized, oldSha), /does not contain exactly 2/);
  assert.throws(() => verifyRuntimeCompose(
    stampRuntimeCompose(compose.replace("    user: '0:0'\n", ""), newSha),
    newSha,
  ), /container root/);
});

test("requires runtime settings on the runtime service, not a decoy service", () => {
  const stamped = stampRuntimeCompose(canonicalCompose, newSha);
  for (const { runtimeSetting, dashboardDecoy, error } of [
    {
      runtimeSetting: "    working_dir: /var/lib/gonvex\n",
      dashboardDecoy: "    working_dir: /var/lib/gonvex\n",
      error: /writable Gonvex runtime directory/,
    },
    {
      runtimeSetting: "    user: '0:0'\n",
      dashboardDecoy: "    user: '0:0'\n",
      error: /container root/,
    },
    {
      runtimeSetting: "    cap_drop:\n      - ALL\n",
      dashboardDecoy: "    cap_drop:\n      - ALL\n",
      error: /drop all runtime Linux capabilities/,
    },
    {
      runtimeSetting: "      GONVEX_REQUIRE_AUTH: 'true'\n",
      dashboardDecoy: "    environment:\n      GONVEX_REQUIRE_AUTH: 'true'\n",
      error: /enforce project authentication/,
    },
  ]) {
    const dashboardDecoys = stamped
      .replace(runtimeSetting, "")
      .replace("  gonvex-dashboard:\n", `  gonvex-dashboard:\n${dashboardDecoy}`);
    assert.throws(() => verifyRuntimeCompose(dashboardDecoys, newSha), error);
  }
});

test("deploys the canonical Compose instead of preserving stale Coolify structure", async () => {
  let savedCompose = compose;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    const method = init.method ?? "GET";
    if (method === "PATCH") {
      const body = JSON.parse(init.body);
      savedCompose = Buffer.from(body.docker_compose_raw, "base64").toString("utf8");
      return Response.json({});
    }
    if (String(url).includes("/deploy?")) return Response.json({});
    return Response.json({ docker_compose_raw: savedCompose });
  };

  try {
    await deployCoolifyServices({
      base: "https://coolify.example.test/api/v1",
      token: "test-token",
      serviceUUIDs: ["service-1"],
      sha: newSha,
      composeTemplate: canonicalCompose,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.doesNotMatch(savedCompose, /gonvex-runtime-permissions:/);
  assert.match(savedCompose, /working_dir: \/var\/lib\/gonvex/);
  assert.match(savedCompose, /user: '0:0'/);
  assert.match(savedCompose, /cap_drop:\n\s+- ALL/);
  assert.match(savedCompose, /GONVEX_REQUIRE_AUTH: 'true'/);
});
