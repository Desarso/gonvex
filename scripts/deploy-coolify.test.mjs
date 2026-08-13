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
      context: 'https://github.com/Desarso/gonvex.git#${oldSha}'
      dockerfile: Dockerfile.runtime
    environment:
      GONVEX_ADDR: '0.0.0.0:8080'
  gonvex-dashboard:
    build:
      context: 'https://github.com/Desarso/gonvex.git#${oldSha}'
      dockerfile: Dockerfile.dashboard
`;

const canonicalCompose = compose.replace(
  "  gonvex-runtime:",
  `  gonvex-runtime-permissions:
    command: ["sh", "-ec", "chown -R 10001:10001 /var/lib/gonvex && chmod -R u+rwX /var/lib/gonvex"]
    cap_add:
      - CHOWN
      - FOWNER
  gonvex-runtime:
    depends_on:
      gonvex-runtime-permissions:
        condition: service_completed_successfully`,
);

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
  assert.throws(
    () => verifyRuntimeCompose(stampRuntimeCompose(compose, newSha), newSha),
    /missing runtime volume initializer fragment/,
  );
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

  assert.match(savedCompose, /gonvex-runtime-permissions:/);
  assert.match(savedCompose, /chmod -R u\+rwX \/var\/lib\/gonvex/);
  assert.match(savedCompose, /- FOWNER/);
});
