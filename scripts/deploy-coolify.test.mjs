import assert from "node:assert/strict";
import test from "node:test";

import { stampRuntimeCompose } from "./deploy-coolify.mjs";

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
