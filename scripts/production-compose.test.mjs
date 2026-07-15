import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const composePath = path.join(root, "docker-compose.production.yml");
const secretEnvironment = {
  ...process.env,
  SERVICE_PASSWORD_64_GONVEX_POSTGRES: "compose-contract-postgres",
  SERVICE_PASSWORD_64_GONVEX_MINIO: "compose-contract-minio",
  SERVICE_PASSWORD_64_GONVEX_DASHBOARD: "compose-contract-dashboard",
  SERVICE_REALBASE64_64_GONVEX_DASHBOARD_SESSION: "compose-contract-session",
};

function resolvedCompose() {
  const result = spawnSync(
    "docker",
    ["compose", "-f", composePath, "config", "--format", "json"],
    { cwd: root, env: secretEnvironment, encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("production compose keeps stateful dependencies private and pinned", () => {
  const compose = resolvedCompose();
  const expected = [
    "dashboard",
    "minio",
    "minio-init",
    "postgres",
    "postgres-backup",
    "runtime",
    "valkey",
  ];
  assert.deepEqual(Object.keys(compose.services).sort(), expected);

  for (const service of Object.values(compose.services)) {
    assert.equal(service.ports, undefined, "production services must not publish host ports");
  }
  for (const name of ["postgres", "postgres-backup", "valkey", "minio", "minio-init"]) {
    assert.match(compose.services[name].image, /@sha256:[a-f0-9]{64}$/);
  }

  assert.equal(compose.volumes["gonvex-postgres-data"].name, "gonvex-maker-postgres-data");
  assert.equal(compose.volumes["gonvex-runtime-data"].name, "gonvex-maker-runtime-data");
});

test("production runtime and dashboard require protected durable configuration", () => {
  const { services } = resolvedCompose();
  assert.equal(services.runtime.environment.GONVEX_ENVIRONMENT, "production");
  assert.equal(services.runtime.environment.GONVEX_REQUIRE_AUTH, "true");
  assert.equal(
    services.runtime.environment.GONVEX_PUBLIC_URL,
    "https://gonvex-maker.whagons.com",
  );
  assert.equal(services.runtime.environment.GONVEX_ADMIN_KEY, undefined);
  assert.deepEqual(services.runtime.cap_drop, ["ALL"]);
  assert.equal(services.dashboard.environment.DASHBOARD_AUTH_ENABLED, "true");
  assert.equal(services.dashboard.environment.DASHBOARD_COOKIE_SECURE, "true");
  assert.equal(services.dashboard.read_only, true);
  assert.equal(services.runtime.depends_on.postgres.condition, "service_healthy");
  assert.equal(services.runtime.depends_on.valkey.condition, "service_healthy");
  assert.equal(services.runtime.depends_on["minio-init"].condition, "service_healthy");
});

test("first retained backup waits for initialized runtime state", () => {
  const { services } = resolvedCompose();
  assert.equal(services["postgres-backup"].depends_on.runtime.condition, "service_healthy");
  assert.match(services["postgres-backup"].entrypoint.join("\n"), /pg_dump/);
  assert.match(services["postgres-backup"].entrypoint.join("\n"), /\.dump\.partial/);
  assert.match(services["postgres-backup"].entrypoint.join("\n"), /-mtime \+14 -delete/);
});
