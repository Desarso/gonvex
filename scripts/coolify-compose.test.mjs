import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const composePath = path.join(root, "docker-compose.coolify.yml");
const environment = {
  ...process.env,
  GONVEX_POSTGRES_PASSWORD: "compose-postgres",
  GONVEX_DASHBOARD_PASSWORD: "compose-dashboard",
  GONVEX_DASHBOARD_SESSION_SECRET: "compose-session",
  GONVEX_GOOGLE_LOGIN_ENABLED: "true",
  GONVEX_DASHBOARD_AUTH_PROJECT_ID: "dashboard-auth-test",
  GONVEX_BOOTSTRAP_EMAIL: "malek.gabriel33@gmail.com",
  SERVICE_FQDN_GONVEXRUNTIME_8080: "https://gonvex-unified-dev.example.test",
  SERVICE_FQDN_GONVEXDASHBOARD_80: "https://gonvex-unified-dashboard-dev.example.test",
  SERVICE_URL_GONVEXRUNTIME_8080: "https://gonvex-unified-dev.example.test",
  GONVEX_EXTERNAL_URL: "https://gonvex-unified-dev.example.test",
};

function resolvedCompose() {
  const result = spawnSync(
    "docker",
    ["compose", "-f", composePath, "config", "--format", "json"],
    { cwd: root, env: environment, encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("Coolify stack combines the current Gonvex resources and volume initializer", () => {
  const compose = resolvedCompose();
  assert.deepEqual(Object.keys(compose.services).sort(), [
    "gonvex-dashboard",
    "gonvex-postgres",
    "gonvex-runtime",
    "gonvex-runtime-permissions",
    "gonvex-valkey",
  ]);

  for (const service of Object.values(compose.services)) {
    assert.equal(service.ports, undefined, "Coolify services must not publish host ports");
  }

  assert.equal(compose.services["gonvex-postgres"].expose, undefined);
  assert.equal(compose.services["gonvex-valkey"].expose, undefined);
});

test("runtime is health-gated on private durable dependencies", () => {
  const { services, volumes } = resolvedCompose();
  const runtime = services["gonvex-runtime"];
  const permissions = services["gonvex-runtime-permissions"];
  const dashboard = services["gonvex-dashboard"];

  assert.equal(
    runtime.environment.DATABASE_URL,
    "postgres://gonvex:compose-postgres@gonvex-postgres:5432/gonvex?sslmode=disable",
  );
  assert.equal(runtime.environment.VALKEY_URL, "redis://gonvex-valkey:6379/0");
  assert.equal(
    runtime.environment.GONVEX_PUBLIC_URL,
    "https://gonvex-unified-dev.example.test",
  );
  assert.equal(runtime.environment.GONVEX_REQUIRE_AUTH, "true");
  assert.equal(runtime.depends_on["gonvex-postgres"].condition, "service_healthy");
  assert.equal(runtime.depends_on["gonvex-valkey"].condition, "service_healthy");
  assert.equal(
    runtime.depends_on["gonvex-runtime-permissions"].condition,
    "service_completed_successfully",
  );
  assert.equal(permissions.user, "0:0");
  assert.deepEqual(permissions.cap_drop, ["ALL"]);
  assert.deepEqual(permissions.cap_add, ["CHOWN"]);
  assert.deepEqual(permissions.command, ["chown", "-R", "10001:10001", "/var/lib/gonvex"]);
  assert.equal(permissions.read_only, true);
  assert.equal(dashboard.depends_on["gonvex-runtime"].condition, "service_healthy");
  assert.equal(dashboard.environment.GONVEX_RUNTIME_URL, "http://gonvex-runtime:8080");
  assert.equal(dashboard.environment.DASHBOARD_AUTH_ENABLED, "true");
  assert.equal(
    dashboard.build.args.VITE_GONVEX_DASHBOARD_AUTH_PROJECT_ID,
    "dashboard-auth-test",
  );
  assert.equal(dashboard.build.args.VITE_GONVEX_GOOGLE_LOGIN_ENABLED, "true");
  assert.equal(
    dashboard.build.args.VITE_GONVEX_ALLOWED_EMAILS,
    "malek.gabriel33@gmail.com",
  );
  assert.equal(dashboard.read_only, true);

  assert.ok(volumes["gonvex-postgres-data"]);
  assert.ok(volumes["gonvex-valkey-data"]);
  assert.ok(volumes["gonvex-runtime-data"]);
});

test("Coolify magic URLs expose only runtime and dashboard", () => {
  const compose = resolvedCompose();
  const runtime = compose.services["gonvex-runtime"];
  const dashboard = compose.services["gonvex-dashboard"];

  assert.equal(
    runtime.environment.GONVEX_PUBLIC_URL,
    "https://gonvex-unified-dev.example.test",
  );
  assert.equal(
    dashboard.environment.SERVICE_FQDN_GONVEXDASHBOARD_80,
    "https://gonvex-unified-dashboard-dev.example.test",
  );
});
