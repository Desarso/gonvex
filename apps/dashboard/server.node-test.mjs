import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  envFlag,
  normalizeEmail,
  signSession,
  verifyCredentials,
  verifySessionCookie,
} from "./server.mjs";

test("normalizes dashboard login emails", () => {
  assert.equal(normalizeEmail(" Malek.Gabriel33@GMAIL.COM "), "malek.gabriel33@gmail.com");
});

test("parses dashboard auth flags", () => {
  assert.equal(envFlag(undefined, true), true);
  assert.equal(envFlag("false", true), false);
  assert.equal(envFlag("true", false), true);
  assert.equal(envFlag("wat", false), false);
});

test("verifies credentials without accepting casing or password drift", () => {
  assert.equal(verifyCredentials("MALEK.GABRIEL33@gmail.com", "secret", "malek.gabriel33@gmail.com", "secret"), true);
  assert.equal(verifyCredentials("other@gmail.com", "secret", "malek.gabriel33@gmail.com", "secret"), false);
  assert.equal(verifyCredentials("malek.gabriel33@gmail.com", "nope", "malek.gabriel33@gmail.com", "secret"), false);
});

test("signs and rejects dashboard sessions", () => {
  const session = {
    email: "malek.gabriel33@gmail.com",
    expiresAt: Date.now() + 60_000,
    name: "Malek Gabriel33",
    provider: "gonvex",
  };
  const cookie = signSession(session, "session-secret");

  assert.equal(verifySessionCookie(cookie, "session-secret").email, session.email);
  assert.equal(verifySessionCookie(cookie, "wrong-secret"), null);
  assert.equal(verifySessionCookie(cookie.replace(/\.[^.]+$/, ".tampered"), "session-secret"), null);
});

test("proxies runtime project discovery instead of serving the SPA shell", async (t) => {
  const staticRoot = await mkdtemp(join(tmpdir(), "gonvex-dashboard-"));
  await writeFile(join(staticRoot, "index.html"), "<!doctype html><title>Gonvex</title>");
  t.after(() => rm(staticRoot, { recursive: true, force: true }));

  const runtime = createServer((request, response) => {
    assert.equal(request.url, "/dev/projects?source=dashboard");
    assert.equal(request.headers.authorization, "Bearer dashboard-session");
    response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    response.end(JSON.stringify({ projects: [{ id: "maker", name: "Maker" }] }));
  });
  await new Promise((resolve) => runtime.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve, reject) => runtime.close((error) => error ? reject(error) : resolve())));
  const runtimeAddress = runtime.address();
  assert.ok(runtimeAddress && typeof runtimeAddress === "object");

  const previousEnvironment = {
    DASHBOARD_AUTH_ENABLED: process.env.DASHBOARD_AUTH_ENABLED,
    DASHBOARD_STATIC_ROOT: process.env.DASHBOARD_STATIC_ROOT,
    GONVEX_RUNTIME_URL: process.env.GONVEX_RUNTIME_URL,
  };
  process.env.DASHBOARD_AUTH_ENABLED = "false";
  process.env.DASHBOARD_STATIC_ROOT = staticRoot;
  process.env.GONVEX_RUNTIME_URL = `http://127.0.0.1:${runtimeAddress.port}`;
  t.after(() => {
    for (const [name, value] of Object.entries(previousEnvironment)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  const { createDashboardServer } = await import(`./server.mjs?proxy-test=${Date.now()}`);
  const dashboard = createDashboardServer();
  await new Promise((resolve) => dashboard.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve, reject) => dashboard.close((error) => error ? reject(error) : resolve())));
  const dashboardAddress = dashboard.address();
  assert.ok(dashboardAddress && typeof dashboardAddress === "object");

  const response = await fetch(`http://127.0.0.1:${dashboardAddress.port}/dev/projects?source=dashboard`, {
    headers: { authorization: "Bearer dashboard-session" },
  });

  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^application\/json\b/);
  assert.deepEqual(await response.json(), { projects: [{ id: "maker", name: "Maker" }] });
});
