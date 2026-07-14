import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { main } from "../dist/index.js";

const settingsKeys = ["GONVEX_PROJECT_ID", "GONVEX_PROJECT", "GONVEX_RUNTIME_URL", "GONVEX_PROJECT_KEY", "GONVEX_DEPLOY_KEY", "GONVEX_KEY", "GONVEX_APP_ORIGIN", "VITE_APP_URL"];

test("auth add google registers the callback and writes the native auth module", async (t) => {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  t.after(() => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  let received;
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    received = { body, headers: request.headers, method: request.method, url: request.url };
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      provider: "google",
      enabled: true,
      redirectUris: ["http://localhost:4173/auth/callback"],
      runtimeConfigured: true,
      brokerCallbackUrl: "https://auth.gonvex.dev/auth/google/callback",
    }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  assert.equal(typeof address, "object");
  const runtimeURL = `http://127.0.0.1:${address.port}`;

  const root = await mkdtemp(join(tmpdir(), "gonvex-auth-cli-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(root, "gonvex.json"), JSON.stringify({ project: "project-auth", runtime: runtimeURL }, null, 2));
  await writeFile(join(root, ".env.local"), [
    "GONVEX_PROJECT_ID=project-auth",
    `GONVEX_RUNTIME_URL=${runtimeURL}`,
    "GONVEX_PROJECT_KEY=gvx_project-auth-key",
    "",
  ].join("\n"));

  await main(["auth", "add", "google", "--project", root, "--origin", "http://localhost:4173"]);

  assert.equal(received.method, "PUT");
  assert.equal(received.url, "/dev/projects/project-auth/auth/google");
  assert.equal(received.headers.authorization, "Bearer gvx_project-auth-key");
  assert.equal(received.headers["x-gonvex-key"], "gvx_project-auth-key");
  assert.deepEqual(JSON.parse(received.body), { redirectUri: "http://localhost:4173/auth/callback" });

  const config = JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
  assert.deepEqual(config.auth.providers.google, {
    enabled: true,
    callbackPath: "/auth/callback",
    redirectUris: ["http://localhost:4173/auth/callback"],
  });
  const module = await readFile(join(root, "gonvex", "auth.tsx"), "utf8");
  assert.match(module, /createGonvexAuth/);
  assert.match(module, /project-auth/);
  assert.match(module, /GoogleSignInButton/);
});
