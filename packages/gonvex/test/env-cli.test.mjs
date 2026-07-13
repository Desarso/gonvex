import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { main } from "../dist/index.js";

const settingsKeys = ["GONVEX_PROJECT_ID", "GONVEX_PROJECT", "GONVEX_RUNTIME_URL", "GONVEX_PROJECT_KEY", "GONVEX_DEPLOY_KEY", "GONVEX_KEY"];

function isolateProjectSettings() {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  return () => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  };
}

test("env push uploads one file to the selected project with its project key", async (t) => {
  const restoreSettings = isolateProjectSettings();
  t.after(restoreSettings);

  let received;
  const server = createServer(async (request, response) => {
    let body = "";
    request.setEncoding("utf8");
    for await (const chunk of request) body += chunk;
    received = { body, headers: request.headers, method: request.method, url: request.url };
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: true, count: 2 }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  }));
  const address = server.address();
  assert.notEqual(address, null);
  assert.equal(typeof address, "object");

  const root = await mkdtemp(join(tmpdir(), "gonvex-env-push-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(root, ".env.local"), [
    "GONVEX_PROJECT_ID=project-v6",
    `GONVEX_RUNTIME_URL=http://127.0.0.1:${address.port}`,
    "GONVEX_PROJECT_KEY=project-v6-key",
    "",
  ].join("\n"));
  const content = "API_URL=https://api.example.test\nSECRET_TOKEN=shh\n";
  await writeFile(join(root, ".env.production"), content);

  const messages = [];
  const originalLog = console.log;
  console.log = (message) => messages.push(String(message));
  t.after(() => { console.log = originalLog; });

  await main(["env", "push", ".env.production", "--project", root]);

  assert.equal(received.method, "PUT");
  assert.equal(received.url, "/dev/projects/project-v6/env");
  assert.equal(received.headers.authorization, "Bearer project-v6-key");
  assert.equal(received.headers["x-gonvex-key"], "project-v6-key");
  assert.deepEqual(JSON.parse(received.body), { content });
  assert.ok(messages.some((message) => message.includes("2 variable(s)") && message.includes("project-v6")));
});

test("env push refuses to upload a project credential", async (t) => {
  const restoreSettings = isolateProjectSettings();
  t.after(restoreSettings);
  const root = await mkdtemp(join(tmpdir(), "gonvex-env-credential-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(root, ".env.local"), "GONVEX_PROJECT_ID=project-v6\nGONVEX_PROJECT_KEY=project-v6-key\n");
  await writeFile(join(root, ".env.deploy"), "GONVEX_PROJECT_KEY=do-not-upload\nAPI_URL=https://api.example.test\n");

  await assert.rejects(
    main(["env", "push", ".env.deploy", "--project", root]),
    /keep CLI project credentials out of uploaded function environment variables/,
  );
});
