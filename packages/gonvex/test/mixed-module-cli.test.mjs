import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("codegen ships a Go bundle and configured TypeScript sidecar together", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-mixed-module-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "mixed-test", module: { entrypoint: "gonvex/index.ts" } }));
    writeFileSync(join(project, "gonvex", "functions.go"), `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) { app.Query("go.read", Read) }
`);
    writeFileSync(join(project, "gonvex", "index.ts"), `const query = (options) => options;\nexport const tsRead = query({ run: async () => ({ source: "typescript" }) });\n`);
    const environment = Object.fromEntries(Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")));
    const generated = spawnSync(process.execPath, [cli, "codegen", "--project", project], { env: environment, encoding: "utf8" });
    assert.equal(generated.status, 0, generated.stderr);
    const manifest = JSON.parse(readFileSync(join(project, "gonvex", "_generated", "manifest.json"), "utf8"));
    assert.ok(manifest.bundle, "mixed manifest should retain the Go bundle");
    assert.ok(manifest.module, "mixed manifest should include the TypeScript artifact");
    assert.equal(manifest.functions["go.read"].kind, "query");
    assert.equal(manifest.functions.tsRead.kind, "query");
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});

test("codegen rejects duplicate Go and TypeScript function paths", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-mixed-duplicate-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "mixed-duplicate", module: { entrypoint: "gonvex/index.ts" } }));
    writeFileSync(join(project, "gonvex", "functions.go"), `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) { app.Query("shared.path", Read) }
`);
    writeFileSync(join(project, "gonvex", "index.ts"), `const query = (options) => options;\nexport const shared = query({ name: "shared.path", run: async () => null });\n`);
    const environment = Object.fromEntries(Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")));
    const generated = spawnSync(process.execPath, [cli, "codegen", "--project", project], { env: environment, encoding: "utf8" });
    assert.notEqual(generated.status, 0);
    assert.match(`${generated.stdout}\n${generated.stderr}`, /duplicate module function path/);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
