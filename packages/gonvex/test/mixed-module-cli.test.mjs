import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

function runCodegen(project) {
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
  );
  return spawnSync(process.execPath, [cli, "codegen", "--project", project], { env: environment, encoding: "utf8" });
}

test("codegen rejects Go application sources even when a TypeScript module is present", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-go-rejected-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "go-rejected", module: { entrypoint: "gonvex/index.ts" } }));
    writeFileSync(join(project, "gonvex", "functions.go"), `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) {}
`);
    writeFileSync(join(project, "gonvex", "index.ts"), "export const module = {};\n");

    const generated = runCodegen(project);
    assert.notEqual(generated.status, 0);
    assert.match(`${generated.stdout}\n${generated.stderr}`, /Go application modules were removed in Gonvex v2/);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});

test("codegen rejects a Go-only backend", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-go-only-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "go-only" }));
    writeFileSync(join(project, "gonvex", "functions.go"), `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) {}
`);

    const generated = runCodegen(project);
    assert.notEqual(generated.status, 0);
    assert.match(`${generated.stdout}\n${generated.stderr}`, /Go application modules were removed in Gonvex v2/);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
