import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("workspace tests build client artifacts before dependent CLI tests", () => {
  const root = fileURLToPath(new URL("../../..", import.meta.url));
  const packageJson = JSON.parse(readFileSync(join(root, "packages", "gonvex", "package.json"), "utf8"));
  const clientPackageJson = JSON.parse(readFileSync(join(root, "packages", "client", "package.json"), "utf8"));
  assert.equal(packageJson.dependencies["@gonvex/client"], "workspace:*");
  assert.equal(packageJson.scripts.prebuild, undefined);
  assert.match(clientPackageJson.scripts.test, /^pnpm build && /);
});
