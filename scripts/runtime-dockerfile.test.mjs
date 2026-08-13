import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dockerfile = readFileSync(path.join(root, "Dockerfile.runtime"), "utf8");
const moduleCache = "/var/lib/gonvex/go/pkg/mod";

test("runtime binary and project plugins compile from the same module-cache path", () => {
  const firstFrom = dockerfile.indexOf("FROM ");
  const secondFrom = dockerfile.indexOf("\nFROM ", firstFrom + 1);
  const build = dockerfile.slice(firstFrom, secondFrom);
  const runtime = dockerfile.slice(secondFrom + 1);

  assert.match(build, new RegExp(`GOMODCACHE=${moduleCache}`));
  assert.match(runtime, new RegExp(`GOMODCACHE=${moduleCache}`));
  assert.match(
    runtime,
    new RegExp(`COPY --from=build[^\\n]* ${moduleCache} ${moduleCache}`),
  );
  assert.doesNotMatch(dockerfile, /COPY --from=build[^\n]* \/go\/pkg\/mod/);
  assert.match(runtime, /WORKDIR \/var\/lib\/gonvex\nUSER 0:0/);
  assert.doesNotMatch(runtime, /useradd|groupadd|chown/);
});
