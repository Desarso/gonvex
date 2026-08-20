import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dockerfile = readFileSync(path.join(root, "Dockerfile.runtime"), "utf8");
const packageJSON = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
const localCompose = readFileSync(path.join(root, "docker-compose.yml"), "utf8");
const productionCompose = readFileSync(path.join(root, "docker-compose.production.yml"), "utf8");
const persistentRoot = "/var/lib/gonvex";
const moduleRoot = "/opt/gonvex/go";
const moduleCache = `${moduleRoot}/pkg/mod`;
const buildCache = "/var/cache/gonvex/go-build";
const temporaryBuildRoot = "/var/cache/gonvex/tmp";

test("runtime compiler state stays visible when persistent and tmpfs roots are mounted", () => {
  const stages = dockerfile.split(/(?=^FROM )/m).filter((stage) => stage.startsWith("FROM "));
  const build = stages.find((stage) => / AS build\s*$/m.test(stage));
  const runtime = stages.at(-1);
  assert.ok(build, "Go build stage must exist");
  assert.ok(runtime, "runtime stage must exist");

  assert.match(build, new RegExp(`GOCACHE=${buildCache}`));
  assert.match(build, new RegExp(`GOMODCACHE=${moduleCache}`));
  assert.match(build, new RegExp(`GOPATH=${moduleRoot}`));
  assert.match(runtime, new RegExp(`GOCACHE=${buildCache}`));
  assert.match(runtime, new RegExp(`GOMODCACHE=${moduleCache}`));
  assert.match(runtime, new RegExp(`GOPATH=${moduleRoot}`));
  assert.match(runtime, new RegExp(`TMPDIR=${temporaryBuildRoot}(?:\\s|$)`));
  assert.match(
    runtime,
    new RegExp(`COPY --from=build[^\\n]* ${moduleCache} ${moduleCache}`),
  );
  for (const compilerPath of [moduleRoot, moduleCache, buildCache, temporaryBuildRoot]) {
    assert.equal(
      compilerPath.startsWith(`${persistentRoot}/`),
      false,
      `${compilerPath} must not be hidden by the production volume`,
    );
  }
  assert.equal(
    temporaryBuildRoot === "/tmp" || temporaryBuildRoot.startsWith("/tmp/"),
    false,
    `${temporaryBuildRoot} must not live on the production noexec tmpfs`,
  );
  assert.match(runtime, /GONVEX_DATA_DIR=\/var\/lib\/gonvex\/data/);
  assert.doesNotMatch(dockerfile, /COPY --from=build[^\n]* \/go\/pkg\/mod/);
  assert.match(runtime, /WORKDIR \/var\/lib\/gonvex\nUSER 0:0/);
  assert.doesNotMatch(runtime, /useradd|groupadd|chown/);
});

test("runtime image always contains and supervises the Rust TypeScript module host", () => {
  const stages = dockerfile.split(/(?=^FROM )/m).filter((stage) => stage.startsWith("FROM "));
  const rustBuild = stages.find((stage) => / AS module-host-build\s*$/m.test(stage));
  const runtime = stages.at(-1);
  assert.ok(rustBuild, "Rust module-host build stage must exist");
  assert.ok(runtime, "runtime stage must exist");

  assert.match(rustBuild, /COPY rust\/Cargo\.toml rust\/Cargo\.lock \.\//);
  assert.match(rustBuild, /COPY rust\/crates \.\/crates/);
  assert.match(rustBuild, /cargo build --locked --release -p gonvex-module-host/);
  assert.match(
    runtime,
    /COPY --from=module-host-build \/src\/rust\/target\/release\/gonvex-module-host \/usr\/local\/bin\/gonvex-module-host/,
  );
  assert.match(runtime, /GONVEX_MODULE_HOST_ENABLED=true/);
  assert.match(runtime, /GONVEX_MODULE_HOST_BINARY=\/usr\/local\/bin\/gonvex-module-host/);
});

test("local runtime development builds and selects the debug Rust module host", () => {
  const command = packageJSON.scripts?.["dev:runtime"] ?? "";
  assert.match(command, /cargo build --locked --manifest-path rust\/Cargo\.toml -p gonvex-module-host/);
  assert.match(command, /GONVEX_MODULE_HOST_BINARY=.*rust\/target\/debug\/gonvex-module-host/);
  assert.match(command, /air -c \.air\.toml/);
});

test("compose deployments use the canonical Control Plane configuration", () => {
  for (const [name, compose] of [["local", localCompose], ["production", productionCompose]]) {
    assert.match(compose, /GONVEX_CONTROL_PLANE_DATABASE_URL:/, `${name} compose must configure the Control Plane`);
    assert.doesNotMatch(compose, /GONVEX_LANDLORD_DATABASE_URL:/, `${name} compose must not configure the legacy landlord alias`);
  }
});
