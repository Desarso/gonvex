import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const gonvexContextPattern = /https:\/\/github\.com\/Whagons-International\/gonvex\.git#(?:[0-9a-f]{40}|main)/g;
const runtimeVersionPattern = /^(\s{6}GONVEX_RUNTIME_VERSION:)\s*.*$/m;

export function stampRuntimeCompose(compose, sha) {
  if (!/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error("GONVEX_DEPLOY_SHA must be a full lowercase Git commit SHA");
  }

  const contexts = compose.match(gonvexContextPattern) ?? [];
  if (contexts.length !== 2) {
    throw new Error(`expected exactly 2 Gonvex Git build contexts, found ${contexts.length}`);
  }

  let stamped = compose.replace(gonvexContextPattern, `https://github.com/Whagons-International/gonvex.git#${sha}`);
  if (runtimeVersionPattern.test(stamped)) {
    stamped = stamped.replace(runtimeVersionPattern, `$1 '${sha}'`);
  } else {
    const runtimeEnvironment = /(^  gonvex-runtime:\n[\s\S]*?^    environment:\n)/m;
    if (!runtimeEnvironment.test(stamped)) {
      throw new Error("could not find the gonvex-runtime environment block");
    }
    stamped = stamped.replace(runtimeEnvironment, `$1      GONVEX_RUNTIME_VERSION: '${sha}'\n`);
  }
  return stamped;
}

export function verifyRuntimeCompose(compose, sha) {
  const context = `https://github.com/Whagons-International/gonvex.git#${sha}`;
  const contextCount = compose.split(context).length - 1;
  if (contextCount !== 2) {
    throw new Error(`saved Compose does not contain exactly 2 build contexts for ${sha}`);
  }
  const runtimeVersion = new RegExp(
    `^\\s{6}GONVEX_RUNTIME_VERSION:\\s*["']?${sha}["']?\\s*$`,
    "m",
  );
  if (!runtimeVersion.test(compose)) {
    throw new Error(`saved Compose does not identify runtime ${sha}`);
  }
  if (compose.includes("gonvex-runtime-permissions:")) {
    throw new Error("saved Compose must not depend on a runtime permission initializer");
  }
  if (!/^\s{4}working_dir:\s*\/var\/lib\/gonvex\s*$/m.test(compose)) {
    throw new Error("saved Compose must use the writable Gonvex runtime directory");
  }
  if (!/^\s{4}user:\s*["']?0:0["']?\s*$/m.test(compose)) {
    throw new Error("saved Compose must run the Gonvex runtime as container root");
  }
  if (!/^\s{4}cap_drop:\s*\n\s{6}-\s*ALL\s*$/m.test(compose)) {
    throw new Error("saved Compose must drop all runtime Linux capabilities");
  }
  if (!/^\s{6}GONVEX_REQUIRE_AUTH:\s*["']?true["']?\s*$/m.test(compose)) {
    throw new Error("saved shared dev Compose must enforce project authentication");
  }
}

function requiredEnvironment(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function apiBase(value) {
  const base = value.replace(/\/$/, "");
  return base.endsWith("/api/v1") ? base : `${base}/api/v1`;
}

async function coolifyRequest(base, token, path, init = {}) {
  const response = await fetch(`${base}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(init.body ? { "Content-Type": "application/json" } : {}),
      ...init.headers,
    },
  });
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 500);
    throw new Error(`Coolify ${init.method ?? "GET"} ${path} failed (${response.status}): ${detail}`);
  }
  const text = await response.text();
  return text ? JSON.parse(text) : undefined;
}

export async function deployCoolifyServices({ base, token, serviceUUIDs, sha, composeTemplate }) {
  if (typeof composeTemplate !== "string" || composeTemplate.trim() === "") {
    throw new Error("canonical Coolify Compose template is required");
  }
  const compose = stampRuntimeCompose(composeTemplate, sha);
  verifyRuntimeCompose(compose, sha);

  for (const uuid of serviceUUIDs) {
    const service = await coolifyRequest(base, token, `/services/${encodeURIComponent(uuid)}`);
    if (typeof service?.docker_compose_raw !== "string") {
      throw new Error(`Coolify service ${uuid} did not return docker_compose_raw`);
    }
  }

  for (const uuid of serviceUUIDs) {
    await coolifyRequest(base, token, `/services/${encodeURIComponent(uuid)}`, {
      method: "PATCH",
      body: JSON.stringify({ docker_compose_raw: Buffer.from(compose).toString("base64") }),
    });
  }

  for (const uuid of serviceUUIDs) {
    const service = await coolifyRequest(base, token, `/services/${encodeURIComponent(uuid)}`);
    verifyRuntimeCompose(service.docker_compose_raw, sha);
  }

  for (const uuid of serviceUUIDs) {
    await coolifyRequest(
      base,
      token,
      `/deploy?uuid=${encodeURIComponent(uuid)}&force=true`,
    );
    console.log(`Queued Coolify service ${uuid} at ${sha}`);
  }
}

async function main() {
  const base = apiBase(requiredEnvironment("COOLIFY_API_URL"));
  const token = requiredEnvironment("COOLIFY_API_TOKEN");
  const sha = requiredEnvironment("GONVEX_DEPLOY_SHA");
  const serviceUUIDs = requiredEnvironment("COOLIFY_SERVICE_UUIDS")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  if (serviceUUIDs.length === 0) throw new Error("COOLIFY_SERVICE_UUIDS is empty");
  const composeTemplate = await readFile(
    new URL("../docker-compose.coolify.yml", import.meta.url),
    "utf8",
  );
  await deployCoolifyServices({ base, token, serviceUUIDs, sha, composeTemplate });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
