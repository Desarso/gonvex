import type {JsonValue,ReplicaDirective} from "@gonvex/protocol";
import type {LocalExecution} from "@gonvex/local-runtime";
import type {GonvexClientAuth} from "./index.js";

/** Shared correlated-auth wire parsing and persistence namespace. Neither the
 * browser legacy adapter nor the dedicated owner may invent a second identity
 * namespace when restoring a session or replaying an intent. */
export function localIdentityFromAuthResult(result: JsonValue): LocalExecution["identity"] | undefined {
  if (!result || typeof result !== "object" || Array.isArray(result)) return;
  const identity = result.localIdentity as LocalExecution["identity"] | undefined;
  if (!identity?.auth?.account?.id || !identity.tenant?.id || !identity.member?.id) return;
  return structuredClone(identity);
}

export function authIdentityKey(auth: GonvexClientAuth) {
  if (auth.token) {
    const tokenIdentity = authIdentityKeyFromToken(auth);
    if (tokenIdentity) {
      // Conflicting account credentials cannot reuse either account's cache.
      if (auth.identity?.sub && !tokenIdentity.endsWith(`\u0000${auth.identity.sub}`)) return "";
      return tokenIdentity;
    }
  }
  // Token-free fallback: an explicit identity hint carries the same claims a
  // token would supply, so both paths derive the same key for the same Account.
  const hint = auth.identity;
  if (hint && typeof hint.sub === "string" && hint.sub.trim()) {
    return [auth.project ?? "", auth.tenant ?? "", hint.iss ?? "", hint.sub].join("\u0000");
  }
  return "";
}

function authIdentityKeyFromToken(auth: GonvexClientAuth) {
  if (!auth.token) return "";
  const parts = auth.token.split(".");
  if (parts.length < 2) return "";
  try {
    const encoded = parts[1]!.replace(/-/g, "+").replace(/_/g, "/");
    const padded = encoded.padEnd(Math.ceil(encoded.length / 4) * 4, "=");
    const payload = JSON.parse(globalThis.atob(padded)) as { sub?: unknown; iss?: unknown };
    if (typeof payload.sub !== "string" || !payload.sub.trim()) return "";
    return [
      auth.project ?? "",
      auth.tenant ?? "",
      typeof payload.iss === "string" ? payload.iss : "",
      payload.sub,
    ].join("\u0000");
  } catch {
    return "";
  }
}

export function sameAuthTokenIdentity(left: GonvexClientAuth, right: GonvexClientAuth) {
  const leftIdentity = authIdentityKey(left);
  const rightIdentity = authIdentityKey(right);
  return leftIdentity !== "" && leftIdentity === rightIdentity;
}

export function reducerOutboxScope(url: string, auth: GonvexClientAuth, ephemeralScope: string) {
  const identity = authIdentityKey(auth);
  if (identity) return ["identity", url, identity].join("\u0000");
  if (auth.token || auth.identity || auth.fetchToken) {
    // Opaque tokens (or credentials installed before tenant selection) do not
    // expose a stable Account key. A per-client scope preserves current-session
    // queue semantics without ever restoring those rows under another Account.
    return ["ephemeral-auth", url, ephemeralScope].join("\u0000");
  }
  // Anonymous/dev-auth clients still need a stable namespace, but it must be
  // isolated by deployment and tenant. Once an authenticated identity is
  // installed, applyAuth switches away from this scope before restoring or
  // sending its durable reducers.
  return ["anonymous", url, auth.project ?? "", auth.tenant ?? ""].join("\u0000");
}

export function replicaDirectiveFromAuthResult(result: JsonValue): ReplicaDirective | undefined {
  if (!isJsonRecord(result) || !isJsonRecord(result.replica)) return undefined;
  const directive = result.replica;
  if (
    directive.protocolVersion !== 1
    || typeof directive.scope !== "string"
    || typeof directive.visibilityScope !== "string"
    || typeof directive.epoch !== "string"
  ) return undefined;
  return {
    protocolVersion: 1,
    scope: directive.scope,
    visibilityScope: directive.visibilityScope,
    epoch: directive.epoch,
  };
}

export function developerSessionTokenFromAuthResult(result: JsonValue): string | undefined {
  if (!isJsonRecord(result)) return undefined;
  const token = result.developerSessionToken;
  return typeof token === "string" && token.startsWith("gvx_dev_") ? token : undefined;
}

export function artifactHashFromAuthResult(result: JsonValue): string | undefined {
  if (!isJsonRecord(result)) return undefined;
  const hash = result.artifactHash;
  return typeof hash === "string" && hash.length > 0 ? hash : undefined;
}

function isJsonRecord(value:unknown):value is Record<string,JsonValue>{return value!==null&&typeof value==="object"&&!Array.isArray(value);}
