// Gonvex module bootstrap.
//
// Evaluated as a classic script in every isolate; the script's completion value
// is the dispatcher the Rust host calls, so the dispatcher never has to be
// parked on globalThis. Nothing about an invocation is stored globally either:
// identity, capabilities and budgets arrive as call arguments and die with the
// call, which is what makes recycling an isolate across tenants safe.
//
// The context objects built here are the `@gonvex/module-sdk` surface:
// `QueryContext` gets `db.query`, `ReducerContext` adds `db.insert/update/
// delete` and `runReducer`, `ActionContext` gets `fetch`, `runReducer` and
// `storage` but no database handle at all. Writes travel as a table name, a key
// and a JSON object — never as SQL text a module interpolated values into. The
// Go host quotes the identifiers and binds the values as parameters.
//
// Reaching `Deno.core.ops.op_gonvex_host_call` directly is not a privilege
// escalation: the Rust op re-checks the capability and the host-call budget of
// the active invocation before it forwards anything, so these context objects
// are ergonomics, not the security boundary.
((core) => {
  "use strict";

  const ops = core.ops;

  const format = (value) => {
    if (typeof value === "string") return value;
    if (value instanceof Error) return value.stack || `${value.name}: ${value.message}`;
    try {
      const encoded = JSON.stringify(value);
      return encoded === undefined ? String(value) : encoded;
    } catch {
      return String(value);
    }
  };

  // deno_core ships no console; without one, a stray console.log in a module
  // fails as a ReferenceError far from its cause.
  if (typeof globalThis.console === "undefined") {
    const write = (isError) => (...args) => core.print(`[gonvex:module] ${args.map(format).join(" ")}\n`, isError);
    globalThis.console = Object.freeze({
      log: write(false),
      info: write(false),
      debug: write(false),
      warn: write(true),
      error: write(true),
      trace: write(true),
    });
  }

  class GonvexHostError extends Error {
    constructor(message, status) {
      super(message);
      this.name = status === "denied" ? "GonvexCapabilityError" : "GonvexHostError";
      this.status = status;
    }
  }

  const hostCall = async (payload) => {
    // The op answers with a JSON envelope so a denial, a host failure and a
    // successful response are one shape the host fully controls.
    const outcome = JSON.parse(await ops.op_gonvex_host_call(JSON.stringify(payload)));
    if (outcome.status !== "ok") throw new GonvexHostError(outcome.message, outcome.status);
    if (outcome.value === "") return undefined;
    try {
      return JSON.parse(outcome.value);
    } catch {
      throw new GonvexHostError(`host operation ${payload.kind} returned a response that is not JSON`, "failed");
    }
  };

  const text = (name, value) => {
    if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} must be a non-empty string`);
    return value;
  };

  const plainObject = (name, value) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError(`${name} must be an object`);
    }
    return value;
  };

  const rowKey = (value) => {
    if (typeof value === "string" || typeof value === "number") return value;
    throw new TypeError("row id must be a string or a number");
  };

  const optional = (value) => (value === undefined ? null : value);

  const parameterList = (parameters) => {
    if (parameters === undefined || parameters === null) return [];
    if (!Array.isArray(parameters)) throw new TypeError("query parameters must be an array");
    // Values stay values: they are bound as $1..$n by the host, never spliced
    // into the statement here.
    return [...parameters];
  };

  // A deliberately small Response: enough for the module SDK's fetch contract
  // without pretending the isolate has streams, redirects, or a cookie jar.
  const createResponse = (raw) => {
    const headers = raw && typeof raw.headers === "object" && raw.headers !== null ? raw.headers : {};
    const body = typeof raw?.body === "string" ? raw.body : "";
    return Object.freeze({
      status: raw?.status ?? 0,
      statusText: raw?.statusText ?? "",
      ok: (raw?.status ?? 0) >= 200 && (raw?.status ?? 0) < 300,
      url: raw?.url ?? "",
      headers: Object.freeze({
        get: (name) => headers[String(name).toLowerCase()] ?? null,
        has: (name) => Object.prototype.hasOwnProperty.call(headers, String(name).toLowerCase()),
        entries: () => Object.entries(headers),
      }),
      text: async () => body,
      json: async () => JSON.parse(body),
    });
  };

  const requestInit = (input, init) => {
    const url = typeof input === "string" ? input : String(input?.href ?? input ?? "");
    const options = init ?? {};
    const headers = {};
    const source = options.headers;
    if (source && typeof source.entries === "function") {
      for (const [name, value] of source.entries()) headers[String(name).toLowerCase()] = String(value);
    } else if (source && typeof source === "object") {
      for (const [name, value] of Object.entries(source)) headers[String(name).toLowerCase()] = String(value);
    }
    let body = null;
    if (options.body !== undefined && options.body !== null) {
      if (typeof options.body === "string") body = options.body;
      else if (typeof options.body === "object") {
        body = JSON.stringify(options.body);
        if (headers["content-type"] === undefined) headers["content-type"] = "application/json";
      } else body = String(options.body);
    }
    return { url: text("fetch url", url), method: String(options.method ?? "GET").toUpperCase(), headers, body };
  };

  // Capability separation is structural: the Rust side intersects what the
  // function kind may ever reach with what the host granted this invocation,
  // and a method that is not granted is simply absent from the context. A
  // Query has no way to name a write, an Action has no database handle.
  const createContext = (request) => {
    const granted = request.capabilities;
    const identity = request.identity ?? {};
    const account = identity.account ?? null;
    const context = {
      kind: request.kind,
      function: request.function,
      now: request.now,
      account,
      auth: Object.freeze({ account }),
      tenant: identity.tenant ?? null,
      member: identity.member ?? null,
      permissions: identity.permissions ?? null,
    };

    const db = {};
    if (granted.dbRead) {
      db.query = (statement, parameters) => hostCall({
        kind: "dbQuery",
        statement: text("statement", statement),
        parameters: parameterList(parameters),
      });
    }
    if (granted.dbWrite) {
      db.insert = (table, row) => hostCall({
        kind: "dbInsert",
        table: text("table", table),
        row: plainObject("row", row),
      });
      db.update = (table, id, patch) => hostCall({
        kind: "dbUpdate",
        table: text("table", table),
        id: rowKey(id),
        patch: plainObject("patch", patch),
      });
      db.delete = (table, id) => hostCall({
        kind: "dbDelete",
        table: text("table", table),
        id: rowKey(id),
      });
    }
    if (granted.dbRead || granted.dbWrite) context.db = Object.freeze(db);

    if (granted.runReducer) {
      context.runReducer = (name, args) => hostCall({ kind: "runReducer", function: text("reducer", name), args: optional(args) });
    }
    if (granted.network) {
      context.fetch = async (input, init) => createResponse(await hostCall({ kind: "fetch", request: requestInit(input, init) }));
    }
    if (granted.storage) {
      const storage = (operation, payload) => hostCall({ kind: "storage", operation, payload: optional(payload) });
      context.storage = Object.freeze({
        generateUploadUrl: (options) => storage("generateUploadUrl", options ?? {}),
        getUrl: (fileId) => storage("getUrl", { fileId: text("fileId", fileId) }),
        generateDownloadUrl: (fileId, ttlMs) => storage("generateDownloadUrl", { fileId: text("fileId", fileId), ttlMs: ttlMs ?? 0 }),
        getMetadata: (fileId) => storage("getMetadata", { fileId: text("fileId", fileId) }),
        delete: (fileId) => storage("delete", { fileId: text("fileId", fileId) }),
        // Bytes travel base64-encoded: the op boundary is JSON text in both
        // directions, so binary payloads have to be named as such.
        store: (contentBase64, options) => storage("store", { contentBase64: text("content", contentBase64), ...(options ?? {}) }),
        call: (operation, payload) => storage(text("operation", operation), payload),
      });
    }
    return Object.freeze(context);
  };

  // `export const list = query({ handler })` and `export async function list()`
  // are both legal module shapes, so unwrap one level before giving up. The
  // module SDK's declaration helpers park the executable handler on `run`.
  const resolveHandler = (binding) => {
    if (typeof binding === "function") return binding;
    if (binding !== null && typeof binding === "object") {
      if (typeof binding.handler === "function") return binding.handler;
      if (typeof binding.run === "function") return binding.run;
      if (binding.options !== null && typeof binding.options === "object" && typeof binding.options.run === "function") {
        return binding.options.run;
      }
      if (typeof binding.default === "function") return binding.default;
    }
    return null;
  };

  const failure = (kind, message, stack) =>
    JSON.stringify(stack ? { status: "error", kind, message, stack } : { status: "error", kind, message });

  return async (binding, requestJson, argsJson) => {
    const request = JSON.parse(requestJson);

    const handler = resolveHandler(binding);
    if (handler === null) {
      return failure("dispatch", `module export for ${request.function} is not a callable ${request.kind} handler`);
    }

    let args;
    try {
      args = argsJson === "" ? undefined : JSON.parse(argsJson);
    } catch {
      return failure("dispatch", `arguments for ${request.function} are not valid JSON`);
    }

    let value;
    try {
      value = await handler(createContext(request), args);
    } catch (cause) {
      const message = cause instanceof Error ? `${cause.name}: ${cause.message}` : format(cause);
      return failure("handler", message, cause instanceof Error ? cause.stack : undefined);
    }

    let encoded;
    try {
      encoded = value === undefined ? "null" : JSON.stringify(value);
      if (encoded === undefined) encoded = "null";
    } catch (cause) {
      return failure("result", `${request.function} returned a value that cannot be encoded as JSON: ${format(cause)}`);
    }

    // A cheap pre-check in UTF-16 units; the host re-checks the exact byte
    // length, so this only keeps a runaway result from being copied twice.
    if (encoded.length > request.maxResultBytes) {
      return failure("resultSize", `${request.function} returned more than the ${request.maxResultBytes} byte result limit`);
    }
    return JSON.stringify({ status: "ok", value: encoded });
  };
})(Deno.core)
