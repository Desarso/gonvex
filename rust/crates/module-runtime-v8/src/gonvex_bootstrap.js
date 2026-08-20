// Gonvex module bootstrap.
//
// Evaluated as a classic script in every isolate; the script's completion value
// is the dispatcher the Rust host calls, so the dispatcher never has to be
// parked on globalThis. Nothing about an invocation is stored globally either:
// identity, capabilities and budgets arrive as call arguments and die with the
// call, which is what makes recycling an isolate across tenants safe.
//
// Reaching `Deno.core.ops.op_gonvex_host_call` directly is not a privilege
// escalation: the Rust op re-checks the capability and the host-call budget of
// the active invocation before it forwards anything, so the context objects
// built here are ergonomics, not the security boundary.
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

  const optional = (value) => (value === undefined ? null : value);

  // Capability separation is structural: the Rust side intersects what the
  // function kind may ever reach with what the host granted this invocation,
  // and a method that is not granted is simply absent from the context. A
  // query has no way to name a write, an action has no database handle.
  const createContext = (request) => {
    const granted = request.capabilities;
    const context = { kind: request.kind, function: request.function, identity: Object.freeze(request.identity) };

    const db = {};
    if (granted.dbRead) {
      db.query = (statement, parameters) => hostCall({ kind: "dbQuery", statement: text("statement", statement), parameters: optional(parameters) });
    }
    if (granted.dbWrite) {
      db.write = (statement, parameters) => hostCall({ kind: "dbWrite", statement: text("statement", statement), parameters: optional(parameters) });
    }
    if (granted.dbRead || granted.dbWrite) context.db = Object.freeze(db);

    if (granted.runReducer) {
      context.runReducer = (name, args) => hostCall({ kind: "runReducer", function: text("reducer", name), args: optional(args) });
    }
    if (granted.network) {
      context.fetch = (init) => hostCall({ kind: "fetch", request: optional(init) });
    }
    if (granted.storage) {
      context.storage = Object.freeze({
        call: (operation, payload) => hostCall({ kind: "storage", operation: text("operation", operation), payload: optional(payload) }),
      });
    }
    return Object.freeze(context);
  };

  // `export const list = query({ handler })` and `export async function list()`
  // are both legal module shapes, so unwrap one level before giving up.
  const resolveHandler = (binding) => {
    if (typeof binding === "function") return binding;
    if (binding !== null && typeof binding === "object") {
      if (typeof binding.handler === "function") return binding.handler;
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
