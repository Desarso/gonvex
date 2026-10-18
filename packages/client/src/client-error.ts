import type {ReducerErrorClass} from "@gonvex/protocol";
import type {OutboxErrorClass} from "./outbox.js";

export type GonvexClientErrorCode = "server" | "timeout" | "disconnected" | "closed" | "auth" | "superseded";

/**
 * Typed error for every rejected Gonvex operation. `code` distinguishes
 * server-side failures from transport-level ones so apps can decide whether
 * a retry is safe:
 *
 * - `server`: the runtime executed the function and returned an error.
 * - `timeout`: no response arrived within the operation timeout. For
 *   reducers/actions the write may or may not have been applied.
 * - `disconnected`: the socket dropped while the operation was pending.
 *   Reducers/actions fail closed unless a reducer opted into the outbox.
 * - `closed`: the client was explicitly closed.
 * - `auth`: authentication was rejected.
 * - `superseded`: the operation belonged to an authentication scope that the
 *   caller replaced before the operation completed.
 */
export class GonvexClientError extends Error {
  readonly code: GonvexClientErrorCode;
  readonly path?: string;
  readonly operation?: "query" | "reducer" | "action";
  /**
   * For `server` Reducer errors: the runtime's classification, or a legacy
   * inference when an older runtime sent none. Undefined means a legacy
   * runtime's unclassified error, which is handled as a rejection.
   */
  readonly errorClass?: ReducerErrorClass;
  /** True when the same call (same idempotency key) may succeed later. */
  readonly retryable?: boolean;

  constructor(message: string, options: { code: GonvexClientErrorCode; path?: string; operation?: "query" | "reducer" | "action"; errorClass?: ReducerErrorClass; retryable?: boolean }) {
    super(message);
    this.name = "GonvexClientError";
    this.code = options.code;
    this.path = options.path;
    this.operation = options.operation;
    if (options.errorClass) this.errorClass = options.errorClass;
    if (options.retryable !== undefined) this.retryable = options.retryable;
  }
}

export const DEFAULT_OUTBOX_MAX_ATTEMPTS = 10;
export const DEFAULT_OUTBOX_RETRY_MAX_BACKOFF_MS = 60_000;

/**
 * How the outbox treats one failed delivery. Runtimes before
 * 0.5.2-staging.15 send no class: their server errors stay rejections, as
 * before, except the two cases the legacy runtime could only express in prose.
 */
/** @internal Shared by the SQLite owner delivery loop. */
export function deliveryErrorClass(error: unknown): OutboxErrorClass {
  if (!(error instanceof GonvexClientError)) return "transient";
  switch (error.code) {
    case "server": return error.errorClass ?? "rejected";
    case "timeout": return "transient";
    default: return "network";
  }
}

const legacyUnauthenticatedReducerError = "authenticate with an active tenant before calling a Reducer";

export function reducerErrorClassFromMessage(message: { error: string; class?: ReducerErrorClass }): ReducerErrorClass | undefined {
  if (message.class === "rejected" || message.class === "transient" || message.class === "update_required" || message.class === "unauthenticated") {
    return message.class;
  }
  // Legacy runtimes: the only structured signals were these literal messages.
  if (/STALE_REDUCER_ARTIFACT|CLIENT_UPDATE_REQUIRED/.test(message.error)) return "update_required";
  if (message.error === legacyUnauthenticatedReducerError) return "unauthenticated";
  return undefined;
}

