import type { ReducerErrorClass } from "@gonvex/protocol";

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

