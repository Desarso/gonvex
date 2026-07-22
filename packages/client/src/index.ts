import type { BrowserTelemetryInfo, ClientMessage, JsonValue, MessageTrace, QueryCacheDirective, ServerMessage } from "@gonvex/protocol";
import { createQueryCacheStore, type QueryCacheOptions, type QueryCacheStatus, type QueryCacheStore } from "./query-cache.js";
import { GonvexErrorReporter, type ErrorReporterOptions } from "./error-reporter.js";
export * from "./cache.js";
export * from "./cache-coordinator.js";
export * from "./browser-cache.js";
export * from "./browser-cache-client.js";
export * from "./browser-cache-shared-worker.js";
export * from "./browser-capabilities.js";
export * from "./persistent-cache.js";
export * from "./query-cache.js";
export * from "./error-reporter.js";
export type { QueryCacheDirective } from "@gonvex/protocol";

type SubscriptionHandler = (message: ServerMessage) => void;
type WatchUpdateHandler = () => void;
type TelemetryHandler = (event: GonvexTelemetryEvent) => void;
type ConnectionStateHandler = (state: ConnectionState) => void;
type QuerySubscription = {
  id: string;
  key: string;
  path: string;
  args: JsonValue;
  listeners: Set<SubscriptionHandler>;
  unsubscribeTimer?: ReturnType<typeof setTimeout>;
  lastMessage?: ServerMessage;
  serverSettled: boolean;
  cacheReadGeneration?: number;
  socketGeneration?: number;
};
type OneShotQuery = {
  id: string;
  path: string;
  args: JsonValue;
  reject: (error: Error) => void;
  socketGeneration?: number;
  timeoutTimer?: ReturnType<typeof setTimeout>;
};
type PendingCall = {
  id: string;
  kind: "mutation" | "action";
  path: string;
  reject: (error: Error) => void;
  timeoutTimer?: ReturnType<typeof setTimeout>;
};

export type FunctionReference = {
  kind: string;
  path: string;
};

export type GonvexClientErrorCode = "server" | "timeout" | "disconnected" | "closed" | "auth";

/**
 * Typed error for every rejected Gonvex operation. `code` distinguishes
 * server-side failures from transport-level ones so apps can decide whether
 * a retry is safe:
 *
 * - `server`: the runtime executed the function and returned an error.
 * - `timeout`: no response arrived within the operation timeout. For
 *   mutations/actions the write may or may not have been applied.
 * - `disconnected`: the socket dropped while the operation was pending.
 *   Mutations/actions fail closed and are never replayed automatically.
 * - `closed`: the client was explicitly closed.
 * - `auth`: authentication was rejected.
 */
export class GonvexClientError extends Error {
  readonly code: GonvexClientErrorCode;
  readonly path?: string;
  readonly operation?: "query" | "mutation" | "action";

  constructor(message: string, options: { code: GonvexClientErrorCode; path?: string; operation?: "query" | "mutation" | "action" }) {
    super(message);
    this.name = "GonvexClientError";
    this.code = options.code;
    this.path = options.path;
    this.operation = options.operation;
  }
}

export type ConnectionState = {
  isWebSocketConnected: boolean;
  hasEverConnected: boolean;
  connectionCount: number;
  connectionRetries: number;
  hasInflightRequests: boolean;
  inflightMutations: number;
  inflightActions: number;
  inflightOneShotQueries: number;
};

export type GonvexTimeoutOptions = {
  /** One-shot `client.query()` timeout. Default 20s. `0` disables. */
  queryTimeoutMs?: number;
  /** `client.mutation()` timeout. Default 20s. `0` disables. */
  mutationTimeoutMs?: number;
  /** `client.action()` timeout. Default 60s. `0` disables. */
  actionTimeoutMs?: number;
};

export const DEFAULT_QUERY_TIMEOUT_MS = 20_000;
export const DEFAULT_MUTATION_TIMEOUT_MS = 20_000;
export const DEFAULT_ACTION_TIMEOUT_MS = 60_000;

export type CallOptions = {
  /** Per-call override of the operation timeout. `0` disables. */
  timeoutMs?: number;
};

export type GonvexClientAuth = {
  project?: string;
  token?: string;
  tenant?: string;
  telemetry?: boolean;
};

export type GonvexClientOptions = GonvexClientAuth & {
  queryCache?: false | QueryCacheOptions;
  errorReporting?: false | Omit<ErrorReporterOptions, "endpoint" | "project" | "tenant">;
  timeouts?: GonvexTimeoutOptions;
};

export type GonvexTelemetryEvent = {
  type: "mutation" | "action" | "query";
  id: string;
  path: string;
  reason?: "initial" | "invalidate";
  outcome: "ok" | "error";
  error?: string;
  clientSentAtMs?: number;
  clientReceivedAtMs: number;
  clientDurationMs?: number;
  serverTrace?: MessageTrace;
  device?: BrowserTelemetryInfo;
};

export class GonvexClient {
  private socket: WebSocket | undefined;
  private readonly handlers = new Map<string, SubscriptionHandler>();
  private readonly querySubscriptions = new Map<string, QuerySubscription>();
  private readonly oneShotQueries = new Map<string, OneShotQuery>();
  private readonly telemetryHandlers = new Set<TelemetryHandler>();
  private readonly pendingMessages: ClientMessage[] = [];
  private auth: GonvexClientAuth = {};
  private authInFlight = false;
  private telemetryEnabled = false;
  private readonly queryCache: QueryCacheStore | undefined;
  private queryCacheDirective: QueryCacheDirective | undefined;
  private queryCacheGeneration = 0;
  private readonly sessionScopeHandlers = new Set<() => void>();
  private readonly errorReporter: GonvexErrorReporter | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempt = 0;
  private socketGeneration = 0;
  private manuallyClosed = false;
  private readonly pendingCalls = new Map<string, PendingCall>();
  private readonly connectionStateHandlers = new Set<ConnectionStateHandler>();
  private isWebSocketConnected = false;
  private hasEverConnected = false;
  private connectionCount = 0;
  private readonly timeouts: Required<GonvexTimeoutOptions>;

  constructor(private readonly url: string, options: GonvexClientOptions = {}) {
    this.auth = authFromOptions(options);
    this.telemetryEnabled = options.telemetry === true;
    this.queryCache = createQueryCacheStore(options.queryCache);
    this.timeouts = {
      queryTimeoutMs: options.timeouts?.queryTimeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS,
      mutationTimeoutMs: options.timeouts?.mutationTimeoutMs ?? DEFAULT_MUTATION_TIMEOUT_MS,
      actionTimeoutMs: options.timeouts?.actionTimeoutMs ?? DEFAULT_ACTION_TIMEOUT_MS,
    };
    if (options.errorReporting && options.project) {
      this.errorReporter = new GonvexErrorReporter({ endpoint: url, project: options.project, tenant: options.tenant, ...options.errorReporting });
    }
  }

  connectionState(): ConnectionState {
    const inflightMutations = countPendingCalls(this.pendingCalls, "mutation");
    const inflightActions = countPendingCalls(this.pendingCalls, "action");
    const inflightOneShotQueries = this.oneShotQueries.size;
    return {
      isWebSocketConnected: this.isWebSocketConnected,
      hasEverConnected: this.hasEverConnected,
      connectionCount: this.connectionCount,
      connectionRetries: this.reconnectAttempt,
      hasInflightRequests: inflightMutations + inflightActions + inflightOneShotQueries > 0,
      inflightMutations,
      inflightActions,
      inflightOneShotQueries,
    };
  }

  subscribeToConnectionState(handler: ConnectionStateHandler): () => void {
    this.connectionStateHandlers.add(handler);
    return () => {
      this.connectionStateHandlers.delete(handler);
    };
  }

  private notifyConnectionState() {
    if (this.connectionStateHandlers.size === 0) return;
    const state = this.connectionState();
    for (const handler of Array.from(this.connectionStateHandlers)) {
      handler(state);
    }
  }

  setAuth(auth: GonvexClientAuth) {
    const scopeMayChange = (hasOwn(auth, "token") && auth.token !== this.auth.token)
      || (hasOwn(auth, "tenant") && auth.tenant !== this.auth.tenant)
      || (hasOwn(auth, "project") && auth.project !== this.auth.project);
    if (scopeMayChange) {
      this.resetQueryCacheScope();
    }
    this.auth = { ...this.auth, ...auth };
    if (auth.tenant !== undefined) this.errorReporter?.setTenant(auth.tenant);
    if (auth.project !== undefined) this.errorReporter?.setProject(auth.project);
    if (auth.telemetry !== undefined) {
      this.telemetryEnabled = auth.telemetry === true;
    }
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.sendAuth(true);
    }
  }

  connect() {
    if (this.socket && this.socket.readyState <= WebSocket.OPEN) return;

    const isReconnect = this.socket !== undefined;
    this.manuallyClosed = false;
    const generation = ++this.socketGeneration;
    const socket = new WebSocket(this.url);
    this.socket = socket;
    socket.addEventListener("open", () => {
      if (this.socket !== socket) return;
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;
      }
      this.reconnectAttempt = 0;
      this.isWebSocketConnected = true;
      this.hasEverConnected = true;
      this.connectionCount += 1;
      this.sendAuth(false);
      if (isReconnect) this.resubscribeQueries(generation);
      this.notifyConnectionState();
    });
    socket.addEventListener("close", () => {
      if (this.socket !== socket || this.manuallyClosed) return;
      this.isWebSocketConnected = false;
      this.authInFlight = false;
      // A subscription queued for the old socket is superseded by the complete
      // resubscribe below. Queued mutations/actions are rejected below, so
      // drop them too — flushing them after reconnect would fire writes whose
      // callers already saw a rejection.
      this.pendingMessages.length = 0;
      // Mutations/actions must fail closed on transport loss: silently
      // replaying a non-idempotent write after reconnect is unsafe, and
      // leaving the promise pending hangs the caller forever.
      this.rejectPendingCalls((call) => new GonvexClientError(
        `Connection lost while waiting for ${call.kind} ${call.path}. The operation may or may not have been applied.`,
        { code: "disconnected", path: call.path, operation: call.kind },
      ));
      this.scheduleReconnect();
      this.notifyConnectionState();
    });
    socket.addEventListener("message", (event) => {
      if (this.socket !== socket) return;
      let message: ServerMessage;
      try {
        message = JSON.parse(String(event.data)) as ServerMessage;
      } catch {
        return;
      }
      if (message.type === "session.ready") {
        if (!this.auth.token && !this.auth.tenant) {
          this.installQueryCacheDirective(message.queryCache);
        }
        return;
      }
      if (message.type === "session.scope") {
        this.installQueryCacheDirective(message.queryCache);
        return;
      }
      if (message.type === "auth.result" || message.type === "auth.error") {
        this.authInFlight = false;
        if (message.type === "auth.result") {
          this.installQueryCacheDirective(queryCacheDirectiveFromAuthResult(message.result));
        } else {
          this.resetQueryCacheScope();
        }
        this.flushPendingMessages();
      }
      const id = "id" in message ? message.id : "system";
      this.handlers.get(id)?.(message);
    });
  }

  close() {
    this.manuallyClosed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    for (const query of this.oneShotQueries.values()) {
      if (query.timeoutTimer) clearTimeout(query.timeoutTimer);
      query.reject(new GonvexClientError("Gonvex client was closed", { code: "closed", path: query.path, operation: "query" }));
    }
    this.oneShotQueries.clear();
    this.rejectPendingCalls((call) => new GonvexClientError(
      `Gonvex client was closed while waiting for ${call.kind} ${call.path}`,
      { code: "closed", path: call.path, operation: call.kind },
    ));
    this.handlers.clear();
    this.querySubscriptions.clear();
    this.sessionScopeHandlers.clear();
    this.queryCacheGeneration += 1;
    this.queryCacheDirective = undefined;
    this.queryCache?.close();
    this.errorReporter?.close();
    const socket = this.socket;
    this.socket = undefined;
    this.isWebSocketConnected = false;
    this.notifyConnectionState();
    this.connectionStateHandlers.clear();
    if (!socket) return;
    socket.close();
  }

  private rejectPendingCalls(makeError: (call: PendingCall) => GonvexClientError) {
    if (this.pendingCalls.size === 0) return;
    const calls = Array.from(this.pendingCalls.values());
    this.pendingCalls.clear();
    for (const call of calls) {
      if (call.timeoutTimer) clearTimeout(call.timeoutTimer);
      this.handlers.delete(call.id);
      call.reject(makeError(call));
    }
  }

  onTelemetry(handler: TelemetryHandler) {
    this.telemetryHandlers.add(handler);
    return () => this.telemetryHandlers.delete(handler);
  }

  onSessionScopeChange(handler: () => void) {
    this.sessionScopeHandlers.add(handler);
    return () => this.sessionScopeHandlers.delete(handler);
  }

  async clearQueryCache(options: { allScopes?: boolean } = {}) {
    if (!this.queryCache) return;
    const scope = options.allScopes ? undefined : this.queryCacheDirective?.scope;
    if (!options.allScopes && !scope) return;
    await this.queryCache.clear(scope);
  }

  getQueryCacheStatus(): QueryCacheStatus {
    return this.queryCache?.status() ?? {
      enabled: false,
      readsEnabled: false,
      writesEnabled: false,
      reason: "disabled-by-client",
    };
  }

  subscribeQuery(ref: FunctionReference, args: JsonValue = {}, onMessage: SubscriptionHandler) {
    this.connect();
    const key = querySubscriptionKey(ref, args);
    const existing = this.querySubscriptions.get(key);
    if (existing) {
      const wasOrphaned = existing.listeners.size === 0;
      if (existing.unsubscribeTimer) {
        clearTimeout(existing.unsubscribeTimer);
        existing.unsubscribeTimer = undefined;
      }
      existing.listeners.add(onMessage);
      this.startQueryCacheRead(existing);
      // Replay the latest result/error to this late joiner. Coalesced subscriptions
      // share a single server subscription, so the server only sends `initial` once —
      // to the first subscriber. Without this replay, components that mount after the
      // initial result arrives (e.g. a dialog opened later) would never receive data
      // until the next server-side invalidation. Replaying here (not via the shared
      // handler) keeps the cached value flowing without emitting extra telemetry/traffic.
      const cached = existing.lastMessage;
      if (wasOrphaned && cached?.type === "query.error") {
        // A React error boundary can unmount and remount a failed query while the
        // unsubscribe grace timer is still active. Replaying that terminal error
        // traps the boundary even if the runtime was updated in the meantime.
        // Treat the remount as a fresh attempt while continuing to coalesce active
        // subscribers normally.
        existing.lastMessage = undefined;
        existing.serverSettled = false;
        existing.socketGeneration = undefined;
        this.sendSubscription(existing);
      } else if (cached) {
        queueMicrotask(() => {
          if (existing.listeners.has(onMessage)) onMessage(cached);
        });
      }
      return () => this.unsubscribeQueryListener(key, onMessage);
    }

    const subscription: QuerySubscription = {
      id: randomID(),
      key,
      path: ref.path,
      args,
      listeners: new Set([onMessage]),
      serverSettled: false,
    };
    this.querySubscriptions.set(key, subscription);
    this.handlers.set(subscription.id, (message) => {
      if (message.type === "query.result") {
        if (message.cacheScope && message.cacheScope !== this.queryCacheDirective?.scope) {
          return;
        }
        subscription.serverSettled = true;
        subscription.lastMessage = message;
        this.recordTelemetry({
          type: "query",
          id: message.id,
          path: subscription.path,
          reason: message.reason,
          outcome: "ok",
          clientReceivedAtMs: nowMs(),
          serverTrace: message.trace,
        });
      }
      if (message.type === "query.error") {
        subscription.serverSettled = true;
        subscription.lastMessage = message;
        this.recordTelemetry({
          type: "query",
          id: message.id,
          path: subscription.path,
          outcome: "error",
          error: message.error,
          clientReceivedAtMs: nowMs(),
        });
      }
      for (const listener of Array.from(subscription.listeners)) {
        listener(message);
      }
      if (message.type === "query.result") {
        this.persistQueryResult(subscription, message);
      } else if (message.type === "query.error") {
        this.deleteCachedQuery(subscription);
      }
    });
    this.sendSubscription(subscription);
    this.startQueryCacheRead(subscription);

    return () => this.unsubscribeQueryListener(key, onMessage);
  }

  watchQuery<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}) {
    let latest: T | undefined;
    let latestError: Error | undefined;
    const updateHandlers = new Set<WatchUpdateHandler>();

    const unsubscribe = this.subscribeQuery(ref, args, (message) => {
      if (message.type === "query.result") {
        latest = message.result as T;
        latestError = undefined;
        for (const handler of updateHandlers) handler();
      }
      if (message.type === "query.error") {
        latestError = new Error(message.error);
        for (const handler of updateHandlers) handler();
      }
    });
    const unsubscribeScope = this.onSessionScopeChange(() => {
      latest = undefined;
      latestError = undefined;
      for (const handler of updateHandlers) handler();
    });

    return {
      localQueryResult() {
        if (latestError) throw latestError;
        return latest;
      },
      onUpdate(handler: WatchUpdateHandler) {
        updateHandlers.add(handler);
        return () => {
          updateHandlers.delete(handler);
          if (updateHandlers.size === 0) {
            unsubscribe();
            unsubscribeScope();
          }
        };
      },
    };
  }

  mutation<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}, options: CallOptions = {}): Promise<T> {
    return this.call<T>("mutation", ref, args, options.timeoutMs ?? this.timeouts.mutationTimeoutMs);
  }

  action<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}, options: CallOptions = {}): Promise<T> {
    return this.call<T>("action", ref, args, options.timeoutMs ?? this.timeouts.actionTimeoutMs);
  }

  query<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}, options: CallOptions = {}): Promise<T> {
    this.connect();
    const id = randomID();
    const timeoutMs = options.timeoutMs ?? this.timeouts.queryTimeoutMs;
    return new Promise<T>((resolve, reject) => {
      const query: OneShotQuery = { id, path: ref.path, args, reject };
      const settle = () => {
        if (query.timeoutTimer) clearTimeout(query.timeoutTimer);
        this.oneShotQueries.delete(id);
        this.handlers.delete(id);
        this.notifyConnectionState();
      };
      if (timeoutMs > 0) {
        query.timeoutTimer = setTimeout(() => {
          settle();
          this.send({ type: "query.unsubscribe", id });
          reject(new GonvexClientError(
            `Query ${ref.path} timed out after ${timeoutMs}ms`,
            { code: "timeout", path: ref.path, operation: "query" },
          ));
        }, timeoutMs);
      }
      this.oneShotQueries.set(id, query);
      this.handlers.set(id, (message) => {
        if (message.type === "query.result") {
          settle();
          this.recordTelemetry({
            type: "query",
            id: message.id,
            path: ref.path,
            reason: message.reason,
            outcome: "ok",
            clientReceivedAtMs: nowMs(),
            serverTrace: message.trace,
          });
          this.send({ type: "query.unsubscribe", id });
          resolve(message.result as T);
        }
        if (message.type === "query.error") {
          settle();
          this.recordTelemetry({
            type: "query",
            id: message.id,
            path: ref.path,
            outcome: "error",
            error: message.error,
            clientReceivedAtMs: nowMs(),
          });
          this.send({ type: "query.unsubscribe", id });
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: "query" }));
        }
      });
      this.sendOneShotQuery(query);
      this.notifyConnectionState();
    });
  }

  /**
   * Force a live query subscription to re-request its result from the server,
   * e.g. after a `query.error` or when a subscriber gave up waiting. No-op if
   * nothing is subscribed to this query.
   */
  retryQuery(ref: FunctionReference, args: JsonValue = {}) {
    const subscription = this.querySubscriptions.get(querySubscriptionKey(ref, args));
    if (!subscription || subscription.listeners.size === 0) return;
    subscription.serverSettled = false;
    subscription.socketGeneration = undefined;
    this.connect();
    this.sendSubscription(subscription);
  }

  private call<T>(kind: "mutation" | "action", ref: FunctionReference, args: JsonValue, timeoutMs: number): Promise<T> {
    this.connect();
    const id = randomID();
    const clientSentAtMs = nowMs();
    return new Promise<T>((resolve, reject) => {
      const pending: PendingCall = { id, kind, path: ref.path, reject };
      const settle = () => {
        if (pending.timeoutTimer) clearTimeout(pending.timeoutTimer);
        this.pendingCalls.delete(id);
        this.handlers.delete(id);
        this.notifyConnectionState();
      };
      if (timeoutMs > 0) {
        pending.timeoutTimer = setTimeout(() => {
          settle();
          reject(new GonvexClientError(
            `${kind === "mutation" ? "Mutation" : "Action"} ${ref.path} timed out after ${timeoutMs}ms. The operation may or may not have been applied.`,
            { code: "timeout", path: ref.path, operation: kind },
          ));
        }, timeoutMs);
      }
      this.pendingCalls.set(id, pending);
      this.handlers.set(id, (message) => {
        if (kind === "mutation" && message.type === "mutation.result") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "ok", clientSentAtMs, message.trace);
          resolve(message.result as T);
        }
        if (kind === "mutation" && message.type === "mutation.error") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "error", clientSentAtMs, message.trace, message.error);
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: kind }));
        }
        if (kind === "action" && message.type === "action.result") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "ok", clientSentAtMs, message.trace);
          resolve(message.result as T);
        }
        if (kind === "action" && message.type === "action.error") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "error", clientSentAtMs, message.trace, message.error);
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: kind }));
        }
      });
      if (kind === "mutation") {
        this.send({ type: "mutation.call", id, path: ref.path, args, trace: { clientSentAtMs } });
      } else {
        this.send({ type: "action.call", id, path: ref.path, args, trace: { clientSentAtMs } });
      }
      this.notifyConnectionState();
    });
  }

  private unsubscribeQueryListener(key: string, listener: SubscriptionHandler) {
    const subscription = this.querySubscriptions.get(key);
    if (!subscription) return;
    subscription.listeners.delete(listener);
    if (subscription.listeners.size > 0 || subscription.unsubscribeTimer) return;

    // React can briefly unmount/remount the same hook during route transitions,
    // StrictMode, or error-boundary recovery. Holding the server subscription for
    // one tick prevents unsubscribe/subscribe ping-pong while still cleaning up
    // abandoned subscriptions promptly.
    subscription.unsubscribeTimer = setTimeout(() => {
      const latest = this.querySubscriptions.get(key);
      if (!latest || latest.listeners.size > 0) return;
      this.querySubscriptions.delete(key);
      this.send({ type: "query.unsubscribe", id: latest.id });
      setTimeout(() => this.handlers.delete(latest.id), 500);
    }, 250);
  }

  private sendSubscription(subscription: QuerySubscription) {
    if (subscription.socketGeneration === this.socketGeneration) return;
    subscription.socketGeneration = this.socketGeneration;
    this.send({
      type: "query.subscribe",
      id: subscription.id,
      path: subscription.path,
      args: subscription.args,
    });
  }

  private sendOneShotQuery(query: OneShotQuery) {
    if (query.socketGeneration === this.socketGeneration) return;
    query.socketGeneration = this.socketGeneration;
    this.send({ type: "query.subscribe", id: query.id, path: query.path, args: query.args });
  }

  private resubscribeQueries(generation: number) {
    if (generation !== this.socketGeneration) return;
    for (const subscription of this.querySubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      subscription.serverSettled = false;
      this.sendSubscription(subscription);
    }
    for (const query of this.oneShotQueries.values()) {
      this.sendOneShotQuery(query);
    }
  }

  private scheduleReconnect() {
    if (this.manuallyClosed || this.reconnectTimer) return;
    const delay = Math.min(250 * (2 ** this.reconnectAttempt), 5_000);
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.manuallyClosed) {
        this.connect();
        this.notifyConnectionState();
      }
    }, delay);
  }

  private installQueryCacheDirective(value: QueryCacheDirective | undefined) {
    if (!validQueryCacheDirective(value)) {
      if (this.queryCacheDirective) this.resetQueryCacheScope();
      return;
    }
    if (this.queryCacheDirective?.scope === value.scope) {
      this.queryCacheDirective = value;
      return;
    }
    if (this.queryCacheDirective) {
      this.resetQueryCacheScope();
    }
    this.queryCacheDirective = value;
    for (const subscription of this.querySubscriptions.values()) {
      this.startQueryCacheRead(subscription);
    }
  }

  private resetQueryCacheScope() {
    const hadScope = this.queryCacheDirective !== undefined;
    this.queryCacheGeneration += 1;
    this.queryCacheDirective = undefined;
    for (const subscription of this.querySubscriptions.values()) {
      subscription.lastMessage = undefined;
      subscription.serverSettled = false;
      subscription.cacheReadGeneration = undefined;
    }
    if (hadScope || this.querySubscriptions.size > 0) {
      for (const handler of this.sessionScopeHandlers) handler();
    }
  }

  private startQueryCacheRead(subscription: QuerySubscription) {
    const store = this.queryCache;
    const directive = this.queryCacheDirective;
    if (!store || !directive || subscription.serverSettled) return;
    const generation = this.queryCacheGeneration;
    if (subscription.cacheReadGeneration === generation) return;
    subscription.cacheReadGeneration = generation;
    void store.read(directive.scope, subscription.path, subscription.args, directive.maxAgeMs).then((cached) => {
      const current = this.querySubscriptions.get(subscription.key);
      if (
        !cached
        || current !== subscription
        || subscription.serverSettled
        || subscription.listeners.size === 0
        || this.queryCacheGeneration !== generation
        || this.queryCacheDirective?.scope !== directive.scope
      ) {
        return;
      }
      const message: ServerMessage = {
        type: "query.result",
        id: subscription.id,
        path: subscription.path,
        result: cached.result,
        reason: "initial",
        cacheScope: directive.scope,
        cacheRevision: cached.revision,
      };
      subscription.lastMessage = message;
      for (const listener of Array.from(subscription.listeners)) {
        listener(message);
      }
    }).catch(() => {
      // Persistent cache failures never affect the server query path.
    });
  }

  private persistQueryResult(subscription: QuerySubscription, message: Extract<ServerMessage, { type: "query.result" }>) {
    const store = this.queryCache;
    const directive = this.queryCacheDirective;
    if (
      !store
      || !directive
      || message.cacheScope !== directive.scope
      || !message.cacheRevision
    ) {
      return;
    }
    const generation = this.queryCacheGeneration;
    queueMicrotask(() => {
      if (this.queryCacheGeneration !== generation || this.queryCacheDirective?.scope !== directive.scope) return;
      void store.write({
        scope: directive.scope,
        path: subscription.path,
        args: subscription.args,
        result: message.result,
        revision: message.cacheRevision!,
        maxAgeMs: directive.maxAgeMs,
      }).catch(() => undefined);
    });
  }

  private deleteCachedQuery(subscription: QuerySubscription) {
    const store = this.queryCache;
    const directive = this.queryCacheDirective;
    if (!store || !directive) return;
    void store.delete(directive.scope, subscription.path, subscription.args).catch(() => undefined);
  }

  private emitTelemetryFromCall(
    kind: "mutation" | "action",
    id: string,
    path: string,
    outcome: "ok" | "error",
    clientSentAtMs: number,
    serverTrace: MessageTrace | undefined,
    error?: string,
  ) {
    const clientReceivedAtMs = nowMs();
    this.recordTelemetry({
      type: kind,
      id,
      path,
      outcome,
      error,
      clientSentAtMs,
      clientReceivedAtMs,
      clientDurationMs: clientReceivedAtMs - clientSentAtMs,
      serverTrace,
    });
  }

  private recordTelemetry(event: GonvexTelemetryEvent) {
    this.emitTelemetry(event);
    if (event.outcome === "error") {
      this.errorReporter?.captureException(new Error(event.error || `${event.type} failed`), {
        gonvexOperation: { type: event.type, path: event.path, operationId: event.id, reason: event.reason },
        serverTrace: event.serverTrace,
      });
    }
    if (this.telemetryEnabled) {
      this.reportTelemetry(event);
    }
  }

  private emitTelemetry(event: GonvexTelemetryEvent) {
    for (const handler of this.telemetryHandlers) {
      handler(event);
    }
  }

  private reportTelemetry(event: GonvexTelemetryEvent) {
    this.send({
      type: "telemetry.event",
      id: event.id,
      kind: event.type,
      path: event.path,
      reason: event.reason,
      outcome: event.outcome,
      error: event.error,
      clientSentAtMs: event.clientSentAtMs,
      clientReceivedAtMs: event.clientReceivedAtMs,
      clientDurationMs: event.clientDurationMs,
      trace: event.serverTrace,
      device: event.device ?? browserTelemetryInfo(),
    });
  }

  private sendAuth(force: boolean) {
    if (!force && !this.auth.token && !this.auth.tenant && !this.auth.project) return;
    this.authInFlight = true;
    this.sendNow({ type: "auth", id: randomID(), token: this.auth.token, project: this.auth.project, tenant: this.auth.tenant });
  }

  private send(message: ClientMessage) {
    if (this.authInFlight && message.type !== "auth" && message.type !== "telemetry.event") {
      this.pendingMessages.push(message);
      return;
    }
    this.sendNow(message);
  }

  private sendNow(message: ClientMessage) {
    const socket = this.socket;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      socket?.addEventListener(
        "open",
        () => {
          if (message.type === "auth") {
            socket.send(JSON.stringify(message));
            return;
          }
          this.send(message);
        },
        { once: true },
      );
      return;
    }
    socket.send(JSON.stringify(message));
  }

  private flushPendingMessages() {
    const pending = this.pendingMessages.splice(0);
    for (const message of pending) {
      this.send(message);
    }
  }
}

function querySubscriptionKey(ref: FunctionReference, args: JsonValue) {
  return `${ref.path}\u0000${stableStringify(args)}`;
}

function countPendingCalls(calls: Map<string, PendingCall>, kind: "mutation" | "action") {
  let count = 0;
  for (const call of calls.values()) {
    if (call.kind === kind) count += 1;
  }
  return count;
}

function stableStringify(value: JsonValue): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const record = value as Record<string, JsonValue>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
    .join(",")}}`;
}

export class ConvexReactClient extends GonvexClient {
  constructor(url: string, options: GonvexClientOptions = {}) {
    super(toWebSocketURL(url, options.project), options);
  }
}

function authFromOptions(options: GonvexClientOptions): GonvexClientAuth {
  return {
    project: options.project,
    token: options.token,
    tenant: options.tenant,
    telemetry: options.telemetry,
  };
}

function queryCacheDirectiveFromAuthResult(result: JsonValue): QueryCacheDirective | undefined {
  if (!isJsonRecord(result)) return undefined;
  return validQueryCacheDirective(result.queryCache) ? result.queryCache : undefined;
}

function validQueryCacheDirective(value: unknown): value is QueryCacheDirective {
  if (!isJsonRecord(value)) return false;
  return value.protocolVersion === 1
    && typeof value.scope === "string"
    && value.scope.length >= 16
    && typeof value.epoch === "string"
    && value.epoch.length >= 16
    && typeof value.maxAgeMs === "number"
    && Number.isFinite(value.maxAgeMs)
    && value.maxAgeMs > 0;
}

function isJsonRecord(value: unknown): value is Record<string, JsonValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasOwn<T extends object>(value: T, key: PropertyKey) {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function toWebSocketURL(url: string, project?: string) {
  const wsURL = url.startsWith("ws://") || url.startsWith("wss://")
    ? new URL(url)
    : new URL(`${url.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "")}/ws`);
  if (project && !wsURL.searchParams.has("project")) {
    wsURL.searchParams.set("project", project);
  }
  return wsURL.toString();
}

function randomID() {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  return `gonvex_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function nowMs() {
  const performanceValue = globalThis.performance;
  if (
    performanceValue
    && Number.isFinite(performanceValue.timeOrigin)
    && typeof performanceValue.now === "function"
  ) {
    return performanceValue.timeOrigin + performanceValue.now();
  }
  return Date.now();
}

function browserTelemetryInfo(): BrowserTelemetryInfo | undefined {
  const navigatorValue = globalThis.navigator;
  if (!navigatorValue) return undefined;
  const userAgent = navigatorValue.userAgent || "";
  const connection = (navigatorValue as any).connection || (navigatorValue as any).mozConnection || (navigatorValue as any).webkitConnection;
  const viewportWidth = typeof globalThis.innerWidth === "number" ? globalThis.innerWidth : undefined;
  const viewportHeight = typeof globalThis.innerHeight === "number" ? globalThis.innerHeight : undefined;
  return {
    userAgent,
    ...parseBrowser(userAgent),
    deviceType: detectDeviceType(userAgent),
    platform: navigatorValue.platform || "",
    language: navigatorValue.language || "",
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "",
    viewportWidth,
    viewportHeight,
    hardwareConcurrency: navigatorValue.hardwareConcurrency,
    deviceMemory: typeof (navigatorValue as any).deviceMemory === "number" ? (navigatorValue as any).deviceMemory : undefined,
    touchPoints: navigatorValue.maxTouchPoints,
    connectionType: typeof connection?.type === "string" ? connection.type : undefined,
    effectiveConnectionType: typeof connection?.effectiveType === "string" ? connection.effectiveType : undefined,
  };
}

function parseBrowser(userAgent: string): Pick<BrowserTelemetryInfo, "browserName" | "browserVersion"> {
  const patterns: Array<[string, RegExp]> = [
    ["Edge", /Edg\/([0-9.]+)/],
    ["Chrome", /Chrome\/([0-9.]+)/],
    ["Firefox", /Firefox\/([0-9.]+)/],
    ["Safari", /Version\/([0-9.]+).*Safari/],
  ];
  for (const [browserName, pattern] of patterns) {
    const match = userAgent.match(pattern);
    if (match) return { browserName, browserVersion: match[1] };
  }
  return { browserName: "", browserVersion: "" };
}

function detectDeviceType(userAgent: string) {
  if (/ipad|tablet/i.test(userAgent)) return "tablet";
  if (/mobi|iphone|android/i.test(userAgent)) return "mobile";
  return "desktop";
}
