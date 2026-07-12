export type ErrorUser = { id?: string; email?: string; name?: string };
export type ErrorContext = Record<string, unknown>;

export type ErrorReporterOptions = {
  endpoint: string;
  project: string;
  tenant?: string;
  release?: string;
  environment?: string;
  user?: ErrorUser;
  tags?: Record<string, string>;
  sampleRate?: number;
  beforeSend?: (event: ErrorEventPayload) => ErrorEventPayload | null;
  captureGlobalErrors?: boolean;
};

export type ErrorEventPayload = {
  eventId: string;
  timestamp: string;
  level: "error" | "warning";
  message: string;
  name?: string;
  stack?: string;
  culprit?: string;
  project: string;
  tenant?: string;
  release?: string;
  environment?: string;
  user?: ErrorUser;
  deviceId: string;
  sessionId: string;
  url?: string;
  userAgent?: string;
  language?: string;
  viewport?: string;
  online?: boolean;
  tags?: Record<string, string>;
  context?: ErrorContext;
  breadcrumbs: Array<{ timestamp: string; category: string; message: string; data?: ErrorContext }>;
};

const REDACTED = "[Filtered]";
const SECRET = /password|passwd|secret|token|authorization|cookie|api[-_]?key/i;

export class GonvexErrorReporter {
  private readonly options: ErrorReporterOptions;
  private readonly breadcrumbs: ErrorEventPayload["breadcrumbs"] = [];
  private queue: ErrorEventPayload[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private readonly deviceId = persistedId("gonvex-error-device");
  private readonly sessionId = randomId();
  private removeGlobal?: () => void;

  constructor(options: ErrorReporterOptions) {
    this.options = { sampleRate: 1, captureGlobalErrors: true, ...options };
    if (this.options.captureGlobalErrors && typeof window !== "undefined") this.installGlobalHandlers();
  }

  setUser(user?: ErrorUser) { this.options.user = user; }
  setTenant(tenant?: string) { this.options.tenant = tenant; }

  addBreadcrumb(category: string, message: string, data?: ErrorContext) {
    this.breadcrumbs.push({ timestamp: new Date().toISOString(), category, message, data: scrub(data) as ErrorContext });
    if (this.breadcrumbs.length > 30) this.breadcrumbs.shift();
  }

  captureException(error: unknown, context?: ErrorContext): string | undefined {
    if (Math.random() > (this.options.sampleRate ?? 1)) return;
    const normalized = normalizeError(error);
    let event: ErrorEventPayload = {
      eventId: randomId(), timestamp: new Date().toISOString(), level: "error",
      message: normalized.message, name: normalized.name, stack: normalized.stack,
      culprit: firstAppFrame(normalized.stack), project: this.options.project,
      tenant: this.options.tenant, release: this.options.release, environment: this.options.environment,
      user: scrub(this.options.user) as ErrorUser, deviceId: this.deviceId, sessionId: this.sessionId,
      url: typeof location === "undefined" ? undefined : stripQuery(location.href),
      userAgent: typeof navigator === "undefined" ? undefined : navigator.userAgent,
      language: typeof navigator === "undefined" ? undefined : navigator.language,
      online: typeof navigator === "undefined" ? undefined : navigator.onLine,
      viewport: typeof window === "undefined" ? undefined : `${window.innerWidth}x${window.innerHeight}`,
      tags: this.options.tags, context: scrub(context) as ErrorContext, breadcrumbs: [...this.breadcrumbs],
    };
    event = scrub(event) as ErrorEventPayload;
    const prepared = this.options.beforeSend?.(event) ?? event;
    if (!prepared) return;
    this.queue.push(prepared);
    this.scheduleFlush();
    return event.eventId;
  }

  async flush(): Promise<void> {
    if (!this.queue.length) return;
    const batch = this.queue.splice(0, 20);
    try {
      const response = await fetch(this.options.endpoint.replace(/\/$/, "") + "/errors/envelope", {
        method: "POST", headers: { "content-type": "application/json" }, keepalive: true,
        body: JSON.stringify({ events: batch }),
      });
      if (!response.ok) throw new Error(`error ingestion returned ${response.status}`);
    } catch {
      this.queue = [...batch, ...this.queue].slice(0, 100);
    }
  }

  close() { this.removeGlobal?.(); if (this.timer) clearTimeout(this.timer); void this.flush(); }

  private scheduleFlush() {
    if (this.timer) return;
    this.timer = setTimeout(() => { this.timer = undefined; void this.flush(); }, 1000);
  }

  private installGlobalHandlers() {
    const onError = (event: ErrorEvent) => this.captureException(event.error ?? event.message, { source: event.filename, line: event.lineno, column: event.colno });
    const onRejection = (event: PromiseRejectionEvent) => this.captureException(event.reason, { mechanism: "unhandledrejection" });
    window.addEventListener("error", onError);
    window.addEventListener("unhandledrejection", onRejection);
    this.removeGlobal = () => { window.removeEventListener("error", onError); window.removeEventListener("unhandledrejection", onRejection); };
  }
}

function normalizeError(value: unknown): { name?: string; message: string; stack?: string } {
  if (value instanceof Error) return { name: value.name, message: value.message || value.name, stack: value.stack };
  if (typeof value === "string") return { message: value };
  try { return { message: JSON.stringify(value) }; } catch { return { message: String(value) }; }
}

function scrub(value: unknown, key = "", seen = new WeakSet<object>()): unknown {
  if (SECRET.test(key)) return REDACTED;
  if (!value || typeof value !== "object") return typeof value === "string" ? value.slice(0, 4000) : value;
  if (seen.has(value as object)) return "[Circular]";
  seen.add(value as object);
  if (Array.isArray(value)) return value.slice(0, 50).map((item) => scrub(item, key, seen));
  return Object.fromEntries(Object.entries(value as Record<string, unknown>).slice(0, 100).map(([k, v]) => [k, scrub(v, k, seen)]));
}

function firstAppFrame(stack?: string) { return stack?.split("\n").find((line) => /at\s/.test(line) && !/node_modules/.test(line))?.trim(); }
function stripQuery(url: string) { try { const parsed = new URL(url); parsed.search = ""; parsed.hash = ""; return parsed.toString(); } catch { return url.split(/[?#]/)[0]; } }
function randomId() { return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`; }
function persistedId(key: string) { try { const current = localStorage.getItem(key); if (current) return current; const next = randomId(); localStorage.setItem(key, next); return next; } catch { return randomId(); } }
