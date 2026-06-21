import {
  BrowserCacheCoordinatorHarness,
  type LocalDBRequest,
  type LocalDBResponse,
} from "./browser-cache.js";
import type { CacheDBHealth } from "./cache.js";
import type { CacheCoordinatorSnapshot, CacheTab } from "./cache-coordinator.js";
import { detectBrowserCacheCapabilities, type BrowserCacheCapabilities } from "./browser-capabilities.js";

export type BrowserCacheClient = {
  requestLocalRead<T>(request: Omit<LocalDBRequest<T>, "operation">): Promise<LocalDBResponse<T>>;
  requestLocalWrite<T>(request: Omit<LocalDBRequest<T>, "operation">): Promise<LocalDBResponse<T>>;
  getCacheHealth(): Promise<BrowserCacheHealth>;
};

export type BrowserCacheHealth = {
  dbHealth: CacheDBHealth;
  coordinator: CacheCoordinatorSnapshot;
};

export type BrowserCacheClientOptions = {
  tabId: string;
  harness?: BrowserCacheCoordinatorHarness;
};

export type SharedWorkerCacheClientOptions = {
  tabId: string;
  workerUrl: string | URL;
  capabilities?: BrowserCacheCapabilities;
  globalValue?: {
    SharedWorker?: new (url: string | URL, options?: { name?: string; type?: "classic" | "module" }) => { port: WorkerLikePort };
  };
};

type WorkerLikePort = {
  postMessage(message: unknown): void;
  start?: () => void;
  addEventListener?: (type: "message", listener: (event: { data: unknown }) => void) => void;
  removeEventListener?: (type: "message", listener: (event: { data: unknown }) => void) => void;
  onmessage?: ((event: { data: unknown }) => void) | null;
};

type CacheWorkerMessage =
  | { type: "gonvex.cache.health.result"; id: string; health: BrowserCacheHealth }
  | { type: "gonvex.cache.response"; id: string; response: LocalDBResponse };

export class HarnessBrowserCacheClient implements BrowserCacheClient {
  private dbHealth: CacheDBHealth = "initializing";
  private readonly tabId: string;
  private readonly harness: BrowserCacheCoordinatorHarness;

  constructor(options: BrowserCacheClientOptions) {
    this.tabId = options.tabId;
    this.harness = options.harness ?? new BrowserCacheCoordinatorHarness();
  }

  registerTab(tab: Omit<CacheTab, "id">) {
    this.harness.registerTab({ id: this.tabId, ...tab });
  }

  setDBHealth(health: CacheDBHealth) {
    this.dbHealth = health;
  }

  async requestLocalRead<T>(request: Omit<LocalDBRequest<T>, "operation">) {
    if (this.dbHealth !== "healthy") {
      return serverResponse<T>("db-not-healthy");
    }
    return this.harness.request(this.tabId, { ...request, operation: "read" });
  }

  async requestLocalWrite<T>(request: Omit<LocalDBRequest<T>, "operation">) {
    if (this.dbHealth !== "healthy") {
      return serverResponse<T>("db-not-healthy");
    }
    return this.harness.request(this.tabId, { ...request, operation: "write" });
  }

  async getCacheHealth(): Promise<BrowserCacheHealth> {
    return {
      dbHealth: this.dbHealth,
      coordinator: this.harness.coordinator.snapshot(),
    };
  }
}

export function createHarnessBrowserCacheClient(options: BrowserCacheClientOptions) {
  return new HarnessBrowserCacheClient(options);
}

export class DisabledBrowserCacheClient implements BrowserCacheClient {
  constructor(private readonly reason: string) {}

  async requestLocalRead<T>() {
    return serverResponse<T>(this.reason);
  }

  async requestLocalWrite<T>() {
    return serverResponse<T>(this.reason);
  }

  async getCacheHealth(): Promise<BrowserCacheHealth> {
    return {
      dbHealth: "unavailable",
      coordinator: { status: "unsafe_fallback_disabled", tabs: [] },
    };
  }
}

export class SharedWorkerBrowserCacheClient implements BrowserCacheClient {
  private readonly port: WorkerLikePort;
  private readonly pending = new Map<string, (message: CacheWorkerMessage) => void>();

  constructor(private readonly options: SharedWorkerCacheClientOptions) {
    const capabilities = options.capabilities ?? detectBrowserCacheCapabilities();
    if (!capabilities.supported) {
      throw new Error(capabilities.disableReason ?? "browser-cache-unsupported");
    }
    const globalValue = options.globalValue ?? globalThis;
    if (typeof globalValue.SharedWorker !== "function") {
      throw new Error("shared-worker-unavailable");
    }
    this.port = new globalValue.SharedWorker(options.workerUrl, {
      name: "gonvex-cache-coordinator",
      type: "module",
    }).port as WorkerLikePort;
    this.port.start?.();
    this.listen((message) => this.resolve(message));
    this.port.postMessage({ type: "gonvex.cache.register", tabId: options.tabId });
  }

  requestLocalRead<T>(request: Omit<LocalDBRequest<T>, "operation">) {
    return this.request<T>({ ...request, operation: "read" });
  }

  requestLocalWrite<T>(request: Omit<LocalDBRequest<T>, "operation">) {
    return this.request<T>({ ...request, operation: "write" });
  }

  async getCacheHealth(): Promise<BrowserCacheHealth> {
    const message = await this.rpc("gonvex.cache.health", {});
    if (message.type !== "gonvex.cache.health.result") {
      throw new Error("unexpected cache worker health response");
    }
    return message.health;
  }

  private async request<T>(request: LocalDBRequest<T>): Promise<LocalDBResponse<T>> {
    const message = await this.rpc("gonvex.cache.request", {
      request: {
        id: request.id,
        operation: request.operation,
        sql: request.sql,
        params: request.params,
      },
    });
    if (message.type !== "gonvex.cache.response") {
      throw new Error("unexpected cache worker response");
    }
    return message.response as LocalDBResponse<T>;
  }

  private rpc(type: string, payload: Record<string, unknown>) {
    const id = randomID();
    return new Promise<CacheWorkerMessage>((resolve) => {
      this.pending.set(id, resolve);
      this.port.postMessage({ ...payload, type, id, tabId: this.options.tabId });
    });
  }

  private listen(listener: (message: CacheWorkerMessage) => void) {
    if (this.port.addEventListener) {
      this.port.addEventListener("message", (event) => {
        if (isCacheWorkerMessage(event.data)) {
          listener(event.data);
        }
      });
      return;
    }
    this.port.onmessage = (event) => {
      if (isCacheWorkerMessage(event.data)) {
        listener(event.data);
      }
    };
  }

  private resolve(message: CacheWorkerMessage) {
    const resolver = this.pending.get(message.id);
    if (!resolver) {
      return;
    }
    this.pending.delete(message.id);
    resolver(message);
  }
}

export function createBrowserCacheClient(options: SharedWorkerCacheClientOptions): BrowserCacheClient {
  const capabilities = options.capabilities ?? detectBrowserCacheCapabilities();
  if (!capabilities.supported) {
    return new DisabledBrowserCacheClient(capabilities.disableReason ?? "browser-cache-unsupported");
  }
  return new SharedWorkerBrowserCacheClient({ ...options, capabilities });
}

function serverResponse<T>(reason: string): LocalDBResponse<T> {
  return { ok: false, route: "server", reason };
}

function isCacheWorkerMessage(value: unknown): value is CacheWorkerMessage {
  return typeof value === "object" && value !== null && "type" in value && "id" in value;
}

function randomID() {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  return `gonvex_cache_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}
