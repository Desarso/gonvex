import {
  BrowserCacheCoordinatorHarness,
  type LocalDBOperation,
  type LocalDBResponse,
} from "./browser-cache.js";
import type { CacheDBHealth } from "./cache.js";
import { CacheCoordinatorModel, type CacheTab } from "./cache-coordinator.js";

export type SharedWorkerPortLike = {
  postMessage(message: unknown): void;
  addEventListener?: (type: "message", listener: (event: { data: unknown }) => void) => void;
  start?: () => void;
};

export type CacheOwnerExecutor = {
  execute<T>(request: RoutedCacheRequest): Promise<T>;
  reset?: () => Promise<void>;
};

export type RoutedCacheRequest = {
  id: string;
  operation: LocalDBOperation;
  sql: string;
  params?: unknown[];
};

type WorkerInboundMessage =
  | { type: "gonvex.cache.register"; tabId: string }
  | { type: "gonvex.cache.tab"; tabId: string; tab: Partial<Omit<CacheTab, "id">> }
  | { type: "gonvex.cache.owner.ready"; tabId: string }
  | { type: "gonvex.cache.owner.lost"; tabId: string }
  | { type: "gonvex.cache.health"; id: string; tabId: string }
  | { type: "gonvex.cache.request"; id: string; tabId: string; request: RoutedCacheRequest };

export class SharedWorkerCacheCoordinator {
  readonly coordinator = new CacheCoordinatorModel();
  private readonly ports = new Map<string, SharedWorkerPortLike>();
  private dbHealth: CacheDBHealth = "initializing";
  private ownerExecutor: CacheOwnerExecutor | undefined;

  constructor(private readonly harness?: BrowserCacheCoordinatorHarness) {}

  attach(port: SharedWorkerPortLike) {
    port.addEventListener?.("message", (event) => {
      void this.handle(port, event.data);
    });
    port.start?.();
  }

  setDBHealth(health: CacheDBHealth) {
    this.dbHealth = health;
  }

  setOwnerExecutor(executor: CacheOwnerExecutor | undefined) {
    this.ownerExecutor = executor;
  }

  async handle(port: SharedWorkerPortLike, raw: unknown) {
    if (!isInboundMessage(raw)) {
      return;
    }
    if ("tabId" in raw) {
      this.ports.set(raw.tabId, port);
    }
    switch (raw.type) {
      case "gonvex.cache.register":
        this.coordinator.registerTab({
          id: raw.tabId,
          hasLivenessLock: false,
          dedicatedWorkerReady: false,
          storageReady: false,
          integrityVerified: false,
        });
        this.replyHealth(port, "");
        return;
      case "gonvex.cache.tab":
        this.coordinator.updateTab(raw.tabId, raw.tab);
        return;
      case "gonvex.cache.owner.ready":
        this.coordinator.markActiveOwnerReady();
        this.dbHealth = "healthy";
        return;
      case "gonvex.cache.owner.lost":
        this.coordinator.closeTab(raw.tabId);
        this.dbHealth = "initializing";
        return;
      case "gonvex.cache.health":
        this.replyHealth(port, raw.id);
        return;
      case "gonvex.cache.request":
        await this.handleRequest(port, raw.id, raw.request);
        return;
    }
  }

  private async handleRequest(port: SharedWorkerPortLike, id: string, request: RoutedCacheRequest) {
    if (this.dbHealth !== "healthy") {
      this.reply(port, { type: "gonvex.cache.response", id, response: serverResponse("db-not-healthy") });
      return;
    }
    const route = this.coordinator.routeRequest();
    if (route.route === "server") {
      this.reply(port, { type: "gonvex.cache.response", id, response: serverResponse(route.reason) });
      return;
    }
    const ownerExecutor = this.ownerExecutor;
    if (!this.harness && !ownerExecutor) {
      this.reply(port, { type: "gonvex.cache.response", id, response: serverResponse("owner-executor-unavailable") });
      return;
    }
    try {
      if (this.harness) {
        const value = await this.harness.request(route.activeTabId, { ...request, execute: () => undefined });
        this.reply(port, { type: "gonvex.cache.response", id, response: value });
        return;
      }
      if (!ownerExecutor) {
        this.reply(port, { type: "gonvex.cache.response", id, response: serverResponse("owner-executor-unavailable") });
        return;
      }
      const value = await ownerExecutor.execute(request);
      this.reply(port, {
        type: "gonvex.cache.response",
        id,
        response: { ok: true, ownerTabId: route.activeTabId, value },
      });
    } catch (error) {
      this.dbHealth = "corrupt";
      await this.ownerExecutor?.reset?.();
      this.reply(port, { type: "gonvex.cache.response", id, response: serverResponse(error instanceof Error ? error.message : "cache-request-failed") });
    }
  }

  private replyHealth(port: SharedWorkerPortLike, id: string) {
    this.reply(port, {
      type: "gonvex.cache.health.result",
      id,
      health: {
        dbHealth: this.dbHealth,
        coordinator: this.coordinator.snapshot(),
      },
    });
  }

  private reply(port: SharedWorkerPortLike, message: unknown) {
    port.postMessage(message);
  }
}

function isInboundMessage(value: unknown): value is WorkerInboundMessage {
  return typeof value === "object" && value !== null && "type" in value;
}

function serverResponse(reason: string): LocalDBResponse {
  return { ok: false, route: "server", reason };
}
