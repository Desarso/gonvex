import { cacheFailureAction, type CacheFailureAction } from "./cache.js";
import { CacheCoordinatorModel, type CacheTab } from "./cache-coordinator.js";

export type LocalDBOperation = "read" | "write";

export type LocalDBRequest<T = unknown> = {
  id: string;
  operation: LocalDBOperation;
  sql: string;
  params?: unknown[];
  execute: (state: Map<string, unknown>) => T | Promise<T>;
};

export type LocalDBResponse<T = unknown> =
  | { ok: true; ownerTabId: string; value: T }
  | { ok: false; route: "server"; reason: string; failure?: CacheFailureAction };

export class FakeTabWorker {
  readonly state = new Map<string, unknown>();
  openCount = 0;
  writeCount = 0;

  constructor(readonly tabId: string) {}

  async boot() {
    this.openCount += 1;
  }

  async execute<T>(request: LocalDBRequest<T>): Promise<T> {
    if (request.operation === "write") {
      this.writeCount += 1;
    }
    return request.execute(this.state);
  }
}

export class BrowserCacheCoordinatorHarness {
  readonly coordinator = new CacheCoordinatorModel();
  readonly workers = new Map<string, FakeTabWorker>();
  readonly requestLog: Array<{ requestingTabId: string; ownerTabId?: string; operation: LocalDBOperation }> = [];

  registerTab(tab: CacheTab) {
    this.coordinator.registerTab(tab);
    this.workers.set(tab.id, new FakeTabWorker(tab.id));
  }

  async bootActiveOwner() {
    const activeTabId = this.coordinator.snapshot().activeTabId;
    if (!activeTabId) {
      return;
    }
    const worker = this.workers.get(activeTabId);
    if (!worker) {
      return;
    }
    await worker.boot();
    this.coordinator.markActiveOwnerReady();
  }

  closeTab(tabId: string) {
    this.coordinator.closeTab(tabId);
  }

  async request<T>(requestingTabId: string, request: LocalDBRequest<T>): Promise<LocalDBResponse<T>> {
    const route = this.coordinator.routeRequest();
    if (route.route === "server") {
      this.requestLog.push({ requestingTabId, operation: request.operation });
      return { ok: false, route: "server", reason: route.reason };
    }

    const owner = this.workers.get(route.activeTabId);
    if (!owner) {
      return { ok: false, route: "server", reason: "active-worker-missing" };
    }

    this.requestLog.push({ requestingTabId, ownerTabId: route.activeTabId, operation: request.operation });
    try {
      const value = await owner.execute(request);
      return { ok: true, ownerTabId: route.activeTabId, value };
    } catch (error) {
      return { ok: false, route: "server", reason: "local-db-error", failure: cacheFailureAction(error) };
    }
  }

  activeWriters() {
    return [...this.workers.values()].filter((worker) => worker.writeCount > 0).map((worker) => worker.tabId);
  }
}
