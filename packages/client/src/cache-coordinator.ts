export type CacheOwnerStatus = "no_owner" | "electing" | "active_owner_ready" | "owner_lost" | "unsafe_fallback_disabled";

export type CacheTab = {
  id: string;
  hasLivenessLock: boolean;
  dedicatedWorkerReady: boolean;
  storageReady?: boolean;
  integrityVerified?: boolean;
};

export type CacheCoordinatorSnapshot = {
  status: CacheOwnerStatus;
  activeTabId?: string;
  tabs: CacheTab[];
};

export type DBRequestRoute =
  | { route: "active-worker"; activeTabId: string }
  | { route: "server"; reason: "no-active-owner" | "owner-not-ready" | "unsafe-fallback-disabled" };

export class CacheCoordinatorModel {
  private tabs = new Map<string, CacheTab>();
  private activeTabId: string | undefined;
  private status: CacheOwnerStatus = "no_owner";

  registerTab(tab: CacheTab) {
    this.tabs.set(tab.id, normalizeTab(tab));
    this.electIfNeeded();
  }

  updateTab(tabId: string, patch: Partial<Omit<CacheTab, "id">>) {
    const current = this.tabs.get(tabId);
    if (!current) {
      return;
    }
    this.tabs.set(tabId, normalizeTab({ ...current, ...patch }));
    if (tabId === this.activeTabId && !this.canOwn(this.tabs.get(tabId))) {
      this.activeTabId = undefined;
      this.status = "owner_lost";
    }
    this.electIfNeeded();
  }

  closeTab(tabId: string) {
    this.tabs.delete(tabId);
    if (this.activeTabId === tabId) {
      this.activeTabId = undefined;
      this.status = "owner_lost";
    }
    this.electIfNeeded();
  }

  disableUnsafeFallback() {
    this.activeTabId = undefined;
    this.status = "unsafe_fallback_disabled";
  }

  verifyActiveOwnerIntegrity() {
    if (!this.activeTabId || this.status === "unsafe_fallback_disabled") {
      return;
    }
    this.updateTab(this.activeTabId, { integrityVerified: true });
  }

  markActiveOwnerStorageReady() {
    if (!this.activeTabId || this.status === "unsafe_fallback_disabled") {
      return;
    }
    this.updateTab(this.activeTabId, { storageReady: true });
  }

  markActiveOwnerReady() {
    if (!this.activeTabId || this.status === "unsafe_fallback_disabled") {
      return;
    }
    this.updateTab(this.activeTabId, {
      dedicatedWorkerReady: true,
      storageReady: true,
      integrityVerified: true,
    });
    this.electIfNeeded();
  }

  routeRequest(): DBRequestRoute {
    if (this.status === "unsafe_fallback_disabled") {
      return { route: "server", reason: "unsafe-fallback-disabled" };
    }
    if (!this.activeTabId) {
      return { route: "server", reason: "no-active-owner" };
    }
    const active = this.tabs.get(this.activeTabId);
    if (!this.canOwn(active) || !this.ownerReady(active) || this.status !== "active_owner_ready") {
      return { route: "server", reason: "owner-not-ready" };
    }
    return { route: "active-worker", activeTabId: this.activeTabId };
  }

  snapshot(): CacheCoordinatorSnapshot {
    return {
      status: this.status,
      activeTabId: this.activeTabId,
      tabs: [...this.tabs.values()].map((tab) => ({ ...tab })),
    };
  }

  private electIfNeeded() {
    if (this.status === "unsafe_fallback_disabled") {
      return;
    }
    if (this.activeTabId && this.canOwn(this.tabs.get(this.activeTabId))) {
      this.status = this.ownerReady(this.tabs.get(this.activeTabId)) ? "active_owner_ready" : "electing";
      return;
    }

    const nextOwner = [...this.tabs.values()]
      .filter((tab) => this.canOwn(tab))
      .sort((left, right) => left.id.localeCompare(right.id))[0];

    if (!nextOwner) {
      this.activeTabId = undefined;
      this.status = this.tabs.size > 0 ? "electing" : "no_owner";
      return;
    }

    this.activeTabId = nextOwner.id;
    this.status = this.ownerReady(nextOwner) ? "active_owner_ready" : "electing";
  }

  private canOwn(tab: CacheTab | undefined) {
    return Boolean(tab?.hasLivenessLock);
  }

  private ownerReady(tab: CacheTab | undefined) {
    return Boolean(tab?.dedicatedWorkerReady && tab.storageReady && tab.integrityVerified);
  }
}

function normalizeTab(tab: CacheTab): CacheTab {
  return {
    ...tab,
    storageReady: tab.storageReady ?? false,
    integrityVerified: tab.integrityVerified ?? false,
  };
}
