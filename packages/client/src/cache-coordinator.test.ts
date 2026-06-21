import { describe, expect, it } from "vitest";
import { CacheCoordinatorModel } from "./cache-coordinator";

describe("CacheCoordinatorModel", () => {
  it("elects one active tab and routes all requests to its worker", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-b", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });

    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "owner-not-ready" });
    coordinator.markActiveOwnerReady();

    expect(coordinator.snapshot()).toMatchObject({
      status: "active_owner_ready",
      activeTabId: "tab-b",
    });
    expect(coordinator.routeRequest()).toEqual({ route: "active-worker", activeTabId: "tab-b" });
  });

  it("keeps routing non-active tabs through the active tab worker", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.registerTab({ id: "tab-b", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.markActiveOwnerReady();

    expect(coordinator.routeRequest()).toEqual({ route: "active-worker", activeTabId: "tab-a" });
  });

  it("elects a replacement when the active tab closes", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.registerTab({ id: "tab-b", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.markActiveOwnerReady();

    coordinator.closeTab("tab-a");
    coordinator.markActiveOwnerReady();

    expect(coordinator.snapshot()).toMatchObject({
      status: "active_owner_ready",
      activeTabId: "tab-b",
    });
    expect(coordinator.routeRequest()).toEqual({ route: "active-worker", activeTabId: "tab-b" });
  });

  it("treats liveness lock loss as owner loss and routes to server until replacement is ready", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.registerTab({ id: "tab-b", hasLivenessLock: false, dedicatedWorkerReady: true });
    coordinator.markActiveOwnerReady();

    coordinator.updateTab("tab-a", { hasLivenessLock: false });

    expect(coordinator.snapshot()).toMatchObject({
      status: "electing",
      activeTabId: undefined,
    });
    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "no-active-owner" });
  });

  it("routes to server after active owner loss until the replacement verifies DB integrity", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.registerTab({ id: "tab-b", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.markActiveOwnerReady();

    coordinator.closeTab("tab-a");

    expect(coordinator.snapshot()).toMatchObject({
      status: "electing",
      activeTabId: "tab-b",
    });
    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "owner-not-ready" });

    coordinator.markActiveOwnerStorageReady();
    coordinator.verifyActiveOwnerIntegrity();

    expect(coordinator.snapshot()).toMatchObject({
      status: "active_owner_ready",
      activeTabId: "tab-b",
    });
    expect(coordinator.routeRequest()).toEqual({ route: "active-worker", activeTabId: "tab-b" });
  });

  it("does not route to local DB while the elected worker is still booting", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: false });

    expect(coordinator.snapshot()).toMatchObject({
      status: "electing",
      activeTabId: "tab-a",
    });
    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "owner-not-ready" });

    coordinator.updateTab("tab-a", { dedicatedWorkerReady: true });
    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "owner-not-ready" });

    coordinator.markActiveOwnerStorageReady();
    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "owner-not-ready" });

    coordinator.verifyActiveOwnerIntegrity();
    expect(coordinator.routeRequest()).toEqual({ route: "active-worker", activeTabId: "tab-a" });
  });

  it("disables local routing entirely when no safe fallback can be proven", () => {
    const coordinator = new CacheCoordinatorModel();
    coordinator.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    coordinator.markActiveOwnerReady();

    coordinator.disableUnsafeFallback();

    expect(coordinator.snapshot()).toMatchObject({
      status: "unsafe_fallback_disabled",
      activeTabId: undefined,
    });
    expect(coordinator.routeRequest()).toEqual({ route: "server", reason: "unsafe-fallback-disabled" });
  });
});
