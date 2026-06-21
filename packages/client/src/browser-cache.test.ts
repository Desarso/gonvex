import { describe, expect, it } from "vitest";
import { BrowserCacheCoordinatorHarness, type LocalDBRequest } from "./browser-cache";

const putRequest: LocalDBRequest<string> = {
  id: "put",
  operation: "write",
  sql: "insert into gonvex_shapes",
  execute(state) {
    state.set("tasks.visible", "ready");
    return "ok";
  },
};

const getRequest: LocalDBRequest<string | undefined> = {
  id: "get",
  operation: "read",
  sql: "select * from gonvex_shapes",
  execute(state) {
    return state.get("tasks.visible") as string | undefined;
  },
};

describe("BrowserCacheCoordinatorHarness", () => {
  it("routes all tab DB requests through one active owner worker", async () => {
    const harness = new BrowserCacheCoordinatorHarness();
    harness.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    harness.registerTab({ id: "tab-b", hasLivenessLock: true, dedicatedWorkerReady: true });
    await harness.bootActiveOwner();

    await harness.request("tab-a", putRequest);
    const response = await harness.request("tab-b", getRequest);

    expect(response).toEqual({ ok: true, ownerTabId: "tab-a", value: "ready" });
    expect(harness.requestLog.map((entry) => entry.ownerTabId)).toEqual(["tab-a", "tab-a"]);
    expect(harness.activeWriters()).toEqual(["tab-a"]);
  });

  it("falls back to server while the elected worker is booting", async () => {
    const harness = new BrowserCacheCoordinatorHarness();
    harness.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: false });

    expect(await harness.request("tab-a", getRequest)).toMatchObject({
      ok: false,
      route: "server",
      reason: "owner-not-ready",
    });
  });

  it("elects a replacement after active owner close but waits for integrity", async () => {
    const harness = new BrowserCacheCoordinatorHarness();
    harness.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    harness.registerTab({ id: "tab-b", hasLivenessLock: true, dedicatedWorkerReady: true });
    await harness.bootActiveOwner();

    harness.closeTab("tab-a");
    expect(await harness.request("tab-b", getRequest)).toMatchObject({
      ok: false,
      route: "server",
      reason: "owner-not-ready",
    });

    await harness.bootActiveOwner();
    expect(await harness.request("tab-b", getRequest)).toMatchObject({
      ok: true,
      ownerTabId: "tab-b",
    });
  });

  it("converts corruption from local worker into reset plus server fallback", async () => {
    const harness = new BrowserCacheCoordinatorHarness();
    harness.registerTab({ id: "tab-a", hasLivenessLock: true, dedicatedWorkerReady: true });
    await harness.bootActiveOwner();

    const response = await harness.request("tab-a", {
      id: "bad",
      operation: "read",
      sql: "pragma integrity_check",
      execute() {
        throw new Error("sqlite integrity check failed");
      },
    });

    expect(response).toMatchObject({
      ok: false,
      route: "server",
      reason: "local-db-error",
      failure: { action: "reset-and-server-fallback" },
    });
  });
});
