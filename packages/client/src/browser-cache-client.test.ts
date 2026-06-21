import { describe, expect, it } from "vitest";
import { BrowserCacheCoordinatorHarness, type LocalDBRequest } from "./browser-cache";
import { createBrowserCacheClient, createHarnessBrowserCacheClient } from "./browser-cache-client";

const readReadyShape: Omit<LocalDBRequest<string | undefined>, "operation"> = {
  id: "read-shape",
  sql: "select status from gonvex_shapes where id = ?",
  params: ["tasks.visible"],
  execute(state) {
    return state.get("tasks.visible") as string | undefined;
  },
};

const writeReadyShape: Omit<LocalDBRequest<string>, "operation"> = {
  id: "write-shape",
  sql: "insert into gonvex_shapes",
  execute(state) {
    state.set("tasks.visible", "ready");
    return "ready";
  },
};

describe("HarnessBrowserCacheClient", () => {
  it("exposes the intended SharedWorker-facing API surface", async () => {
    const harness = new BrowserCacheCoordinatorHarness();
    const tabA = createHarnessBrowserCacheClient({ tabId: "tab-a", harness });
    const tabB = createHarnessBrowserCacheClient({ tabId: "tab-b", harness });

    tabA.registerTab({ hasLivenessLock: true, dedicatedWorkerReady: true });
    tabB.registerTab({ hasLivenessLock: true, dedicatedWorkerReady: true });
    tabA.setDBHealth("healthy");
    tabB.setDBHealth("healthy");
    await harness.bootActiveOwner();

    await expect(tabA.requestLocalWrite(writeReadyShape)).resolves.toMatchObject({
      ok: true,
      ownerTabId: "tab-a",
    });
    await expect(tabB.requestLocalRead(readReadyShape)).resolves.toEqual({
      ok: true,
      ownerTabId: "tab-a",
      value: "ready",
    });
  });

  it("reports server fallback while the DB is not healthy", async () => {
    const client = createHarnessBrowserCacheClient({ tabId: "tab-a" });
    client.registerTab({ hasLivenessLock: true, dedicatedWorkerReady: true });

    await expect(client.requestLocalRead(readReadyShape)).resolves.toEqual({
      ok: false,
      route: "server",
      reason: "db-not-healthy",
    });
  });

  it("returns coordinator health without booting storage from the caller", async () => {
    const client = createHarnessBrowserCacheClient({ tabId: "tab-a" });
    client.registerTab({ hasLivenessLock: true, dedicatedWorkerReady: false });

    await expect(client.getCacheHealth()).resolves.toMatchObject({
      dbHealth: "initializing",
      coordinator: {
        status: "electing",
        activeTabId: "tab-a",
      },
    });
  });

  it("disables the production browser client when safe ownership primitives are missing", async () => {
    const client = createBrowserCacheClient({
      tabId: "tab-a",
      workerUrl: "/gonvex-cache-worker.js",
      capabilities: {
        sharedWorker: false,
        webLocks: true,
        dedicatedWorker: true,
        opfs: true,
        supported: false,
        disableReason: "shared-worker-unavailable",
      },
    });

    await expect(client.requestLocalRead(readReadyShape)).resolves.toEqual({
      ok: false,
      route: "server",
      reason: "shared-worker-unavailable",
    });
  });

  it("routes production browser requests through the SharedWorker port without sending executable functions", async () => {
    const messages: unknown[] = [];
    let listener: ((event: { data: unknown }) => void) | undefined;
    class FakeSharedWorker {
      readonly port = {
        postMessage(message: unknown) {
          messages.push(message);
          if (typeof message === "object" && message !== null && "type" in message && message.type === "gonvex.cache.request") {
            expect(JSON.stringify(message)).not.toContain("execute");
            listener?.({
              data: {
                type: "gonvex.cache.response",
                id: message.id,
                response: { ok: true, ownerTabId: "tab-a", value: "ready" },
              },
            });
          }
        },
        addEventListener(_type: "message", next: (event: { data: unknown }) => void) {
          listener = next;
        },
        start() {},
      };
    }
    const client = createBrowserCacheClient({
      tabId: "tab-b",
      workerUrl: "/gonvex-cache-worker.js",
      capabilities: {
        sharedWorker: true,
        webLocks: true,
        dedicatedWorker: true,
        opfs: true,
        supported: true,
      },
      globalValue: { SharedWorker: FakeSharedWorker },
    });

    await expect(client.requestLocalRead(readReadyShape)).resolves.toEqual({
      ok: true,
      ownerTabId: "tab-a",
      value: "ready",
    });
    expect(messages).toEqual(expect.arrayContaining([
      expect.objectContaining({ type: "gonvex.cache.register", tabId: "tab-b" }),
      expect.objectContaining({ type: "gonvex.cache.request", tabId: "tab-b" }),
    ]));
  });
});
