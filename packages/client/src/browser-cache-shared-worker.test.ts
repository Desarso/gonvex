import { describe, expect, it } from "vitest";
import { SharedWorkerCacheCoordinator, type SharedWorkerPortLike } from "./browser-cache-shared-worker";

function port() {
  const messages: unknown[] = [];
  let listener: ((event: { data: unknown }) => void) | undefined;
  return {
    messages,
    port: {
      postMessage(message: unknown) {
        messages.push(message);
      },
      addEventListener(_type: "message", next: (event: { data: unknown }) => void) {
        listener = next;
      },
      start() {},
    } satisfies SharedWorkerPortLike,
    send(message: unknown) {
      listener?.({ data: message });
    },
  };
}

describe("SharedWorkerCacheCoordinator", () => {
  it("registers tabs and reports server fallback until the owner is verified", async () => {
    const worker = new SharedWorkerCacheCoordinator();
    const tab = port();
    worker.attach(tab.port);

    tab.send({ type: "gonvex.cache.register", tabId: "tab-a" });
    tab.send({ type: "gonvex.cache.tab", tabId: "tab-a", tab: { hasLivenessLock: true, dedicatedWorkerReady: true } });
    await worker.handle(tab.port, {
      type: "gonvex.cache.request",
      id: "request-1",
      tabId: "tab-a",
      request: { id: "read", operation: "read", sql: "select 1" },
    });

    expect(tab.messages).toContainEqual({
      type: "gonvex.cache.response",
      id: "request-1",
      response: { ok: false, route: "server", reason: "db-not-healthy" },
    });
  });

  it("routes verified requests through exactly one active owner executor", async () => {
    const worker = new SharedWorkerCacheCoordinator();
    const tabA = port();
    const tabB = port();
    worker.attach(tabA.port);
    worker.attach(tabB.port);
    worker.setOwnerExecutor({
      async execute(request) {
        return { sql: request.sql, rows: [{ id: "1" }] };
      },
    });

    tabA.send({ type: "gonvex.cache.register", tabId: "tab-a" });
    tabB.send({ type: "gonvex.cache.register", tabId: "tab-b" });
    tabA.send({ type: "gonvex.cache.tab", tabId: "tab-a", tab: { hasLivenessLock: true, dedicatedWorkerReady: true } });
    tabB.send({ type: "gonvex.cache.tab", tabId: "tab-b", tab: { hasLivenessLock: true, dedicatedWorkerReady: true } });
    tabA.send({ type: "gonvex.cache.owner.ready", tabId: "tab-a" });

    await worker.handle(tabB.port, {
      type: "gonvex.cache.request",
      id: "request-2",
      tabId: "tab-b",
      request: { id: "read", operation: "read", sql: "select * from gonvex_shapes" },
    });

    expect(tabB.messages).toContainEqual({
      type: "gonvex.cache.response",
      id: "request-2",
      response: {
        ok: true,
        ownerTabId: "tab-a",
        value: { sql: "select * from gonvex_shapes", rows: [{ id: "1" }] },
      },
    });
  });

  it("falls back to server after active owner loss", async () => {
    const worker = new SharedWorkerCacheCoordinator();
    const tab = port();
    worker.attach(tab.port);
    tab.send({ type: "gonvex.cache.register", tabId: "tab-a" });
    tab.send({ type: "gonvex.cache.tab", tabId: "tab-a", tab: { hasLivenessLock: true, dedicatedWorkerReady: true } });
    tab.send({ type: "gonvex.cache.owner.ready", tabId: "tab-a" });
    tab.send({ type: "gonvex.cache.owner.lost", tabId: "tab-a" });

    await worker.handle(tab.port, {
      type: "gonvex.cache.request",
      id: "request-3",
      tabId: "tab-a",
      request: { id: "read", operation: "read", sql: "select 1" },
    });

    expect(tab.messages).toContainEqual({
      type: "gonvex.cache.response",
      id: "request-3",
      response: { ok: false, route: "server", reason: "db-not-healthy" },
    });
  });
});
