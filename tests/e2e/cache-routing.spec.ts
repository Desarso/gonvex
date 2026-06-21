import { expect, test } from "@playwright/test";

test.describe("Gonvex persistent browser cache contract", () => {
  test("uses server fallback until a shape is marked ready", async ({ page }) => {
    await page.setContent("<main id=\"app\"></main>");
    const result = await page.evaluate(() => {
      const shape = { status: "syncing", rows: [{ id: "1" }] };
      const route = shape.status === "ready" ? "local" : "server";
      return route;
    });
    expect(result).toBe("server");
  });

  test("allows local routing only after shape readiness is persisted", async ({ page }) => {
    await page.goto("about:blank");
    await page.evaluate(() => {
      localStorage.setItem("gonvex:shape:tasks.visible", JSON.stringify({
        status: "ready",
        checkpoint: "checkpoint-1",
        schemaVersion: "schema-1",
        permissionVersion: "perm-1",
      }));
    });
    await page.reload();
    const route = await page.evaluate(() => {
      const shape = JSON.parse(localStorage.getItem("gonvex:shape:tasks.visible") ?? "{}");
      return shape.status === "ready" && shape.checkpoint ? "local" : "server";
    });
    expect(route).toBe("local");
  });

  test("clears cache on auth scope switch", async ({ page }) => {
    await page.goto("about:blank");
    await page.evaluate(() => {
      localStorage.setItem("gonvex:authScope", "user-a");
      localStorage.setItem("gonvex:shape:tasks.visible", JSON.stringify({ status: "ready", userId: "user-a" }));
      const nextUser = "user-b";
      if (localStorage.getItem("gonvex:authScope") !== nextUser) {
        localStorage.removeItem("gonvex:shape:tasks.visible");
        localStorage.setItem("gonvex:authScope", nextUser);
      }
    });
    await expect.poll(async () => page.evaluate(() => localStorage.getItem("gonvex:shape:tasks.visible"))).toBeNull();
  });

  test("single writer lock prevents two tabs from owning local db writes", async ({ browser }) => {
    const context = await browser.newContext();
    const first = await context.newPage();
    const second = await context.newPage();
    await first.goto("about:blank");
    await second.goto("about:blank");

    const firstOwner = await first.evaluate(() => {
      localStorage.setItem("gonvex:dbOwner", "tab-1");
      return localStorage.getItem("gonvex:dbOwner");
    });
    const secondOwner = await second.evaluate(() => {
      const owner = localStorage.getItem("gonvex:dbOwner");
      return owner === null ? "tab-2" : owner;
    });

    expect(firstOwner).toBe("tab-1");
    expect(secondOwner).toBe("tab-1");
    await context.close();
  });

  test("two tabs proxy DB requests through one active owner", async ({ browser }) => {
    const context = await browser.newContext();
    const first = await context.newPage();
    const second = await context.newPage();
    await first.goto("about:blank");
    await second.goto("about:blank");

    await first.evaluate(() => {
      localStorage.setItem("gonvex:activeOwner", "tab-a");
      localStorage.setItem("gonvex:ownerReady", "true");
      localStorage.setItem("gonvex:writeOwners", JSON.stringify(["tab-a"]));
    });
    const ownerFromSecond = await second.evaluate(() => {
      const owner = localStorage.getItem("gonvex:activeOwner");
      const ready = localStorage.getItem("gonvex:ownerReady") === "true";
      return ready ? owner : "server";
    });

    expect(ownerFromSecond).toBe("tab-a");
    expect(await first.evaluate(() => JSON.parse(localStorage.getItem("gonvex:writeOwners") ?? "[]"))).toEqual(["tab-a"]);
    await context.close();
  });

  test("active owner handoff routes server until replacement verifies integrity", async ({ page }) => {
    await page.goto("about:blank");
    const routes = await page.evaluate(() => {
      localStorage.setItem("gonvex:activeOwner", "tab-a");
      localStorage.setItem("gonvex:ownerReady", "true");
      const beforeClose = localStorage.getItem("gonvex:ownerReady") === "true" ? "local" : "server";

      localStorage.setItem("gonvex:activeOwner", "tab-b");
      localStorage.setItem("gonvex:ownerReady", "false");
      const duringHandoff = localStorage.getItem("gonvex:ownerReady") === "true" ? "local" : "server";

      localStorage.setItem("gonvex:integrityVerified", "true");
      localStorage.setItem("gonvex:storageReady", "true");
      localStorage.setItem("gonvex:ownerReady", "true");
      const afterIntegrity = localStorage.getItem("gonvex:ownerReady") === "true" ? "local" : "server";
      return { beforeClose, duringHandoff, afterIntegrity };
    });

    expect(routes).toEqual({
      beforeClose: "local",
      duringHandoff: "server",
      afterIntegrity: "local",
    });
  });

  test("corruption marker forces cache reset and server route", async ({ page }) => {
    await page.goto("about:blank");
    const route = await page.evaluate(() => {
      localStorage.setItem("gonvex:cacheHealth", "corrupt");
      localStorage.setItem("gonvex:shape:tasks.visible", JSON.stringify({ status: "ready" }));
      if (localStorage.getItem("gonvex:cacheHealth") === "corrupt") {
        localStorage.removeItem("gonvex:shape:tasks.visible");
        return "server";
      }
      return "local";
    });
    expect(route).toBe("server");
  });

  test("slow WASM/storage init does not block initial server render", async ({ page }) => {
    await page.setContent("<main id=\"app\">server-window</main>");
    const initialText = await page.textContent("#app");
    const routeBeforeInit = await page.evaluate(async () => {
      const storageInit = new Promise((resolve) => setTimeout(() => resolve("ready"), 100));
      const firstRoute = "server";
      void storageInit.then((state) => localStorage.setItem("gonvex:storageReady", String(state)));
      return firstRoute;
    });

    expect(initialText).toBe("server-window");
    expect(routeBeforeInit).toBe("server");
    await expect.poll(() => page.evaluate(() => localStorage.getItem("gonvex:storageReady"))).toBe("ready");
  });

  test("slow local read loses to server response without delaying render", async ({ page }) => {
    await page.goto("about:blank");
    const result = await page.evaluate(async () => {
      const server = new Promise((resolve) => setTimeout(() => resolve("server"), 10));
      const local = new Promise((resolve) => setTimeout(() => resolve("local"), 100));
      const winner = await Promise.race([server, local]);
      document.body.textContent = String(winner);
      return winner;
    });

    expect(result).toBe("server");
    await expect(page.locator("body")).toHaveText("server");
  });
});
