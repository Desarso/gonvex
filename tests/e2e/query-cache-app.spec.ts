import { createHash } from "node:crypto";
import { expect, test } from "@playwright/test";

function testScope(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

async function control(channel: string, options: { value?: string; delayMs?: number; broadcast?: boolean }) {
  const response = await fetch("http://127.0.0.1:4180/control", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ channel, ...options }),
  });
  if (!response.ok) throw new Error(`cache test control failed: ${response.status}`);
}

test.describe("transparent query result cache", () => {
  test("paints a warm snapshot before silently replacing it from the server", async ({ page }, testInfo) => {
    const channel = `warm-${testInfo.workerIndex}-${Date.now()}`;
    const scope = testScope(channel);
    await control(channel, { value: "cached-seed", delayMs: 0 });
    await page.goto(`/?channel=${channel}&scope=${scope}`);
    await expect(page.getByTestId("query-value")).toHaveText("cached-seed");
    await page.waitForTimeout(150);

    await control(channel, { value: "fresh-server", delayMs: 700 });
    await page.reload();
    await expect(page.getByTestId("query-value")).toHaveText("cached-seed", { timeout: 400 });
    await expect(page.getByTestId("query-value")).toHaveText("fresh-server", { timeout: 2_000 });
    await expect.poll(() => page.evaluate(() => window.__gonvexValues)).toEqual(["cached-seed", "fresh-server"]);
  });

  test("persists invalidation results for the next reload", async ({ page }, testInfo) => {
    const channel = `invalidate-${testInfo.workerIndex}-${Date.now()}`;
    const scope = testScope(channel);
    await control(channel, { value: "initial", delayMs: 0 });
    await page.goto(`/?channel=${channel}&scope=${scope}`);
    await expect(page.getByTestId("query-value")).toHaveText("initial");

    await control(channel, { value: "invalidated", broadcast: true });
    await expect(page.getByTestId("query-value")).toHaveText("invalidated");
    await page.waitForTimeout(150);

    await control(channel, { value: "after-reload", delayMs: 700 });
    await page.reload();
    await expect(page.getByTestId("query-value")).toHaveText("invalidated", { timeout: 400 });
    await expect(page.getByTestId("query-value")).toHaveText("after-reload", { timeout: 2_000 });
  });

  test("never replays a snapshot from another confirmed scope", async ({ page }, testInfo) => {
    const channel = `scope-${testInfo.workerIndex}-${Date.now()}`;
    const firstScope = testScope(`${channel}-a`);
    const secondScope = testScope(`${channel}-b`);
    await control(channel, { value: "scope-a-private", delayMs: 0 });
    await page.goto(`/?channel=${channel}&scope=${firstScope}`);
    await expect(page.getByTestId("query-value")).toHaveText("scope-a-private");
    await page.waitForTimeout(150);

    await control(channel, { value: "scope-b", delayMs: 700 });
    await page.goto(`/?channel=${channel}&scope=${secondScope}`);
    await expect(page.getByTestId("query-value")).toHaveText("loading", { timeout: 400 });
    await expect(page.getByTestId("query-value")).toHaveText("scope-b", { timeout: 2_000 });
    expect(await page.evaluate(() => window.__gonvexValues)).toEqual(["scope-b"]);
  });
});
