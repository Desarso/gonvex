import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  fullyParallel: true,
  webServer: [
    {
      command: "node tests/support/query-cache-server.mjs",
      url: "http://127.0.0.1:4180/health",
      reuseExistingServer: true,
    },
    {
      command: "pnpm --filter @gonvex/query-cache-test-app dev --host 127.0.0.1 --port 4173",
      url: "http://127.0.0.1:4173",
      reuseExistingServer: true,
    },
  ],
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
