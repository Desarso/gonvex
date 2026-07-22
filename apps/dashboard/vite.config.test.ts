import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { loadConfigFromFile } from "vite";

describe("dashboard Vite config", () => {
  it("deduplicates React across linked workspace packages", async () => {
    const loaded = await loadConfigFromFile(
      { command: "build", mode: "production" },
      resolve(process.cwd(), "vite.config.ts"),
    );

    expect(loaded?.config.resolve?.dedupe).toEqual(
      expect.arrayContaining(["react", "react-dom"]),
    );
  });
});
