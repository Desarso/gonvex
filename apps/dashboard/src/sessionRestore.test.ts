import { describe, expect, it } from "vitest";

import { shouldRestoreDashboardCookieSession } from "./App";

describe("dashboard session restore", () => {
  it("does not probe the password-session cookie for native Google auth", () => {
    expect(shouldRestoreDashboardCookieSession({
      accessToken: "native-token",
      email: "gabriel@example.com",
      name: "Gabriel",
      provider: "google",
    })).toBe(false);
    expect(shouldRestoreDashboardCookieSession({
      accessToken: "password-token",
      email: "gabriel@example.com",
      name: "Gabriel",
      provider: "gonvex",
    })).toBe(true);
    expect(shouldRestoreDashboardCookieSession(null)).toBe(true);
  });
});
