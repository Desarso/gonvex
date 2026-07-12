import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App, dashboardEmailAllowed, googleLoginEnabled, parseEmailAllowlist } from "./App";

async function renderProjectApp() {
  const user = userEvent.setup();
  window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
  render(<App />);
  await user.click(screen.getByRole("button", { name: /open project/i }));
  return user;
}

describe("App", () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
  });

  it("signs in to the project list", async () => {
    const user = userEvent.setup();
    render(<App />);

    expect(screen.getByRole("heading", { name: /welcome back/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /sign in with google/i })).not.toBeInTheDocument();
    expect(window.location.pathname).toBe("/login");

    await user.type(screen.getByLabelText(/email/i), "gabriel@example.com");
    await user.click(screen.getByRole("button", { name: /^continue$/i }));

    expect(screen.getByRole("heading", { name: /choose a project/i })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/projects");
    expect(screen.getByRole("button", { name: /open project/i })).toBeInTheDocument();
  });

  it("redirects protected URLs to login", () => {
    window.history.replaceState(null, "", "/settings");
    render(<App />);

    expect(screen.getByRole("heading", { name: /welcome back/i })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/login");
  });

  it("normalizes dashboard email allowlists", () => {
    expect(parseEmailAllowlist(" Gabriel@Example.com, gabriel@example.com ; admin@example.com\n")).toEqual([
      "gabriel@example.com",
      "admin@example.com",
    ]);
  });

  it("checks dashboard email allowlists case-insensitively", () => {
    const allowlist = parseEmailAllowlist("gabriel@example.com");

    expect(dashboardEmailAllowed(" Gabriel@Example.com ", allowlist, false)).toBe(true);
    expect(dashboardEmailAllowed("other@example.com", allowlist, false)).toBe(false);
    expect(dashboardEmailAllowed("other@example.com", [], true)).toBe(true);
    expect(dashboardEmailAllowed("other@example.com", [], false)).toBe(false);
  });

  it("keeps Google login disabled unless explicitly enabled", () => {
    expect(googleLoginEnabled(undefined, true)).toBe(false);
    expect(googleLoginEnabled("false", true)).toBe(false);
    expect(googleLoginEnabled("true", false)).toBe(false);
    expect(googleLoginEnabled("true", true)).toBe(true);
  });

  it("creates a runtime project card", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn().mockResolvedValue({
      json: async () => ({
        project: {
          id: "acme-app",
          name: "Acme App",
          environment: "local dev",
          database: "gonvex_acme_app",
          storageBucket: "acme-app-dev",
          status: "local",
          description: "Runtime-created project database.",
          provisioned: true,
          runtimeCreated: true,
          databaseMode: "single",
          testTab: false,
        },
      }),
      ok: true,
      statusText: "OK",
    });
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
    render(<App />);

    await user.click(screen.getByRole("button", { name: /create project/i }));
    const dialog = screen.getByRole("dialog", { name: /create project/i });
    expect(within(dialog).getByText(/database structure/i)).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: /single project database/i })).toBeInTheDocument();
    await user.type(within(dialog).getByLabelText(/name/i), "Acme App");
    await user.click(within(dialog).getByRole("button", { name: /create project/i }));

    expect(screen.getByText("Acme App")).toBeInTheDocument();
    expect(JSON.parse(window.localStorage.getItem("gonvex-dashboard-database-modes") ?? "{}")).toEqual({
      "acme-app": "single",
    });
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body ?? "{}"))).toMatchObject({ databaseMode: "single", testTab: false });
    expect(screen.queryByRole("button", { name: /^test$/i })).not.toBeInTheDocument();
    vi.unstubAllGlobals();
  });

  it("renders the dashboard overview", async () => {
    await renderProjectApp();

    expect(window.location.pathname).toBe("/projects/app/overview");
    expect(screen.getByRole("region", { name: /runtime summary/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /^functions$/i })).toBeInTheDocument();
  });

  it("hides the task-grid test surface for existing projects", async () => {
    await renderProjectApp();

    expect(within(screen.getByLabelText("Primary sections")).queryByRole("button", { name: /^test$/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /livegrid test surface/i })).not.toBeInTheDocument();
  });

  it("renders an accessible React Aria button", async () => {
    const user = await renderProjectApp();

    const button = screen.getByRole("button", { name: /light mode/i });
    await user.click(button);

    expect(button).toBeInTheDocument();
  });

  it("switches pages from the sidebar", async () => {
    const user = await renderProjectApp();

    await user.click(screen.getByRole("button", { name: /data/i }));
    expect(screen.getByRole("heading", { name: /no tables/i })).toBeInTheDocument();
    expect(screen.getByLabelText("Tables", { selector: "aside" })).toBeInTheDocument();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));
    expect(screen.getByRole("heading", { name: /file storage/i })).toBeInTheDocument();
  });

  it("shows real file storage shell without fake rows", async () => {
    const user = await renderProjectApp();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));

    expect(screen.getByRole("table", { name: /stored files/i })).toBeInTheDocument();
    expect(screen.getByText(/no files yet/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /column settings/i })).not.toBeDisabled();
    expect(screen.queryByText(/kg27zrhezshg/i)).not.toBeInTheDocument();
  });

  it("expands a grouped error with tenant, release, and machine context", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).includes("/dev/errors/groups")) {
        return { ok: true, statusText: "OK", json: async () => ({ groups: [{
          fingerprint: "group-1", title: "Checkout failed", culprit: "at submitOrder (src/checkout.ts:40:3)", status: "unresolved", priority: "high",
          count: 8, firstSeen: "2026-07-11T10:00:00Z", lastSeen: "2026-07-12T10:00:00Z", tenants: { acme: 5, beta: 3 }, users: { ada: 4 }, devices: { laptop: 5, phone: 3 }, releases: { "5.1.0": 8 }, environments: { production: 8 },
          latest: { timestamp: "2026-07-12T10:00:00Z", stack: "TypeError: Checkout failed\n at submitOrder (src/checkout.ts:40:3)", tenant: "acme", release: "5.1.0", environment: "production", url: "https://acme.example.com/checkout", userAgent: "Chrome" },
        }] }) } as Response;
      }
      return { ok: true, statusText: "OK", json: async () => ({}) } as Response;
    }));
    const user = await renderProjectApp();
    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /^errors$/i }));

    expect(await screen.findByText("Checkout failed")).toBeInTheDocument();
    expect(screen.getByText("2", { selector: ".errors-stat-strip strong" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /checkout failed/i }));
    expect(screen.getByText(/latest exception/i)).toBeInTheDocument();
    expect(screen.getByText("5.1.0", { selector: ".error-detail-heading strong" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /copy agent brief/i })).toBeInTheDocument();
  });

  it("shows settings sections", async () => {
    const user = await renderProjectApp();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /settings/i }));
    expect(screen.getByRole("button", { name: /general/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /connection/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /environment variables/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /authentication/i })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /connection/i }));
    expect(screen.getByText(/x-gonvex-project-id: app/i)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /authentication/i }));
    expect(screen.getByText(/authentication providers configured/i)).toBeInTheDocument();
  });

  it("does not show schema before the project pushes tables", async () => {
    const user = await renderProjectApp();

    await user.click(screen.getByRole("button", { name: /data/i }));

    const tables = screen.getByLabelText("Tables");
    expect(within(tables).queryByRole("button", { name: /tasks/i })).not.toBeInTheDocument();
    expect(within(tables).queryByRole("button", { name: /files/i })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /no tables/i })).toBeInTheDocument();
    expect(screen.getByText(/connect a project and push its schema/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/active database/i)).toHaveTextContent(/gonvex_dev/i);
    expect(screen.queryByText(/^landlord$/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /\+ tenant db/i })).not.toBeInTheDocument();
  });
});
