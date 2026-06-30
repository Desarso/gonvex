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
    expect(
      screen.getByRole("heading", { name: /realtime control room/i }),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/watch the local project runtime/i),
    ).toBeInTheDocument();
  });

  it("hides the task-grid test surface for existing projects", async () => {
    await renderProjectApp();

    expect(within(screen.getByLabelText("Primary sections")).queryByRole("button", { name: /^test$/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /livegrid test surface/i })).not.toBeInTheDocument();
  });

  it("renders an accessible React Aria button", async () => {
    const user = await renderProjectApp();

    const button = screen.getByRole("button", { name: /ready for tests/i });
    await user.click(button);

    expect(button).toBeInTheDocument();
  });

  it("switches pages from the sidebar", async () => {
    const user = await renderProjectApp();

    await user.click(screen.getByRole("button", { name: /data/i }));
    expect(screen.getByRole("heading", { name: /postgres project schema/i })).toBeInTheDocument();
    expect(screen.getByText(/tables, columns, and indexes/i)).toBeInTheDocument();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));
    expect(screen.getByRole("heading", { name: /file storage/i })).toBeInTheDocument();
  });

  it("shows real file storage shell without fake rows", async () => {
    const user = await renderProjectApp();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));

    expect(screen.getByRole("table", { name: /stored files/i })).toBeInTheDocument();
    expect(screen.getByText(/no files yet/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /dashboard lab/i })).not.toBeDisabled();
    expect(screen.queryByText(/kg27zrhezshg/i)).not.toBeInTheDocument();
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
