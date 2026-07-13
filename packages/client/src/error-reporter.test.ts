import { afterEach, describe, expect, it, vi } from "vitest";
import { GonvexErrorReporter } from "./error-reporter";

describe("GonvexErrorReporter", () => {
  afterEach(() => vi.restoreAllMocks());

  it("registers error tracking for the configured project", () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);

    const reporter = new GonvexErrorReporter({ endpoint: "https://errors.test", project: "shop", captureGlobalErrors: false });

    expect(fetchMock).toHaveBeenCalledWith("https://errors.test/errors/register", expect.objectContaining({
      method: "POST",
      headers: expect.objectContaining({ "x-gonvex-project-id": "shop" }),
    }));

    reporter.setProject("admin");
    expect(fetchMock).toHaveBeenCalledWith("https://errors.test/errors/register", expect.objectContaining({
      headers: expect.objectContaining({ "x-gonvex-project-id": "admin" }),
    }));
  });

  it("batches errors with tenant, release and device context while filtering secrets", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);
    const reporter = new GonvexErrorReporter({ endpoint: "https://errors.test", project: "shop", tenant: "acme", release: "1.4.2", captureGlobalErrors: false });
    reporter.captureException(new Error("checkout failed"), { password: "nope", cartId: "cart-1" });
    await reporter.flush();
    const envelopeCall = fetchMock.mock.calls.find(([url]) => String(url).endsWith("/errors/envelope"));
    expect(envelopeCall).toBeTruthy();
    const body = JSON.parse(envelopeCall![1].body);
    expect(body.events[0]).toMatchObject({ project: "shop", tenant: "acme", release: "1.4.2", message: "checkout failed", context: { password: "[Filtered]", cartId: "cart-1" } });
    expect(body.events[0].deviceId).toBeTruthy();
    expect(envelopeCall![1].headers).toMatchObject({ "x-gonvex-project-id": "shop", "x-gonvex-tenant-id": "acme" });
  });

  it("requeues a failed batch", async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce({ ok: true }).mockRejectedValueOnce(new Error("offline")).mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);
    const reporter = new GonvexErrorReporter({ endpoint: "https://errors.test/", project: "shop", captureGlobalErrors: false });
    reporter.captureException("boom");
    await reporter.flush();
    await reporter.flush();
    expect(fetchMock.mock.calls.filter(([url]) => String(url).endsWith("/errors/envelope"))).toHaveLength(2);
  });
});
