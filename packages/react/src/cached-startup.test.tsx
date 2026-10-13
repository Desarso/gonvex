import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage } from "@gonvex/client";
import { GonvexAuthProvider, useGonvexAuth, useReplicaEntities } from "./index.js";

class Socket {
  static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
  static all: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.all.push(this); }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]); }
  send(value: string) { this.sent.push(JSON.parse(value)); }
  emit(type: string, event: any = {}) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
  open() { this.readyState = 1; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(value: unknown) { this.emit("message", { data: JSON.stringify(value) }); }
}
const runtimeUrl = "https://cached-render.test";
const url = "wss://cached-render.test/ws";
const storageKey = "gonvex-auth:https%3A%2F%2Fcached-render.test:project";
const ids = ["task"];
const clients: GonvexClient[] = [];
const session = () => ({
  accessToken: "access", expiresAt: Date.now() + 900_000,
  refreshToken: "refresh", refreshExpiresAt: Date.now() + 86_400_000,
  account: { id: "account", email: "a@example.test", emailVerified: true, provider: "password" },
  tenants: [{ id: "tenant", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
  activeTenantId: "tenant",
});
let currentAuth: ReturnType<typeof useGonvexAuth>;
function CachedApp() {
  const auth = useGonvexAuth();
  currentAuth = auth;
  const rows = useReplicaEntities("tasks", ids);
  return <div data-testid="app">{auth.isAuthenticated ? `${auth.sessionState}:${rows[0]?.title ?? "empty"}` : "signedOut"}</div>;
}
async function fixture() {
  const storage = new MemoryLocalReplicaStorage();
  const owner = ["identity", url, "project", "tenant", "project", "account"].join("\u0000");
  await storage.saveSession(owner, {
    directive: { protocolVersion: 1, scope: "tenant", visibilityScope: "visible", epoch: "epoch" },
    identity: { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", permissions: {} } },
    artifactHash: "artifact",
  });
  await storage.replaceSnapshot({ entities: { tasks: { task: { _id: "task", title: "cached task" } } }, liveQueries: {} }, "visible");
  localStorage.setItem(storageKey, JSON.stringify(session()));
  const client = new GonvexClient(url, { project: "project", localReplica: { storage } });
  clients.push(client);
  const rendered = render(<GonvexAuthProvider client={client} runtimeUrl={runtimeUrl} projectId="project"><CachedApp /></GonvexAuthProvider>);
  await act(async () => { await client.localSessionReady; });
  act(() => { vi.advanceTimersToNextFrame(); });
  return { client, rendered, storage, owner };
}
beforeEach(() => { Socket.all = []; vi.useFakeTimers(); vi.stubGlobal("WebSocket", Socket); localStorage.clear(); });
afterEach(() => { cleanup(); clients.splice(0).forEach(client => client.close()); vi.useRealTimers(); vi.unstubAllGlobals(); });

it("renders persisted rows while offline, then signs out and hides them on terminal rejection", async () => {
  const { client, rendered } = await fixture();
  expect(rendered.getByTestId("app").textContent).toBe("reconnecting:cached task");
  expect(Socket.all.every(socket => socket.sent.length === 0)).toBe(true);
  client.setAuth({ fetchToken: undefined });
  const socket = Socket.all.at(-1)!;
  await act(async () => { socket.open(); });
  const auth = socket.sent.find(message => message.type === "auth");
  expect(auth).toBeDefined();
  act(() => { socket.receive({ type: "auth.error", id: auth.id, error: "revoked" }); vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("signedOut");
  expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  expect(localStorage.getItem(storageKey)).toBeNull();
});

it("removes the previous account's rows immediately when the canonical account switches", async () => {
  const { client, rendered } = await fixture();
  expect(rendered.getByTestId("app").textContent).toContain("cached task");
  const next = { ...session(), accessToken: "other-access", account: { ...session().account, id: "other" } };
  localStorage.setItem(storageKey, JSON.stringify(next));
  await act(async () => {
    window.dispatchEvent(new StorageEvent("storage", { key: storageKey, newValue: JSON.stringify(next) }));
    await client.localSessionReady;
  });
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  expect(rendered.getByTestId("app").textContent).not.toContain("cached task");
});


it("forgets cached local authority on explicit provider sign-out", async () => {
  const { client, rendered, storage, owner } = await fixture();
  expect(rendered.getByTestId("app").textContent).toContain("cached task");
  await act(async () => { await currentAuth.signOut(); });
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("signedOut");
  expect(await storage.loadSession(owner)).toBeUndefined();
  await expect(client.restoreSession({ project: "project", tenant: "tenant", token: "access", identity: { sub: "account", iss: "project" } })).resolves.toBe(false);
  expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
});
