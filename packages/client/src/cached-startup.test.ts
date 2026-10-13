import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, type GonvexClientAuth, type ReplicaSnapshot } from "./index.js";

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
const url = "ws://startup.test";
const auth: GonvexClientAuth = { project: "project", tenant: "tenant", token: "opaque-session", identity: { sub: "account", iss: "project" } };
const owner = ["identity", url, "project", "tenant", "project", "account"].join("\u0000");
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "epoch" };
const identity = { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", permissions: {} } };
const clients: GonvexClient[] = [];
class Storage extends MemoryLocalReplicaStorage {
  loadWorkingSet = vi.fn((scope: string) => this.load(scope));
}
async function fixture(initialAuth = auth) {
  const storage = new Storage();
  await storage.saveSession(owner, { directive, identity, artifactHash: "artifact" });
  await storage.replaceSnapshot({ entities: { tasks: { task: { _id: "task", title: "cached" } } }, liveQueries: {} }, "visible");
  const client = new GonvexClient(url, { ...initialAuth, localReplica: { storage } });
  clients.push(client);
  return { storage, client };
}
function socketFor(client: GonvexClient) {
  client.connect();
  const socket = Socket.all.at(-1)!;
  socket.open();
  return socket;
}
beforeEach(() => { Socket.all = []; vi.stubGlobal("WebSocket", Socket); });
afterEach(() => { clients.splice(0).forEach(client => client.close()); vi.unstubAllGlobals(); });

describe("cached session startup", () => {
  it("reads cached data offline without a local reducer runtime and loads only the real scope", async () => {
    const { client, storage } = await fixture();
    await expect(client.localSessionReady).resolves.toBe(true);
    expect(client.localSessionState()).toBe("restored");
    expect(client.localReplica.entity("tasks", "task")).toEqual({ _id: "task", title: "cached" });
    expect(client.localReplica.freshness()).toBe("offline");
    expect(storage.loadWorkingSet.mock.calls.map(([scope]) => scope)).toEqual(["visible"]);
    expect(Socket.all).toHaveLength(0);
  });

  it("keeps authenticate server-only while local readiness resolves before auth and transitions online", async () => {
    const { client, storage } = await fixture();
    const states: string[] = [];
    client.onLocalSessionChange(() => states.push(client.localSessionState()));
    let accepted = false;
    const pending = client.authenticate(auth).then(() => { accepted = true; });
    await expect(client.localSessionReady).resolves.toBe(true);
    expect(accepted).toBe(false);
    const socket = socketFor(client);
    const watch = client.watchReplica({ kind: "query", path: "tasks.rows", delivery: "replica", replica: { table: "tasks", key: "_id", columns: ["_id", "title"] } }, {});
    const unsubscribe = watch.onUpdate(() => undefined);
    expect(socket.sent.map(message => message.type)).toEqual(["auth"]);
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    await pending;
    expect(client.localSessionState()).toBe("authenticated");
    expect(states).toEqual(["restored", "authenticated"]);
    expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1);
    expect(socket.sent.some(message => message.type === "replica.open")).toBe(true);
    unsubscribe();
  });

  it("finishes accepted auth when a refresh callback is attached during hydration", async () => {
    const { client, storage } = await fixture({ ...auth, tenant: "new-tenant" });
    await client.localSessionReady;
    let release: () => void = () => undefined;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { release = () => resolve(undefined); }));
    const accepted = client.authenticate({ ...auth, tenant: "new-tenant" });
    const socket = socketFor(client);
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    await vi.waitFor(() => expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1));
    client.setAuth({ fetchToken: async () => "opaque-session" });
    release();
    await accepted;
    expect(client.localSessionState()).toBe("authenticated");
  });

  it("starts normally without a saved session and persists read-only sessions after acceptance", async () => {
    const { client, storage } = await fixture({ ...auth, tenant: "new-tenant" });
    await expect(client.localSessionReady).resolves.toBe(false);
    const pending = client.authenticate({ ...auth, tenant: "new-tenant" });
    const socket = socketFor(client);
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: { ...directive, visibilityScope: "new-visible" }, localIdentity: identity } });
    await pending;
    expect(client.localSessionState()).toBe("authenticated");
    expect(storage.loadWorkingSet.mock.calls.map(([scope]) => scope)).toEqual(["new-visible"]);
    const nextOwner = owner.replace("\u0000tenant\u0000", "\u0000new-tenant\u0000");
    expect(await storage.loadSession(nextOwner)).toBeDefined();
  });

  it("quarantines the replica and forgets restored authority after a server rejection", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "revoked" });
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    await expect(client.localSessionReady).resolves.toBe(false);
    expect(rejected).toHaveBeenCalledWith("revoked");
    expect(await storage.loadSession(owner)).toBeUndefined();
    expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1);
  });

  it("does not expose the previous account or tenant while switching", async () => {
    const { client } = await fixture();
    await client.localSessionReady;
    const pending = client.restoreSession({ ...auth, identity: { sub: "other", iss: "project" } });
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    await expect(pending).resolves.toBe(false);
    expect(client.localSessionState()).toBe("unavailable");
    await expect(client.restoreSession(auth)).resolves.toBe(true);
    await expect(client.restoreSession({ ...auth, tenant: "other-tenant" })).resolves.toBe(false);
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  });

  it("does not restore either account when token claims conflict with the account hint", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    await storage.saveSession(owner.replace("\u0000account", "\u0000other"), { directive, identity, artifactHash: "artifact" });
    const token = `header.${btoa(JSON.stringify({ sub: "other", iss: "project" }))}.signature`;
    await expect(client.restoreSession({ ...auth, token })).resolves.toBe(false);
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  });

  it("drops the old JWT account immediately when its identity hint switches accounts", async () => {
    const token = `header.${btoa(JSON.stringify({ sub: "account", iss: "project" }))}.signature`;
    const { client } = await fixture({ ...auth, token });
    await expect(client.localSessionReady).resolves.toBe(true);
    const ready = client.restoreSession({ identity: { sub: "other", iss: "project" } });
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    await expect(ready).resolves.toBe(false);
  });

  it("sign-out forgets the saved directive and never restores the prior account", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    await expect(client.restoreSession({ token: undefined, identity: undefined, fetchToken: undefined, tenant: undefined })).resolves.toBe(false);
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    expect(await storage.loadSession(owner)).toBeUndefined();
    await expect(client.restoreSession(auth)).resolves.toBe(false);
  });

  it("ignores a delayed hydration that completes after rejection", async () => {
    const { client, storage } = await fixture();
    let finish: (snapshot: ReplicaSnapshot | undefined) => void = () => undefined;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const ready = client.localSessionReady;
    await vi.waitFor(() => expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1));
    const snapshot = await storage.load("visible");
    const socket = socketFor(client);
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "revoked" });
    finish(snapshot);
    await expect(ready).resolves.toBe(false);
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  });
});
