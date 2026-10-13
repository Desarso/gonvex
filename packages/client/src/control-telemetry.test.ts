import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, control, type FunctionReference } from "./index";

type Listener = (event: { data?: string }) => void;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  static instances: FakeWebSocket[] = [];

  readonly sent: string[] = [];
  readyState = FakeWebSocket.CONNECTING;
  private readonly listeners = new Map<string, Array<{ listener: Listener; once: boolean }>>();

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: Listener, options?: { once?: boolean }) {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push({ listener, once: Boolean(options?.once) });
    this.listeners.set(type, listeners);
  }

  send(message: string) {
    this.sent.push(message);
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  disconnect() {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  receive(message: unknown) {
    this.emit("message", { data: typeof message === "string" ? message : JSON.stringify(message) });
  }

  private emit(type: string, event: { data?: string }) {
    const listeners = this.listeners.get(type) ?? [];
    this.listeners.set(type, listeners.filter((entry) => !entry.once));
    for (const entry of listeners) entry.listener(event);
  }
}


const clients: GonvexClient[] = [];
const directive = { protocolVersion: 1, scope: "p:t", visibilityScope: "visibility", epoch: "epoch" };
const reference: FunctionReference = { kind: "query", path: "tasks.list", delivery: "live",
  live: { entity: "tasks", key: "id", resultPath: [], plan: { table: "tasks", key: "id", columns: ["id"] } } };
const messages = (socket: FakeWebSocket) => socket.sent.map(value => JSON.parse(value));
const telemetry = (socket: FakeWebSocket) => messages(socket).filter(message => message.type.startsWith("telemetry."));
const tick = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
function client(options: ConstructorParameters<typeof GonvexClient>[1] = {}) {
  const result = new GonvexClient("ws://runtime.test/ws", options);
  clients.push(result);
  return result;
}
async function connect(client: GonvexClient, batch = true, replica = true) {
  client.connect();
  const socket = FakeWebSocket.instances.at(-1)!;
  socket.open();
  socket.receive({ type: "session.ready", capabilities: batch ? { telemetryBatch: 1 } : {} });
  const auth = messages(socket).find(message => message.type === "auth");
  if (auth) socket.receive({ type: "auth.result", id: auth.id, result: { accountId: "a", tenantId: "t", ...(replica ? { replica: directive } : {}) } });
  await tick();
  return socket;
}
async function reducer(client: GonvexClient, socket: FakeWebSocket) {
  const promise = client.reducer(control.tenants.updateTimezone, { timezone: "UTC" });
  await tick();
  const call = messages(socket).findLast(message => message.type === "reducer.call");
  socket.receive({ type: "reducer.result", id: call.id, result: null });
  await promise;
}
beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
});
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("control connections", () => {
  it.each([false, true])("ignores replica directives and generated local execution with directive=%s", async (replica) => {
    const create = vi.fn(() => { throw new Error("executor must stay cold"); });
    const storage = new MemoryLocalReplicaStorage();
    const load = vi.spyOn(storage, "load");
    const loadSession = vi.spyOn(storage, "loadSession");
    const saveSession = vi.spyOn(storage, "saveSession");
    const sdk = client({ role: "control", project: "p", tenant: "t", token: "session", localReplica: { storage }, localRuntime: {
      create, artifactHash: "hash", tables: ["tasks"], collections: [{ ...reference, replica: { table: "tasks", columns: ["id"], maxRows: 100 } }],
    } });
    const socket = await connect(sdk, true, replica);
    expect(messages(socket)[0]).toMatchObject({ type: "auth", tenant: "t", controlOnly: true, capabilities: { tenantControl: 1 } });
    const pending = sdk.query(control.accounts.me, {});
    await tick();
    const call = messages(socket).findLast(message => message.type === "query.call");
    expect(call).toMatchObject({ scope: "control" });
    socket.receive({ type: "query.result", id: call.id, result: null });
    await expect(pending).resolves.toBeNull();
    await reducer(sdk, socket);
    const action = sdk.action(control.auth.refreshSession, { refreshToken: "refresh" });
    await tick();
    const actionCall = messages(socket).findLast(message => message.type === "action.call");
    socket.receive({ type: "action.result", id: actionCall.id, result: null });
    await expect(action).resolves.toBeNull();
    expect(create).not.toHaveBeenCalled();
    expect(load).not.toHaveBeenCalled();
    expect(loadSession).not.toHaveBeenCalled();
    expect(saveSession).not.toHaveBeenCalled();
    expect(messages(socket).some(message => message.type.startsWith("replica."))).toBe(false);
    expect(() => sdk.subscribeReplica(reference, {}, () => {})).toThrow("Control connections");
  });
  it("settles tenant control authentication and resumes control subscriptions without a replica", async () => {
    const sdk = client({ role: "control", project: "p", tenant: "t", token: "session" });
    const socket = await connect(sdk, true, false);
    sdk.subscribeLiveQuery(control.tenants.mine, {}, () => {});
    await tick();
    const before = messages(socket).filter(message => message.type === "query.subscribe").length;
    const rotated = vi.fn(); sdk.onDeveloperCredential(rotated);
    const auth = sdk.authenticate({ token: "gvx_dev_grant", tenant: "other" });
    const frame = messages(socket).findLast(message => message.type === "auth");
    expect(frame).toMatchObject({ tenant: "other", controlOnly: true });
    socket.receive({ type: "auth.result", id: frame.id, result: { accountId: "target", tenantId: "other", developerSessionToken: "gvx_dev_rotated" } });
    await expect(auth).resolves.toBeUndefined();
    expect(rotated).toHaveBeenCalledWith("gvx_dev_rotated");
    expect(messages(socket).filter(message => message.type === "query.subscribe")).toHaveLength(before + 1);
    expect(messages(socket).some(message => message.type.startsWith("replica."))).toBe(false);
  });

});

describe("performance telemetry", () => {
  it("sends 50 events in one frame without repeating device info", async () => {
    vi.stubGlobal("navigator", { userAgent: "Chrome/126.0.0.0", language: "en-US" });
    const sdk = client({ project: "p", token: "session", telemetry: true });
    const local = vi.fn(); sdk.onTelemetry(local);
    const socket = await connect(sdk);
    for (let i = 0; i < 49; i++) await reducer(sdk, socket);
    expect(telemetry(socket)).toHaveLength(0);
    await reducer(sdk, socket);
    expect(telemetry(socket)).toHaveLength(1);
    expect(telemetry(socket)[0]).toMatchObject({ type: "telemetry.batch" });
    expect(telemetry(socket)[0].events).toHaveLength(50);
    expect(telemetry(socket)[0].events.every((event: any) => !event.device)).toBe(true);
    expect(local).toHaveBeenCalledTimes(50);
    expect(messages(socket).find(message => message.type === "auth").device).toMatchObject({ language: "en-US" });
    await vi.advanceTimersByTimeAsync(5_000);
    expect(telemetry(socket)).toHaveLength(1);
  });

  it("snapshots traces before delayed delivery", async () => {
    const sdk = client({ project: "p", token: "session", telemetry: true });
    const received: any[] = []; sdk.onTelemetry(event => received.push(event));
    const socket = await connect(sdk);
    const promise = sdk.reducer(control.tenants.updateTimezone, { timezone: "UTC" });
    await tick();
    const call = messages(socket).findLast(message => message.type === "reducer.call");
    socket.receive({ type: "reducer.result", id: call.id, result: null, trace: { serverCompletedAtMs: 123 } });
    await promise;
    received[0].serverTrace.serverCompletedAtMs = 999;
    await vi.advanceTimersByTimeAsync(5_000);
    expect(telemetry(socket)[0].events[0].trace).toEqual({ serverCompletedAtMs: 123 });
  });

  it("flushes after five seconds and on lifecycle events, and removes listeners at close", async () => {
    const page = new EventTarget(); const doc = new EventTarget();
    Object.assign(doc, { hidden: false });
    vi.stubGlobal("document", doc);
    vi.stubGlobal("addEventListener", page.addEventListener.bind(page));
    vi.stubGlobal("removeEventListener", page.removeEventListener.bind(page));
    const sdk = client({ project: "p", token: "session", telemetry: true });
    const socket = await connect(sdk);
    await reducer(sdk, socket);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(telemetry(socket)).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(telemetry(socket)).toHaveLength(1);
    await reducer(sdk, socket);
    doc.dispatchEvent(new Event("visibilitychange"));
    expect(telemetry(socket)).toHaveLength(1);
    Object.assign(doc, { hidden: true }); doc.dispatchEvent(new Event("visibilitychange"));
    expect(telemetry(socket)).toHaveLength(2);
    await reducer(sdk, socket); page.dispatchEvent(new Event("pagehide"));
    expect(telemetry(socket)).toHaveLength(3);
    await reducer(sdk, socket); sdk.close();
    expect(telemetry(socket)).toHaveLength(4);
    page.dispatchEvent(new Event("pagehide"));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(telemetry(socket)).toHaveLength(4);
  });

  it("falls back to single events and reuses cached device info on old runtimes", async () => {
    const userAgent = vi.fn(() => "Chrome/126.0.0.0");
    vi.stubGlobal("navigator", { get userAgent() { return userAgent(); } });
    const sdk = client({ project: "p", token: "session", telemetry: true });
    const socket = await connect(sdk, false);
    const reads = userAgent.mock.calls.length;
    await reducer(sdk, socket); await reducer(sdk, socket);
    expect(telemetry(socket).map(message => message.type)).toEqual(["telemetry.event", "telemetry.event"]);
    expect(telemetry(socket)[0].device).toBeDefined();
    expect(userAgent).toHaveBeenCalledTimes(reads);
  });

  it("keeps patch callbacks and state while reporting initial and full server results only", async () => {
    const sdk = client({ telemetry: true });
    const socket = await connect(sdk);
    const local = vi.fn(); sdk.onTelemetry(local);
    const updates = vi.fn(); sdk.subscribeLiveQuery(reference, {}, updates);
    const subscription = messages(socket).findLast(message => message.type === "query.subscribe");
    socket.receive({ type: "query.result", id: subscription.id, result: [{ id: "a", title: "old" }], reason: "initial", subscriptionRevision: { epoch: "r", sequence: 1 } });
    await tick();
    socket.receive({ type: "query.patch", id: subscription.id, updated: [{ id: "a", title: "new" }], reason: "change", subscriptionRevision: { epoch: "r", sequence: 2 }, baseRevision: { epoch: "r", sequence: 1 } });
    await tick();
    expect(local).toHaveBeenCalledTimes(2);
    expect(updates.mock.calls.at(-1)?.[0]).toMatchObject({ type: "query.result", result: [{ id: "a", title: "new" }] });
    socket.receive({ type: "query.result", id: subscription.id, result: [], reason: "change", subscriptionRevision: { epoch: "r", sequence: 3 } });
    await tick();
    socket.receive({ type: "query.progress", id: subscription.id, throughRevision: { epoch: "r", sequence: 4 } });
    await tick(); await vi.advanceTimersByTimeAsync(5_000);
    expect(telemetry(socket)[0].events).toHaveLength(2);
    expect(local).toHaveBeenCalledTimes(3);
  });

  it.each([true, false])("drops disconnected batches and renegotiates on reconnect with close notification=%s", async (notified) => {
    const sdk = client({ project: "p", token: "session", telemetry: true });
    const socket = await connect(sdk);
    await reducer(sdk, socket);
    if (notified) socket.disconnect();
    else socket.readyState = FakeWebSocket.CLOSED;
    sdk.connect();
    const replacement = FakeWebSocket.instances.at(-1)!;
    replacement.open();
    replacement.receive({ type: "session.ready", capabilities: {} });
    const auth = messages(replacement).find(message => message.type === "auth");
    replacement.receive({ type: "auth.result", id: auth.id, result: { accountId: "a", tenantId: "" } });
    await tick();
    await reducer(sdk, replacement);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(telemetry(socket)).toHaveLength(0);
    expect(telemetry(replacement).map(message => message.type)).toEqual(["telemetry.event"]);
  });

  it("flushes under the old identity before auth changes", async () => {
    const sdk = client({ project: "p", token: "session", telemetry: true });
    const socket = await connect(sdk);
    await reducer(sdk, socket);
    sdk.setAuth({ token: "new-session", tenant: "other" });
    const frames = messages(socket);
    expect(frames.findLastIndex(frame => frame.type === "telemetry.batch")).toBeLessThan(frames.findLastIndex(frame => frame.type === "auth"));
  });
});
