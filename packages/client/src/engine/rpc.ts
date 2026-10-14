import { setEngineProperty } from "./patch.js";
import { GonvexClientError } from "../client-error.js";
export interface EngineEndpoint {
  postMessage(message: unknown): void;
  addEventListener(type: "message" | "error" | "messageerror", listener: (event: any) => void): void;
  removeEventListener(type: "message" | "error" | "messageerror", listener: (event: any) => void): void;
  start?(): void;
  close?(): void;
}

export type EngineEnvelope = { generation: string; scope: string; id: string; method: string; args: unknown[] };
export function serializeEngineError(error: unknown) {
  return { name: error instanceof Error ? error.name : "Error", message: error instanceof Error ? error.message : String(error),
    ...(error instanceof GonvexClientError ? { code: error.code, path: error.path, operation: error.operation, errorClass: error.errorClass, retryable: error.retryable } : {}) };
}
export function deserializeEngineError(value: ReturnType<typeof serializeEngineError>): Error {
  const error = value.code ? new GonvexClientError(value.message, { ...value, code: value.code }) : new Error(value.message);
  error.name = value.name; return error;
}
export type EngineReply = { generation: string; scope: string; id: string; result?: unknown; error?: { name: string; message: string } };

export class EngineRpcTimeout extends GonvexClientError {
  constructor(readonly request: EngineEnvelope) { super(`Engine RPC timed out: ${request.method}`, { code: "timeout" }); this.name = "EngineRpcTimeout"; Object.defineProperty(this, "request", { value: request, enumerable: false }); }
}
export class EngineRpcUnavailable extends GonvexClientError {
  constructor(readonly request: EngineEnvelope, cause: Error) { super(cause.message, { code: cause instanceof GonvexClientError ? cause.code : "closed" }); this.name = "EngineRpcUnavailable"; Object.defineProperty(this, "request", { value: request, enumerable: false }); }
}

/** A timeout drops the waiter, never the durable command identity. */
export class EngineRpc {
  private readonly pending = new Map<string, { resolve(value: any): void; reject(error: Error): void; request: EngineEnvelope; timer?: ReturnType<typeof setTimeout> }>();
  private readonly chunks = new Map<string, any>();
  private closed = false;
  private admissionBatching = false;
  private admissionBatchLimit = 128;
  setAdmissionBatching(enabled: boolean, limit = 128) {
    this.admissionBatching = enabled;
    this.admissionBatchLimit = Number.isSafeInteger(limit) && limit >= 1 && limit <= 1024 ? limit : 128;
  }
  private admissions: EngineEnvelope[] = [];
  private admissionScheduled = false;
  private flushAdmissions() {
    this.admissionScheduled = false;
    const requests = this.admissions.splice(0).filter(request => this.pending.has(request.id));
    for (let offset = 0; offset < requests.length; offset += this.admissionBatchLimit) {
      this.endpoint.postMessage({ admissions: requests.slice(offset, offset + this.admissionBatchLimit) });
    }
  }
  readonly onCrash = new Set<(error: Error) => void>();
  readonly onEvent = new Set<(method: string, args: any[]) => void>();
  private readonly receive = ({ data }: MessageEvent) => {
    if (data?.generation !== this.generation || data?.scope !== this.scope) return;
    if (data.deliveryId) this.endpoint.postMessage({ generation: this.generation, scope: this.scope, deliveryAck: data.deliveryId });
    if (data.packet) {
      const packet = data.packet;
      if (packet.kind === "textBegin") this.chunks.set(data.id, []);
      else if (packet.kind === "text") this.chunks.get(data.id).push(packet.value);
      else if (packet.kind === "textEnd") {
        const decoded = JSON.parse(this.chunks.get(data.id).join("")); this.chunks.delete(data.id); data = decoded.payload;
        for (const path of decoded.undefinedPaths) { let parent = data; for (const key of path.slice(0, -1)) parent = parent[key]; setEngineProperty(parent, path[path.length - 1], undefined); }
      }
      else if (packet.kind === "begin") this.chunks.set(data.id, packet.value);
      else if (packet.kind === "end") { data = this.chunks.get(data.id); this.chunks.delete(data.id); }
      else {
        const value = this.chunks.get(data.id);
        let parent = value;
        for (const key of packet.path.slice(0, -1)) parent = parent[key];
        const key = packet.path[packet.path.length - 1];
        const target = packet.path.length ? parent[key] : parent;
        if (packet.kind === "alias") { let source = value; for (const part of packet.source) source = source[part]; setEngineProperty(parent, key, source); }
        else if (packet.kind === "array") target.push(...packet.items);
        else if (packet.kind === "object") for (const [name, value] of packet.items) setEngineProperty(target, name, value);
        else parent[key] += packet.value;
      }
      if (packet.kind !== "end" && packet.kind !== "textEnd") return;
    }
    if (data.event) { for (const listener of this.onEvent) listener(data.event, data.args); return; }
    const waiter = this.pending.get(data.id);
    if (!waiter) return;
    this.pending.delete(data.id);
    clearTimeout(waiter.timer);
    if (data.error) { const error = deserializeEngineError(data.error); waiter.reject(error); }
    else waiter.resolve(data.result);
  };
  private readonly crashed = () => {
    const error = new GonvexClientError("Engine worker crashed; reopen and retry the same command", { code: "disconnected" });
    this.fail(error); for (const listener of this.onCrash) listener(error);
  };
  constructor(readonly endpoint: EngineEndpoint, readonly generation: string, readonly scope: string) {
    endpoint.addEventListener("message", this.receive);
    endpoint.addEventListener("error", this.crashed);
    endpoint.addEventListener("messageerror", this.crashed);
    endpoint.start?.();
  }
  request(method: string, args: unknown[], id = crypto.randomUUID()): EngineEnvelope { return { generation: this.generation, scope: this.scope, id, method, args }; }
  call<T>(request: EngineEnvelope, timeoutMs = 20_000): Promise<T> {
    if (this.closed) return Promise.reject(new Error("Engine RPC is closed"));
    if (this.pending.has(request.id)) return Promise.reject(new Error("Engine RPC request is already pending"));
    return new Promise((resolve, reject) => {
      const timer = timeoutMs > 0 ? setTimeout(() => { this.pending.delete(request.id); this.chunks.delete(request.id); reject(new EngineRpcTimeout(request)); }, timeoutMs) : undefined;
      this.pending.set(request.id, { resolve, reject, request, timer });
      if (request.method === "reducer" && this.admissionBatching) {
        this.admissions.push(request);
        if (!this.admissionScheduled) { this.admissionScheduled = true; queueMicrotask(() => this.flushAdmissions()); }
      } else {
        // Control operations cannot overtake already issued admissions.
        if (this.admissions.length) this.flushAdmissions();
        this.endpoint.postMessage(request);
      }
    });
  }
  private fail(error: Error) {
    this.closed = true;
    for (const waiter of this.pending.values()) { clearTimeout(waiter.timer); waiter.reject(new EngineRpcUnavailable(waiter.request, error)); }
    this.pending.clear(); this.chunks.clear(); this.admissions.length = 0;
  }
  isClosed() { return this.closed; }
  close() {
    this.fail(new Error("Engine RPC is closed"));
    this.endpoint.removeEventListener("message", this.receive);
    this.endpoint.removeEventListener("error", this.crashed);
    this.endpoint.removeEventListener("messageerror", this.crashed);
  }
}

const sending = new WeakMap<EngineEndpoint, Promise<void>>();

type Segment = { path: string[]; kind: "array" | "object"; items: unknown[] } | { path: string[]; kind: "string"; value: string };

/** Large arrays stage decoded results; the UI never reparses the server's JSON. */
class TextCodecRequired extends Error {}

function splitMessage(message: Record<string, unknown>) {
  const segments: Segment[] = [];
  const seen = new WeakMap<object, { value: any; path: string[]; changed: boolean }>();
  const aliases: Array<{ path: string[]; source: string[] }> = [];
  const prepare = (value: any, path: string[]): any => {
    if (typeof value === "string" && value.length > 16 * 1024) { segments.push({ path, kind: "string", value }); return ""; }
    if (!value || typeof value !== "object") return value;
    const prior = seen.get(value);
    if (prior) { aliases.push({ path, source: prior.path }); return prior.value; }
    const array = Array.isArray(value);
    let result: any = array ? [] : {};
    seen.set(value, { value: result, path, changed: false });
    if (Object.keys(value).some(key => key.length > 1024)) throw new TextCodecRequired();
    let unchanged = true;
    for (const key of Object.keys(value)) { setEngineProperty(result, key, prepare(value[key], [...path, key])); unchanged &&= result[key] === value[key]; }
    if (JSON.stringify(result).length > 16 * 1024) {
      segments.push({ path, kind: array ? "array" : "object", items: array ? result : Object.entries(result) }); result = array ? [] : {};
    } else if (unchanged) result = value;
    seen.set(value, { value: result, path, changed: result !== value }); return result;
  };
  const value = prepare(message, []);
  // Parents arrive before segments that reference their child containers.
  segments.sort((a, b) => a.path.length - b.path.length);
  return { value, segments, aliases };
}

/** One acknowledged packet bounds transport buffers; decoded staging belongs to the requested result. */
export function sendEngineMessage(endpoint: EngineEndpoint, message: Record<string, unknown>): Promise<void> {
  const sendPacket = (value: Record<string, unknown>) => new Promise<void>((resolve, reject) => {
    const deliveryId = crypto.randomUUID();
    const timer = setTimeout(() => { endpoint.removeEventListener("message", ack); reject(new Error("Engine delivery timed out")); }, 20_000);
    const ack = ({ data }: MessageEvent) => {
      if (data?.deliveryAck !== deliveryId || data.generation !== message.generation || data.scope !== message.scope) return;
      clearTimeout(timer); endpoint.removeEventListener("message", ack); resolve();
    };
    endpoint.addEventListener("message", ack); endpoint.postMessage({ ...value, generation: message.generation, scope: message.scope, id: message.id, deliveryId });
  });
  const send = async () => {
    if (JSON.stringify(message).length <= 16 * 1024) {
      if (message.event) await sendPacket(message);
      else endpoint.postMessage(message);
      return;
    }
    let split: ReturnType<typeof splitMessage>;
    try { split = splitMessage(message); } catch (error) {
      if (!(error instanceof TextCodecRequired)) throw error;
      // Unusually long JSON keys cannot be used as bounded packet addresses.
      const paths: string[][] = [], parents = new WeakMap<object, string[]>();
      const payload = JSON.stringify(message, function(key, value) {
        const path = key === "" ? [] : [...(parents.get(this) ?? []), key];
        if (value === undefined) { paths.push(path); return null; }
        if (value && typeof value === "object") parents.set(value, path);
        return value;
      });
      const text = `{"payload":${payload},"undefinedPaths":${JSON.stringify(paths)}}`;
      await sendPacket({ packet: { kind: "textBegin" } });
      for (let offset = 0; offset < text.length; offset += 16 * 1024) await sendPacket({ packet: { kind: "text", value: text.slice(offset, offset + 16 * 1024) } });
      await sendPacket({ packet: { kind: "textEnd" } }); return;
    }
    const { value, segments, aliases } = split;
    await sendPacket({ packet: { kind: "begin", value } });
    for (const segment of segments) {
      if (segment.kind === "string") {
        for (let offset = 0; offset < segment.value.length; offset += 16 * 1024) await sendPacket({ packet: { kind: "string", path: segment.path, value: segment.value.slice(offset, offset + 16 * 1024) } });
      } else {
        let items: unknown[] = [], size = 0;
        for (const item of segment.items) {
          const bytes = (JSON.stringify(item)?.length ?? 4) + 1;
          if (items.length && size + bytes > 16 * 1024) { await sendPacket({ packet: { kind: segment.kind, path: segment.path, items } }); items = []; size = 0; }
          items.push(item); size += bytes;
        }
        if (items.length) await sendPacket({ packet: { kind: segment.kind, path: segment.path, items } });
      }
    }
    for (const alias of aliases) await sendPacket({ packet: { kind: "alias", ...alias } });
    await sendPacket({ packet: { kind: "end" } });
  };
  const operation = (sending.get(endpoint) ?? Promise.resolve()).then(send);
  sending.set(endpoint, operation.catch(() => undefined));
  return operation;
}
