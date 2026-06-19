import type { ClientMessage, JsonValue, ServerMessage } from "@gonvex/protocol";

type SubscriptionHandler = (message: ServerMessage) => void;

export type FunctionReference = {
  kind: string;
  path: string;
};

export type GonvexClientAuth = {
  token?: string;
  tenant?: string;
};

export class GonvexClient {
  private socket: WebSocket | undefined;
  private readonly handlers = new Map<string, SubscriptionHandler>();
  private auth: GonvexClientAuth = {};

  constructor(private readonly url: string, auth: GonvexClientAuth = {}) {
    this.auth = auth;
  }

  setAuth(auth: GonvexClientAuth) {
    this.auth = auth;
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.sendAuth();
    }
  }

  connect() {
    if (this.socket && this.socket.readyState <= WebSocket.OPEN) return;

    this.socket = new WebSocket(this.url);
    this.socket.addEventListener("open", () => this.sendAuth());
    this.socket.addEventListener("message", (event) => {
      let message: ServerMessage;
      try {
        message = JSON.parse(String(event.data)) as ServerMessage;
      } catch {
        return;
      }
      const id = "id" in message ? message.id : "system";
      this.handlers.get(id)?.(message);
    });
  }

  close() {
    this.handlers.clear();
    if (!this.socket) return;
    this.socket.close();
    this.socket = undefined;
  }

  subscribeQuery(ref: FunctionReference, args: JsonValue = {}, onMessage: SubscriptionHandler) {
    this.connect();
    const id = randomID();
    this.handlers.set(id, onMessage);
    this.send({ type: "query.subscribe", id, path: ref.path, args });

    return () => {
      this.send({ type: "query.unsubscribe", id });
      // Keep the handler briefly so in-flight results aren't dropped on resubscribe.
      window.setTimeout(() => this.handlers.delete(id), 500);
    };
  }

  mutation<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}): Promise<T> {
    return this.call<T>("mutation", ref, args);
  }

  action<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}): Promise<T> {
    return this.call<T>("action", ref, args);
  }

  query<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}): Promise<T> {
    this.connect();
    const id = randomID();
    return new Promise<T>((resolve, reject) => {
      this.handlers.set(id, (message) => {
        if (message.type === "query.result") {
          this.handlers.delete(id);
          this.send({ type: "query.unsubscribe", id });
          resolve(message.result as T);
        }
        if (message.type === "query.error") {
          this.handlers.delete(id);
          this.send({ type: "query.unsubscribe", id });
          reject(new Error(message.error));
        }
      });
      this.send({ type: "query.subscribe", id, path: ref.path, args });
    });
  }

  private call<T>(kind: "mutation" | "action", ref: FunctionReference, args: JsonValue): Promise<T> {
    this.connect();
    const id = randomID();
    return new Promise<T>((resolve, reject) => {
      this.handlers.set(id, (message) => {
        if (kind === "mutation" && message.type === "mutation.result") {
          this.handlers.delete(id);
          resolve(message.result as T);
        }
        if (kind === "mutation" && message.type === "mutation.error") {
          this.handlers.delete(id);
          reject(new Error(message.error));
        }
        if (kind === "action" && message.type === "action.result") {
          this.handlers.delete(id);
          resolve(message.result as T);
        }
        if (kind === "action" && message.type === "action.error") {
          this.handlers.delete(id);
          reject(new Error(message.error));
        }
      });
      if (kind === "mutation") {
        this.send({ type: "mutation.call", id, path: ref.path, args });
      } else {
        this.send({ type: "action.call", id, path: ref.path, args });
      }
    });
  }

  private sendAuth() {
    if (!this.auth.token && !this.auth.tenant) return;
    this.send({ type: "auth", id: randomID(), token: this.auth.token, tenant: this.auth.tenant });
  }

  private send(message: ClientMessage) {
    const socket = this.socket;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      socket?.addEventListener("open", () => socket.send(JSON.stringify(message)), { once: true });
      return;
    }
    socket.send(JSON.stringify(message));
  }
}

export class ConvexReactClient extends GonvexClient {
  constructor(url: string, auth: GonvexClientAuth = {}) {
    super(toWebSocketURL(url), auth);
  }
}

function toWebSocketURL(url: string) {
  if (url.startsWith("ws://") || url.startsWith("wss://")) return url;
  return url.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/ws";
}

function randomID() {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  return `gonvex_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}
