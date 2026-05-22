import type { ClientMessage, JsonValue, ServerMessage } from "@gonvex/protocol";

type SubscriptionHandler = (message: ServerMessage) => void;

export type FunctionReference = {
  kind: string;
  path: string;
};

export class GonvexClient {
  private socket: WebSocket | undefined;
  private readonly handlers = new Map<string, SubscriptionHandler>();

  constructor(private readonly url: string) {}

  connect() {
    if (this.socket && this.socket.readyState <= WebSocket.OPEN) return;

    this.socket = new WebSocket(this.url);
    this.socket.addEventListener("message", (event) => {
      const message = JSON.parse(String(event.data)) as ServerMessage;
      const id = "id" in message ? message.id : "system";
      this.handlers.get(id)?.(message);
    });
  }

  subscribeQuery(ref: FunctionReference, args: JsonValue, onMessage: SubscriptionHandler) {
    this.connect();
    const id = randomID();
    this.handlers.set(id, onMessage);
    this.send({ type: "query.subscribe", id, path: ref.path, args });

    return () => {
      this.send({ type: "query.unsubscribe", id });
      this.handlers.delete(id);
    };
  }

  mutation<T = JsonValue>(ref: FunctionReference, args: JsonValue): Promise<T> {
    this.connect();
    const id = randomID();
    return new Promise<T>((resolve, reject) => {
      this.handlers.set(id, (message) => {
        if (message.type === "mutation.result") {
          this.handlers.delete(id);
          resolve(message.result as T);
        }
        if (message.type === "mutation.error") {
          this.handlers.delete(id);
          reject(new Error(message.error));
        }
      });
      this.send({ type: "mutation.call", id, path: ref.path, args });
    });
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

function randomID() {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  return `gonvex_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}
