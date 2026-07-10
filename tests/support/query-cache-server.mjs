import { createServer } from "node:http";
import { WebSocket, WebSocketServer } from "ws";

const host = "127.0.0.1";
const port = 4180;
const processStartedAt = Date.now();
let revision = 0;
const channels = new Map();
const clients = new Set();

function channelState(id) {
  let state = channels.get(id);
  if (!state) {
    state = { value: "server-default", delayMs: 0 };
    channels.set(id, state);
  }
  return state;
}

function nextRevision() {
  revision += 1;
  return `${String(processStartedAt).padStart(13, "0")}:${String(revision).padStart(20, "0")}`;
}

function resultMessage(client, id, reason) {
  const state = channelState(client.channel);
  return {
    type: "query.result",
    id,
    path: "cache.demo",
    result: { value: state.value },
    reason,
    cacheScope: client.scope,
    cacheRevision: nextRevision(),
  };
}

async function readJSON(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", `http://${host}:${port}`);
  if (request.method === "GET" && url.pathname === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: true }));
    return;
  }
  if (request.method === "POST" && url.pathname === "/control") {
    const body = await readJSON(request);
    const channel = String(body.channel ?? "default");
    const state = channelState(channel);
    if (typeof body.value === "string") state.value = body.value;
    if (typeof body.delayMs === "number") state.delayMs = Math.max(0, body.delayMs);
    if (body.broadcast === true) {
      for (const client of clients) {
        if (client.channel !== channel || client.socket.readyState !== WebSocket.OPEN) continue;
        for (const id of client.subscriptions) {
          client.socket.send(JSON.stringify(resultMessage(client, id, "invalidate")));
        }
      }
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: true, state }));
    return;
  }
  response.writeHead(404);
  response.end();
});

const webSockets = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  webSockets.handleUpgrade(request, socket, head, (webSocket) => {
    webSockets.emit("connection", webSocket, request);
  });
});

webSockets.on("connection", (socket, request) => {
  const url = new URL(request.url ?? "/ws", `http://${host}:${port}`);
  const client = {
    socket,
    channel: url.searchParams.get("channel") ?? "default",
    scope: url.searchParams.get("scope") ?? "a".repeat(64),
    subscriptions: new Set(),
  };
  clients.add(client);
  socket.send(JSON.stringify({
    type: "session.ready",
    project: "query-cache-test",
    tenant: "browser",
    queryCache: {
      protocolVersion: 1,
      scope: client.scope,
      epoch: "e".repeat(64),
      maxAgeMs: 86_400_000,
    },
  }));

  socket.on("message", (raw) => {
    let message;
    try {
      message = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (message.type === "query.subscribe") {
      client.subscriptions.add(message.id);
      const delayMs = channelState(client.channel).delayMs;
      setTimeout(() => {
        if (client.subscriptions.has(message.id) && socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify(resultMessage(client, message.id, "initial")));
        }
      }, delayMs);
    }
    if (message.type === "query.unsubscribe") {
      client.subscriptions.delete(message.id);
    }
  });
  socket.on("close", () => clients.delete(client));
});

server.listen(port, host, () => {
  process.stdout.write(`query cache test server listening on http://${host}:${port}\n`);
});

function shutdown() {
  for (const client of clients) client.socket.close();
  webSockets.close();
  server.close(() => process.exit(0));
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
