# @gonvex/client

Browser client for Gonvex realtime queries, mutations, actions, telemetry, and
local cache helpers.

Most React apps should use `@gonvex/react`, which wraps this package with hooks.
Use `@gonvex/client` directly when you want lower-level control.

## Install

```bash
npm install @gonvex/client
```

## Usage

```ts
import { GonvexClient } from "@gonvex/client";

const client = new GonvexClient("ws://localhost:8080/ws", {
  project: "my-project",
  tenant: "demo",
});

client.connect();

const unsubscribe = client.subscribeQuery(
  { kind: "query", path: "tasks.list" },
  { status: "open" },
  (message) => {
    if (message.type === "query.result") {
      console.log(message.result);
    }
  },
);

await client.mutation(
  { kind: "mutation", path: "tasks.create" },
  { title: "Ship Gonvex" },
);

unsubscribe();
client.close();
```

## Transparent Browser Cache

Live query results are persisted automatically in supported browsers when the
runtime advertises a safe cache scope. A warm `subscribeQuery`, `watchQuery`, or
React `useQuery` can replay its last result while the normal server subscription
runs in parallel. The server result always wins and refreshes the snapshot,
including results produced by realtime invalidation.

Caching is isolated by runtime deployment, project, tenant, user, and current
permissions. New clients connected to older runtimes stay server-only.

No setup is required. To opt out or clear the disposable cache:

```ts
const client = new GonvexClient(url, { queryCache: false });

await client.clearQueryCache();
await client.clearQueryCache({ allScopes: true });
```

Dexie is loaded asynchronously only after a cache-capable session is confirmed,
so IndexedDB setup does not delay the WebSocket query path.

## Lightweight Error Tracking

Capture global browser failures and failed Gonvex operations with the same
client. Reports are batched, retried locally, scrubbed, persisted by the runtime,
and grouped in the Gonvex dashboard:

```ts
const client = new GonvexClient(url, {
  project: "my-project",
  tenant: "acme",
  errorReporting: {
    release: "2.4.0+abc123",
    environment: "production",
  },
});
```

Use `GonvexErrorReporter` directly when integrating an existing application
logger. See the Error Tracking guide in the full documentation for privacy,
grouping, persistence, and dashboard details.

## Connection reliability

The client reconnects automatically after an unexpected socket close (exponential
backoff from ~250ms to 5s). On reconnect it re-authenticates, then resubscribes
active live queries and pending one-shot queries. Explicit `close()` disables
reconnect.

```ts
client.connectionState();
// {
//   isWebSocketConnected, hasEverConnected, connectionCount, connectionRetries,
//   hasInflightRequests, inflightMutations, inflightActions, inflightOneShotQueries
// }

const stop = client.subscribeToConnectionState((state) => {
  // drive banners / health UI
});
```

### Timeouts (defaults)

| Operation | Default |
| --- | --- |
| One-shot `query()` | 20s |
| `mutation()` | 20s |
| `action()` | 60s |

Override per client (`timeouts` option) or per call (`{ timeoutMs }`). Use `0` to disable.

### Typed errors

Rejected operations throw `GonvexClientError` with `code`:

- `server` — runtime executed the function and returned an error
- `timeout` — no response within the timeout
- `disconnected` — socket dropped while the operation was pending
- `closed` — client was explicitly closed
- `auth` — authentication rejected

### Mutation / action fail-closed policy

Pending mutations and actions are **never** auto-replayed after disconnect.
They reject with `code: "disconnected"` (or `timeout` / `closed`). Silent
re-fire of non-idempotent writes is unsafe; offline queues belong in the app
(or mobile offline layer), not this client.

Live queries keep last-good data at the React layer (`useQueryResult`) and
resubscribe after reconnect. Call `client.retryQuery(ref, args)` to force a
re-request after a server error or soft timeout.

## Exports

The package exports:

- `GonvexClient`
- `ConvexReactClient` compatibility alias
- `GonvexClientError`, `ConnectionState`, timeout defaults
- transparent persistent query caching and lower-level experimental cache helpers
- browser capability and telemetry helpers
- `GonvexErrorReporter` and automatic operation error reporting

## Related Packages

- `@gonvex/react` - React hooks over this client
- `@gonvex/protocol` - protocol message and JSON types
- `@gonvex/cli` - development CLI

## Documentation

Full docs live at https://desarso.github.io/gonvex/
