# @gonvex/client

Browser client for Gonvex Queries, Reducers, Actions, Live Queries, the
persistent Local Replica, and telemetry.

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

await client.reducer(
  { kind: "reducer", path: "tasks.create" },
  { title: "Ship Gonvex" },
);

unsubscribe();
client.close();
```

## Persistent Live Query Windows

Live Query windows are persisted automatically in supported browsers when the
runtime advertises a safe cache scope. A warm subscription can replay its last
membership immediately while the server verifies it against the committed
revision stream. Returned entities are materialized into the same normalized
Local Replica used by Replica Collections.

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

## Replica Collections

Replica Collections materialize bounded, authorized entity sets in normalized
IndexedDB storage and resume them from a durable Postgres revision:

```ts
const watch = client.watchReplica<Task>(
  { kind: "replica", path: "tasks.recent" },
  { workspaceId: "workspace-a" },
);

const stop = watch.onUpdate(() => {
  render(watch.localReplicaResult() ?? []);
  console.log(watch.status()); // { isLoading, isUpToDate }
});
```

Configure or disable Local Replica persistence when constructing the client:

```ts
const client = new GonvexClient(url, {
  replica: {
    databaseName: "my-product-sync",
    maxBytes: 150 * 1024 * 1024,
  },
});

const memoryOnly = new GonvexClient(url, { replica: false });
```

The default global IndexedDB budget is 100 MiB. Server-declared per-collection
row/byte budgets still apply, and least-recently-used collections are evicted
first. Storage is isolated by runtime, project, tenant, authenticated identity,
and permissions.

## Optimistic Reducers

Every public interactive Reducer declares its optimistic contract. The
authoritative transaction remains separate from the optimistic overlay:

```go
app.Reducer(
  "tasks.update",
  updateTask,
  gonvex.Optimistic("tasks").RowIDArg("taskId").FieldsArg("updates"),
)

app.Query(
  "tasks.byWorkspace",
  tasksByWorkspace,
  gonvex.LiveTable("tasks").Key("_id").ResultRowsAt("page"),
)
```

Generated references carry this metadata, so a normal Reducer call is enough:

```ts
await client.reducer(api.tasks.update, {
  taskId,
  updates: { priority_id: priorityId },
});
```

The client persists the pending command, layers it over Local Replica selectors,
and notifies watchers immediately. Reducer success includes an
`originCommandId` and committed revision. The overlay is removed only after the
corresponding authoritative transaction has been applied locally, preventing an
empty or stale frame between optimistic and committed state.

Authoritative entity state stays separate from overlays. Durable pending state
lives in the command outbox and is re-applied after reload. Outbox
rows are isolated by project, tenant, and authenticated identity; an account
switch removes the previous identity's overlay and can never replay its writes
under the new session. Unscoped rows from the pre-isolation schema are removed
during migration because their owner cannot be proven. If an opaque credential
does not expose a stable identity (and no `identity` hint is supplied), its
outbox is deliberately session-only rather than risking cross-user replay. For
a complex projection that cannot be derived from one nested fields argument,
callers can still provide explicit `optimistic` entity patches.

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
//   hasInflightRequests, inflightReducers, inflightActions, inflightOneShotQueries
// }

const stop = client.subscribeToConnectionState((state) => {
  // drive banners / health UI
});
```

### Timeouts (defaults)

| Operation | Default |
| --- | --- |
| One-shot `query()` | 20s |
| `reducer()` | 20s |
| `action()` | 60s |

Override per client (`timeouts` option) or per call (`{ timeoutMs }`). Use `0` to disable.

### Typed errors

Rejected operations throw `GonvexClientError` with `code`:

- `server` — runtime executed the function and returned an error
- `timeout` — no response within the timeout
- `disconnected` — socket dropped while the operation was pending
- `closed` — client was explicitly closed
- `auth` — authentication rejected

### Reducer / Action disconnect policy

Actions and Reducers without `{ offline: "queue" }` fail closed after a
disconnect. They reject with `code: "disconnected"` (or `timeout` / `closed`).
Optimistic Reducers are persisted before transport even in fail-closed mode,
so a process reload cannot expose an older cached row while an accepted write
is still waiting for its authoritative subscription update.

Pass `{ offline: "queue" }` to a Reducer to durably accept a transport failure
and replay the same idempotency key after reconnect, whether or not that
Reducer also declares optimistic UI metadata. Actions are never queued.
Deterministic server errors are never queued and always roll an optimistic
entity overlay back when one exists.

Live queries keep last-good data at the React layer (`useQueryResult`) and
resubscribe after reconnect. Call `client.retryQuery(ref, args)` to force a
re-request after a server error or soft timeout.

## Exports

The package exports:

- `GonvexClient`
- `ConvexReactClient` compatibility alias
- `GonvexClientError`, `ConnectionState`, timeout defaults
- transparent persistent query caching and lower-level experimental cache helpers
- normalized Local Replica entities and Live Query memberships
- durable optimistic Reducer overlays reconciled by command ID and revision
- opt-in command outbox replay with stable idempotency keys
- `subscribeReplica`, `watchReplica`, and persistent Replica storage
- browser capability and telemetry helpers
- `GonvexErrorReporter` and automatic operation error reporting

## Related Packages

- `@gonvex/react` - React hooks over this client
- `@gonvex/protocol` - protocol message and JSON types
- `@gonvex/cli` - development CLI

## Documentation

Full docs live at https://desarso.github.io/gonvex/
