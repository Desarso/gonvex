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

## Exports

The package exports:

- `GonvexClient`
- `ConvexReactClient` compatibility alias
- transparent persistent query caching and lower-level experimental cache helpers
- browser capability and telemetry helpers
- `GonvexErrorReporter` and automatic operation error reporting

## Related Packages

- `@gonvex/react` - React hooks over this client
- `@gonvex/protocol` - protocol message and JSON types
- `@gonvex/cli` - development CLI

## Documentation

Full docs live at https://desarso.github.io/gonvex/
