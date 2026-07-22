# @gonvex/react

React bindings for Gonvex.

This package provides the provider and hooks used by generated Gonvex bindings:
`useQuery`, `useMutation`, `useAction`, auth-aware providers, and Convex-style
compatibility exports.

## Install

```bash
npm install @gonvex/react @gonvex/client
```

## Usage

```tsx
import { GonvexClient } from "@gonvex/client";
import { GonvexProvider, useMutation, useQuery } from "@gonvex/react";
import { api } from "./gonvex/_generated/api";

const client = new GonvexClient("ws://localhost:8080/ws", {
  project: "my-project",
});

export function AppRoot() {
  return (
    <GonvexProvider client={client}>
      <Tasks />
    </GonvexProvider>
  );
}

function Tasks() {
  const tasks = useQuery(api.tasks.list, { status: "open" });
  const createTask = useMutation(api.tasks.create);

  return (
    <button onClick={() => void createTask({ title: "New task" })}>
      {tasks?.length ?? 0} open tasks
    </button>
  );
}
```

`useQuery` automatically benefits from the browser query-result cache. On a
warm load it may receive the last scoped snapshot first, followed by the
authoritative server result. Its signature remains `T | undefined`, but a
server `query.error` now **throws** during render (Convex-compatible) so error
boundaries can catch it instead of looking like an endless loading state.

### `useQueryResult` (preferred for new UI)

Use when you need loading vs error vs timeout vs disconnected, last-good data,
or a retry button:

```tsx
const { data, status, error, isStale, retry } = useQueryResult(api.tasks.list, { status: "open" });

if (status === "loading" && !data) return <Spinner />;
if (status === "error") {
  return <button onClick={retry}>Retry: {error?.message}</button>;
}
// status success | timeout | disconnected — data may still be last-good (isStale)
```

Statuses: `skip` | `loading` | `success` | `error` | `timeout` | `disconnected`.
Soft timeout default is 15s (subscription stays alive; does not reject).

### Connection state

```tsx
const { isWebSocketConnected, hasEverConnected, connectionRetries } = useConvexConnectionState();
```

This reflects the real WebSocket lifecycle (not a stub). Mutations/actions
reject with `GonvexClientError` on timeout or disconnect and never hang forever.

## Native Google auth

Enable Google for the project and generate a configured auth module:

```bash
npx gonvex auth add google --origin http://localhost:5173
```

```tsx
import { GonvexAuthProvider, GoogleSignInButton, useGonvexAuth } from "./gonvex/auth";

function Root() {
  return (
    <GonvexAuthProvider client={client}>
      <Account />
    </GonvexAuthProvider>
  );
}

function Account() {
  const { activeTenant, user } = useGonvexAuth();
  return <>{user?.email} · {activeTenant?.name}<GoogleSignInButton /></>;
}
```

The provider performs Authorization Code + PKCE against the Gonvex runtime and
attaches the resulting project-scoped session to the realtime client. Access tokens
are short-lived and refresh tokens rotate across tabs. Multi-tenant memberships are
verified by the runtime and switched with `setActiveTenant`. The provider does not
load a Google browser SDK.

## Convex Compatibility

The package also exports Convex-style names for incremental migration:

- `ConvexProvider`
- `ConvexProviderWithAuth`
- `ConvexReactClient`
- `useConvex`
- `useConvexAuth`
- `useConvexConnectionState`
- `usePaginatedQuery`
- `useQueryResult`

## Related Packages

- `@gonvex/client` - browser WebSocket client
- `@gonvex/protocol` - shared protocol types
- `@gonvex/cli` - generated bindings and runtime sync

## Documentation

Full docs live at https://desarso.github.io/gonvex/
