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
authoritative server result. Its signature and return type do not change.

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
  const { user } = useGonvexAuth();
  return <>{user?.email}<GoogleSignInButton /></>;
}
```

The provider performs Authorization Code + PKCE against the Gonvex runtime and
attaches the resulting project-scoped session to the realtime client. It does not
load Firebase or a Google browser SDK.

## Convex Compatibility

The package also exports Convex-style names for incremental migration:

- `ConvexProvider`
- `ConvexProviderWithAuth`
- `ConvexReactClient`
- `useConvex`
- `useConvexAuth`
- `usePaginatedQuery`

## Related Packages

- `@gonvex/client` - browser WebSocket client
- `@gonvex/protocol` - shared protocol types
- `@gonvex/cli` - generated bindings and runtime sync

## Documentation

Full docs live at https://desarso.github.io/gonvex/
