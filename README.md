# Gonvex

Gonvex is an open source Convex-style backend for teams that want the same fast app-building loop with Go, Postgres, TypeScript, React, and realtime subscriptions.

You write backend functions next to your app, Gonvex generates type-safe frontend bindings, and your React UI calls queries and mutations over a realtime runtime.

```tsx
import { api } from "./gonvex/_generated/api";
import { useMutation, useQuery } from "./gonvex/_generated/react";

export function Tasks() {
  const tasks = useQuery(api.tasks.list, { status: "open" });
  const createTask = useMutation(api.tasks.create);

  return <TaskList tasks={tasks ?? []} onCreate={createTask} />;
}
```

Gonvex is for developers who like Convex's product shape but want infrastructure they can inspect, extend, and self-host.

> Status: beta. Gonvex is usable for local development and experimentation, but the production hosting, self-hosting, auth, migrations, and multi-tenant operations story is still stabilizing before 1.0.

## Why Gonvex

- **Go backend functions**: define queries, mutations, actions, HTTP handlers, schema, and LiveGrid-style data views in Go.
- **TypeScript client bindings**: generate frontend-safe API references and React hooks from your backend.
- **Realtime by default**: subscribe to query results over WebSockets and refresh UI when data changes.
- **Postgres underneath**: keep your data in a database you already know how to run, back up, inspect, and tune.
- **Self-hostable runtime**: run Gonvex with Postgres, Valkey/Redis, and optional S3-compatible object storage.
- **Open source core**: the runtime, CLI, client packages, dashboard, docs, and starter templates live in this repo.

## Quickstart

Create a new app:

```bash
npm create gonvex@latest my-app
cd my-app
npm run dev
```

Or start Gonvex in an existing app:

```bash
npm install -D @gonvex/cli
npm install @gonvex/client @gonvex/react
npx gonvex init
npx gonvex dev -- vite
```

A Gonvex app keeps backend code beside the frontend:

```txt
my-app/
  gonvex/
    schema.go
    tasks.go
    _generated/
      api.ts
      client.ts
      react.ts
  src/
  gonvex.json
  package.json
```

## Define Your Backend

Schema and functions are written in Go:

```go
package backend

import "github.com/gonvex/gonvex/pkg/gonvex"

func Schema(s *gonvex.Schema) {
  s.Table("tasks", func(t *gonvex.Table) {
    t.ID("id")
    t.String("title")
    t.String("status")
    t.Time("created_at")
    t.Index("by_status", "status")
  })
}

type ListTasksArgs struct {
  Status string `json:"status,omitempty"`
}

func Register(app *gonvex.App) {
  app.Query("tasks.list", ListTasks)
  app.Mutation("tasks.create", CreateTask)
}

func ListTasks(ctx *gonvex.QueryCtx, args ListTasksArgs) ([]Task, error) {
  // Query Postgres through Gonvex APIs.
}
```

During development, `gonvex dev` watches the `gonvex/` folder, regenerates TypeScript bindings, syncs schema/function metadata, and runs your app dev server.

## Self-Hosting

Gonvex is designed to be self-hosted. A full deployment has:

```txt
Gonvex runtime       Executes functions, serves HTTP/WebSocket traffic, routes projects and tenants
Postgres            Stores app data and Gonvex control-plane data
Valkey or Redis     Coordinates cache, realtime invalidation, and runtime state
Object storage      Optional S3-compatible storage for apps that use file APIs
Dashboard           Optional web UI for inspecting projects, tables, functions, logs, and metrics
```

For local self-hosting, run the full stack with Docker:

```bash
git clone https://github.com/Desarso/gonvex.git
cd gonvex
cp .env.example .env
make stack
```

This starts:

```txt
Runtime:   http://localhost:8080
Dashboard: http://localhost:3000
Postgres:  localhost:5432
Valkey:    localhost:6380
S3 API:    http://localhost:9000
MinIO UI:  http://localhost:9001
```

For production self-hosting, put the runtime behind TLS, provide managed Postgres and Valkey/Redis, configure backups, set allowed origins, and use S3-compatible storage only if your app needs files. Production deployment automation is still early, so treat the Docker stack as the best current reference implementation rather than a finished operations guide.

## Current Scope

Gonvex currently includes:

- local runtime server
- app-local Go schema/function declarations
- generated TypeScript API references
- React provider and hooks
- browser WebSocket client
- Postgres-backed data paths
- realtime subscriptions and invalidation
- dashboard for local inspection
- Vite React starter template
- docs site and package workspace

Still in progress before a stable production release:

- generic production function dispatch across arbitrary apps
- hosted control plane
- hardened self-hosting deployment workflow
- production auth, membership, roles, and tenant routing
- migration previews and safe rollout controls
- versioned runtime/dashboard distribution

## Documentation

- Docs: https://desarso.github.io/gonvex/
- Quickstart: https://desarso.github.io/gonvex/docs/quickstart/
- Installation: https://desarso.github.io/gonvex/docs/installation/
- Deployment model: https://desarso.github.io/gonvex/docs/deployment/
- Current limits: https://desarso.github.io/gonvex/docs/current-limits/

Run the docs locally:

```bash
pnpm install
pnpm dev:docs
```

## Repository Layout

```txt
apps/dashboard/          Dashboard and local integration harness
apps/docs/               Documentation site
packages/client/         Browser WebSocket client
packages/react/          React provider and hooks
packages/protocol/       Shared TypeScript protocol types
packages/gonvex/         CLI package
packages/create-gonvex/  App initializer
templates/vite-react/    Default starter template
cmd/gonvex/              Local Go CLI prototype
server/                  Go runtime server
infra/                   Local infrastructure helpers
```

## Contributing

Install dependencies:

```bash
pnpm install
```

Start the local development stack:

```bash
make dev
```

Run the full Docker stack:

```bash
make stack
```

### Releasing npm packages

The package release runs through Make. Without `VERSION`, it selects the next
patch after both the newest `v*` Git tag and the highest checked-in publishable
package version. This keeps a stale tag from producing an already-published or
lower package version.

```bash
make version
make release-test
make release-dry-run
make release-prod
```

Set `VERSION=x.y.z` on the Make command only when choosing an explicit higher
version. The release script rejects a version that does not advance every
package and the latest release tag.

Useful checks:

```bash
pnpm typecheck
pnpm test
pnpm test:go
pnpm build
```

Useful development commands:

```bash
make services      # start local Postgres and Valkey
make runtime       # run the Go runtime with Air
make dashboard     # run the dashboard app
make packages      # watch/build npm packages
make docs          # run docs at http://localhost:3001
```

## License

Gonvex is open source under the Apache License 2.0.
