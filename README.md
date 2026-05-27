# Gonvex

Gonvex is a Convex-style backend platform for developers who want the Convex developer experience with Go, Postgres, and TypeScript.

Write backend functions and schema next to your app, generate frontend-safe TypeScript bindings, run a local runtime during development, and subscribe to realtime data from React.

> Status: beta. The repo contains the working runtime, client packages, app template, dashboard lab, and documentation site. APIs and packaging are still evolving, so expect breaking changes before a stable 1.0.

## Documentation

- Hosted docs: https://desarso.github.io/gonvex/
- Local docs: `pnpm dev:docs`
- Architecture notes: [`gonvex_architecture.md`](./gonvex_architecture.md)

Start with:

1. [Quickstart](https://desarso.github.io/gonvex/docs/quickstart/)
2. [Installation](https://desarso.github.io/gonvex/docs/installation/)
3. [Schema](https://desarso.github.io/gonvex/docs/schema/)
4. [Functions and Bindings](https://desarso.github.io/gonvex/docs/functions-and-bindings/)
5. [Realtime Subscriptions](https://desarso.github.io/gonvex/docs/realtime-subscriptions/)

## Developer Experience

The intended app workflow is:

```bash
npm create gonvex@latest my-app
cd my-app
npx gonvex dev
```

In an app repo, Gonvex code lives beside the frontend:

```txt
my-app/
  src/
  gonvex/
    schema.go
    tasks.go
    _generated/
      api.ts
      react.ts
      client.ts
```

Backend functions are registered in Go:

```go
func Register(app *gonvex.App) {
  app.Query("tasks.list", ListTasks)
  app.Mutation("tasks.create", CreateTask)
}
```

React imports generated bindings:

```ts
import { api } from "./gonvex/_generated/api";
import { useMutation, useQuery } from "./gonvex/_generated/react";

const tasks = useQuery(api.tasks.list, { status: "open" });
const createTask = useMutation(api.tasks.create);
```

## What Gonvex Provides

- App-local Go backend functions: queries, mutations, actions, HTTP handlers, and LiveGrid definitions.
- Generated TypeScript bindings for frontend calls.
- Postgres-backed schema and data APIs.
- Realtime WebSocket subscriptions and query invalidation.
- React client hooks.
- File upload/storage plumbing for S3-compatible backends.
- A dashboard for inspecting functions, tables, data, metrics, and realtime grid behavior.
- Project and tenant concepts for multi-tenant apps.

## Repository Layout

```txt
apps/dashboard/         Gonvex dashboard and integration test harness
apps/docs/              Fumadocs documentation site
packages/client/        Browser WebSocket client
packages/react/         React provider and hooks
packages/protocol/      Shared TypeScript protocol types
packages/gonvex/        npm CLI package for `npx gonvex`
packages/create-gonvex/ npm initializer for `npm create gonvex`
templates/vite-react/   default Vite React starter template
cmd/gonvex/             contributor/local Go CLI prototype
server/                 Go Gonvex runtime server
infra/                  local infrastructure helpers
```

## Local Development

Install dependencies:

```bash
pnpm install
```

Start the main development stack:

```bash
make dev
```

Useful individual commands:

```bash
make services      # start local Postgres and Valkey helpers
make storage       # optionally start example MinIO S3-compatible storage
make runtime       # run the Go runtime with Air
make dashboard     # run the dashboard app
make packages      # watch/build npm packages
make docs          # run docs at http://localhost:3001
```

Useful checks:

```bash
pnpm typecheck
pnpm test
pnpm test:go
pnpm build
```

## Releases

Current automated releases publish the npm packages that developers install in apps:

```txt
packages/protocol/       `@gonvex/protocol`
packages/client/         `@gonvex/client`
packages/react/          `@gonvex/react`
packages/gonvex/         `npx gonvex`
packages/create-gonvex/  `npm create gonvex`
```

Release helpers:

```bash
make release-notes-preview VERSION=0.0.1
make release-dry-run VERSION=0.0.1
make release-prod VERSION=0.0.1
```

The Go runtime, hosted control plane, dashboard, and deployment artifacts are not yet independently packaged by this release flow. For now they are developed from this monorepo; runtime distribution will become a separate release track when the hosting/self-hosting story is ready.

## Local Services

Local runtime configuration lives in `.env`, which is intentionally gitignored. `.env.example` contains safe template values for the local Postgres and Valkey services.

Start local database/cache services with:

```bash
make services
```

File APIs are optional. If you use files, point Gonvex at any S3-compatible storage provider. For local testing, this repo includes an optional MinIO example:

```bash
make storage
```

Example MinIO defaults:

```txt
S3 API: http://localhost:9000
Console: http://localhost:9001
Bucket: gonvex-dev
```

## Publishing Docs

Documentation is deployed to GitHub Pages from `apps/docs` through the `Deploy docs to GitHub Pages` workflow. Pushes to `main` that touch docs or the workflow rebuild the static site and deploy it to:

```txt
https://desarso.github.io/gonvex/
```

GitHub repository settings must use **Pages -> Source -> GitHub Actions**.

## License

Gonvex is open source under the Apache License 2.0.
