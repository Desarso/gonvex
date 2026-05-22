# Gonvex

Experimental Gonvex implementation workspace.

## Layout

```txt
apps/dashboard/         Gonvex dashboard and integration test harness
apps/docs/              Fumadocs documentation site
packages/client/        browser WebSocket client
packages/react/         React provider and hooks
packages/protocol/      shared TypeScript protocol types
packages/gonvex/        npm CLI package for `npx gonvex`
packages/create-gonvex/ npm initializer for `npm create gonvex`
templates/vite-react/   default Vite React starter template
cmd/gonvex/             contributor/local Go CLI prototype
server/                 Go Gonvex runtime server
infra/                  Local infrastructure helpers
gonvex_architecture.md  Architecture notes
```

The actual Gonvex implementation should live separately from the dashboard app as the repo grows.

## Dashboard App

```bash
pnpm install
pnpm dev:dashboard
```

The dashboard uses Glide Data Grid as the future dashboard/grid testing surface. Its normal `pnpm dev` script runs the Gonvex watcher/codegen process and Vite together, matching the template behavior we want long term for app projects.

Useful checks:

```bash
pnpm typecheck
pnpm test
pnpm test:go
pnpm build
```

Run docs:

```bash
pnpm dev:docs
```

## License

Gonvex is open source under the MIT license.

## Core Pieces

```txt
gonvex CLI
  TypeScript npm CLI that runs `gonvex dev`, watches app-local gonvex/*.go files,
  generates TypeScript bindings, and syncs manifests to a configured runtime.
  App users should not need Go installed for normal cloud-runtime development.

gonvex runtime
  Go runtime that receives synced manifests, applies safe schema changes, runs backend functions, owns
  realtime query invalidation, WebSockets, Postgres, and S3-compatible storage.
```

Current dev commands:

```bash
make dev
pnpm dev:runtime
pnpm --filter @gonvex/dashboard gonvex:once
pnpm --filter @gonvex/dashboard gonvex:dev
pnpm --filter @gonvex/dashboard dev
```

## Local Services

This workspace reuses the existing local Postgres container on `localhost:5432`.

Local runtime configuration lives in `.env`, which is intentionally gitignored. `.env.example` contains safe template values for the required database and S3-compatible storage variables.

Required database variables:

```txt
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
POSTGRES_SSLMODE
POSTGRES_URL
DATABASE_URL
```

MinIO is used for file upload/storage testing. If no MinIO container is already running, start the dev MinIO service with:

```bash
docker compose -f infra/docker-compose.dev.yml up -d minio
```

The included compose file exposes:

```txt
S3 API: http://localhost:9000
Console: http://localhost:9001
Bucket: gonvex-dev
```

Required storage variables:

```txt
S3_ENDPOINT
S3_REGION
S3_BUCKET
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
S3_FORCE_PATH_STYLE
```
