# @gonvex/cli

The Gonvex CLI for app-local development with the Gonvex runtime.

Gonvex gives you a Convex-style workflow with Go backend functions, generated
TypeScript bindings, React hooks, realtime subscriptions, and a local runtime
backed by Postgres.

## Install

```bash
npm install -D @gonvex/cli
```

Most apps run it through a package script:

```json
{
  "scripts": {
    "dev": "gonvex dev -- vite",
    "gonvex:dev": "gonvex dev"
  }
}
```

## Commands

Initialize Gonvex files in an existing app:

```bash
npx gonvex init
```

Watch `gonvex/`, regenerate bindings, sync the runtime, and optionally run your
frontend dev server:

```bash
npx gonvex dev -- vite
```

Run a one-shot sync for CI or Docker builds:

```bash
npx gonvex dev --once
```

By default, `gonvex dev` streams only runtime warnings and errors to the
terminal. To tail every query, mutation, and action:

```bash
npx gonvex dev --verbose-logs -- vite
```

Manage project environment variables:

```bash
npx gonvex env list
npx gonvex env set NAME value
npx gonvex env push .env.production
npx gonvex env remove NAME
```

`env push` resolves the file from the selected project root and atomically
replaces that project's server-side environment-variable set. Pass a dedicated
deployment env file; the CLI refuses to upload `GONVEX_PROJECT_KEY` and related
CLI credentials.

## Runtime Settings

The CLI reads `.env.local` and `.env`:

```txt
GONVEX_PROJECT_ID=my-project
GONVEX_RUNTIME_URL=http://localhost:8080
GONVEX_PROJECT_KEY=gvx_...
```

For Vite/browser clients, also expose:

```txt
VITE_GONVEX_PROJECT_ID=my-project
VITE_GONVEX_URL=http://localhost:8080
VITE_GONVEX_WS_URL=ws://localhost:8080/ws
```

## Related Packages

- `@gonvex/client` - browser WebSocket client
- `@gonvex/react` - React provider and hooks
- `@gonvex/protocol` - shared TypeScript protocol types
- `create-gonvex` - project initializer

## Documentation

Full docs live at https://desarso.github.io/gonvex/
