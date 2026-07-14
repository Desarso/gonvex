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

Authenticate with an account session or an existing personal access token:

```bash
npx gonvex login --runtime-url https://gonvex.example.com --email you@example.com
npx gonvex login --runtime-url https://gonvex.example.com --token gvx_pat_...
```

Create a scoped account token and provision a project without leaving the terminal:

```bash
npx gonvex token create "Developer CLI"
npx gonvex project create my-app
```

The default token permissions are `projects:read`, `projects:create`, and
`projects:keys:read`. Use repeated `--permission` flags, `--permission 'projects:*'`,
or `--full` to choose broader access. Account credentials are stored per runtime in a
user config file with mode `0600`; project credentials stay in the app's
`.env.local`.

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
npx gonvex env get NAME
npx gonvex env set NAME value
npx gonvex env push .env.production
npx gonvex env remove NAME
```

Enable native Google login for an app without Firebase or a per-app Google Cloud
project:

```bash
npx gonvex auth add google --origin http://localhost:5173
npx gonvex auth status
npx gonvex auth users
```

The command registers the exact callback with the runtime and writes
`gonvex/auth.tsx`, which exports a configured provider, hook, and Google sign-in
button. A Gonvex installation operator configures one central Google OAuth client;
future app projects reuse it through the runtime.

`env push` resolves the file from the selected project root and atomically
replaces that project's server-side environment-variable set. Pass a dedicated
deployment env file; the CLI refuses to upload `GONVEX_PROJECT_KEY` and related
CLI credentials.

Environment commands require a runtime built from Gonvex v0.1.9 or newer. The
CLI sends the selected project key in both supported authentication headers, and
the runtime scopes that key to the exact project in the request. If environment
commands return `dashboard sign-in is required` while `gonvex dev --once` works,
upgrade and recreate the runtime; updating this npm package alone does not update
the deployed Go runtime.

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
