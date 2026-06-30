# create-gonvex

Project initializer for Gonvex apps.

Use it to scaffold a new Gonvex app with a frontend template, app-local
`gonvex/` functions, generated bindings, and local runtime configuration.

## Usage

```bash
npm create gonvex@latest my-app
cd my-app
npm run dev
```

With pnpm:

```bash
pnpm create gonvex my-app
```

Choose the Vite React template explicitly:

```bash
npm create gonvex@latest my-app -- --template vite-react
```

## What It Creates

```txt
my-app/
  gonvex/
    schema.go
    messages.go
    _generated/
  src/
  gonvex.json
  .env.local
  package.json
```

The generated app uses `@gonvex/cli`, `@gonvex/client`, and `@gonvex/react`.

## Related Packages

- `@gonvex/cli` - development CLI
- `@gonvex/client` - browser WebSocket client
- `@gonvex/react` - React hooks
- `@gonvex/protocol` - shared protocol types

## Documentation

Full docs live at https://desarso.github.io/gonvex/
