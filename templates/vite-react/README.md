# Gonvex + Vite React

This is the default Gonvex starter for a React app.

## Development

```bash
npm install
npm run dev
```

`npm run dev` runs:

```bash
gonvex dev -- vite
```

The Gonvex CLI watches the TypeScript modules in `gonvex/` and versioned SQL
migrations, regenerates `gonvex/_generated/*`, builds the module artifact, syncs
schema/function metadata to the runtime, and runs Vite.

It does not start the runtime or its dependencies. Start the Gonvex reference
stack separately with `make stack`, or point `.env.local` at an existing
self-hosted runtime.

## Runtime

The app connects to a runtime URL:

```txt
VITE_GONVEX_WS_URL=ws://localhost:8080/ws
```

Production should use the public origin of your self-hosted runtime:

```txt
VITE_GONVEX_WS_URL=wss://gonvex.example.com/ws
```

Application functions are written in TypeScript and run in Gonvex's module host.
The starter does not require a separate backend language toolchain.
