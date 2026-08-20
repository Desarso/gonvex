# Gonvex Rust module host

The Rust workspace contains the sandboxed TypeScript execution layer used by
the Gonvex runtime.

```text
TypeScript module
  -> deterministic JavaScript ESM bundle
  -> gonvex-module-host
  -> bounded V8 isolate
  -> capability-checked host calls
  -> Gonvex runtime transaction and services
```

Application modules expose exactly three executable kinds: Query, Reducer, and
Action. Query database access is read-only. A Reducer receives database calls
bound to the one transaction held by the runtime and may enqueue durable Action
work. An Action may use external capabilities and must call a Reducer to change
application tables.

The V8 runtime provides no Node.js process, filesystem, environment, raw socket,
or database credentials. Each invocation has explicit capabilities, a deadline,
bounded output, and an isolate heap limit. Module generations are prewarmed and
atomically activated; calls already running on an older generation may finish
before its isolates are destroyed.

## Crates

- `module-runtime`: language-neutral invocation ABI and capability model.
- `module-runtime-v8`: JavaScript/V8 implementation.
- `module-host`: process protocol, artifact verification, generation lifecycle,
  and host-call forwarding.
- `server`: generation-registry primitives shared by the Rust components.

The HTTP/WebSocket server, Postgres pools and transaction ownership currently
remain in Go. Rust owns TypeScript evaluation and module lifecycle. That is a
deliberate intermediate boundary, not a claim that the complete Gonvex host has
already moved to Rust.

## Development

```bash
cargo fmt --check
cargo test --workspace
cargo build -p gonvex-module-host
```

The production runtime image builds and installs `gonvex-module-host`; local
development selects it with `GONVEX_MODULE_HOST_BINARY`.
