# Gonvex v2 Rust module host scaffold

This workspace defines the future language-neutral application-module seam. It
does not replace the current Go runtime yet.

The active production/dev path remains:

```text
gonvex dev → /dev/sync → Go SourceBundle → projectbundle.Loader → Go App
```

The new seam is:

```text
module artifact + manifest
        ↓
ModuleEngine (V8 or Wasm adapter)
        ↓
GenerationRegistry
        ↓
Rust host (future HTTP/Postgres/change-feed integration)
```

`gonvex-module-runtime` owns only the ABI: function contracts, invocation
identity/tenant context, capability declarations, host calls, and result/error
types. It deliberately moves values as bytes and JSON metadata so the host is
not coupled to TypeScript, Go, or a database client.

`gonvex-module-runtime-v8` describes the bounded TypeScript/V8 adapter without
pulling a heavyweight V8 dependency into the first workspace checkpoint.
`gonvex-module-runtime-wasm` does the same for a future Wasmtime Component
Model adapter. Their `ModuleEngine` implementations fail explicitly until the
engine is linked.

`gonvex-server-host` provides atomic module generations. New calls acquire a
lease on the active generation; publishing generation N+1 prevents new calls
from entering N, while existing calls finish. Retired generations are reaped
only after their in-flight lease count reaches zero.

## Coexistence plan

1. Keep the Go `SourceBundle` and `projectbundle.Loader` as the default engine.
2. Add a Go-side selector that recognizes an optional module artifact and
   delegates only when an engine is explicitly enabled.
3. Keep `/dev/sync`, persisted manifests, generated client bindings, database
   transactions, and WebSocket protocol unchanged while the selector is
   introduced.
4. Move HTTP/Postgres/change-feed ownership into the Rust host after a
   differential harness proves Go and module-engine behavior agree.

The Rust crates intentionally do not edit or import the existing Go runtime,
so this checkpoint is safe to land before the integration selector exists.
