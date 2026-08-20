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

`gonvex-module-runtime-v8` executes TypeScript modules: it loads the bundled
JavaScript ESM artifact into deno_core isolates and dispatches calls to the
exported handlers a manifest names. `gonvex-module-runtime-wasm` is still a
seam for a future Wasmtime Component Model adapter, and its `ModuleEngine`
implementation fails explicitly until that engine is linked.

`gonvex-server-host` provides atomic module generations. New calls acquire a
lease on the active generation; publishing generation N+1 prevents new calls
from entering N, while existing calls finish. Retired generations are reaped
only after their in-flight lease count reaches zero.

## TypeScript/V8 execution

`V8ModuleEngine::from_artifact` takes a `ModuleArtifact` whose payload is one
self-contained JavaScript ESM bundle. No module loader is installed, so an
artifact that still contains unresolved imports fails to load rather than
fetching anything at call time. `prewarm` starts an isolate eagerly, which turns
a broken bundle into a publish-time error instead of a first-call error.

`invoke` resolves the export a `FunctionContract` names, in this order: the
manifest's `export` or `handler` metadata, the function path as a flat export,
the function path walked as a dotted path across re-exported namespaces
(`messages.list`), and finally the last path segment. The export may be a
function or an object with a callable `handler`/`default`. Anything else is a
load error, and a missing export is `FunctionNotFound` — the engine never
answers with a placeholder value.

Handlers receive `(ctx, args)` and return a JSON-encodable value. Capability
separation is structural in both directions: the host's grant is intersected
with what the function kind may ever reach, and the context object simply lacks
the methods for capabilities that were not granted.

| Kind    | Context surface                                   |
| ------- | ------------------------------------------------- |
| Query   | `ctx.db.query`                                    |
| Reducer | `ctx.db.query`, `ctx.db.write`                    |
| Action  | `ctx.runReducer`, `ctx.fetch`, `ctx.storage.call` |
| Http    | same as Action                                    |

Those methods are the only host surface: they funnel into a single op that
re-checks the capability and the host-call budget in Rust before forwarding a
`HostCall`, so reaching the raw op from module code grants nothing extra. A
denied capability or an exhausted budget fails the whole invocation even if the
module catches the error, so a module cannot silently probe for authority it was
not granted.

Every call is bounded by `V8Config`:

* `execution_timeout`, shortened by the invocation's own deadline, enforced by a
  watchdog thread that terminates the isolate — a JavaScript loop that never
  yields is still interruptible;
* `max_heap_bytes` per isolate, enforced by a near-heap-limit callback that
  terminates the call instead of aborting the process;
* `max_host_calls` and `max_result_bytes`, both checked in Rust;
* `isolate_pool_size`, which bounds live isolates and therefore concurrent
  module calls;
* `recycle_after_calls`, after which an isolate is retired. Any termination,
  bridge failure, or unexplained dispatch failure retires it immediately.

Identity, capabilities, the host channel, and the remaining budgets live in
`OpState` for exactly one call and are taken back out afterwards; the dispatcher
itself is the bootstrap script's completion value rather than a global. Nothing
about a tenant is reachable from a JavaScript global, which is what makes reusing
an isolate across tenants safe. Module-level JavaScript state does survive
between calls in a reused isolate, so modules must stay stateless — correctness
never depends on isolate-local state.

An isolate runs on its own thread, since `JsRuntime` is not `Send`. The invoking
task keeps the `&dyn ModuleHost` and answers host calls over a channel, so the
host is never required to be `'static` and the transaction stays with the host.

deno_core is pinned to one version in `Cargo.toml`: its op, module, and event
loop APIs move between minor releases, so upgrading is a deliberate change.

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
