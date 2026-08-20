# Gonvex v2 Rust module host

This workspace is the language-neutral application-module seam and the host
process that executes it. It does not replace the Go runtime: Go still owns
HTTP, Postgres, tenancy, identity, the change feed, and every transaction.

Go projects keep the path they have today:

```text
gonvex dev → /dev/sync → Go SourceBundle → projectbundle.Loader → Go App
```

A project whose manifest ships a TypeScript module artifact takes the bridge:

```text
gonvex dev → /dev/sync → manifest.Module (JS + hash + function metadata)
        ↓
Go runtime.SyncManifest → moduleengine.RemoteEngine
        ↓  length-prefixed JSON over a Unix socket (loopback TCP fallback)
gonvex-module-host: load → warm isolates → activate generation
        ↓
V8ModuleEngine  ·  GenerationRegistry
        ↓  host calls back over the same connection, while the call runs
Go QueryCtx / ReducerCtx / ActionCtx
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
only after their in-flight lease count reaches zero. `ModuleRegistry` holds one
such registry per module id, plus the staging slot a generation sits in between
being loaded and being activated.

`gonvex-module-host` is the binary. It listens on one local endpoint, serves
every project from one process, and owns the registries and the isolates.

## Transport

One length-prefixed JSON frame is a 4-byte big-endian length followed by that
many bytes of UTF-8 JSON. Nothing is line-delimited and nothing is unbounded: a
reader knows a frame's size before it allocates, and a frame past
`--max-frame-bytes` ends the connection with `frame_too_large` rather than
growing until the process dies.

Every frame names its shape in `type`, and requests carry an `id` and an
optional `deadlineUnixMs`:

| Direction | `type` | Meaning |
| --------- | ------ | ------- |
| host → runtime | `ready` | protocol and version, once per connection |
| runtime → host | `request` | `ping`, `load`, `activate`, `describe`, `invoke`, `unload`, `shutdown` |
| host → runtime | `response` / `error` | the answer to one request id |
| host → runtime | `hostCall` | an operation a running invocation asked for |
| runtime → host | `hostResponse` / `hostError` | the answer to one host call |
| runtime → host | `cancel` | the caller gave up; the invocation is dropped |

Host calls are tagged with the invocation id they belong to, so a module's
database reads are answered by the exact call's transaction and identity. The
runtime's failure vocabulary is a small set of codes (`function_not_found`,
`wrong_function_kind`, `budget_exceeded`, `generation_conflict`,
`module_not_loaded`, `deadline_exceeded`, …) that the Go engine maps back onto
its own dispatch errors.

## Generations

`load` decodes the artifact, verifies the bundle's SHA-256 against the hash the
build recorded, lowers the declarative function metadata into
`FunctionContract`s — the `handler` and `export` names land in
`FunctionContract.metadata`, which is what export resolution reads — and warms
every isolate in the pool. Only then does `activate` make the generation the
target for new calls, and only strictly newer generations may activate.

The swap is atomic and invisible: calls already running finish on the old
generation, the reaper waits for them on a blocking thread and then drops the
engine, and the Go runtime never recycles its worker for a module swap, so
connected WebSocket clients are not disturbed by a developer saving a file.

## Shutdown

`shutdown`, SIGTERM, SIGINT, and EOF on stdin (the orphan guard a supervised
host is started with) all begin the same bounded drain: the listener stops
accepting, every module is retired, in-flight calls get the grace window, and a
call still running past it is reported rather than silently abandoned.

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

Handlers receive `(ctx, args)` and return a JSON-encodable value. The context is
the `@gonvex/module-sdk` shape — `account`, `tenant`, `member`, `now`, plus the
capability methods. Capability separation is structural in both directions: the
host's grant is intersected with what the function kind may ever reach, and the
context object simply lacks the methods for capabilities that were not granted.

| Kind    | Context surface                                                     |
| ------- | ------------------------------------------------------------------- |
| Query   | `ctx.db.query`                                                      |
| Reducer | `ctx.db.query`, `ctx.db.insert`, `ctx.db.update`, `ctx.db.delete`   |
| Action  | `ctx.runReducer`, `ctx.fetch`, `ctx.storage`                        |
| Http    | same as Action                                                      |

A Query's reads run inside a read-only transaction the Go host opens for the
invocation, so a write is refused by Postgres itself. A Reducer's calls run on
the exact transaction the Go host already holds for that mutation, so the
module's writes and the host's bookkeeping commit together or not at all. An
Action has no database handle at all: durable change re-enters through
`runReducer`.

Writes never travel as SQL text. `insert`, `update` and `delete` name a table, a
key and a JSON object; the Go host validates and quotes the identifiers and
binds every value as a parameter, so there is no place for a module to
interpolate one. No database URL or credential is ever exposed to JavaScript.

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

## Running the host

```sh
cargo run -p gonvex-module-host -- --listen unix:/tmp/gonvex-module-host.sock
```

The Go runtime normally starts one itself. It is opt-in and fallback safe:

| Variable | Meaning |
| -------- | ------- |
| `GONVEX_MODULE_HOST_ENABLED` | defaults on; off means no host is ever started |
| `GONVEX_MODULE_HOST_BINARY` | the executable to supervise; defaults to `gonvex-module-host` on `PATH` |
| `GONVEX_MODULE_HOST_ENDPOINT` | set alone, an externally managed host to connect to |
| `GONVEX_MODULE_HOST_ISOLATE_POOL_SIZE`, `..._MAX_CONCURRENT_CALLS`, `..._EXECUTION_TIMEOUT`, `..._MAX_FRAME_BYTES`, `..._START_TIMEOUT`, `..._REQUEST_TIMEOUT`, `..._DRAIN_TIMEOUT`, `..._SHUTDOWN_TIMEOUT` | bounds |

With neither a binary on `PATH` nor an endpoint, nothing starts and Go modules
keep working exactly as before; a project that ships a TypeScript artifact fails
its sync with an explicit error instead of loading nothing.

## Coexistence

1. The Go `SourceBundle` and `projectbundle.Loader` remain the engine for every
   Go project. `Runtime.SyncManifest` chooses the module host only when a
   manifest carries a TypeScript module artifact.
2. `/dev/sync`, persisted manifests, generated client bindings, database
   transactions, and the WebSocket protocol are unchanged.
3. HTTP, Postgres, the change feed, and commit ownership stay in Go. Moving any
   of them here waits on a differential harness proving the two agree.
