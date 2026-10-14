# Dedicated client engine, Phase 2

The opt-in `@gonvex/client/engine` factory hosts the settled Gonvex client in a dedicated worker. WebSocket transport, server JSON decode, integrity verification, canonical IDB storage, subscription evaluation, portable reducers, and the existing outbox scheduler run there. The default `GonvexClient` keeps its synchronous replica API.

This implements the Phase 2 row in [the merged design](client-sync-engine.md). Its audit at `95da6c9` identified transport JSON decode on the caller's thread, scope hydration/decode, and the distinction between a reducer-only worker and a socket-owning sync engine. This branch starts at the settled `11820f6` hot paths; those earlier optimizations are retained rather than reimplemented.

The UI retains mounted results and shared projected rows. Unmount removes their row and negative-entry metadata. Updates use changed-field patches when small and decoded, acknowledged packets otherwise. A committed watch batch completes its result/cache swaps before notifying any subscriber. Large explicitly mounted results remain large; this phase does not introduce pagination or silently trim full-result hooks.

## API and integration

Generation retains `localRuntime.create()` and adds `gonvex-engine.ts`, `gonvex-engine-worker.ts`, and a static portable reducer binding. Use the bundler-generated factory so the worker and its dependencies remain separate assets.

```ts
import { createEngineClient } from "@gonvex/client/engine";
import { createEngineWorker, engineWorkerUrl, engineManifest } from "./gonvex/_generated/gonvex-engine";
import { localRuntime } from "./gonvex/_generated/local-runtime";

const client = await createEngineClient(existingWebSocketUrl, {
  engine: {
    workerUrl: engineWorkerUrl,
    workerFactory: createEngineWorker,
    artifactHash: engineManifest.artifactHash,
  },
  auth: existingAuthOptions, // includes the active project, tenant, account identity and UI fetchToken
  fallbackBinding: localRuntime,
  storage: {
    replicaName: existingReplicaName,
    outboxName: existingOutboxName,
    contract: existingClientContract,
    initialVersion: existingInitialVersion,
  },
  residency: existingReplicaResidency,
});
```

`GonvexProvider`, watch-based hooks, reducer/action/query hooks, and managed auth providers accept the engine client. `useEntityState` distinguishes pending from a completed lookup; `useEntity` retains its undefined loading result. Retained/Live Query state has an explicit optional `pending` flag. Imperative worker reads use `await client.readEntity(...)` or `readEntities(...)`. `useGonvexEngineClient` exposes the engine API. Direct synchronous `localReplica` and legacy `useGonvexClient` consumers require an explicit migration.

Provider token fetching stays on the UI and crosses only the private endpoint. Discovery messages contain no credentials. Engine initialization validates the RPC protocol and reducer artifact; commands are bound to the authenticated journal scope. A scope/account change clears UI results before accepting new data. Same-account refresh retains results.

For application retries, keep the command or use the error's retained request:

```ts
const command = client.createCommand();
await client.reducer(ref, args, { engineCommand: command });
// Reuse command after reopening; it includes timestamp, entropy, artifact and scope.
// For RPC timeout/unavailability:
await client.retryReducer(error);
```

The original outcome persists as flat metadata with the intent before its prediction publishes. Recovery also understands the earlier nested outcome format. Retirement writes a recovery receipt before removing that intent. Failed receipt persistence leaves the canonical entry intact. Control-plane credentials and online-only outcomes are not copied into that receipt store. Server replay and the existing durable checkpoint retire predictions; no new scheduler or wire codec is introduced.

## Rollback and limits

Participating tabs hold Web Locks presence registrations. A second foreground participant makes both clients settle outstanding RPCs, close the worker, and use the legacy host against the same IDB journal. The worker holds a separate exclusive ownership lock before opening storage. This is presence/fallback, not follower sharing or leader handoff. Older SDK tabs do not implement this presence protocol; existing IDB fences and peer synchronization remain in force.

To turn the opt-in off, await `client.close()` before constructing the existing legacy client with the same stores and contract. A crash releases worker ownership; `recover()` reloads the canonical journal. Reducer RPC timeouts abort the worker and preserve the original command identity for retry. A worker initialization timeout reports failure rather than replaying potentially blocked code on the UI. Browser suspension can delay recovery and cutover.

Custom adapters/executors, SQLite, multi-tab engine sharing, paged UI hooks, native migration, and new server capabilities are outside this phase. Existing storage migrations must be statically supplied to a custom worker entry. No Whagons files, application contract, runtime protocol, package version, or published package changed.

## Round 2 validation and measurements

Round 1's Chromium main-thread reductions passed its Phase 2 gate, but its 500-intent wall median regressed from 49,469 to 73,843 ms. Default gzip grew by 590 bytes. Firefox failed in Playwright before validation. The original samples remain in `packages/client/bench/engine-worker/results.json`; these are historical results, not the current gate.

At Round 1 commit `310d3a4`, `engine/receipts.ts:find` performed an additional complete scoped outbox load for every missing recovery receipt. `engine/host.ts` then waited for the entire local admission and an additional receipt write before starting the next command. The canonical entry already contained the outcome. The diagnostic old-source run confirms 500 receipt reads, 500 admission receipt writes, and 1,000 scoped journal reads over 249,500 records for 500 worker calls. Its legacy mode made 500 scoped reads over 124,750 records. The corresponding early Round 2 diagnostic has no receipt reads or writes during newly generated command admission. The old diagnostic UI mean was 25.98 ms versus 25.66 ms inside the host, leaving approximately 0.31 ms for transfer and scheduling; storage and admission serialization dominated. Other shared-store reads vary with in-flight replica work; these diagnostic runs are not headline timing comparisons.

New commands generated by the UI proxy skip recovery lookup. Explicit commands and retries retain lookup, argument checks, original entropy/time, account fencing, and canonical-journal deduplication. Entity reads stay ready through same-account authenticated/restored transitions; account/scope changes and unavailable sessions still force a cold read. Fresh calls start synchronously within a received batch, matching the legacy preparation schedule. Retained command identities also share the batch lease, with their lookup/start order preserved. A start barrier keeps later RPC packets behind a cohort awaiting ownership. Explicit commands use an ordered lookup/start lane that releases before durable acknowledgement so reducer preparation can pipeline. `EngineRpc` groups same-microtask admissions into packets of at most 1,024 commands, with individual request identities, replies and errors. This is enabled only when the host advertises `admissionBatch: 1` in its private init reply. An optional `admissionBatchLimit` advertises the larger limit. Missing limit retains 128-call packets; missing batching support uses individual envelopes. Small RPC replies keep the ordered sender lane and do not wait for their own delivery acknowledgement. They cannot overtake preceding watch batches. Watch events and large packets remain acknowledged.

Local commands in one received packet share the existing delivery/local/intent leases. The journal cache reloads once at that ownership transfer, rather than once per command. An explicit retry already awaiting lookup prevents this optimization so later fresh calls cannot jump it. Each admission remains serialized inside the lease and receives its own durable acknowledgement. The established local admission, storage fences, causal dependency scheduler, `reducer.callMany`, and authoritative replay remain in use. Counter reservation and canonical intent insertion still persist per command before its prediction or acknowledgement. This change batches retirement receipt writes per tick, rather than staging dependent predictions before a bulk canonical commit. That retains the existing atomic admission semantics and older SDK coordination.

Recovery receipts are capped at 1,024 retired commands across the journal's scopes and expire after 30 days. A private receipt-store upgrade prunes existing unbounded history atomically before updating the retained records. Ongoing pruning frees up to 64 older receipts at a time, keeping the hard cap while avoiding a prune on every server acknowledgement. Expired records are checked on activity at least daily, and lookups enforce the 30-day limit immediately. One durable timestamp floor prevents a pruned outcome from silently becoming a second admission. Explicit retries outside retained recovery history fail closed; outstanding canonical intents retain their original outcomes without that limit. A newly generated command can still be admitted at the same timestamp as a pruning boundary. Clock changes can conservatively reject an ambiguous explicit retry. No application contract or server protocol version changes.

The default minified library asset is byte-identical to the settled `11820f6` baseline, including its SHA-256. The engine-only connection, admission, cold-read and close hooks live behind the opt-in entry. Default runtime code and storage adapters no longer carry the Round 1 engine branches. The current staging comparison includes the later optional membership-store work; its larger size is not an optimization attributable to Round 2.

| Library asset | Minified bytes | Gzip bytes |
| --- | ---: | ---: |
| Default, settled baseline | 350,383 | 98,587 |
| Default, Round 2 | 350,383 | 98,587 |
| Default, recorded staging `1f2a649` | 377,362 | 103,651 |

The hash is `f31f43b017247a5a6977f244267dccf315e2df32b365b8a85e51de2f6680df0b`. Opt-in entries and shared chunks are reported in `bundle-sizes.json`. The bundle guard verifies equality and rejects gzip growth. These are library sizes with identical dependency builds, not a Whagons application build.

<!-- ROUND2_MEASUREMENTS -->

Firefox's Playwright 1.61 assertion reproduces with a standalone worker opening a WebSocket and no Gonvex SDK. `_onWebSocketOpened` expects a page request record that worker events do not always provide. The benchmark's Firefox-only loader guard skips only untracked request/response events, preserves tracked page instrumentation, checks the exact patched fragment, and does not edit installed dependencies. No product transport workaround ships.

The machine is shared. A first full attempt crossed the 50%-of-logical-CPUs load cutoff after warmups, before any measured samples. It is archived as `round2-chromium-load-abort.json` and excluded. The final harness checks load before modes, every ten seconds during long workloads, and after each workload. WAIT_LOAD retries an entire excluded pair after load drops; no overloaded mode enters the measured samples. Without it, a partial run records failure.

Focused validation passes 32 client engine tests, one React engine test and the generator integration test. The tests cover negotiated batching/fallback, reply ordering, pipelining without awaiting the first durable ack, interrupted batches, lost replies, original command IDs on recovery, canonical durability, online server outcomes, retirement races, receipt transaction failures, migration and retention. The browser fixture kills a worker after the first durable acknowledgement in a 32-command batch, then retries all commands with their original identities. Client and React typechecks/builds pass. Generator validation compiles the generated static worker. No full suite, cargo workspace run, npm publish or PR merge was performed.

```sh
pnpm --filter @gonvex/client exec vitest run src/engine/engine.test.ts --maxWorkers=1
pnpm --filter @gonvex/react exec vitest run src/engine.test.tsx --maxWorkers=1
node --test packages/gonvex/test/local-bindings.test.mjs
pnpm --filter @gonvex/client typecheck
pnpm --filter @gonvex/client build
pnpm --filter @gonvex/react typecheck
pnpm --filter @gonvex/react build
node packages/client/bench/engine-worker/bundles.mjs
```

## Files and follow-up

- Client: package subpath exports; `auth-scope.ts`, `client-error.ts`; compile-time engine metadata in `index.ts`/`outbox.ts`; `engine/` proxy, host, worker, RPC, patches, receipts, connection/admission hooks, read/close helpers, and focused tests. The default `local-replica.ts` and `browser-upgrades.ts` runtime changes from Round 1 were removed.
- React: `src/index.tsx` and `src/engine.test.tsx`.
- Generator: `src/local-bindings.ts` and `test/local-bindings.test.mjs`.
- Measurements: `packages/client/bench/engine-worker/` source, raw browser samples, summary, and bundle sizes; this report.

Whagons needs an explicit feature-flagged factory switch using the integration code above, matching generated worker assets and the existing stores/contract. Audit direct `localReplica`/`useGonvexClient` consumers before that switch; convert their reads to awaited engine reads or engine hooks. Keep worker and reducer-artifact assets in the application's offline precache/update policy. This PR does not make that application change or bump its client contract.

Recovery metadata has the bounded contract above. Suspended workers/tabs, stale deployed worker assets, and mixed old/new tabs remain rollout risks. The engine-only hooks mirror legacy connection/admission logic and must be kept in sync when those routines change. Full-result subscriptions can still exceed a small UI budget. No worker CPU, memory, battery, production-runtime, dependent-intent-chain, or Safari/iOS suspension improvement is claimed from these browser fixtures.
