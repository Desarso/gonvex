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

The original outcome persists with the intent before its prediction publishes. Retirement writes a recovery receipt before removing that intent. Failed receipt persistence leaves the canonical entry intact. Control-plane credentials and online-only outcomes are not copied into that receipt store. Server replay and the existing durable checkpoint retire predictions; no new scheduler or wire codec is introduced.

## Rollback and limits

Participating tabs hold Web Locks presence registrations. A second foreground participant makes both clients settle outstanding RPCs, close the worker, and use the legacy host against the same IDB journal. The worker holds a separate exclusive ownership lock before opening storage. This is presence/fallback, not follower sharing or leader handoff. Older SDK tabs do not implement this presence protocol; existing IDB fences and peer synchronization remain in force.

To turn the opt-in off, await `client.close()` before constructing the existing legacy client with the same stores and contract. A crash releases worker ownership; `recover()` reloads the canonical journal. Reducer RPC timeouts abort the worker and preserve the original command identity for retry. A worker initialization timeout reports failure rather than replaying potentially blocked code on the UI. Browser suspension can delay recovery and cutover.

Custom adapters/executors, SQLite, multi-tab engine sharing, paged UI hooks, native migration, and new server capabilities are outside this phase. Existing storage migrations must be statically supplied to a custom worker entry. No Whagons files, application contract, runtime protocol, package version, or published package changed.

## Validation and measurements

Focused client, React, and generator tests and the changed-package typechecks/builds passed. The benchmark under `packages/client/bench/engine-worker/` uses persistent browser profiles and production assets. It records raw samples, hardware/load, main-thread script time, long tasks, RAF intervals, a synthetic paint proxy, and deterministic bootstrap confidence intervals. The isolated fixture uses the existing protocol and storage format; it is not a Rust runtime benchmark or a battery measurement.

The strict zero-growth bundle target is not met. The default entry imports no worker implementation, but shared compatibility support adds 2,295 minified bytes and 590 gzip bytes against the settled baseline. Worker/proxy chunks and complete sizes are in `bundle-sizes.json`. Engine mode stays opt-in.

| Production library asset | Minified bytes | Gzip bytes |
| --- | ---: | ---: |
| Default entry, settled baseline | 350,383 | 98,587 |
| Default entry, this change | 352,678 | 99,177 |
| Engine proxy entry | 15,013 | 4,916 |
| Worker entry | 2,867 | 1,336 |

Entry sizes exclude separately loaded shared chunks. The largest shared implementation chunk is 265,980 bytes (70,820 gzip); Dexie is 99,729 bytes (32,620 gzip), and the RPC chunk is 7,968 bytes (2,978 gzip). The default entry's dependency graph does not include these engine entrypoints. This is a reproducible minified library comparison, not the eventual Whagons application bundle.

Chromium 149.0.7827.55 ran five warmup pairs and 20 measured pairs, alternating mode order, on a Ryzen 7 5800X / 16 logical CPUs / 32 GiB Linux workstation. The prepared canonical task store had 50,000 rows at 2,554 encoded bytes per row. The largest recorded one-minute load was 22.4% of logical CPU count; the cutoff was 50%. No builds ran alongside measurements. Runtime source at measurement was `6462f7e`; raw samples and metadata are in `results.json`, with computed statistics in `summary.json`.

| Workload | Legacy main script median | Worker main script median | Reduction | Worker / legacy ratio, bootstrap 95% CI | Legacy → worker wall median |
| --- | ---: | ---: | ---: | --- | ---: |
| Cached 50k-row cold start | 430.4 ms | 39.1 ms | 90.9% | 0.0908 [0.0848, 0.0934] | 2,348 → 2,422 ms |
| 1,000-row burst | 77.8 ms | 25.2 ms | 67.6% | 0.3237 [0.3126, 0.3312] | 1,542 → 1,544 ms |
| 20k-row background paged snapshot | 575.2 ms | 3.4 ms | 99.4% | 0.0058 [0.0042, 0.0068] | 22,547 → 22,546 ms |
| Admit/drain 500 independent intents | 1,172.9 ms | 840.1 ms | 28.4% | 0.7163 [0.6896, 0.7384] | 49,469 → 73,843 ms |

**Phase 2 gate: PASS in this Chromium fixture.** Worker mode recorded zero page long tasks above 50 ms during the burst, durable admitted-command crash/retry preserved one journal entry and its result, and all three reducer network globals were denied while the private transport reconnected. Both foreground clients selected legacy mode in the final two-tab fixture. Legacy also had zero burst long tasks: the settled baseline already passed that part of the gate.

Burst p95 frame interval was 16.7 ms in both modes. Scheduled-click to two-RAF paint-proxy p95 was 33.5 → 32.9 ms; no meaningful input-latency improvement is claimed from that small difference. Across all 20 cold runs, legacy recorded 20 long tasks, maximum 291 ms, versus none in worker mode. Across snapshots it recorded 40, maximum 542 ms, versus none in worker mode. These observers cover all page tasks and therefore upper-bound SDK tasks; no separate trace attribution was collected.

The broader cold-start target fails: worker p95 correct cached paint was 2,481 ms, not 500 ms, and wall median increased about 3.2%. The independent-drain target also fails: total wall median increased about 49.3%, and neither mode drained within five seconds. Aggregate admission median was 25,432 → 49,932 ms; post-admission median was 23,820 → 23,740 ms. Per-round average admission per intent was 50.9 → 99.9 ms; this is not an individual-command p95. The additional receipt lookup/journal work is a likely overhead source, but this run does not isolate its cost. Edits accumulate across rounds, so the raw temporal trend and drain bootstrap interval are descriptive rather than a fixed-size production latency estimate.

This benchmark compares legacy and worker mode in the same settled implementation. It models auth/reducer RTT and snapshot page throughput, not a deployed runtime or whole-connection network shaper. Cached online startup is measured; offline-only cold start, true first install, dependent chains, total worker CPU, heap, battery, and device suspension are not. Full-result subscription delivery still carries the full result; the snapshot case deliberately uses background retention and a bounded mounted UI view.

WebKit 26.5 passed the smaller 1,000-row parity run (one warmup pair, one measured pair), including all three denied globals, crash/retry, private reconnect, and both-tab legacy fallback. Its script-time metric is unavailable because CDP is Chromium-specific. Firefox 151.0 was attempted, but Playwright 1.61.0 exited on an internal `FFPage._onWebSocketOpened` assertion during the first worker connection, before parity or measured samples. Firefox is **not validated**; `firefox-parity.json` records the failure. WebKit is not Safari/iOS device QA.

Commands completed successfully:

```sh
pnpm --filter @gonvex/client exec vitest run src/engine/engine.test.ts --maxWorkers=1
pnpm --filter @gonvex/react exec vitest run src/engine.test.tsx --maxWorkers=1
node --test packages/gonvex/test/local-bindings.test.mjs
pnpm --filter @gonvex/client typecheck
pnpm --filter @gonvex/react typecheck
pnpm --filter @gonvex/local-runtime typecheck
pnpm --filter @gonvex/cli typecheck
pnpm --filter @gonvex/client build
pnpm --filter @gonvex/react build
pnpm --filter @gonvex/local-runtime build
pnpm --filter @gonvex/cli build
```

There are 14 client tests, one React test, and one generator integration test, including compilation of its generated static worker. No full suite or cargo workspace run was needed. The local-runtime implementation is reused unchanged and was typechecked/built for compatibility.

## Files and follow-up

- Client: package subpath exports; `auth-scope.ts`, `client-error.ts`; `index.ts`, `local-replica.ts`, `outbox.ts`, `browser-upgrades.ts`; the `engine/` proxy, host, worker, RPC, patches, receipts, and focused tests.
- React: `src/index.tsx` and `src/engine.test.tsx`.
- Generator: `src/local-bindings.ts` and `test/local-bindings.test.mjs`.
- Measurements: `packages/client/bench/engine-worker/` source, raw browser samples, summary, and bundle sizes; this report.

Whagons needs an explicit feature-flagged factory switch using the integration code above, matching generated worker assets and the existing stores/contract. Audit direct `localReplica`/`useGonvexClient` consumers before that switch; convert their reads to awaited engine reads or engine hooks. Keep worker and reducer-artifact assets in the application's offline precache/update policy. This PR does not make that application change or bump its client contract.

Receipt retention is additional durable recovery metadata and currently has no pruning policy: pruning an original outcome needs an explicit retry-retention contract. Offline admission overhead, suspended workers/tabs, stale deployed worker assets, and mixed old/new tabs remain rollout risks. Full-result subscriptions can still exceed a small UI budget. No worker CPU, memory, battery, production-runtime, dependent-intent-chain, or Safari/iOS suspension improvement is claimed from these browser fixtures.
