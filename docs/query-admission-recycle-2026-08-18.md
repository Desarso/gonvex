# Unified query admission and reconnect-safe worker recycling — 2026-08-18

CI runs of whagons5-client (multi-tenant Playwright suite plus a `/dev/sync`
deploy) degraded TTLU on the shared dev runtime. Two runtime defects composed:

1. The 32-slot rerun limiter only covered `invalidate`/`recover` executions.
   Initial subscription executions, initial sync snapshots, and attach-time
   visibility-context reads all hit the database at unbounded concurrency, so a
   reconnect burst starved the DB admission budget (12 total/8 per-pool in the
   certified configuration) and reactive work queued behind the pileup.
2. Every successful `/dev/sync` recycled the runtime worker — even when the
   bundle hash was unchanged — and the old worker's exit dropped every
   WebSocket simultaneously, manufacturing a synchronized reconnect wave and
   a cold rehydration spike per CI deploy.

## What changed

### Query admission (`server/internal/server/query_admission.go`)

Every database-backed query execution now passes one classed admission
controller:

- **Classes**: reactive (invalidate/recover reruns, authoritative sync
  recomputation), foreground (internal/sandbox one-shot queries), bootstrap
  (initial subscription executions, initial sync snapshots, attach-time
  visibility-context reads).
- **Global cap**: `GONVEX_SUBSCRIPTION_RERUN_CONCURRENCY` (default 32; `0`
  still disables limiting entirely, preserving embedded/test behavior).
- **Bootstrap share**: `GONVEX_QUERY_BOOTSTRAP_CONCURRENCY` (default a quarter
  of the global cap). Hydration is guaranteed this share under contention and
  may borrow idle capacity when no reactive/foreground work waits, so a quiet
  environment's cold reload still hydrates at full speed.
- Tenant round-robin fairness within each class; foreground aging (250ms) so
  sustained reactive load cannot starve interactive queries; cancelled waiters
  leave their queue immediately.
- Permits are leaf-scoped around single executions — no caller holds one while
  acquiring another, so the controller cannot deadlock.
- Cache hits never consume permits (`invalidate` always bypasses the cache, so
  reactive correctness is unchanged).

Metrics: the legacy `subscriptionRerunQueueDepth`/`subscriptionRerunQueueWaitMs`
fields keep working, and `/dev/metrics` gains a `queryAdmission` block with
permits, active counts, per-class depth/admitted/waited/cancelled/max-wait,
tenants queued, largest tenant queue, and reactive-delayed-by-bootstrap.

### Worker recycling (`server/internal/supervisor`, `/dev/sync`)

- The recycle header is set only when a sync actually replaced an
  already-loaded plugin module, or a load attempt failed (a failed
  `plugin.Open` can poison the process). First loads and unchanged bundle
  hashes no longer recycle; the supervisor triggers on the header alone.
- On a genuine recycle the supervisor activates the replacement first, then
  sends SIGTERM: a supervised worker drains its WebSockets spread across
  `GONVEX_WS_DRAIN_WINDOW` (default 12s) with close code 1012 (service
  restart), idle connections first, giving in-flight mutations/actions until
  the end of the window. SIGINT and the 20s hard deadline keep the existing
  immediate-shutdown behavior; standalone (unsupervised) runtimes keep their
  historic SIGTERM semantics.

## Validation

### Synthetic CI-shaped burst (gonvex-load)

Profile: certified Whagons subscription mix, 24 users / 40 connections /
2,021 subscriptions, 3s connection burst, mutations during hydration,
certified runtime configuration (rerun concurrency 64, DB 12/8). Reports in
`tmp/load-reports/admission-*`.

Single tenant (baseline → patched): TTLU p95 112.1→108.3ms, max 170.5→154.7ms,
hydration unchanged, zero errors both.

11 tenants (the observed CI incident shape), baseline → patched:

| metric | baseline | patched |
|---|---|---|
| TTLU max | 136.8ms | 75.1ms (−45%) |
| TTLU p95 | 74.4ms | 70.1ms |
| hydration avg / max | 448ms / 3202ms | 284ms / 2566ms |
| subscriptions / errors | 2021 / 0 | 2021 / 0 |

Patched admission counters for that run: 435 bootstrap admissions (116 waited,
max 247ms — bounded exactly as designed), 150 reactive admissions with zero
waits and zero delayed-behind-bootstrap.

### Real whagons5-client CI (main PR gate)

Full `test:ci` pipeline from client `origin/main` (bf3cc3827) against local
runtimes, CI-parity stack (legacy bridge, e2e preview build):

- Baseline: 151/151 E2E in 8m21s. Patched: 150/151 in 8m32s; the one failure
  (a live-propagation spec with a 60s budget) passed 3/3 reruns in isolation
  on the patched runtime — parallel-load flake, not a regression.
- DB pool waits 3477→3327, DB budget waits 4350→4142.
- Patched admission during the CI run: 15,408 bootstrap admissions with 89
  waits (max 79ms); 8,575 reactive with 112 waits (max 403ms); 30 reactive
  executions delayed behind bootstrap. The controller is effectively invisible
  at CI load and only bites during bursts.

### Supervised recycle smoke

`GONVEX_RELOAD_SUPERVISOR=1` runtime: two consecutive `gonvex dev --once`
deploys of an unchanged bundle kept the same worker PID (previously every
deploy recycled); a one-line function change replaced the worker. The
staggered 1012 drain (idle-first, in-flight writes waited on) is pinned by
`TestDrainWebSocketsSendsStaggered1012Closes`.

## Compatibility

- `GONVEX_SUBSCRIPTION_RERUN_CONCURRENCY` keeps its meaning; `0` still means
  unlimited. Default caps (32 global / 8 bootstrap) sit above the default DB
  budget (20 total / 16 per-pool), so no existing environment can be
  under-utilized by admission.
- Rolling upgrades degrade to current behavior in both directions: an old
  worker under a new supervisor still sets the recycle header on every sync;
  a new worker under an old supervisor still recycles on success. An old
  worker receiving the new SIGTERM drain signal shuts down immediately, which
  the supervisor's existing hard deadline already assumed.
