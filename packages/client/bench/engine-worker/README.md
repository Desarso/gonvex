# Dedicated engine browser benchmark

Run serially with production-built assets and a persistent Chromium profile. The fixture uses the existing IDB record format, a 50,000-row task store with 50 columns, five related tables, a 250-row visible view, and 1,000 exact-ID subscriptions. Actual row bytes are recorded in the output. Both modes use a 200 MiB resident budget and an effectively unlimited row count.

```sh
export TMPDIR=/home/gabriel/.cache/sdk-wave1/tmp
node packages/client/bench/engine-worker/run.mjs
node packages/client/bench/engine-worker/summarize.mjs
node packages/client/bench/engine-worker/bundles.mjs
```

Defaults are five warmup pairs and 20 measured pairs. Order alternates between legacy and worker. `ITERATIONS`, `WARMUPS`, `SEED_ROWS`, `BROWSER`, `RESULTS`, and `PORT` can select smaller smoke runs. The default port is 48839. Profiles and built assets stay under TMPDIR. Prepared data survives reruns; a changed row count causes reseeding. The fixture advances its revision beyond the stored checkpoint before each run.

The local WebSocket fixture models 50 ms auth/reducer RTT and paces snapshot pages at 20 Mbit/s. It does not represent a deployed Rust runtime, whole-connection network shaping, or Safari on a device. No shared runtime or production data is accessed. Seeding writes the adapter's exact records in bulk outside the measured interval. Measured hydration and commits use the SDK adapter.

Cases measure cached cold paint, a 1,000-row burst over one second plus 500 ms settling, a 20,000-row paged snapshot, and admission/drain of 500 independent queued intents. Snapshot retention stays in the engine; UI interests remain the 250-row view and 1,000 IDs. A full-result hook still returns its full array when mounted.

The measured comparison is the settled legacy implementation and the opt-in engine in the same checkout, with identical adapters, reducer fixtures and residency limits. Bundle comparison separately uses commit `11820f669782589c9d3af66bc7dfbeda2351be27`. Reloads empty JavaScript/UI caches but retain IDB and browser disk caches. Independent edits accumulate in the persistent fixture across rounds; raw drain results include separate admission time so that this trend is visible. This is not a fixed-size, isolated admission microbenchmark.

Chromium CDP `ScriptDuration` measures page script time. `PerformanceObserver` records all page long tasks as an upper bound on SDK tasks. The synthetic button changes its text every 50 ms; scheduled-event to a two-animation-frame boundary is a conservative paint proxy, including timer delay. RAF intervals are recorded separately. Browser version, hardware, OS, commit, encoded row size, normalized load, and raw samples accompany the results. Sampling stops if one-minute load exceeds half the logical CPU count. The summary uses 10,000 deterministic bootstrap resamples of median script-time ratios.

Correctness smoke fixtures attempt fetch, WebSocket, and XMLHttpRequest from a reducer, reconnect the captured private transport, terminate a worker after durable admission and retry its original command, and open a second tab to verify both foreground clients choose legacy mode. The unit tests cover RPC ordering/timeouts, cache clearing, shared row identity, atomic batch publication, and receipt write failure.

Smaller Firefox/WebKit smoke runs can use `BROWSER=firefox` or `BROWSER=webkit`, `ITERATIONS=1 WARMUPS=1 SEED_ROWS=1000 SKIP_SNAPSHOT=1 SKIP_DRAIN=1 RESULTS=<name>.json`. Their script-time values are absent because CDP is Chromium-specific; they are parity checks, not headline performance samples. WebKit does not replace Safari/iOS device QA. The dependent-intent chain, true first install, offline-only cold paint, worker CPU/energy, trace attribution, and device suspension cases remain separate follow-ups.
