# LocalReplica hot paths

Run from the repository root after `pnpm install --frozen-lockfile`:

```sh
TMPDIR=/home/gabriel/.cache/sdk-wave1/tmp nice -n 10 node --expose-gc packages/client/bench/replica-hot-paths/run.mjs
```

The runner builds `origin/staging` and the current source with the same esbuild settings, alternates baseline/candidate order, and runs Node followed by real headless Chromium. It requires the workspace Playwright Chromium installation. It runs one workload at a time and is deliberately outside CI. No npm packages are published. Baseline source is extracted under this worktree, without another checkout or shared/symlinked node_modules. Test files are excluded from extraction.

Optional controls:

```sh
REPLICA_BENCH_SIZES=10000 REPLICA_BENCH_TRIALS=1 REPLICA_BENCH_SAMPLES=5 node --expose-gc packages/client/bench/replica-hot-paths/run.mjs --node-only
REPLICA_BASELINE_REF=<commit> node --expose-gc packages/client/bench/replica-hot-paths/run.mjs --browser-only
node packages/client/bench/replica-hot-paths/summarize.mjs
```

Default sizes are 10k, 50k and 100k resident rows across 20 tables, with 50 windows and 52 subscribers. Tasks have 50 columns and represent 60% of the corpus. Statuses, priorities, members, tags, taskUsers and customFieldValues represent the supporting tables. Each build gets an independent fixture. Warmups precede each scenario. Browser pages use COOP/COEP isolation for high-resolution timers; capture timings amortize batches of 20 operations. Three paired trials provide 45 samples per hot scenario and 15 per startup scenario. Medians, p95 and 95% paired hierarchical bootstrap intervals use a fixed bootstrap seed.

Scenarios include single-row, single-row with a subscriber reading a 250-row visible snapshot, and 1,000-row authoritative transactions through notification, a real portable reducer prediction and replay, offline query calls after unrelated and matching writes, a sorted limited query, public entityRows plus cached window snapshot reads, lazy execution capture, JSON decode plus activation, and real Chromium IndexedDB activation. Startup timings also include the task-row count check and replica disposal. Unmeasured IndexedDB fixtures use the adapter's normal versioned record builder and batches of 128 native puts in bounded transactions and primary-key order, preserving lookup indexes while avoiding production write throttling during setup. Hot-path storage calls are no-ops so storage latency cannot hide replica costs. Each fixture is measured after load and exactly 10,000 row updates before the timed scenarios. Burst long tasks are observed with PerformanceObserver. CPU profiles sample single-row and burst workloads for at least 250 ms. Post-GC CDP heap readings cover every browser fixture; full CDP heap snapshots cover the largest size in the first trial before and after the updates.

`results.json` contains raw measurements and profile summaries. `summary.json` and `RESULTS.md` contain medians, p95, intervals, memory and limitations. Large raw `.cpuprofile` and `.heapsnapshot` files stay under ignored `.generated/` and can be opened in Chrome DevTools. The generated source/bundles also stay there. Browser memory numbers include the fixture's serialized startup input. Node heap comparisons share a process and include compiled-module history, so use Chromium for memory conclusions.

No claim is made about React paint, network, server runtime, durable-outbox IO, a cold OS disk, or production battery life. Speed ratios below 1 mean a regression. Read the full tables and memory tradeoffs before interpreting the headline numbers.

Use `--resume` to reuse completed fixture pairs after interruption. It verifies source/bundle hashes and trial settings; fixture seeding is unmeasured.

At 100k rows, fresh pages share one persistent browser context and the same read-only seeded IndexedDB database across builds/trials. Lower sizes use fresh contexts/databases. This avoids repeating expensive unmeasured secondary-index insertion; all normal lookup keys remain populated. Each startup creates a new LocalReplica and reads the same full corpus. CDP memory is page JavaScript heap, excluding the shared native IndexedDB backend.

The committed run resumed unchanged production bundles: Node and 10k/50k Chromium fixtures were collected earlier, with the same normal records seeded in an unmeasured single transaction. The final 100k fixtures use the bounded sorted setup and shared persistent profile described above. Raw output records the collection start/resume dates and this setup difference.
