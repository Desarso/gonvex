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

At 50k and 100k rows, fresh pages share one persistent browser context and a read-only seeded IndexedDB database per size across builds/trials. At 10k, each fixture uses a fresh context/database. This avoids repeating expensive unmeasured secondary-index insertion; all normal lookup keys remain populated. Each startup creates a new LocalReplica and reads the same full corpus. CDP memory is page JavaScript heap, excluding the shared native IndexedDB backend.

The round 1 run at `4e4b043` retained earlier Node and 10k/50k Chromium fixtures seeded in a single unmeasured transaction, then used bounded sorted setup for 100k. Round 2 retains only completed Node/10k pairs on unchanged production bundle hashes, uses bounded sorted setup at every size, and reruns all 50k/100k Chromium pairs with the shared read-only fixtures described above. Raw output records collection/resume dates and setup notes.

Round 2 adds a first-use offline query scenario. It clears the query-result cache and equality indexes before each call, with a warmed resident row array. Equality indexes build in 512-row turns; synchronous queries evaluate the full table until construction finishes. The runner explicitly waits for construction before measuring warm unrelated/matching queries. These timings distinguish cold index work from steady query work; they do not measure a cold browser/module cache.

Resident tables now use copy-on-write 256-slot pages and ID-to-slot hash buckets. Forks copy page/bucket directories, rather than individual row entries. Hash directories start at 16 buckets and grow at bounded load thresholds. Large membership windows use sorted ID-reference arrays and binary search; small windows retain reverse maps. Recency and resident row/execution versions share integer slots. Initial population, directory growth, membership replacement and tombstone compaction still perform corpus work.

After a completed run:

```sh
node packages/client/bench/replica-hot-paths/compare-rounds.mjs
python packages/client/bench/replica-hot-paths/heap-report.py \
  packages/client/bench/replica-hot-paths/.generated/baseline-100000-updated.heapsnapshot \
  packages/client/bench/replica-hot-paths/.generated/round1-candidate-100000-updated.heapsnapshot \
  packages/client/bench/replica-hot-paths/.generated/candidate-100000-updated.heapsnapshot
```

The round comparison reads the original committed measurements from `4e4b043` and the latest `results.json`. It reports both normalized baseline/candidate gains and absolute candidate latency changes with independent-round hierarchical bootstrap intervals. Different machine load and three trial pairs limit cross-round conclusions. The heap report groups shallow bytes by constructor and recognizes replica record shapes from their property names; it does not calculate dominator retained sizes. Use CDP post-GC heap readings for retained-heap conclusions. Preserve the original candidate snapshot under the shown `round1-` name before rerunning; raw snapshots remain ignored.
