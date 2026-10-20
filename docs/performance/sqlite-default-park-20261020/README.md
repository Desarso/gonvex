# SQLite default: park decision, 2026-10-20

SQLite must not become the default storage in the measured architecture. Preserve the implementation unmerged. No new runs or PRs are authorized by this checkpoint.

## Final bounded heap experiment

One main-thread snapshot per arm, at strict twelve-real-name reload endpoint plus two seconds, with the same saved fixture, viewport, passive HTTP cache and private runtime. IDB is immutable diet-current f0bb8d5f9, not a claim about latest staging. Native includes the subscriber-remount correctness fix 58a83fbc. Snapshot capture can trigger GC; these are live heap graphs, not the earlier unforced heap-used numbers or renderer private memory. Offline strong-edge dominator attribution uses property/constructor heuristics; it is not precise allocation-stack/module accounting. In particular, IDB's zero tagged replica roots is not proof of zero application row objects.

| Main-thread live allocation | IDB MiB | Native MiB |
| --- | ---: | ---: |
| Reachable live heap | 51.62 | 38.94 |
| ExternalStringData, predominantly script source | 13.47 | 7.34 |
| InstructionStream | 7.44 | 6.79 |
| BytecodeArray | 2.19 | 1.79 |
| Native tagged replica exclusive retention | n/a | 0.0047 |
| Native tagged replica payload reachability upper bound | n/a | 0.323 |

The main-thread reduction is 12.69 MiB, predominantly code-related rather than removable replica data. There is no measured 40 MiB main-thread replica graph that could be moved into the worker. React, application/framework state, generated catalogs, i18n and code remain necessary on main; no trustworthy exact per-module split is claimed for minified objects.

Raw snapshots remain private on disk: runs/main-heap-r3-idb/main.heapsnapshot, SHA256 c8359afb31b134485b4fff4ddf5fcbfe8d9de5a45d1fc10d5418e0716df69697, 81,577,371 bytes; runs/main-heap-r4-native/main.heapsnapshot, SHA256 90f281b1788e6afb003c10aa49677005007b5bc855de53e7a6c141f226de4df0, 61,399,672 bytes. Sanitized analysis accompanies this memo. Do not upload raw snapshots, auth profiles or environment files.

## Speed, tails, failures and memory

Earlier 20-profile interleaved caller-paint cohort: 18 qualified, two failed native profiles, ten intended per arm. Native measured efc12228 before FTS/cache/remount fixes; it is not a qualification of the final head. Each qualified profile measures cold and same-profile reload. Saved IDB reference is f0bb8d5f9.

| Cohort measure | IDB | Native |
| --- | ---: | ---: |
| Cold grid p50, ms | 2287 | 5428 |
| Reload grid p50, ms | 1074 | 1081 |
| Reload grid p95, ms | 1896 | 12712 |
| Reloads above requested 1200 ms target | 1/10 | 3/8 |
| Cold settled renderer private p50, MiB | 214 | 266 |
| Reload settled renderer private p50, MiB | 316 | 365 |

The 12.7s reload had no row/frame resume replay; machine I/O stalls and a 789ms SAH open contributed, but full attribution was not established. Never discard it as machine noise. Earlier scheduler experiments caused dependency hangs and were reverted to FIFO. A mislabeled unknown-commit error was a stalled read deadline, not evidence of a lost reducer commit. The sealed boot mount/remount races were subsequently reproduced with public-API failing tests and fixed. Latest gate: 101 files / 1316 tests plus build and Node checks green. Final native-only heap capture cold5519/reload815ms, errors[]; a single successful pair is not a tail/correctness cohort.

Separate like-for-like first warm settle, before forced GC: IDB main JS used84.08/total130.34MiB versus native86.52/129.05; renderer private299.43 versus381.67MiB. Native additionally has owner JS total37.50MiB, WASM16MiB and shared-router JS total1.50MiB. These are overlapping/different accounting categories, not quantities that can be summed into exact RSS. After five native reloads renderer private reached581.21MiB and dropped to224.51MiB after diagnostic GC; IDB after one reload dropped to136.08MiB. The post-GC cycle counts differ and do not establish a matched memory win or normal operation below200MB.

The rough fixed-footprint arithmetic is unfavorable: about55MiB owner/WASM/router heap capacity against12.7MiB less main live heap, roughly42MiB net additional capacity. This is a planning estimate, consistent with an added worker cost, not an exact explanation of the82.24MiB renderer-private difference. Neither supplied data attribution nor timing supports default rollout.

## What would have to change

A positive small-tenant memory case requires incremental storage/worker footprint below the roughly12.7MiB main-thread saving, with headroom for churn and embedder costs. A target under roughly10MiB is motivated by that measured saving, not an independent product limit. The pinned WASM linear memory alone is16MiB, so this architecture cannot meet it by trimming SQL cache or a few JS rows. It requires a substantially smaller engine/runtime, avoiding a second full generated execution/code closure, or replacing enough existing main-thread work to offset it. Large tenants might amortize fixed cost, but 31k/100k end-to-end memory/speed/offline qualification is unfinished and no such win is claimed.

Cold also needs a structural ingest change: measured roughly48MiB writes, hundreds of transactions and155–192 FULL flushes, with about2.4s flush time in the initial diagnostic. Existing coalescing did not achieve tens of commits. Bulk FTS-at-publication is not after-paint indexing and has not closed the gap. NORMAL was exploratory only and remains unapproved for shipping; FULL intent/outbox/receipt durability remains required. It would be misleading to promise parity in a few small tuning iterations.

## Current IndexedDB correctness follow-ups

- Already merged, do not duplicate: framework #129 / 8bb037ea invalidates retained snapshots after row hydration; framework #130 / 94d3c72f preserves durable predictions when offline reducer re-execution cannot run. App #1429 provides offline cached complete-corpus selection and cache-first/deferred optional imports. Confirm actual published package/app provenance separately; merge is not npm release evidence.
- Candidate small legacy SDK PR: c20e896's outbox-only guarantee refuses a replayable intent when durable enqueue is unavailable, rather than reporting success with memory-only acceptance. Isolate the outbox changes and public tests from SQLite migration, review existing online fallback expectations and current-main equivalent before proposing it. This applies to today's IDB durability boundary even if migration is parked.
- Candidate separate runtime optimization, not a demonstrated legacy correctness fix: c0193908 current-digest Ready-only acknowledgment avoids replay of unchanged authorized replica sets. Both benchmark arms used it, including legacy staging.29. Preserve projection, authorization, epoch/history and explicit-integrity fallbacks. Rebase/review and focused real-PG/runtime qualification are required before any small PR; do not conflate native local resume/certificate changes with the legacy client.
- SQLite-only, not current-IDB fixes: sealed-page initial credential preservation, mount/remount retention and caller display hold; SQLite broker read-vs-write timeout classification and receipt retry test; SQLite local header-only verification, resume-offer persistence and typed staging/FTS fixes. The native feed delta-before-Ready commit b9a243ce also lives only in sqlite-native-feed.ts. Do not market these as IDB repairs.
- Late legacy outbox discovery/import after SQLite cutover protects migration only; with default migration parked there is no reason to ship its watcher alone.

No PR opened at this checkpoint. Implementation branches and this sanitized decision record are preserved remotely; private measurement profiles, full REPORT and logs remain under /mnt/data/work/wh-memory-impl.
