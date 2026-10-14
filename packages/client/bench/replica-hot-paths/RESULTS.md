# Replica hot-path measurements

Baseline `136d596a74bd8b0105822d5dadfe669eb2f92f47`; candidate `36c04f271ec512d4ab40facc32b60ff6d1c07971`, source dirty: false.

2026-10-14T04:16:28.784Z. v26.8.2; Chromium 149.0.7827.55. 3 paired trials; 15 samples per hot path after warmups.

Times include awaited transaction publication and synchronous subscriber notification. Resident counts span 20 tables; tasks contain 60% of the rows and 50 columns. 50 windows, 52 subscribers. Chromium pages are cross-origin isolated for high-resolution timers; capture samples amortize 20 operations. Persistence is a no-op in hot-path scenarios. IndexedDB startup uses real Chromium IndexedDB with a prepopulated, warm browser/storage cache.

Near-zero snapshot timings are timer-quantized; an infinite upper interval means that resampled candidate medians reached zero, not infinite throughput. This workstation also ran unrelated jobs; the paired trials and broad intervals expose some, but not all, scheduling noise.

95% intervals are deterministic paired hierarchical bootstrap intervals for the median speed ratio, resampling trial pairs and samples within each trial. Three pairs cannot characterize all machine noise. No CI gate or statistical significance claim.

Resumed unchanged production bundle hashes. Completed Node and 10k Chromium pairs retained. All 50k and 100k Chromium pairs use fresh pages in a persistent context with a read-only seeded database per size; all sizes use the same bounded sorted fixture seeding. No 50k samples from the earlier isolated setup were retained.

Collection began 2026-10-14T03:59:47.769Z; final resumed runner began 2026-10-14T04:16:28.784Z.

## node

| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |
|---:|---|---:|---:|---:|
| 10,000 | First-use offline query with cold equality index | 0.564 / 1.357 | 0.787 / 1.724 | 0.72x [0.62, 1.02] |
| 10,000 | Single-row transaction + notify | 0.479 / 1.130 | 0.042 / 0.050 | 11.44x [10.73, 18.96] |
| 10,000 | Single-row transaction + 250-row subscriber snapshot | 0.587 / 0.724 | 0.210 / 0.265 | 2.80x [2.65, 3.11] |
| 10,000 | 1,000-row transaction + notify | 20.889 / 23.015 | 5.759 / 8.202 | 3.63x [3.09, 3.94] |
| 10,000 | Portable reducer + optimistic apply + replay | 0.708 / 0.850 | 0.225 / 0.360 | 3.14x [2.75, 3.50] |
| 10,000 | Unrelated transaction + offline query | 0.598 / 0.658 | 0.056 / 0.077 | 10.76x [8.01, 12.83] |
| 10,000 | Matching transaction + offline query | 1.048 / 1.832 | 0.125 / 0.165 | 8.37x [6.61, 11.32] |
| 10,000 | Unfiltered sorted offline query, limit 50 | 3.677 / 4.498 | 0.211 / 0.256 | 17.42x [15.64, 19.80] |
| 10,000 | entityRows + cached window snapshot | 0.011 / 0.012 | 0.0049 / 0.0058 | 2.27x [2.10, 2.43] |
| 10,000 | Capture reducer tables without reading them | 0.222 / 0.441 | 0.0011 / 0.0017 | 197.66x [176.93, 222.04] |
| 10,000 | JSON working-set decode + activation + disposal | 86.784 / 99.712 | 49.536 / 58.056 | 1.75x [1.56, 1.94] |
| 50,000 | First-use offline query with cold equality index | 2.890 / 4.226 | 3.049 / 5.818 | 0.95x [0.73, 1.00] |
| 50,000 | Single-row transaction + notify | 2.115 / 2.843 | 0.044 / 0.055 | 47.97x [46.31, 54.20] |
| 50,000 | Single-row transaction + 250-row subscriber snapshot | 2.232 / 3.004 | 0.221 / 0.250 | 10.10x [9.29, 10.82] |
| 50,000 | 1,000-row transaction + notify | 23.594 / 25.185 | 5.964 / 7.955 | 3.96x [3.86, 4.22] |
| 50,000 | Portable reducer + optimistic apply + replay | 2.464 / 3.919 | 0.222 / 0.298 | 11.11x [10.43, 12.95] |
| 50,000 | Unrelated transaction + offline query | 2.790 / 3.396 | 0.061 / 0.094 | 45.75x [40.31, 66.67] |
| 50,000 | Matching transaction + offline query | 5.059 / 6.354 | 0.226 / 0.272 | 22.36x [19.14, 25.71] |
| 50,000 | Unfiltered sorted offline query, limit 50 | 28.468 / 33.574 | 0.951 / 1.078 | 29.94x [26.90, 34.04] |
| 50,000 | entityRows + cached window snapshot | 0.152 / 0.205 | 0.112 / 0.141 | 1.37x [0.46, 1.60] |
| 50,000 | Capture reducer tables without reading them | 1.175 / 3.083 | 0.0010 / 0.0013 | 1233.72x [1157.04, 1675.66] |
| 50,000 | JSON working-set decode + activation + disposal | 420.725 / 509.460 | 253.412 / 301.734 | 1.66x [1.51, 1.81] |
| 100,000 | First-use offline query with cold equality index | 5.990 / 9.954 | 6.309 / 11.022 | 0.95x [0.75, 1.32] |
| 100,000 | Single-row transaction + notify | 4.199 / 5.382 | 0.052 / 0.068 | 80.40x [70.59, 92.47] |
| 100,000 | Single-row transaction + 250-row subscriber snapshot | 4.349 / 5.400 | 0.216 / 0.262 | 20.12x [16.71, 21.86] |
| 100,000 | 1,000-row transaction + notify | 24.323 / 27.226 | 6.229 / 8.618 | 3.90x [3.52, 4.35] |
| 100,000 | Portable reducer + optimistic apply + replay | 4.513 / 5.534 | 0.227 / 0.311 | 19.91x [17.32, 21.99] |
| 100,000 | Unrelated transaction + offline query | 5.964 / 7.833 | 0.088 / 0.117 | 67.54x [61.32, 116.82] |
| 100,000 | Matching transaction + offline query | 14.658 / 17.523 | 0.404 / 0.552 | 36.27x [32.47, 40.00] |
| 100,000 | Unfiltered sorted offline query, limit 50 | 80.900 / 96.801 | 1.957 / 2.112 | 41.34x [32.72, 50.41] |
| 100,000 | entityRows + cached window snapshot | 0.289 / 0.311 | 0.202 / 0.278 | 1.43x [1.18, 1.51] |
| 100,000 | Capture reducer tables without reading them | 2.602 / 5.264 | 0.0010 / 0.0017 | 2676.77x [2195.76, 3412.55] |
| 100,000 | JSON working-set decode + activation + disposal | 911.137 / 1047.929 | 536.288 / 616.142 | 1.70x [1.61, 1.77] |

| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |
|---:|---:|---:|---:|---|
| 10,000 | 66.89 / 66.96 | 66.98 / 67.12 | 1.00x [0.93, 1.00] | unavailable |
| 50,000 | 160.02 / 159.83 | 160.09 / 159.97 | 1.00x [0.96, 1.00] | unavailable |
| 100,000 | 280.44 / 280.31 | 280.53 / 280.45 | 1.00x [0.97, 1.00] | unavailable |

Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k contexts and databases are isolated; 50k/100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.

## chromium

| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |
|---:|---|---:|---:|---:|
| 10,000 | First-use offline query with cold equality index | 0.410 / 0.560 | 0.580 / 0.875 | 0.71x [0.67, 0.79] |
| 10,000 | Single-row transaction + notify | 0.260 / 0.725 | 0.055 / 0.070 | 4.73x [3.42, 8.09] |
| 10,000 | Single-row transaction + 250-row subscriber snapshot | 0.690 / 1.145 | 0.140 / 0.305 | 4.93x [1.86, 5.56] |
| 10,000 | 1,000-row transaction + notify | 12.000 / 14.660 | 6.295 / 7.840 | 1.91x [1.60, 2.25] |
| 10,000 | Portable reducer + optimistic apply + replay | 0.390 / 1.365 | 0.230 / 0.335 | 1.70x [1.28, 2.05] |
| 10,000 | Unrelated transaction + offline query | 0.405 / 0.460 | 0.050 / 0.065 | 8.10x [6.83, 9.44] |
| 10,000 | Matching transaction + offline query | 0.895 / 1.050 | 0.085 / 0.145 | 10.53x [6.65, 11.24] |
| 10,000 | Unfiltered sorted offline query, limit 50 | 3.085 / 4.075 | 0.235 / 0.245 | 13.13x [11.98, 14.86] |
| 10,000 | entityRows + cached window snapshot | 0.015 / 0.025 | 0.0000 / 0.0050 | Infinityx [2.00, Infinity] |
| 10,000 | Capture reducer tables without reading them | 0.190 / 0.249 | 0.0010 / 0.0017 | 190.50x [182.00, 201.75] |
| 10,000 | JSON working-set decode + activation + disposal | 68.615 / 79.675 | 40.880 / 49.435 | 1.68x [1.57, 1.92] |
| 10,000 | IndexedDB working-set activation + disposal | 379.505 / 408.205 | 353.360 / 386.655 | 1.07x [0.99, 1.14] |
| 50,000 | First-use offline query with cold equality index | 2.385 / 4.490 | 3.540 / 6.800 | 0.67x [0.37, 1.09] |
| 50,000 | Single-row transaction + notify | 1.700 / 7.390 | 0.065 / 0.270 | 26.15x [16.57, 42.50] |
| 50,000 | Single-row transaction + 250-row subscriber snapshot | 1.185 / 2.190 | 0.160 / 0.335 | 7.41x [4.61, 9.54] |
| 50,000 | 1,000-row transaction + notify | 15.630 / 19.720 | 7.935 / 10.755 | 1.97x [1.16, 2.51] |
| 50,000 | Portable reducer + optimistic apply + replay | 1.340 / 2.760 | 0.230 / 2.110 | 5.83x [3.95, 7.13] |
| 50,000 | Unrelated transaction + offline query | 2.395 / 3.005 | 0.060 / 0.085 | 39.92x [28.21, 52.60] |
| 50,000 | Matching transaction + offline query | 3.905 / 5.730 | 0.200 / 0.290 | 19.52x [12.44, 24.11] |
| 50,000 | Unfiltered sorted offline query, limit 50 | 23.405 / 29.480 | 1.135 / 1.510 | 20.62x [15.48, 24.41] |
| 50,000 | entityRows + cached window snapshot | 0.050 / 0.055 | 0.015 / 0.030 | 3.33x [3.33, 5.00] |
| 50,000 | Capture reducer tables without reading them | 1.023 / 1.188 | 0.0010 / 0.0020 | 1023.24x [666.71, 1030.25] |
| 50,000 | JSON working-set decode + activation + disposal | 351.485 / 414.200 | 218.665 / 307.475 | 1.61x [1.27, 1.85] |
| 50,000 | IndexedDB working-set activation + disposal | 2058.640 / 2244.600 | 2216.215 / 3189.790 | 0.93x [0.75, 1.06] |
| 100,000 | First-use offline query with cold equality index | 5.955 / 9.045 | 7.025 / 8.645 | 0.85x [0.63, 1.31] |
| 100,000 | Single-row transaction + notify | 2.315 / 3.190 | 0.030 / 0.315 | 77.17x [53.00, 80.67] |
| 100,000 | Single-row transaction + 250-row subscriber snapshot | 2.055 / 3.775 | 0.130 / 0.365 | 15.81x [11.09, 20.50] |
| 100,000 | 1,000-row transaction + notify | 15.365 / 20.555 | 7.145 / 13.085 | 2.15x [1.42, 2.39] |
| 100,000 | Portable reducer + optimistic apply + replay | 2.145 / 4.710 | 0.225 / 0.335 | 9.53x [6.62, 11.92] |
| 100,000 | Unrelated transaction + offline query | 6.530 / 9.100 | 0.085 / 0.095 | 76.82x [65.88, 124.55] |
| 100,000 | Matching transaction + offline query | 10.425 / 13.535 | 0.420 / 0.650 | 24.82x [22.67, 28.46] |
| 100,000 | Unfiltered sorted offline query, limit 50 | 69.185 / 82.455 | 2.120 / 2.855 | 32.63x [28.10, 35.46] |
| 100,000 | entityRows + cached window snapshot | 0.195 / 0.225 | 0.030 / 0.130 | 6.50x [3.83, 8.00] |
| 100,000 | Capture reducer tables without reading them | 3.120 / 16.751 | 0.0010 / 0.0018 | 3119.50x [2172.41, 16216.27] |
| 100,000 | JSON working-set decode + activation + disposal | 829.910 / 3478.275 | 473.505 / 1884.490 | 1.75x [1.33, 2.34] |
| 100,000 | IndexedDB working-set activation + disposal | 4447.530 / 20947.945 | 4088.595 / 17369.125 | 1.09x [1.01, 1.44] |

| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |
|---:|---:|---:|---:|---|
| 10,000 | 16.77 / 16.92 | 16.96 / 17.09 | 0.99x [0.99, 0.99] | 8,8,8 / 0,0,0 |
| 50,000 | 81.24 / 80.70 | 81.28 / 80.76 | 1.01x [1.01, 1.01] | 5,6,5 / 0,0,0 |
| 100,000 | 162.48 / 161.49 | 162.52 / 161.54 | 1.01x [1.01, 1.01] | 6,7,6 / 0,0,0 |

Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k contexts and databases are isolated; 50k/100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.

## CPU profile self time

Profiles run for at least 250 ms after scenario warmups. Sampling interval is 100 microseconds. Different builds execute different operation counts; self time below is normalized per operation. Profiling is intrusive and these self times are attribution evidence, not the unprofiled latency measurements. Raw Chrome profiles are in `.generated/`.

| Rows | Build | Scenario | Operations | Top self-time functions, microseconds per operation |
|---:|---|---|---:|---|
| 10000 | baseline | single | 845 | `residentRows:baseline.mjs:7292` 183.8; `(program)::0` 82.3; `(garbage collector)::0` 72.8; `applyTransactionNow:baseline.mjs:8069` 16.5; `setResidentRow:baseline.mjs:7303` 8.4 |
| 10000 | baseline | burst | 3 | `applyTransactionNow:baseline.mjs:8069` 87464.7; `(program)::0` 15090.0; `(garbage collector)::0` 1262.7; `setResidentRow:baseline.mjs:7303` 1245.7; `structuredClone::0` 1054.3 |
| 10000 | candidate | single | 10000 | `applyTransactionNow:candidate.mjs:8536` 3.8; `(program)::0` 3.4; `storageSnapshotFrom:candidate.mjs:9234` 3.0; `(garbage collector)::0` 2.2; `set entities:candidate.mjs:7626` 1.9 |
| 10000 | candidate | burst | 55 | `setResidentRow:candidate.mjs:7588` 1226.0; `structuredClone::0` 847.5; `(program)::0` 665.1; `applyTransactionNow:candidate.mjs:8536` 346.3; `cloneRow:candidate.mjs:9167` 339.1 |
| 50000 | baseline | single | 127 | `(program)::0` 3331.6; `residentRows:baseline.mjs:7292` 1259.4; `setResidentRow:baseline.mjs:7303` 328.3; `(garbage collector)::0` 303.6; `structuredClone::0` 20.6 |
| 50000 | baseline | burst | 8 | `applyTransactionNow:baseline.mjs:8069` 25213.5; `(program)::0` 9666.1; `setResidentRow:baseline.mjs:7303` 1389.0; `residentRows:baseline.mjs:7292` 1092.4; `structuredClone::0` 1006.0 |
| 50000 | candidate | single | 3938 | `(program)::0` 108.5; `setResidentRow:candidate.mjs:7588` 18.5; `applyTransactionNow:candidate.mjs:8536` 10.7; `(garbage collector)::0` 7.7; `storageSnapshotFrom:candidate.mjs:9234` 5.4 |
| 50000 | candidate | burst | 26 | `(program)::0` 3253.3; `setResidentRow:candidate.mjs:7588` 2311.5; `structuredClone::0` 1589.3; `windowsForRow:candidate.mjs:7723` 826.5; `applyTransactionNow:candidate.mjs:8536` 744.2 |
| 100000 | baseline | single | 52 | `(program)::0` 5064.6; `residentRows:baseline.mjs:7292` 2513.6; `(garbage collector)::0` 2203.8; `applyTransactionNow:baseline.mjs:8069` 33.7; `cloneRow:baseline.mjs:8619` 12.3 |
| 100000 | baseline | burst | 4 | `(program)::0` 72529.5; `applyTransactionNow:baseline.mjs:8069` 72434.0; `residentRows:baseline.mjs:7292` 2203.0; `setResidentRow:baseline.mjs:7303` 1823.5; `structuredClone::0` 1269.8 |
| 100000 | candidate | single | 5143 | `(program)::0` 109.3; `(garbage collector)::0` 20.0; `applyTransactionNow:candidate.mjs:8536` 5.8; `storageSnapshotFrom:candidate.mjs:9234` 3.2; `set liveQueries:candidate.mjs:7671` 2.4 |
| 100000 | candidate | burst | 25 | `(program)::0` 22979.8; `(garbage collector)::0` 2389.6; `setResidentRow:candidate.mjs:7588` 1821.5; `structuredClone::0` 1058.5; `windowsForRow:candidate.mjs:7723` 848.3 |

## Scope and limits

- Public row arrays remain caller-owned. Internal cached arrays share frozen rows. Entity table iteration and matching-query scans still scale with the requested result corpus.
- Table forks copy page/bucket directories, not row entries. Updates copy touched 256-slot pages; inserts/deletes also copy touched hash buckets. Tombstones compact when slots exceed twice live rows and 1,024. Large memberships use sorted shared-ID arrays and binary search.
- At most four equality indexes build in 512-row turns; synchronous queries use the full evaluator until completion. First-use query timings include this fallback. Warm query measurements wait for index completion. Equality buckets retain IDs without a duplicate per-row value map. Resident budgets estimate metadata, and retained UI windows/pending reducers can exceed those budgets. Active pins are never evicted.
- Point, batch and watched-window reads refresh LRU. Whole-table inspection does not promote every scanned row, keeping scans from replacing the hot working set.
- Top-k removes full sorting for limited windows, but filtering, exact counts and resolving reference sort fields still scan the candidate corpus. Search normalization is evaluated on that corpus.
- Startup timestamps measure a cold LocalReplica with loaded JavaScript and warm browser/storage caches, not a cold OS disk or network login.
- The optimistic scenario runs a real portable reducer twice around an authoritative row commit. Durable outbox admission, socket transport and server replay are tested elsewhere and excluded from these timings.
- Cached window snapshot costs are combined with a public entityRows call. React rendering and paint are not benchmarked.

## Round 2 findings

The two 100k blockers are fixed in these measurements. Chromium burst p95 is 13.085 ms versus 20.555 ms for the original-code baseline in this run and 55.710 ms for the round 1 candidate. Burst median speed ratio is 2.15x, with a 95% interval of [1.42, 2.39]. Retained post-update heap is 161.54 MiB versus 162.52 MiB baseline and 183.27 MiB for the round 1 candidate. Candidate burst long-task counts are zero in all three trials at every size.

IndexedDB at 100k improves from 4447.530 to 4088.595 ms median, with a 1.09x interval of [1.01, 1.44]. This is a warm-browser/storage activation result, including disposal, not cold disk startup.

### Heap attribution

CDP constructor and record-shape counts are in `heap-summary.json`. All three snapshots are from the same post-10k-update checkpoint. V8 omits immediate numeric fields from reference edges; record shapes therefore match their reference properties. These counts and shallow bytes explain allocation cost; they are not dominator retained sizes.

| Snapshot | Plain Object count | Plain Object shallow MiB |
|---|---:|---:|
| Original-code baseline | 100,533 | 2.686 |
| Round 1 candidate | 500,570 | 12.986 |
| Round 2 candidate | 103,215 | 2.739 |

Round 1's two treap nodes, row entry and recency record per resident disappear. Round 2 has 407 slot-page records and 2,241 hash-bucket records. ID-to-slot buckets, shared 256-slot pages, numeric recency slots and shared row/execution versions replace those records. Large membership windows use sorted ID-reference arrays rather than a map entry for every resident. Keys remain shared strings where string lookup is required; no wire IDs change.

CDP burst sampling also drops GC self time from round 1's 13.72 ms per operation to 2.39 ms per operation, and removes `put` from the profile. New top application self times are `setResidentRow` 1.82 ms, `structuredClone` 1.06 ms and `windowsForRow` 0.85 ms per operation. Profiles are intrusive, have different operation counts, and include substantial `(program)` time. Use the unprofiled table for latency claims.

### Preserved wins and uncertainty

`round-comparison.json` independently resamples paired trials from each round. It reports both baseline-normalized gains and absolute candidate latency changes. Of 63 existing scenario/size/mode comparisons, 30 candidate-latency intervals show faster medians, 32 span unchanged latency, and one is timer-quantized. None shows a slower candidate interval wholly below 1. This is limited evidence from three trial pairs on a shared machine, not proof of identical production tails.

Two normalized gains decreased beyond their cross-round intervals. Node 50k sorted queries have a normalized gain change of 0.71 [0.53, 0.85], but candidate latency change is 1.08 [0.92, 1.22]. Chromium 50k single-row writes have a normalized gain change of 0.52 [0.23, 1.00], but candidate latency change is 0.62 [0.50, 1.36]. Their baselines also got faster. Report these changes rather than claiming every speed-ratio headline increased. At 100k, limited sort candidate latency is 0.96x round 1 [0.83, 1.18]; its original win remains, with no established candidate regression.

### Remaining costs

- 10k retained heap is 17.09 MiB versus 16.96 MiB, about 0.13 MiB extra. At 50k it falls to 80.76 versus 81.28 MiB. The 100k target is met; small-fixture fixed overhead remains.
- First-use equality queries deliberately use complete evaluation while indexes build in 512-row turns. With a warm resident row array, 10k median is 0.580 versus 0.410 ms, 0.71x [0.67, 0.79]. At 100k, median/p95 is 7.025/8.645 versus 5.955/9.045 ms, 0.85x [0.63, 1.31]. Extra index work is bounded per turn; filtering/exact counts remain synchronous and corpus-dependent. Warm query samples wait for construction.
- 50k IndexedDB median is 2216.215 versus 2058.640 ms, 0.93x [0.75, 1.06], and sampled p95 is higher. The interval spans 1; this remains an uncertain startup cost, despite the improved 100k result.
- Heap checkpoints precede equality-query warmup. Up to four lazy equality indexes still retain matching ID buckets after construction, without the old duplicate per-row value map. No active-query lifetime tracking was added.
- Forks copy compact page/bucket directories. Inserts/deletes copy touched buckets; growth, tombstone compaction and large membership replacement still do corpus work. Many overlapping large windows add binary-search work. Recency slots reuse holes but retain array high-water capacity until clear. Residency byte budgets remain estimates; active pins stay protected.

### Validation

All 334 focused tests pass: client 242, local-runtime 16, React 76. Only the original eight client files, `local-runtime/src/read-view.test.ts` and `react/src/index.test.tsx` were run, with one worker. Client/local-runtime/React typechecks and builds pass. React uses `NODE_OPTIONS=--no-experimental-webstorage` for Node 26's environment issue. No full suites, Cargo runs, merge, npm publication, runtime/protocol/schema change or Whagons contract bump.

This shared workstation ran unrelated jobs. Baseline/candidate order alternates by trial. The resumed run verified unchanged production bundle hashes, retained completed Node/10k pairs, and reran all 50k/100k Chromium pairs with fresh pages and shared read-only databases per size. All sizes use bounded, sorted seeding with normal lookup indexes populated. Seeding is unmeasured. See raw samples, setup notes and bundle hashes in `results.json`.
