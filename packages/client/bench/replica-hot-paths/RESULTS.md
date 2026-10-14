# Replica hot-path measurements

Baseline `136d596a74bd8b0105822d5dadfe669eb2f92f47`; candidate `c660679e9bfa4f75af2646b4dacc081136d1b08e`, source dirty: false.

2026-10-14T04:39:59.048Z. v26.8.2; Chromium 149.0.7827.55. 3 paired trials; 15 samples per hot path after warmups.

Times include awaited transaction publication and synchronous subscriber notification. Resident counts span 20 tables; tasks contain 60% of the rows and 50 columns. 50 windows, 52 subscribers. Chromium pages are cross-origin isolated for high-resolution timers; capture samples amortize 20 operations. Persistence is a no-op in hot-path scenarios. IndexedDB startup uses real Chromium IndexedDB with a prepopulated, warm browser/storage cache.

Near-zero snapshot timings are timer-quantized; an infinite upper interval means that resampled candidate medians reached zero, not infinite throughput. This workstation also ran unrelated jobs; the paired trials and broad intervals expose some, but not all, scheduling noise.

95% intervals are deterministic paired hierarchical bootstrap intervals for the median speed ratio, resampling trial pairs and samples within each trial. Three pairs cannot characterize all machine noise. No CI gate or statistical significance claim.

Fresh complete timing run after integrating the remote catch-up/cache-reclamation fixes. No timing samples retained from the earlier round 2 run. The browser profile was preserved, but the new localhost origin required fresh fixture seeding. Within this run, 10k uses independent contexts/databases; 50k/100k use fresh pages in a persistent context with a read-only seeded database per size. All sizes use bounded sorted seeding with normal lookup indexes.

Collection began 2026-10-14T04:39:59.048Z; all timing samples are from this run.

## node

| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |
|---:|---|---:|---:|---:|
| 10,000 | First-use offline query with cold equality index | 0.594 / 1.480 | 0.758 / 1.312 | 0.78x [0.69, 1.12] |
| 10,000 | Single-row transaction + notify | 0.481 / 1.467 | 0.042 / 0.058 | 11.50x [9.13, 14.46] |
| 10,000 | Single-row transaction + 250-row subscriber snapshot | 0.579 / 0.757 | 0.217 / 0.281 | 2.67x [2.26, 2.90] |
| 10,000 | 1,000-row transaction + notify | 21.090 / 23.480 | 5.946 / 7.653 | 3.55x [3.42, 3.73] |
| 10,000 | Portable reducer + optimistic apply + replay | 0.707 / 0.908 | 0.223 / 0.286 | 3.17x [2.80, 3.35] |
| 10,000 | Unrelated transaction + offline query | 0.574 / 0.597 | 0.053 / 0.066 | 10.89x [10.34, 12.10] |
| 10,000 | Matching transaction + offline query | 1.026 / 1.753 | 0.111 / 0.132 | 9.22x [8.00, 10.08] |
| 10,000 | Unfiltered sorted offline query, limit 50 | 3.808 / 4.591 | 0.207 / 0.218 | 18.40x [17.93, 19.77] |
| 10,000 | entityRows + cached window snapshot | 0.011 / 0.012 | 0.0049 / 0.0061 | 2.29x [2.14, 2.49] |
| 10,000 | Capture reducer tables without reading them | 0.228 / 0.528 | 0.0011 / 0.0014 | 214.92x [205.63, 244.53] |
| 10,000 | JSON working-set decode + activation + disposal | 88.336 / 99.589 | 53.801 / 61.606 | 1.64x [1.57, 1.78] |
| 50,000 | First-use offline query with cold equality index | 2.780 / 5.180 | 2.882 / 4.613 | 0.96x [0.92, 1.09] |
| 50,000 | Single-row transaction + notify | 2.043 / 2.678 | 0.047 / 0.066 | 43.33x [37.38, 70.57] |
| 50,000 | Single-row transaction + 250-row subscriber snapshot | 2.111 / 3.019 | 0.248 / 0.303 | 8.50x [7.69, 8.94] |
| 50,000 | 1,000-row transaction + notify | 21.743 / 23.790 | 6.038 / 8.236 | 3.60x [3.17, 3.93] |
| 50,000 | Portable reducer + optimistic apply + replay | 2.517 / 4.180 | 0.218 / 0.247 | 11.53x [10.91, 12.87] |
| 50,000 | Unrelated transaction + offline query | 2.818 / 4.226 | 0.055 / 0.078 | 51.29x [48.61, 55.79] |
| 50,000 | Matching transaction + offline query | 5.707 / 9.960 | 0.242 / 0.288 | 23.54x [20.64, 29.91] |
| 50,000 | Unfiltered sorted offline query, limit 50 | 27.528 / 33.326 | 0.929 / 1.125 | 29.63x [26.58, 31.40] |
| 50,000 | entityRows + cached window snapshot | 0.136 / 0.162 | 0.111 / 0.131 | 1.23x [0.52, 1.38] |
| 50,000 | Capture reducer tables without reading them | 1.158 / 2.557 | 0.0010 / 0.0013 | 1180.59x [1102.25, 1255.04] |
| 50,000 | JSON working-set decode + activation + disposal | 422.732 / 522.673 | 266.197 / 303.366 | 1.59x [1.49, 1.68] |
| 100,000 | First-use offline query with cold equality index | 5.505 / 9.874 | 6.254 / 11.679 | 0.88x [0.65, 1.07] |
| 100,000 | Single-row transaction + notify | 4.148 / 5.243 | 0.043 / 0.055 | 95.69x [90.23, 109.80] |
| 100,000 | Single-row transaction + 250-row subscriber snapshot | 4.512 / 5.507 | 0.220 / 0.272 | 20.51x [16.34, 22.68] |
| 100,000 | 1,000-row transaction + notify | 26.121 / 28.207 | 6.181 / 7.920 | 4.23x [3.88, 4.68] |
| 100,000 | Portable reducer + optimistic apply + replay | 4.816 / 5.911 | 0.217 / 0.241 | 22.18x [20.78, 24.42] |
| 100,000 | Unrelated transaction + offline query | 5.687 / 8.385 | 0.056 / 0.074 | 101.15x [93.72, 126.44] |
| 100,000 | Matching transaction + offline query | 13.274 / 15.913 | 0.405 / 0.489 | 32.76x [30.57, 36.54] |
| 100,000 | Unfiltered sorted offline query, limit 50 | 72.392 / 98.924 | 1.807 / 2.092 | 40.06x [33.58, 44.42] |
| 100,000 | entityRows + cached window snapshot | 0.247 / 0.291 | 0.202 / 0.327 | 1.22x [0.54, 1.27] |
| 100,000 | Capture reducer tables without reading them | 2.625 / 5.075 | 0.0010 / 0.0024 | 2673.18x [2091.49, 3363.43] |
| 100,000 | JSON working-set decode + activation + disposal | 907.773 / 976.214 | 545.161 / 617.219 | 1.67x [1.58, 1.79] |

| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |
|---:|---:|---:|---:|---|
| 10,000 | 66.90 / 66.97 | 66.98 / 67.13 | 1.00x [0.93, 1.00] | unavailable |
| 50,000 | 160.03 / 159.84 | 160.10 / 159.98 | 1.00x [0.96, 1.00] | unavailable |
| 100,000 | 280.45 / 280.31 | 280.54 / 280.46 | 1.00x [0.97, 1.00] | unavailable |

Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k contexts and databases are isolated; 50k/100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.

## chromium

| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |
|---:|---|---:|---:|---:|
| 10,000 | First-use offline query with cold equality index | 0.435 / 0.620 | 0.565 / 0.870 | 0.77x [0.72, 0.84] |
| 10,000 | Single-row transaction + notify | 0.215 / 0.710 | 0.055 / 0.080 | 3.91x [3.50, 7.33] |
| 10,000 | Single-row transaction + 250-row subscriber snapshot | 0.265 / 1.265 | 0.135 / 0.265 | 1.96x [1.77, 2.04] |
| 10,000 | 1,000-row transaction + notify | 11.895 / 14.060 | 5.810 / 7.440 | 2.05x [1.81, 2.30] |
| 10,000 | Portable reducer + optimistic apply + replay | 0.385 / 1.170 | 0.200 / 0.355 | 1.92x [1.75, 2.14] |
| 10,000 | Unrelated transaction + offline query | 0.430 / 0.535 | 0.045 / 0.065 | 9.56x [9.44, 10.87] |
| 10,000 | Matching transaction + offline query | 0.975 / 1.095 | 0.080 / 0.120 | 12.19x [7.24, 12.31] |
| 10,000 | Unfiltered sorted offline query, limit 50 | 3.110 / 4.125 | 0.215 / 0.425 | 14.47x [7.46, 16.23] |
| 10,000 | entityRows + cached window snapshot | 0.020 / 0.035 | 0.0050 / 0.0050 | 4.00x [2.00, Infinity] |
| 10,000 | Capture reducer tables without reading them | 0.197 / 0.213 | 0.0010 / 0.0015 | 196.75x [192.00, 263.66] |
| 10,000 | JSON working-set decode + activation + disposal | 68.155 / 78.220 | 38.115 / 50.055 | 1.79x [1.55, 1.97] |
| 10,000 | IndexedDB working-set activation + disposal | 347.190 / 400.935 | 321.375 / 346.010 | 1.08x [1.00, 1.12] |
| 50,000 | First-use offline query with cold equality index | 2.085 / 3.425 | 2.225 / 3.660 | 0.94x [0.82, 1.14] |
| 50,000 | Single-row transaction + notify | 1.300 / 6.860 | 0.055 / 0.260 | 23.64x [16.79, 47.09] |
| 50,000 | Single-row transaction + 250-row subscriber snapshot | 1.070 / 1.735 | 0.135 / 0.300 | 7.93x [7.57, 8.04] |
| 50,000 | 1,000-row transaction + notify | 13.275 / 18.010 | 5.590 / 6.910 | 2.37x [2.25, 2.58] |
| 50,000 | Portable reducer + optimistic apply + replay | 1.240 / 2.950 | 0.195 / 0.255 | 6.36x [5.77, 7.24] |
| 50,000 | Unrelated transaction + offline query | 2.085 / 2.530 | 0.045 / 0.055 | 46.33x [46.11, 52.62] |
| 50,000 | Matching transaction + offline query | 3.045 / 4.045 | 0.170 / 0.185 | 17.91x [17.71, 18.03] |
| 50,000 | Unfiltered sorted offline query, limit 50 | 23.255 / 26.665 | 0.955 / 1.020 | 24.35x [23.66, 25.47] |
| 50,000 | entityRows + cached window snapshot | 0.055 / 0.090 | 0.0100 / 0.235 | 5.50x [5.00, 8.00] |
| 50,000 | Capture reducer tables without reading them | 0.998 / 1.073 | 0.0010 / 0.0015 | 998.25x [957.25, 1328.32] |
| 50,000 | JSON working-set decode + activation + disposal | 339.415 / 367.980 | 174.560 / 202.655 | 1.94x [1.81, 2.06] |
| 50,000 | IndexedDB working-set activation + disposal | 1839.270 / 1954.175 | 1722.325 / 1773.380 | 1.07x [1.05, 1.09] |
| 100,000 | First-use offline query with cold equality index | 4.635 / 7.900 | 4.570 / 7.400 | 1.01x [0.89, 1.45] |
| 100,000 | Single-row transaction + notify | 2.040 / 2.910 | 0.040 / 0.120 | 51.00x [33.92, 70.17] |
| 100,000 | Single-row transaction + 250-row subscriber snapshot | 2.085 / 3.620 | 0.140 / 0.385 | 14.89x [8.93, 15.77] |
| 100,000 | 1,000-row transaction + notify | 14.490 / 23.460 | 6.150 / 10.315 | 2.36x [1.97, 2.60] |
| 100,000 | Portable reducer + optimistic apply + replay | 2.395 / 4.225 | 0.185 / 0.235 | 12.95x [10.90, 13.69] |
| 100,000 | Unrelated transaction + offline query | 4.265 / 6.990 | 0.055 / 0.220 | 77.55x [71.08, 95.89] |
| 100,000 | Matching transaction + offline query | 7.085 / 10.180 | 0.360 / 0.735 | 19.68x [18.29, 23.20] |
| 100,000 | Unfiltered sorted offline query, limit 50 | 57.165 / 72.255 | 1.875 / 5.425 | 30.49x [15.35, 32.10] |
| 100,000 | entityRows + cached window snapshot | 0.125 / 0.210 | 0.025 / 0.155 | 5.00x [2.44, 12.33] |
| 100,000 | Capture reducer tables without reading them | 2.346 / 15.361 | 0.0010 / 0.0022 | 2346.00x [1913.19, 9865.93] |
| 100,000 | JSON working-set decode + activation + disposal | 755.865 / 3170.150 | 383.850 / 1704.660 | 1.97x [1.57, 2.10] |
| 100,000 | IndexedDB working-set activation + disposal | 3878.150 / 16698.420 | 3576.915 / 14432.140 | 1.08x [1.05, 1.15] |

| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |
|---:|---:|---:|---:|---|
| 10,000 | 16.77 / 16.92 | 16.96 / 17.10 | 0.99x [0.99, 0.99] | 8,8,8 / 0,0,0 |
| 50,000 | 81.24 / 80.70 | 81.28 / 80.76 | 1.01x [1.01, 1.01] | 5,6,5 / 0,0,0 |
| 100,000 | 162.48 / 161.49 | 162.53 / 161.54 | 1.01x [1.01, 1.01] | 7,7,6 / 0,0,0 |

Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k contexts and databases are isolated; 50k/100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.

## CPU profile self time

Profiles run for at least 250 ms after scenario warmups. Sampling interval is 100 microseconds. Different builds execute different operation counts; self time below is normalized per operation. Profiling is intrusive and these self times are attribution evidence, not the unprofiled latency measurements. Raw Chrome profiles are in `.generated/`.

| Rows | Build | Scenario | Operations | Top self-time functions, microseconds per operation |
|---:|---|---|---:|---|
| 10000 | baseline | single | 873 | `residentRows:baseline.mjs:7292` 180.5; `(program)::0` 82.5; `(garbage collector)::0` 72.9; `applyTransactionNow:baseline.mjs:8069` 13.4; `setResidentRow:baseline.mjs:7303` 6.4 |
| 10000 | baseline | burst | 4 | `applyTransactionNow:baseline.mjs:8069` 69153.7; `(program)::0` 9296.8; `setResidentRow:baseline.mjs:7303` 1867.0; `structuredClone::0` 1464.8; `(garbage collector)::0` 1037.8 |
| 10000 | candidate | single | 10000 | `applyTransactionNow:candidate.mjs:8540` 4.4; `(program)::0` 4.1; `storageSnapshotFrom:candidate.mjs:9238` 3.1; `set entities:candidate.mjs:7627` 2.0; `(garbage collector)::0` 1.9 |
| 10000 | candidate | burst | 51 | `setResidentRow:candidate.mjs:7589` 1358.9; `structuredClone::0` 987.7; `(program)::0` 598.2; `applyTransactionNow:candidate.mjs:8540` 358.3; `cloneRow:candidate.mjs:9171` 352.4 |
| 50000 | baseline | single | 186 | `(program)::0` 1932.1; `residentRows:baseline.mjs:7292` 1027.3; `(garbage collector)::0` 171.4; `setResidentRow:baseline.mjs:7303` 88.9; `applyTransactionNow:baseline.mjs:8069` 21.0 |
| 50000 | baseline | burst | 13 | `applyTransactionNow:baseline.mjs:8069` 13750.2; `(program)::0` 5422.8; `residentRows:baseline.mjs:7292` 1058.2; `structuredClone::0` 846.1; `setResidentRow:baseline.mjs:7303` 728.5 |
| 50000 | candidate | single | 10000 | `(program)::0` 29.8; `applyTransactionNow:candidate.mjs:8540` 4.1; `setResidentRow:candidate.mjs:7589` 3.3; `(garbage collector)::0` 3.1; `storageSnapshotFrom:candidate.mjs:9238` 3.1 |
| 50000 | candidate | burst | 46 | `(program)::0` 1634.2; `setResidentRow:candidate.mjs:7589` 1576.9; `structuredClone::0` 947.8; `applyTransactionNow:candidate.mjs:8540` 382.7; `cloneRow:candidate.mjs:9171` 376.5 |
| 100000 | baseline | single | 40 | `(program)::0` 6813.5; `(garbage collector)::0` 4169.9; `residentRows:baseline.mjs:7292` 2074.5; `storageSnapshotFrom:baseline.mjs:8686` 15.7; `enqueueApplication:baseline.mjs:7365` 8.0 |
| 100000 | baseline | burst | 4 | `applyTransactionNow:baseline.mjs:8069` 69358.5; `(program)::0` 61557.2; `residentRows:baseline.mjs:7292` 2100.3; `setResidentRow:baseline.mjs:7303` 1483.3; `structuredClone::0` 1478.0 |
| 100000 | candidate | single | 5267 | `(program)::0` 115.8; `(garbage collector)::0` 19.9; `applyTransactionNow:candidate.mjs:8540` 6.6; `storageSnapshotFrom:candidate.mjs:9238` 3.8; `setResidentRow:candidate.mjs:7589` 2.2 |
| 100000 | candidate | burst | 31 | `(program)::0` 18406.3; `setResidentRow:candidate.mjs:7589` 1692.8; `(garbage collector)::0` 1514.9; `structuredClone::0` 1074.6; `indexEqualityRow:candidate.mjs:8727` 675.0 |

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

The 100k blockers are fixed in the final combined-source run. Chromium burst p95 is 10.315 ms versus 23.460 ms baseline, the original round 1 baseline's 27.800 ms, and the round 1 candidate's 55.710 ms. Burst median speed ratio is 2.36x [1.97, 2.60]. Retained post-update heap is 161.54 MiB versus 162.53 MiB baseline and 183.27 MiB for the round 1 candidate. Candidate burst long-task counts are zero in all three trials at every size.

IndexedDB at 100k: 3878.150/16698.420 -> 3576.915/14432.140 ms; 1.08x [1.05, 1.15]. This is warm-browser/storage activation including disposal, not cold disk startup. At 50k: 1839.270/1954.175 -> 1722.325/1773.380 ms; 1.07x [1.05, 1.09]. At 10k: 347.190/400.935 -> 321.375/346.010 ms; 1.08x [1.00, 1.12]; its interval includes 1.

### Heap attribution and CPU profiles

CDP constructor/record-shape data is in `heap-summary.json`, all at the post-10k-update checkpoint. V8 omits immediate numeric fields from reference edges; shapes match reference properties. Constructor shallow bytes are not dominator retained sizes.

| Snapshot | Plain Object count | Plain Object shallow MiB |
|---|---:|---:|
| Original-code baseline | 100,534 | 2.686 |
| Round 1 candidate | 500,570 | 12.986 |
| Round 2 candidate | 103,215 | 2.739 |

Round 1 has approximately 200k treap nodes, 100k entries and 100k recency records. All disappear in round 2, replaced by 407 slot-page and 2,241 bucket records. Hash buckets hold integer slots; pages retain shared ID/value references. Recency and resident row/execution versions share numeric slots. Large memberships use sorted ID-reference arrays rather than per-resident map entries. No wire IDs change.

CDP burst GC self time normalized per operation is 1.51 ms versus round 1's 13.72 ms. `put` disappears from the profile; remaining application work includes row sizing/freezing, structured cloning and membership lookup. Profiling is intrusive, operation counts differ, and `(program)` includes protocol/scheduling overhead. Use the unprofiled tables for latency conclusions.

### Preserved wins and uncertainty

`round-comparison.json` independently resamples paired trials from each round and reports normalized gains, absolute candidate medians and exploratory p95 intervals. Of 63 existing scenario/size/mode comparisons, 38 candidate-median intervals show faster timings, 24 span unchanged timing, and 1 is timer-quantized. Candidate p95 intervals show 32 faster, 30 spanning unchanged and 1 quantized. No median or p95 candidate interval establishes a slowdown. Three pairs and 45 hot-path samples limit tail inference; these are not CI gates or production guarantees.

Two normalized gains decrease beyond their intervals while their candidate intervals span unchanged timing. Their baselines also got faster:

- node 50,000 `sorted`: normalized gain change 0.70 [0.53, 0.82]; candidate median speed change 1.11 [0.92, 1.21].
- chromium 100,000 `sorted`: normalized gain change 0.76 [0.37, 0.96]; candidate median speed change 1.08 [0.54, 1.28].

Sampled p95 can still increase without an established tail slowdown. For example, the 100k limited-sort candidate is 1.875/5.425 ms versus round 1's 2.025/3.685 ms. Raw samples and exploratory tail intervals remain visible; no claim that every sample got faster.

### Remaining costs and deliberate limits

- 10k retained heap is 17.10 MiB versus 16.96 MiB, about 0.13 MiB extra. At 50k it falls to 80.76 versus 81.28 MiB. The 100k target is met; small-fixture fixed overhead remains.
- First-use equality queries use complete evaluation while indexes build in 512-row turns. With warm resident arrays, 10k is 0.435/0.620 -> 0.565/0.870 ms; 0.77x [0.72, 0.84]; 50k is 2.085/3.425 -> 2.225/3.660 ms; 0.94x [0.82, 1.14]; 100k is 4.635/7.900 -> 4.570/7.400 ms; 1.01x [0.89, 1.45]. Extra construction work is bounded per turn; filtering/exact counts remain synchronous and corpus-dependent. Warm samples wait for index completion.
- Heap checkpoints precede query-index warmup. Up to four lazy equality indexes retain ID buckets after construction, without the old duplicate per-row value map. No active-query lifetime tracking was added.
- Forks copy compact page/bucket directories; inserts/deletes copy touched buckets. Growth, tombstone compaction and membership replacement still do corpus work. Many overlapping large windows add binary-search work. Recency slots reuse holes but retain array high-water capacity until clear. Residency budgets remain estimates; active pins stay protected.
- Public arrays remain caller-owned, reducer handlers retain defensive copies, durable outbox/server replay/visibility behavior is unchanged. No protocol/runtime/schema/version change, Whagons contract bump, merge or npm publication.

### Validation and final source

All 339 focused tests pass: client 247, local-runtime 16, React 76. Only the eight client files, `local-runtime/src/read-view.test.ts` and `react/src/index.test.tsx` were run, with one worker. Client/local-runtime/React typechecks and builds pass. React uses `NODE_OPTIONS=--no-experimental-webstorage` for Node 26. No full suites or Cargo runs.

The remote branch's `bbff57f` correctness fix is retained. Catch-up forks carry unpublished changed IDs; derived caches are reclaimed before eviction. The final complete benchmark uses combined production source `c660679e9bfa4f75af2646b4dacc081136d1b08e`, with no earlier timing samples retained. This shared workstation ran unrelated jobs; baseline/candidate order alternates by trial. All fixtures use bounded sorted seeding and normal lookup indexes. 10k contexts/databases are independent; 50k/100k use fresh pages against shared read-only databases per size.

The attempted cross-run profile reuse did not reuse databases because the localhost port changed their origin. This run seeded fresh fixtures; all measurements remain valid and paired. The ineffective option was removed. Reuse within a run remains, and raw snapshots/profiles stay under ignored `.generated/`. JavaScript heap excludes native IndexedDB memory. See dates, setup notes, bundle hashes and samples in `results.json`.
