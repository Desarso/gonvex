# Replica hot-path measurements

Baseline `136d596a74bd8b0105822d5dadfe669eb2f92f47`; candidate `1dd4e09886da4fd45bfaeb8fcb20cebb62fdcf6d`, source dirty: false.

2026-10-14T03:01:18.498Z. v26.8.2; Chromium 149.0.7827.55. 3 paired trials; 15 samples per hot path after warmups.

Times include awaited transaction publication and synchronous subscriber notification. Resident counts span 20 tables; tasks contain 60% of the rows and 50 columns. 50 windows, 52 subscribers. Chromium pages are cross-origin isolated for high-resolution timers; capture samples amortize 20 operations. Persistence is a no-op in hot-path scenarios. IndexedDB startup uses real Chromium IndexedDB with a prepopulated, warm browser/storage cache.

Near-zero snapshot timings are timer-quantized; an infinite upper interval means that resampled candidate medians reached zero, not infinite throughput. This workstation also ran unrelated jobs; the paired trials and broad intervals expose some, but not all, scheduling noise.

95% intervals are deterministic paired hierarchical bootstrap intervals for the median speed ratio, resampling trial pairs and samples within each trial. Three pairs cannot characterize all machine noise. No CI gate or statistical significance claim.

Resumed with unchanged production bundle hashes. Completed Node and 10k/50k Chromium fixtures retained. 100k uses bounded, primary-key-ordered fixture seeding and a shared persistent browser context; smaller Chromium fixtures used independent contexts and a single unmeasured seeding transaction.

Collection began 2026-10-14T01:06:42.746Z; final resumed runner began 2026-10-14T03:01:18.498Z.

## node

| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |
|---:|---|---:|---:|---:|
| 10,000 | Single-row transaction + notify | 0.464 / 1.442 | 0.051 / 0.071 | 9.10x [6.86, 12.52] |
| 10,000 | Single-row transaction + 250-row subscriber snapshot | 0.722 / 0.980 | 0.285 / 0.376 | 2.53x [1.81, 3.20] |
| 10,000 | 1,000-row transaction + notify | 29.099 / 36.561 | 13.985 / 21.354 | 2.08x [1.58, 2.46] |
| 10,000 | Portable reducer + optimistic apply + replay | 0.934 / 1.471 | 0.259 / 0.358 | 3.60x [2.78, 4.25] |
| 10,000 | Unrelated transaction + offline query | 0.772 / 2.819 | 0.070 / 0.094 | 11.04x [8.03, 30.27] |
| 10,000 | Matching transaction + offline query | 1.523 / 3.765 | 0.201 / 3.795 | 7.56x [4.73, 17.22] |
| 10,000 | Unfiltered sorted offline query, limit 50 | 5.448 / 7.635 | 0.243 / 0.350 | 22.44x [13.28, 32.61] |
| 10,000 | entityRows + cached window snapshot | 0.013 / 0.022 | 0.0056 / 0.011 | 2.29x [1.47, 2.59] |
| 10,000 | Capture reducer tables without reading them | 0.339 / 0.888 | 0.0015 / 0.0026 | 228.63x [161.06, 371.99] |
| 10,000 | JSON working-set decode + activation + disposal | 127.514 / 166.317 | 88.170 / 111.855 | 1.45x [1.22, 1.59] |
| 50,000 | Single-row transaction + notify | 2.084 / 3.081 | 0.048 / 0.070 | 43.66x [37.59, 58.20] |
| 50,000 | Single-row transaction + 250-row subscriber snapshot | 2.935 / 5.195 | 0.277 / 0.436 | 10.61x [5.98, 16.69] |
| 50,000 | 1,000-row transaction + notify | 24.038 / 31.187 | 15.320 / 28.107 | 1.57x [1.16, 2.12] |
| 50,000 | Portable reducer + optimistic apply + replay | 2.680 / 5.075 | 0.217 / 0.578 | 12.33x [5.38, 13.84] |
| 50,000 | Unrelated transaction + offline query | 4.215 / 9.498 | 0.058 / 0.099 | 72.33x [42.65, 155.84] |
| 50,000 | Matching transaction + offline query | 8.423 / 11.548 | 0.623 / 0.980 | 13.51x [9.69, 16.01] |
| 50,000 | Unfiltered sorted offline query, limit 50 | 43.473 / 64.913 | 1.028 / 1.213 | 42.30x [37.03, 53.51] |
| 50,000 | entityRows + cached window snapshot | 0.069 / 0.153 | 0.108 / 0.144 | 0.64x [0.56, 1.42] |
| 50,000 | Capture reducer tables without reading them | 1.235 / 2.966 | 0.0010 / 0.0015 | 1208.28x [1115.96, 1346.93] |
| 50,000 | JSON working-set decode + activation + disposal | 498.506 / 733.389 | 458.089 / 620.912 | 1.09x [0.89, 1.27] |
| 100,000 | Single-row transaction + notify | 5.162 / 8.691 | 0.069 / 0.114 | 75.20x [58.18, 116.35] |
| 100,000 | Single-row transaction + 250-row subscriber snapshot | 5.786 / 23.535 | 0.320 / 0.428 | 18.09x [17.07, 23.05] |
| 100,000 | 1,000-row transaction + notify | 31.700 / 40.741 | 19.045 / 32.332 | 1.66x [1.30, 2.13] |
| 100,000 | Portable reducer + optimistic apply + replay | 5.850 / 8.591 | 0.225 / 0.549 | 25.95x [13.05, 35.50] |
| 100,000 | Unrelated transaction + offline query | 13.314 / 16.822 | 0.071 / 0.102 | 188.20x [93.45, 224.40] |
| 100,000 | Matching transaction + offline query | 17.991 / 24.867 | 1.315 / 4.247 | 13.68x [9.88, 14.73] |
| 100,000 | Unfiltered sorted offline query, limit 50 | 105.207 / 143.728 | 3.738 / 7.547 | 28.15x [14.31, 47.42] |
| 100,000 | entityRows + cached window snapshot | 0.272 / 0.302 | 0.224 / 0.396 | 1.22x [0.53, 1.32] |
| 100,000 | Capture reducer tables without reading them | 2.916 / 6.389 | 0.0018 / 0.0026 | 1590.81x [1315.40, 2999.12] |
| 100,000 | JSON working-set decode + activation + disposal | 1007.897 / 1409.202 | 1030.669 / 1324.093 | 0.98x [0.85, 1.25] |

| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |
|---:|---:|---:|---:|---|
| 10,000 | 68.81 / 73.10 | 68.90 / 73.64 | 0.94x [0.86, 0.94] | unavailable |
| 50,000 | 159.93 / 179.93 | 160.00 / 180.07 | 0.89x [0.85, 0.89] | unavailable |
| 100,000 | 280.28 / 320.01 | 280.38 / 320.17 | 0.88x [0.84, 0.88] | unavailable |

Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k/50k contexts and databases are isolated; 100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.

## chromium

| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |
|---:|---|---:|---:|---:|
| 10,000 | Single-row transaction + notify | 0.315 / 0.955 | 0.065 / 0.105 | 4.85x [3.19, 6.45] |
| 10,000 | Single-row transaction + 250-row subscriber snapshot | 0.300 / 1.320 | 0.200 / 0.455 | 1.50x [1.06, 4.94] |
| 10,000 | 1,000-row transaction + notify | 12.955 / 15.755 | 10.920 / 20.655 | 1.19x [0.76, 1.51] |
| 10,000 | Portable reducer + optimistic apply + replay | 0.440 / 1.510 | 0.215 / 0.410 | 2.05x [1.43, 2.49] |
| 10,000 | Unrelated transaction + offline query | 0.475 / 0.650 | 0.070 / 0.125 | 6.79x [4.20, 9.82] |
| 10,000 | Matching transaction + offline query | 0.705 / 1.150 | 0.160 / 0.355 | 4.41x [2.42, 5.04] |
| 10,000 | Unfiltered sorted offline query, limit 50 | 3.625 / 4.475 | 0.215 / 0.460 | 16.86x [9.06, 18.15] |
| 10,000 | entityRows + cached window snapshot | 0.015 / 0.020 | 0.0050 / 0.020 | 3.00x [2.00, Infinity] |
| 10,000 | Capture reducer tables without reading them | 0.208 / 0.252 | 0.0010 / 0.0025 | 207.50x [106.88, 279.00] |
| 10,000 | JSON working-set decode + activation + disposal | 67.505 / 80.370 | 63.040 / 108.925 | 1.07x [0.72, 1.38] |
| 10,000 | IndexedDB working-set activation + disposal | 488.060 / 1004.170 | 363.085 / 2789.105 | 1.34x [0.38, 1.60] |
| 50,000 | Single-row transaction + notify | 1.995 / 16.465 | 0.040 / 0.370 | 49.87x [34.00, 91.45] |
| 50,000 | Single-row transaction + 250-row subscriber snapshot | 2.145 / 16.020 | 0.260 / 1.575 | 8.25x [4.32, 8.76] |
| 50,000 | 1,000-row transaction + notify | 17.920 / 62.370 | 14.600 / 54.025 | 1.23x [0.62, 1.48] |
| 50,000 | Portable reducer + optimistic apply + replay | 2.010 / 3.645 | 0.280 / 0.450 | 7.18x [5.24, 7.73] |
| 50,000 | Unrelated transaction + offline query | 3.460 / 7.260 | 0.125 / 0.695 | 27.68x [21.69, 45.64] |
| 50,000 | Matching transaction + offline query | 5.395 / 12.235 | 0.795 / 9.350 | 6.79x [5.04, 8.90] |
| 50,000 | Unfiltered sorted offline query, limit 50 | 36.210 / 135.605 | 2.145 / 10.900 | 16.88x [14.12, 35.46] |
| 50,000 | entityRows + cached window snapshot | 0.055 / 0.100 | 0.015 / 0.050 | 3.67x [2.14, 5.00] |
| 50,000 | Capture reducer tables without reading them | 1.196 / 4.287 | 0.0018 / 0.0030 | 683.28x [578.63, 1238.13] |
| 50,000 | JSON working-set decode + activation + disposal | 425.560 / 895.920 | 336.315 / 1425.740 | 1.27x [0.67, 1.49] |
| 50,000 | IndexedDB working-set activation + disposal | 2227.630 / 10133.355 | 2160.805 / 2328.835 | 1.03x [0.99, 3.82] |
| 100,000 | Single-row transaction + notify | 2.645 / 4.665 | 0.055 / 0.100 | 48.09x [42.00, 65.20] |
| 100,000 | Single-row transaction + 250-row subscriber snapshot | 2.615 / 4.495 | 0.325 / 0.480 | 8.05x [5.94, 10.90] |
| 100,000 | 1,000-row transaction + notify | 18.920 / 27.800 | 15.325 / 55.710 | 1.23x [0.65, 1.47] |
| 100,000 | Portable reducer + optimistic apply + replay | 2.645 / 5.235 | 0.365 / 0.465 | 7.25x [6.35, 14.49] |
| 100,000 | Unrelated transaction + offline query | 9.860 / 11.985 | 0.110 / 0.170 | 89.64x [72.61, 181.09] |
| 100,000 | Matching transaction + offline query | 11.915 / 15.080 | 1.360 / 2.580 | 8.76x [5.83, 14.82] |
| 100,000 | Unfiltered sorted offline query, limit 50 | 81.400 / 118.635 | 2.025 / 3.685 | 40.20x [31.88, 43.51] |
| 100,000 | entityRows + cached window snapshot | 0.140 / 0.195 | 0.105 / 0.135 | 1.33x [1.17, 5.20] |
| 100,000 | Capture reducer tables without reading them | 5.111 / 19.315 | 0.0013 / 0.0022 | 4088.98x [2687.79, 16118.07] |
| 100,000 | JSON working-set decode + activation + disposal | 1406.090 / 4396.680 | 665.315 / 2133.765 | 2.11x [1.35, 3.18] |
| 100,000 | IndexedDB working-set activation + disposal | 5335.045 / 23854.015 | 5792.780 / 17050.290 | 0.92x [0.84, 1.45] |

| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |
|---:|---:|---:|---:|---|
| 10,000 | 16.77 / 19.12 | 16.96 / 19.57 | 0.87x [0.87, 0.87] | 8,8,8 / 0,0,0 |
| 50,000 | 81.24 / 91.73 | 81.28 / 91.79 | 0.89x [0.89, 0.89] | 8,5,5 / 2,0,0 |
| 100,000 | 162.48 / 183.22 | 162.52 / 183.27 | 0.89x [0.89, 0.89] | 7,7,5 / 0,0,0 |

Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k/50k contexts and databases are isolated; 100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.

## CPU profile self time

Profiles run for at least 250 ms after scenario warmups. Sampling interval is 100 microseconds. Different builds execute different operation counts; self time below is normalized per operation. Profiling is intrusive and these self times are attribution evidence, not the unprofiled latency measurements. Raw Chrome profiles are in `.generated/`.

| Rows | Build | Scenario | Operations | Top self-time functions, microseconds per operation |
|---:|---|---|---:|---|
| 10000 | baseline | single | 800 | `residentRows:baseline.mjs:7292` 195.5; `(garbage collector)::0` 87.9; `(program)::0` 67.5; `applyTransactionNow:baseline.mjs:8069` 14.5; `storageSnapshotFrom:baseline.mjs:8686` 3.4 |
| 10000 | baseline | burst | 4 | `applyTransactionNow:baseline.mjs:8069` 59329.0; `(program)::0` 3466.5; `setResidentRow:baseline.mjs:7303` 1679.3; `structuredClone::0` 981.5; `notify:baseline.mjs:8404` 434.8 |
| 10000 | candidate | single | 6423 | `(program)::0` 6.7; `applyTransactionNow:candidate.mjs:8458` 6.0; `set:candidate.mjs:6248` 5.6; `storageSnapshotFrom:candidate.mjs:9122` 3.4; `setResidentRow:candidate.mjs:7535` 3.3 |
| 10000 | candidate | burst | 20 | `(program)::0` 4167.0; `setResidentRow:candidate.mjs:7535` 2433.5; `applyTransactionNow:candidate.mjs:8458` 1806.6; `put:candidate.mjs:6356` 1717.1; `structuredClone::0` 1301.3 |
| 50000 | baseline | single | 99 | `(program)::0` 7590.3; `residentRows:baseline.mjs:7292` 2054.2; `(garbage collector)::0` 357.9; `applyTransactionNow:baseline.mjs:8069` 67.8; `(idle)::0` 32.9 |
| 50000 | baseline | burst | 2 | `applyTransactionNow:baseline.mjs:8069` 94429.0; `(program)::0` 49826.0; `setResidentRow:baseline.mjs:7303` 37366.5; `notify:baseline.mjs:8404` 5821.0; `markEntityChanged:baseline.mjs:8444` 4343.5 |
| 50000 | candidate | single | 1312 | `(program)::0` 714.2; `setResidentRow:candidate.mjs:7535` 85.7; `(garbage collector)::0` 45.0; `applyTransactionNow:candidate.mjs:8458` 21.4; `storageSnapshotFrom:candidate.mjs:9122` 15.1 |
| 50000 | candidate | burst | 7 | `(program)::0` 24845.1; `setResidentRow:candidate.mjs:7535` 6401.0; `applyTransactionNow:candidate.mjs:8458` 6371.3; `(garbage collector)::0` 4352.3; `put:candidate.mjs:6356` 4129.6 |
| 100000 | baseline | single | 45 | `(program)::0` 8003.4; `(garbage collector)::0` 2804.4; `residentRows:baseline.mjs:7292` 2656.0; `applyTransactionNow:baseline.mjs:8069` 28.7; `storageSnapshotFrom:baseline.mjs:8686` 21.5 |
| 100000 | baseline | burst | 3 | `(program)::0` 108740.0; `applyTransactionNow:baseline.mjs:8069` 72778.3; `(garbage collector)::0` 3008.7; `setResidentRow:baseline.mjs:7303` 2294.0; `residentRows:baseline.mjs:7292` 2096.0 |
| 100000 | candidate | single | 3164 | `(program)::0` 247.8; `(garbage collector)::0` 43.5; `applyTransactionNow:candidate.mjs:8458` 6.4; `set:candidate.mjs:6248` 4.7; `set entities:candidate.mjs:7572` 3.9 |
| 100000 | candidate | burst | 12 | `(program)::0` 61139.6; `(garbage collector)::0` 13716.7; `put:candidate.mjs:6356` 3014.8; `setResidentRow:candidate.mjs:7535` 2342.5; `applyTransactionNow:candidate.mjs:8458` 2282.7 |

## Scope and limits

- Public row arrays remain caller-owned. Internal cached arrays share frozen rows. Entity table iteration and matching-query scans still scale with the requested result corpus.
- Tree updates are expected O(changed rows × log resident rows), with O(1) table forks. Outer table/window maps still copy metadata for snapshot isolation; they do not copy row entries.
- Bounded equality indexes trade retained memory for faster selection. Resident budgets estimate metadata, and retained UI windows/pending reducers can exceed those budgets. Active pins are never evicted.
- Point, batch and watched-window reads refresh LRU. Whole-table inspection does not promote every scanned row, keeping scans from replacing the hot working set.
- Top-k removes full sorting for limited windows, but filtering, exact counts and resolving reference sort fields still scan the candidate corpus. Search normalization is evaluated on that corpus.
- Startup timestamps measure a cold LocalReplica with loaded JavaScript and warm browser/storage caches, not a cold OS disk or network login.
- The optimistic scenario runs a real portable reducer twice around an authoritative row commit. Durable outbox admission, socket transport and server replay are tested elsewhere and excluded from these timings.
- Cached window snapshot costs are combined with a public entityRows call. React rendering and paint are not benchmarked.
