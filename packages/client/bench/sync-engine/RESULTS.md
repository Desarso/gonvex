# Sync engine storage measurements

Native browser storage only, measured source `a3fd7d0d50fd60666bbffc530159dc7063e56b3d`. Source dirty flag `false`. Five warmups, 20 initial measured rounds, a 30-round cap, 15% CV limit and 4,000 seeded bootstrap draws. Serial browsers and alternating adapter order. Scheduling priority was nice 15 to keep the shared machine responsive.

No runtime, worker, DOM, subscriber delivery, wire or energy measurement. WebKit approximates Safari and does not certify Safari/iOS. Startup reopens populated persistent profiles in fresh browser processes, with empty JS/adapter caches. OS disk caches are not evicted; launch/navigation are excluded. Deletes have 100 actual owners per victim. Outbox admission is excluded; both replica modes use the same existing durable outbox.

Hardware: AMD Ryzen 7 5800X 8-Core Processor, 31.23 GiB RAM, 7.2.4-arch1-2, Node v26.8.2. Seed `1196840961`, artifact `ef83e92b1ed10c04d95d262a95e99156178f8719ce1486b6cce2903c2ff2bcc0`. Task rows have 50 mixed columns and four indexed scalar columns, with five related tables and a 200 MiB/unlimited-row working-set budget. Fixture IDs for 250 visible rows and 1,000 exact IDs are not mounted subscriptions. UTF-16 residency accounting can make the 50k working set partial under this budget; omitted rows remain available on disk.

Values are milliseconds unless labeled as ratios. Brackets are bootstrap 95% confidence intervals. Tables show median with its CI, p95, and paired engine/legacy median ratio with its CI. Raw samples and p95 CIs are in [results.json](./results.json). NOISY is not acceptance.

## chromium

Browser 149.0.7827.55. Quiescent: **no**. One-minute load before/after: 23.560 / 12.740; maximum observed 34.930. Threshold is 8 on 16 logical CPUs.

| Case | n | Legacy median [CI] / p95 | Engine median [CI] / p95 | Engine/legacy median [CI] | CV legacy / engine | CV status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| single-row-10000 | 30 | 72.550 [68.000, 77.300] / 169.900 | 7.750 [6.950, 10.850] / 61.400 | 0.107 [0.097, 0.149] | 165.4% / 363.5% | NOISY |
| single-row-50000 | 30 | 325.050 [269.400, 378.000] / 619.500 | 13.700 [10.450, 22.500] / 108.400 | 0.042 [0.032, 0.071] | 38.9% / 111.3% | NOISY |
| startup-working-set-10000 | 30 | 682.100 [639.850, 707.000] / 1296.400 | 856.700 [767.350, 984.250] / 1431.000 | 1.256 [1.136, 1.409] | 36.3% / 44.2% | NOISY |
| startup-working-set-50000 | 30 | 2204.450 [2051.450, 2476.200] / 4791.800 | 3078.500 [3000.700, 3382.350] / 5831.000 | 1.396 [1.321, 1.542] | 31.8% / 28.5% | NOISY |
| delete-100-windows-10000 | 30 | 77.450 [74.500, 81.050] / 105.100 | 58.400 [55.450, 65.650] / 101.000 | 0.754 [0.717, 0.847] | 15.3% / 53.3% | NOISY |
| delete-100-windows-50000 | 30 | 172.300 [150.950, 200.200] / 674.600 | 73.600 [63.450, 97.650] / 202.900 | 0.427 [0.355, 0.596] | 67.0% / 49.1% | NOISY |
| outbox-500-independent | 30 | 15296.050 [12731.850, 18560.750] / 27136.800 | 14397.650 [11539.500, 17413.300] / 23454.000 | 0.941 [0.843, 0.997] | 40.5% / 36.4% | NOISY |

| Storage gate | 95% CI | Limit | Verdict | Reason |
| --- | ---: | ---: | --- | --- |
| single-row durable apply p95 <= 10 ms | [45.800, 108.700] | 10 | NOISY | not quiescent |
| 50k/10k engine p95 <= 1.25x | [0.112, 4.645] | 1.25 | NOISY | not quiescent |
| startup-working-set-10000 engine/legacy p95 <= 1x | [0.874, 1.599] | 1 | NOISY | not quiescent |
| startup-working-set-50000 engine/legacy p95 <= 1x | [1.130, 1.916] | 1 | NOISY | not quiescent |
| delete-100-windows-10000 engine/legacy p95 <= 1x | [0.720, 2.390] | 1 | NOISY | not quiescent |
| delete-100-windows-50000 engine/legacy p95 <= 1x | [0.164, 0.839] | 1 | NOISY | not quiescent |

### Single-row IndexedDB request diagnostics

Five extra engine samples per size, after measured rounds. Latencies include native queue time and instrumentation; they are not included in the performance statistics. Each request completion is counted once.

| Operation | 10k requests/sample | 50k requests/sample | 10k mean latency, ms | 50k mean latency, ms |
| --- | ---: | ---: | ---: | ---: |
| `entities.get` | 1 | 1 | 0.160 | 0.240 |
| `entities.put` | 1 | 1 | 0.480 | 0.420 |
| `members.[scope+signature+generation+hashOnly].getAll` | 1 | 1 | 0.260 | 0.820 |
| `members.get` | 1 | 1 | 0.340 | 0.700 |
| `members.put` | 1 | 1 | 0.240 | 0.520 |
| `meta.get` | 4 | 4 | 0.480 | 1.610 |
| `meta.put` | 2 | 2 | 0.220 | 1.240 |
| `windows.get` | 1 | 1 | 0.160 | 0.200 |
| `windows.put` | 1 | 1 | 0.240 | 0.500 |

## firefox

Browser 151.0. Quiescent: **no**. One-minute load before/after: 7.600 / 5.200; maximum observed 15.830. Threshold is 8 on 16 logical CPUs.

| Case | n | Legacy median [CI] / p95 | Engine median [CI] / p95 | Engine/legacy median [CI] | CV legacy / engine | CV status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| single-row-10000 | 30 | 47.000 [46.000, 48.000] / 61.000 | 4.000 [4.000, 5.000] / 6.000 | 0.085 [0.083, 0.108] | 10.1% / 27.8% | NOISY |
| single-row-50000 | 30 | 137.000 [127.500, 146.000] / 175.000 | 5.000 [5.000, 6.000] / 70.000 | 0.036 [0.034, 0.044] | 14.0% / 400.5% | NOISY |
| startup-working-set-10000 | 20 | 397.000 [378.000, 417.500] / 471.000 | 535.500 [516.000, 562.000] / 632.000 | 1.349 [1.255, 1.439] | 9.2% / 9.0% | OK |
| startup-working-set-50000 | 30 | 1726.000 [1692.000, 1793.000] / 2584.000 | 2340.500 [2303.500, 2454.000] / 3917.000 | 1.356 [1.308, 1.409] | 15.3% / 18.6% | NOISY |
| delete-100-windows-10000 | 30 | 67.000 [65.000, 69.000] / 91.000 | 59.000 [54.000, 60.500] / 122.000 | 0.881 [0.810, 0.910] | 13.7% / 37.0% | NOISY |
| delete-100-windows-50000 | 30 | 144.000 [140.000, 151.500] / 165.000 | 60.000 [58.000, 62.500] / 118.000 | 0.417 [0.396, 0.435] | 10.2% / 25.8% | NOISY |
| outbox-500-independent | 20 | 2024.500 [1915.000, 2297.000] / 2603.000 | 2123.500 [1983.000, 2257.000] / 2546.000 | 1.049 [0.904, 1.124] | 13.3% / 12.2% | OK |

| Storage gate | 95% CI | Limit | Verdict | Reason |
| --- | ---: | ---: | --- | --- |
| single-row durable apply p95 <= 10 ms | [7.000, 688.000] | 10 | NOISY | not quiescent |
| 50k/10k engine p95 <= 1.25x | [1.167, 114.667] | 1.25 | NOISY | not quiescent |
| startup-working-set-10000 engine/legacy p95 <= 1x | [1.143, 1.523] | 1 | NOISY | not quiescent |
| startup-working-set-50000 engine/legacy p95 <= 1x | [1.019, 1.670] | 1 | NOISY | not quiescent |
| delete-100-windows-10000 engine/legacy p95 <= 1x | [0.769, 1.736] | 1 | NOISY | not quiescent |
| delete-100-windows-50000 engine/legacy p95 <= 1x | [0.347, 0.773] | 1 | NOISY | not quiescent |

### Single-row IndexedDB request diagnostics

Five extra engine samples per size, after measured rounds. Latencies include native queue time and instrumentation; they are not included in the performance statistics. Each request completion is counted once.

| Operation | 10k requests/sample | 50k requests/sample | 10k mean latency, ms | 50k mean latency, ms |
| --- | ---: | ---: | ---: | ---: |
| `entities.get` | 1 | 1 | 0.200 | 0.600 |
| `entities.put` | 1 | 1 | 0.000 | 1.600 |
| `members.[scope+signature+generation+hashOnly].getAll` | 1 | 1 | 0.400 | 0.600 |
| `members.get` | 1 | 1 | 0.200 | 0.400 |
| `members.put` | 1 | 1 | 0.400 | 1.000 |
| `meta.get` | 4 | 4 | 0.000 | 0.550 |
| `meta.put` | 2 | 2 | 0.200 | 0.300 |
| `windows.get` | 1 | 1 | 0.000 | 0.400 |
| `windows.put` | 1 | 1 | 0.200 | 0.600 |

## webkit

Browser 26.5. Quiescent: **no**. One-minute load before/after: 5.200 / 4.710; maximum observed 10.840. Threshold is 8 on 16 logical CPUs.

| Case | n | Legacy median [CI] / p95 | Engine median [CI] / p95 | Engine/legacy median [CI] | CV legacy / engine | CV status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| single-row-10000 | 30 | 28.500 [26.000, 32.000] / 39.000 | 11.000 [10.000, 11.000] / 17.000 | 0.386 [0.312, 0.423] | 18.6% / 25.6% | NOISY |
| single-row-50000 | 30 | 159.500 [151.500, 168.000] / 237.000 | 13.000 [13.000, 15.000] / 21.000 | 0.082 [0.078, 0.093] | 20.3% / 28.6% | NOISY |
| startup-working-set-10000 | 20 | 303.500 [293.000, 313.000] / 367.000 | 580.500 [540.000, 609.500] / 662.000 | 1.913 [1.776, 2.034] | 10.8% / 8.4% | OK |
| startup-working-set-50000 | 30 | 1468.000 [1374.500, 1669.000] / 2152.000 | 2826.000 [2755.000, 3157.500] / 3644.000 | 1.925 [1.808, 2.117] | 17.0% / 11.0% | NOISY |
| delete-100-windows-10000 | 20 | 65.500 [60.000, 69.500] / 73.000 | 99.000 [87.500, 105.000] / 112.000 | 1.511 [1.290, 1.733] | 8.3% / 13.5% | OK |
| delete-100-windows-50000 | 30 | 157.500 [152.500, 164.500] / 185.000 | 113.500 [97.500, 132.500] / 145.000 | 0.721 [0.623, 0.834] | 9.6% / 19.1% | NOISY |
| outbox-500-independent | 30 | 14602.500 [14148.500, 15944.000] / 22275.000 | 14119.000 [13742.000, 15204.500] / 21592.000 | 0.967 [0.900, 1.018] | 20.8% / 17.6% | NOISY |

| Storage gate | 95% CI | Limit | Verdict | Reason |
| --- | ---: | ---: | --- | --- |
| single-row durable apply p95 <= 10 ms | [18.000, 31.000] | 10 | NOISY | not quiescent |
| 50k/10k engine p95 <= 1.25x | [0.792, 2.214] | 1.25 | NOISY | not quiescent |
| startup-working-set-10000 engine/legacy p95 <= 1x | [1.572, 2.095] | 1 | NOISY | not quiescent |
| startup-working-set-50000 engine/legacy p95 <= 1x | [1.536, 1.963] | 1 | NOISY | not quiescent |
| delete-100-windows-10000 engine/legacy p95 <= 1x | [1.453, 1.757] | 1 | NOISY | not quiescent |
| delete-100-windows-50000 engine/legacy p95 <= 1x | [0.683, 0.885] | 1 | NOISY | not quiescent |

### Single-row IndexedDB request diagnostics

Five extra engine samples per size, after measured rounds. Latencies include native queue time and instrumentation; they are not included in the performance statistics. Each request completion is counted once.

| Operation | 10k requests/sample | 50k requests/sample | 10k mean latency, ms | 50k mean latency, ms |
| --- | ---: | ---: | ---: | ---: |
| `entities.get` | 1 | 1 | 0.200 | 0.200 |
| `entities.put` | 1 | 1 | 0.400 | 0.800 |
| `members.[scope+signature+generation+hashOnly].getAll` | 1 | 1 | 0.400 | 0.400 |
| `members.get` | 1 | 1 | 0.400 | 0.400 |
| `members.put` | 1 | 1 | 0.000 | 0.200 |
| `meta.get` | 4 | 4 | 0.100 | 0.150 |
| `meta.put` | 2 | 2 | 0.300 | 0.200 |
| `windows.get` | 1 | 1 | 0.200 | 0.200 |
| `windows.put` | 1 | 1 | 0.400 | 0.000 |

## Interpretation and remaining work

All 18 storage gates are **NOISY**. None of the three browser runs stayed quiescent. Stable case CVs do not override that browser's load warning. These are completed measurements on the final storage revision, not reference-hardware acceptance certificates.

The nominal 50k/10k engine p95 ratios are 1.765 in Chromium, 11.667 in Firefox and 1.235 in WebKit. Their 95% bootstrap intervals are respectively [0.112, 4.645], [1.167, 114.667] and [0.792, 2.214]. Every interval includes the 1.25 limit. Thus the earlier 3.70x point estimate cannot be treated as an established native scaling failure, and this final run also cannot close the scaling gate. Large outliers remain in the raw samples.

All thirty diagnostic engine updates issued exactly 13 native requests at both sizes. The operations are four meta gets, two meta puts, one window get/put, one entity get/put, one membership get/put, and one sparse hashOnly-index getAll. The latter returns no retained tombstones in this row-update fixture. No request count or SDK ID/hash serialization grows with collection size. The per-operation latency tables above show native completion/queue variability, including accesses to the tiny metadata table. They do not isolate a collection-size-dependent native backend cost under this changing machine load. No speculative index/schema removal or durability relaxation was used. A quiescent rerun and native backend/CPU trace are needed before attributing a scaling cause or making a driver fix.

Startup remains a nominal regression in all three 50k results. The engine working-set path still reads and materializes full membership/hash maps, including binary-to-hex conversion. The 100-owner path reads owner metadata and writes membership/window records per owner. These are follow-up profiling targets, not completed startup/delete optimizations. Keep the engine opt-in.

The 500-intent case uses the same DexieReducerOutbox in both profiles. Its ratio is a storage control measurement, not a new engine outbox improvement or the design doc's independent/dependent network drain gate. Durable admission, server replay and optimistic-layer retirement are outside this timing.

The first complete matrix was preserved in pre-final-fix-results.json before the concurrent df657fe correctness commit was integrated. That commit guards stale-window pruning and preserves checkpoint-only hashes through full snapshot replacement. It was merged without discarding either history, verified with 39 focused tests, and the entire 21-case matrix was rerun. Only this second run is the current result.

The old fake-indexeddb timings have been removed from this report. They remain in [emulator-results.json](./emulator-results.json), explicitly labeled as emulator data and rejected by gates. Its overwrite path scans secondary indexes; those scans do not establish native IndexedDB performance. Historical non-persistent Chromium snapshots and the aborted development run remain adjacent as historical artifacts, not current acceptance results.

The focused IndexedDB correctness tests passed, 39 tests across indexeddb-engine.test.ts and indexeddb-replica.test.ts. Client typecheck and build passed. All four benchmark statistics/gate checks passed, including CLI exit codes and relevant-adapter noise handling. Fresh-process persisted startup assertions passed in all three browsers for both adapters.

The benchmark implementation changes no storage behavior. The concurrent df657fe engine correctness fix is retained and retimed. Legacy adapter behavior, wire protocol, package version and Whagons integration remain unchanged. Keep the engine opt-in. No app code change is required. Dependent-chain replay, shaped-network outbox drain, first paint, worker/main-thread tasks, multi-tab memory, owner kill/recovery and physical Safari/device gates remain end-to-end work.
