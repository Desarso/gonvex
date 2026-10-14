# Sync engine storage measurements

Native browser storage only, measured source `e2ff2ef8e598ad6ae83da77410adca0bae48a85b`. Source dirty flag `false`. Timed browser code was unchanged after that revision; derived gate evaluation uses `9522ca4e8f90bc6e1cad2ce7f7e4bb89ee3644a1` as identified by the full `environment.gatesCommit` in JSON. Five warmups, 20 initial measured rounds, a 30-round cap, 15% CV limit and 4,000 seeded bootstrap draws. Serial browsers and alternating adapter order. Scheduling priority was nice 15 to keep the shared machine responsive.

No runtime, worker, DOM, subscriber delivery, wire or energy measurement. WebKit approximates Safari and does not certify Safari/iOS. Startup reopens populated persistent profiles in fresh browser processes, with empty JS/adapter caches. OS disk caches are not evicted; launch/navigation are excluded. Deletes have 100 actual owners per victim. Outbox admission is excluded; both replica modes use the same existing durable outbox.

Hardware: AMD Ryzen 7 5800X 8-Core Processor, 31.23 GiB RAM, 7.2.4-arch1-2, Node v26.8.2. Seed `1196840961`, artifact `ef83e92b1ed10c04d95d262a95e99156178f8719ce1486b6cce2903c2ff2bcc0`. Task rows have 50 mixed columns and four indexed scalar columns, with five related tables and a 200 MiB/unlimited-row working-set budget. Fixture IDs for 250 visible rows and 1,000 exact IDs are not mounted subscriptions. UTF-16 residency accounting can make the 50k working set partial under this budget; omitted rows remain available on disk.

Values are milliseconds unless labeled as ratios. Brackets are bootstrap 95% confidence intervals. Tables show median with its CI, p95, and paired engine/legacy median ratio with its CI. Raw samples and p95 CIs are in [results.json](./results.json). NOISY is not acceptance.

## chromium

Browser 149.0.7827.55. Quiescent: **no**. One-minute load before/after: 5.130 / 7.560; maximum observed 8.130. Threshold is 8 on 16 logical CPUs.

| Case | n | Legacy median [CI] / p95 | Engine median [CI] / p95 | Engine/legacy median [CI] | CV legacy / engine | CV status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| single-row-10000 | 30 | 44.700 [40.050, 52.350] / 93.000 | 2.200 [2.100, 2.300] / 2.800 | 0.049 [0.042, 0.055] | 36.3% / 14.0% | NOISY |
| single-row-50000 | 30 | 109.050 [99.150, 110.900] / 160.600 | 2.050 [1.900, 2.250] / 2.500 | 0.019 [0.017, 0.021] | 44.0% / 14.9% | NOISY |
| startup-working-set-10000 | 20 | 432.750 [426.000, 438.550] / 448.200 | 555.500 [547.000, 557.850] / 573.200 | 1.284 [1.259, 1.304] | 2.5% / 2.0% | OK |
| startup-working-set-50000 | 20 | 1710.000 [1685.700, 1742.050] / 2134.300 | 2483.050 [2463.450, 2508.450] / 3248.700 | 1.452 [1.425, 1.480] | 10.1% / 10.7% | OK |
| delete-100-windows-10000 | 20 | 77.450 [74.000, 81.300] / 85.800 | 45.300 [42.900, 49.300] / 53.500 | 0.585 [0.544, 0.631] | 7.7% / 12.1% | OK |
| delete-100-windows-50000 | 30 | 123.250 [119.550, 131.000] / 153.700 | 47.700 [44.050, 50.800] / 77.200 | 0.387 [0.356, 0.417] | 13.0% / 23.7% | NOISY |
| outbox-500-independent | 30 | 10709.550 [9620.300, 14807.650] / 19992.600 | 10470.150 [9397.000, 14110.600] / 19958.600 | 0.978 [0.932, 1.050] | 42.0% / 40.3% | NOISY |

| Storage gate | 95% CI | Limit | Verdict | Reason |
| --- | ---: | ---: | --- | --- |
| single-row durable apply p95 <= 10 ms | [2.400, 2.600] | 10 | NOISY | not quiescent |
| 50k/10k engine p95 <= 1.25x | [0.800, 1.000] | 1.25 | NOISY | not quiescent |
| startup-working-set-10000 engine/legacy p95 <= 1x | [1.232, 1.312] | 1 | NOISY | not quiescent |
| startup-working-set-50000 engine/legacy p95 <= 1x | [1.304, 1.763] | 1 | NOISY | not quiescent |
| delete-100-windows-10000 engine/legacy p95 <= 1x | [0.571, 0.722] | 1 | NOISY | not quiescent |
| delete-100-windows-50000 engine/legacy p95 <= 1x | [0.287, 0.672] | 1 | NOISY | not quiescent |

### Single-row IndexedDB request diagnostics

Five extra engine samples per size, after measured rounds. Latencies include native queue time and instrumentation; they are not included in the performance statistics. Each request completion is counted once.

| Operation | 10k requests/sample | 50k requests/sample | 10k mean latency, ms | 50k mean latency, ms |
| --- | ---: | ---: | ---: | ---: |
| `entities.get` | 1 | 1 | 0.100 | 0.040 |
| `entities.put` | 1 | 1 | 0.160 | 0.140 |
| `members.[scope+signature+generation+hashOnly].getAll` | 1 | 1 | 0.120 | 0.080 |
| `members.get` | 1 | 1 | 0.140 | 0.140 |
| `members.put` | 1 | 1 | 0.160 | 0.160 |
| `meta.get` | 4 | 4 | 0.105 | 0.095 |
| `meta.put` | 2 | 2 | 0.100 | 0.090 |
| `windows.get` | 1 | 1 | 0.060 | 0.060 |
| `windows.put` | 1 | 1 | 0.120 | 0.100 |

## firefox

Browser 151.0. Quiescent: **no**. One-minute load before/after: 7.560 / 19.320; maximum observed 54.120. Threshold is 8 on 16 logical CPUs.

| Case | n | Legacy median [CI] / p95 | Engine median [CI] / p95 | Engine/legacy median [CI] | CV legacy / engine | CV status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| single-row-10000 | 30 | 42.000 [39.000, 46.500] / 83.000 | 5.000 [5.000, 6.500] / 9.000 | 0.119 [0.110, 0.150] | 42.8% / 90.6% | NOISY |
| single-row-50000 | 30 | 117.000 [112.500, 120.000] / 130.000 | 4.000 [4.000, 5.000] / 11.000 | 0.034 [0.033, 0.043] | 7.8% / 395.9% | NOISY |
| startup-working-set-10000 | 20 | 394.000 [376.500, 417.500] / 441.000 | 509.000 [497.000, 538.000] / 573.000 | 1.292 [1.215, 1.378] | 13.6% / 6.8% | OK |
| startup-working-set-50000 | 30 | 1650.500 [1605.000, 1846.500] / 2267.000 | 2307.000 [2274.000, 2474.000] / 3714.000 | 1.398 [1.272, 1.487] | 14.4% / 16.8% | NOISY |
| delete-100-windows-10000 | 30 | 105.500 [97.000, 115.000] / 603.000 | 157.000 [120.000, 186.000] / 344.000 | 1.488 [1.115, 1.788] | 96.5% / 47.4% | NOISY |
| delete-100-windows-50000 | 30 | 210.500 [203.000, 219.500] / 296.000 | 110.000 [98.000, 154.500] / 352.000 | 0.523 [0.465, 0.727] | 18.0% / 53.3% | NOISY |
| outbox-500-independent | 30 | 5281.500 [3526.000, 6064.500] / 15830.000 | 4118.000 [3608.000, 5775.000] / 12810.000 | 0.780 [0.703, 1.117] | 273.1% / 60.1% | NOISY |

| Storage gate | 95% CI | Limit | Verdict | Reason |
| --- | ---: | ---: | --- | --- |
| single-row durable apply p95 <= 10 ms | [5.000, 352.000] | 10 | NOISY | not quiescent |
| 50k/10k engine p95 <= 1.25x | [0.184, 44.000] | 1.25 | NOISY | not quiescent |
| startup-working-set-10000 engine/legacy p95 <= 1x | [0.974, 1.366] | 1 | NOISY | not quiescent |
| startup-working-set-50000 engine/legacy p95 <= 1x | [1.164, 1.824] | 1 | NOISY | not quiescent |
| delete-100-windows-10000 engine/legacy p95 <= 1x | [0.430, 2.408] | 1 | NOISY | not quiescent |
| delete-100-windows-50000 engine/legacy p95 <= 1x | [0.699, 1.454] | 1 | NOISY | not quiescent |

### Single-row IndexedDB request diagnostics

Five extra engine samples per size, after measured rounds. Latencies include native queue time and instrumentation; they are not included in the performance statistics. Each request completion is counted once.

| Operation | 10k requests/sample | 50k requests/sample | 10k mean latency, ms | 50k mean latency, ms |
| --- | ---: | ---: | ---: | ---: |
| `entities.get` | 1 | 1 | 0.000 | 0.000 |
| `entities.put` | 1 | 1 | 0.400 | 0.800 |
| `members.[scope+signature+generation+hashOnly].getAll` | 1 | 1 | 0.200 | 0.200 |
| `members.get` | 1 | 1 | 0.400 | 0.000 |
| `members.put` | 1 | 1 | 0.400 | 0.000 |
| `meta.get` | 4 | 4 | 0.050 | 0.200 |
| `meta.put` | 2 | 2 | 0.400 | 0.300 |
| `windows.get` | 1 | 1 | 0.200 | 0.000 |
| `windows.put` | 1 | 1 | 0.200 | 0.200 |

## webkit

Browser 26.5. Quiescent: **no**. One-minute load before/after: 20.930 / 5.660; maximum observed 60.190. Threshold is 8 on 16 logical CPUs.

| Case | n | Legacy median [CI] / p95 | Engine median [CI] / p95 | Engine/legacy median [CI] | CV legacy / engine | CV status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| single-row-10000 | 30 | 96.500 [83.000, 123.500] / 245.000 | 47.500 [37.000, 69.000] / 162.000 | 0.492 [0.335, 0.703] | 45.8% / 288.9% | NOISY |
| single-row-50000 | 30 | 251.500 [241.000, 263.000] / 326.000 | 20.000 [18.000, 23.000] / 41.000 | 0.080 [0.071, 0.093] | 13.2% / 38.9% | NOISY |
| startup-working-set-10000 | 30 | 572.000 [438.000, 681.500] / 1111.000 | 1000.000 [776.000, 1137.500] / 2054.000 | 1.748 [1.491, 2.216] | 42.1% / 38.3% | NOISY |
| startup-working-set-50000 | 30 | 1998.000 [1781.000, 2247.000] / 5040.000 | 3602.500 [3423.500, 4382.000] / 10077.000 | 1.803 [1.643, 2.085] | 47.0% / 75.0% | NOISY |
| delete-100-windows-10000 | 30 | 113.500 [108.500, 134.000] / 250.000 | 185.000 [171.500, 228.000] / 715.000 | 1.630 [1.451, 1.870] | 50.8% / 72.9% | NOISY |
| delete-100-windows-50000 | 30 | 206.500 [196.500, 216.500] / 256.000 | 157.500 [127.500, 176.500] / 242.000 | 0.763 [0.614, 0.854] | 12.4% / 24.2% | NOISY |
| outbox-500-independent | 30 | 16708.000 [14629.000, 19026.500] / 25868.000 | 15751.000 [14264.500, 16441.000] / 23368.000 | 0.943 [0.824, 1.083] | 22.0% / 21.7% | NOISY |

| Storage gate | 95% CI | Limit | Verdict | Reason |
| --- | ---: | ---: | --- | --- |
| single-row durable apply p95 <= 10 ms | [26.000, 56.000] | 10 | NOISY | not quiescent |
| 50k/10k engine p95 <= 1.25x | [0.018, 0.571] | 1.25 | NOISY | not quiescent |
| startup-working-set-10000 engine/legacy p95 <= 1x | [1.461, 2.149] | 1 | NOISY | not quiescent |
| startup-working-set-50000 engine/legacy p95 <= 1x | [0.812, 6.284] | 1 | NOISY | not quiescent |
| delete-100-windows-10000 engine/legacy p95 <= 1x | [0.649, 4.206] | 1 | NOISY | not quiescent |
| delete-100-windows-50000 engine/legacy p95 <= 1x | [0.739, 1.013] | 1 | NOISY | not quiescent |

### Single-row IndexedDB request diagnostics

Five extra engine samples per size, after measured rounds. Latencies include native queue time and instrumentation; they are not included in the performance statistics. Each request completion is counted once.

| Operation | 10k requests/sample | 50k requests/sample | 10k mean latency, ms | 50k mean latency, ms |
| --- | ---: | ---: | ---: | ---: |
| `entities.get` | 1 | 1 | 2.400 | 0.400 |
| `entities.put` | 1 | 1 | 3.400 | 1.000 |
| `members.[scope+signature+generation+hashOnly].getAll` | 1 | 1 | 0.800 | 0.000 |
| `members.get` | 1 | 1 | 0.200 | 0.200 |
| `members.put` | 1 | 1 | 2.200 | 0.800 |
| `meta.get` | 4 | 4 | 5.500 | 0.150 |
| `meta.put` | 2 | 2 | 1.300 | 0.500 |
| `windows.get` | 1 | 1 | 3.000 | 0.200 |
| `windows.put` | 1 | 1 | 0.400 | 0.200 |

## Interpretation and remaining work

All 18 storage gates are **NOISY**. None of the three browser runs stayed quiescent. The machine reached a one-minute load above 60 on 16 logical CPUs. Stable individual case CVs do not override the run's load warning. These are completed measurements, not reference-hardware acceptance certificates.

The nominal 50k/10k engine p95 ratios are 0.893 in Chromium, 1.222 in Firefox and 0.253 in WebKit. Their bootstrap intervals are recorded above. The earlier 3.70x Chromium result did not reproduce. All thirty diagnostic row updates issued exactly 13 native requests at both sizes. The engine updates one entity, one membership and checkpoint metadata; it does not serialize a collection-sized ID/hash payload. The sparse hashOnly index query has no retained tombstones in this row-update fixture. No SDK request count grew with collection size. Per-request latencies still vary with native storage and scheduling, and this non-quiescent run cannot isolate those effects. No engine adapter change or native scaling fix is justified by these data; the scaling acceptance gate remains open pending a quiet rerun.

Startup costs are worse in the candidate in all three nominal 50k results. The engine working-set path still reads and materializes full window membership/hash maps, including binary-to-hex conversion. That path is a follow-up profiling target, not evidence of a completed startup optimization. Delete results also vary by browser and size; the 100-owner engine path reads owner metadata and writes membership/window records for each owner. Do not infer a universal win from the single-row result.

The 500-intent case uses the same DexieReducerOutbox in both profiles. Its ratio is a storage control measurement, not a new engine outbox improvement or the design doc's independent/dependent network drain gate. Durable admission, server replay and optimistic-layer retirement are outside this timing.

The old fake-indexeddb timings have been removed from this report. They remain in [emulator-results.json](./emulator-results.json), explicitly labeled as emulator data and rejected by gates. Its overwrite path scans secondary indexes; those scans do not establish native IndexedDB performance. Historical non-persistent Chromium snapshots and the aborted development run remain adjacent as historical artifacts, not current acceptance results.

The focused IndexedDB correctness tests passed, 36 tests across indexeddb-engine.test.ts and indexeddb-replica.test.ts. Client typecheck and build passed. All four benchmark statistics/gate checks passed, including CLI exit codes and relevant-adapter noise handling. Fresh-process persisted startup assertions passed in all three browsers for both adapters.

No storage implementation, legacy adapter, wire protocol, package version or Whagons integration changed in this benchmark follow-up. Keep the engine opt-in. No app code change is required. Dependent-chain replay, shaped-network outbox drain, first paint, worker/main-thread tasks, multi-tab memory, owner kill/recovery and physical Safari/device gates remain end-to-end work.
