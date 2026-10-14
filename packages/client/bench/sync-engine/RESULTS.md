# Sync engine storage measurements

## Startup follow-up

The startup regression is fixed in the measured storage cases. The quiet Chromium replication and the final Firefox/WebKit runs pass both 10k and 50k startup p95 gates. This is storage startup, not application first paint or a general release certificate.

The complete matrix used client source `99e928c3d78096fc066918630c1412fe7c58c382` with a clean source tree. [startup-after.json](./startup-after.json) retains every sample, including non-quiescent Chromium observations. The independent [Chromium replication](./startup-chromium-repeat.json) used `7b870d014f5b1124ba6a3ad7e3d71f76de682e63`, also clean. Client source and benchmark logic are identical between these revisions; only the retained remote Rust test fixture and measurement artifacts differ. No original samples were replaced or pooled.

After measurement, remote `5bcaf6c` brought staging's SDK29, snapshot-page negotiation and developer-session updates into the shared branch. That merge was preserved normally. Protocol/module-sdk/local-runtime were rebuilt, and all 46 focused storage tests plus client typecheck/build passed again. The minified benchmark entry bundle before and after rebuilding the merge is byte-identical, SHA-256 `25f6bf2720d908f23e6a073038f03d25fe972c7e6cef4162f8c14718be35f0d8`. These upstream changes do not alter the measured storage code; no transport-performance claim is inferred from these timings.

Only startup cases ran, sequential browsers, nice 15, five warmups, 20 initial samples, a 30-round cap, 15% CV limit and 4,000 seeded bootstrap draws. The machine, seed, fixture, residency budget and fresh-process methodology are unchanged. The table below explicitly uses the quiet Chromium replication and Firefox/WebKit from the complete matrix. All six cases have n=20.

| Browser / rows | Legacy median [95% CI], ms | Engine median [95% CI], ms | Legacy p95 [95% CI], ms | Engine p95 [95% CI], ms | Engine/legacy p95 [95% CI] | Startup gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| chromium / 10000 | 429.150 [425.700, 435.650] | 353.650 [350.850, 356.750] | 447.000 [436.900, 463.400] | 362.000 [357.100, 369.700] | 0.810 [0.778, 0.841] | PASS |
| chromium / 50000 | 1689.950 [1656.950, 1706.500] | 1516.000 [1507.050, 1527.100] | 1732.700 [1721.800, 1753.100] | 1548.200 [1530.500, 1550.600] | 0.894 [0.873, 0.901] | PASS |
| firefox / 10000 | 419.500 [407.500, 440.000] | 337.500 [330.000, 345.500] | 462.000 [444.000, 463.000] | 379.000 [349.000, 380.000] | 0.820 [0.754, 0.856] | PASS |
| firefox / 50000 | 1586.000 [1544.500, 1640.000] | 1273.000 [1203.500, 1330.500] | 1721.000 [1687.000, 1748.000] | 1428.000 [1339.000, 1451.000] | 0.830 [0.781, 0.860] | PASS |
| webkit / 10000 | 286.000 [283.500, 292.000] | 219.500 [216.000, 226.500] | 312.000 [294.000, 335.000] | 243.000 [230.000, 273.000] | 0.779 [0.710, 0.907] | PASS |
| webkit / 50000 | 1258.000 [1248.000, 1419.500] | 935.000 [919.000, 1031.000] | 1571.000 [1421.000, 1651.000] | 1113.000 [1035.000, 1175.000] | 0.708 [0.674, 0.767] | PASS |

| Quiet run | Max observed 1-minute load | Legacy / engine CV at 10k | Legacy / engine CV at 50k |
| --- | ---: | ---: | ---: |
| chromium | 3.09 / 8 threshold | 2.7% / 1.7% | 2.5% / 1.1% |
| firefox | 7.38 / 8 threshold | 5.7% / 5.5% | 5.1% / 6.9% |
| webkit | 4.95 / 8 threshold | 4.8% / 6.5% | 9.7% / 8.4% |

The initial complete Chromium run remained non-quiescent, with peak load 8.50. Its 10k p95 ratio was 0.842 [0.734, 1.310] and its 50k ratio was 0.882 [0.857, 0.916]. Both gates remain NOISY in that file. The quiet replication resolves Chromium independently. Non-startup gates are intentionally missing/NOISY in these startup-only reports; no write, delete, outbox or whole-engine acceptance is claimed.

### Before and after

Historical 50k p95 values below come from the original hardened matrix on `a3fd7d0`, retained in results.json. Every historical browser was non-quiescent. The absolute times span different host loads and must not be interpreted as a controlled absolute speedup. The new matched engine/legacy comparisons above are the acceptance evidence.

| Browser | Historical legacy / engine p95, ms | New quiet legacy / engine p95, ms | New engine p95 reduction relative to matched legacy |
| --- | ---: | ---: | ---: |
| chromium | 4791.800 / 5831.000 | 1732.700 / 1548.200 | 10.6% |
| firefox | 2584.000 / 3917.000 | 1721.000 / 1428.000 | 17.0% |
| webkit | 2152.000 / 3644.000 | 1571.000 / 1113.000 | 29.2% |

### Operation and CPU evidence

The original loader already used getAll-backed entity pages and six bulk membership reads. It did not issue a per-member cursor or a per-ID entity join. Those membership reads still cloned another 50,500 records at 50k, then eagerly decoded 50,000 binary hashes and constructed temporary ordering/hash-entry arrays. The busy-host [before profile](./startup-before.json) assigned about 520 ms to the hex decoder, plus its callback and hash-entry mapping. CPU profile time includes scheduling and instrumentation; it is diagnostic evidence, not a controlled CPU-speedup measurement.

All final browsers produced identical operation counts. Values read exclude key-only records. Total requests count issued requests, not repeated cursor success events. The old meta cursor was issued once and continued three times.

| Counter | Original engine 10k / 50k | Final engine 10k / 50k | Final legacy 10k / 50k |
| --- | ---: | ---: | ---: |
| IDB values read | 21,011 / 99,151 | 10,514 / 48,567 | 10,508 / 48,648 |
| Entity getAll requests | 43 / 190 | 42 / 190 | 43 / 190 |
| Membership getAll requests | 6 / 6 | 0 / 0 | 0 / 0 |
| Total issued IDB requests | 53 / 200 | 60 / 208 | 46 / 193 |
| Value cursors / continue calls | 1 / 3 at both sizes | 0 / 0 | 0 / 0 |
| JSON.parse calls | 10,507 / 48,558 | 10,513 / 48,564 | 10,507 / 48,558 |
| Eager binary hash conversions | 10,000 / 50,000 | 0 / 0 | 0 / 0 |

The final loader trades a few constant-size metadata requests and six extra JSON parses for eliminating the membership materialization pass. It does not claim fewer total requests. At 50k it reads 51% fewer values than the original engine, while retaining exactly the same rows and windows. One-page read-ahead overlaps the next native read with current-page JSON decoding, inside the same atomic transaction. Two pages of at most 256 records bound temporary row reads; the next page is issued only when the current page fits both resident budgets. Four-window batches run after the entity pass.

For an ordinary hashed ID, the original JavaScript expression constructs five transient arrays and one temporary order object, before V8 optimizations, in addition to native MemberRecord/typed-array materialization and the final hash string. The clean-generation cache path constructs none of those per-member temporary structures and performs no hash-byte decoding. The required row JSON parsing and final ID/hash strings remain. These are source-level construction counts, not exact allocator measurements. Sibling .cpu.json and .heap.json files preserve CDP CPU and 32 KiB sampled live-allocation traces. Sampled live bytes after the call do not measure total transient allocation or peak RSS, so no per-row byte or battery claim is inferred from them.

The read index consists of a compact per-generation ID/hash snapshot in the existing meta table and one latest patch per edited member. Both are written with authoritative membership in the same transaction. A window sequence marker prevents reuse after older SDK writers; missing bases, generation mismatches and stale markers fall back to membership. Retirement cleanup removes cache records within its existing bounded budget. Primary-key getAllKeys pages discover scopes without reading cache payloads or using the secondary unique-key cursor that failed during WebKit initialization.

### Rejected trials and limits

- [startup-intermediate.json](./startup-intermediate.json) contains the interrupted pre-index trial with faster hex conversion/direct ID collection and 2,048-row entity pages. Chromium 10k still regressed and 50k remained slower.
- [startup-cache-trial.json](./startup-cache-trial.json) measured the compact index with 2,048-row pages and concurrent entity/window loading on 37e9239. Chromium 50k regressed about 10%; native reads overshot the byte budget by roughly 500 rows.
- [startup-512-trial.json](./startup-512-trial.json) measured 84a6bc7. Larger native row batches still increased entity-read latency. They were rejected; the final cap is 256.
- [startup-small-trial.json](./startup-small-trial.json) measured fdfb215 without read-ahead. Chromium 50k remained about 6% slower. Read-ahead supplied the remaining improvement.
- [startup-pipeline-pre-webkit.json](./startup-pipeline-pre-webkit.json) retains the incomplete f081a17 matrix, including Firefox noise/load warnings and the WebKit cursor failure. [startup-webkit-debug.json](./startup-webkit-debug.json) identifies variant 1 as the failing seed. [startup-webkit-smoke.json](./startup-webkit-smoke.json) proves the primary-key initialization fix, with five samples only and a non-quiescent 50k case. None is substituted into final acceptance evidence.

The benchmark seeds clean generation indexes. Tests cover edited-index replay, moves/rebalancing, duplicate IDs, hash-only certificates, older writers, missing/stale indexes, rollback, cross-scope retirement and prefetch errors; patch-heavy startup timing is not measured. Existing uncached or invalidated generations fall back until a full window replacement writes a fresh index. The cache adds derived disk data and one small metadata put per member edit. Write/delete performance was deliberately not rerun under the startup-only brief. UI-window filtering, lazy hash getters, schema upgrades, protocol changes, first paint, physical Safari/iOS, battery and broader app gates remain outside this work.

Validation: 46 focused storage tests passed; client typecheck/build passed; four benchmark statistical checks passed. No full test suite or cargo workspace run. No Whagons app code change, contract/version bump by this startup follow-up, npm publication or PR merge. The upstream SDK29 version preparation was retained.

## Original full matrix

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

## 2026-10-14 quiet run

The full default matrix completed sequentially in Chromium, Firefox and WebKit. This was scheduled as a quiet run and started below the requested 4.0 load limit, but other heavy processes started during measurement. It does not certify an entirely quiet host. No samples were discarded, replaced or pooled with earlier runs.

Source commit `ad7364e0ea7152da1cc3fd19c85e079cd33a8445`, source dirty flag `false`. Node v26.8.2, process priority nice 15. Default settings: five warmups, 20 initial samples, adaptive cap 60, CV limit 15%, and 4,000 seeded bootstrap draws. TMPDIR was `/home/gabriel/.cache/sdk-wave1/tmp`. The runner's default quiescence threshold is 8 on 16 logical CPUs. The task's 4.0 limit was checked before starting; it was not substituted for the runner's default.

Initial uptime/top load averages were 0.67 / 1.76 / 2.55, then 1.43 / 1.65 / 2.42 immediately before launch. No pre-start wait was needed. The initial top snapshot showed Gabriel's Gonvex, Chrome and Electron processes, plus root's Tailscale daemon. Root's MinIO daemon also appeared later. At 23:10 Pacific, Gabriel's Python PIDs 3921979 and 3909294 used about 380% and 297% CPU at nice 5. They were unrelated to this benchmark and were not stopped. External uptime observations reached 9.99 during Chromium outbox, between runner readings. Chromium's runner quiescent flag therefore does not prove continuous quiet. Firefox recorded peak one-minute load 39.37.

Raw samples and metadata: [quiet-2026-10-14-results.json](./quiet-2026-10-14-results.json). Sibling Chromium CPU/heap profiles are diagnostics, excluded from timings. Existing measurement artifacts and report sections are preserved.

| Browser | Before load 1 / 5 / 15 min | After load 1 / 5 / 15 min | Max runner 1-min load | Runner quiescent |
| --- | --- | --- | ---: | --- |
| chromium | 1.430 / 1.650 / 2.420 | 6.490 / 5.850 / 4.650 | 6.490 | yes |
| firefox | 6.490 / 5.850 / 4.650 | 7.600 / 13.370 / 9.480 | 39.370 | no |
| webkit | 7.600 / 13.370 / 9.480 | 1.890 / 2.420 / 4.390 | 7.600 | yes |

Milliseconds; brackets are bootstrap 95% confidence intervals. CV status is separate from gate verdicts.

| Browser | Case | n | Legacy p95 [95% CI], ms | Engine p95 [95% CI], ms | Legacy / engine CV | Case CV status |
| --- | --- | ---: | ---: | ---: | --- | --- |
| chromium | single-row-10000 | 60 | 36.700 [31.800, 38.200] | 2.600 [2.500, 3.000] | 16.5% / 22.7% | NOISY |
| chromium | single-row-50000 | 60 | 101.500 [99.300, 103.300] | 2.500 [2.400, 4.800] | 5.8% / 31.4% | NOISY |
| chromium | startup-working-set-10000 | 20 | 445.500 [430.100, 474.700] | 354.800 [351.500, 354.900] | 3.2% / 1.3% | OK |
| chromium | startup-working-set-50000 | 20 | 1692.800 [1685.100, 1710.400] | 1547.300 [1507.300, 1610.500] | 2.5% / 2.3% | OK |
| chromium | delete-100-windows-10000 | 20 | 68.000 [63.700, 71.800] | 62.000 [60.800, 67.100] | 7.3% / 7.9% | OK |
| chromium | delete-100-windows-50000 | 20 | 114.000 [111.100, 117.400] | 63.900 [58.600, 84.200] | 4.8% / 13.5% | OK |
| chromium | outbox-500-independent | 60 | 33785.800 [29682.100, 37841.800] | 33243.200 [29616.100, 37991.100] | 51.5% / 50.3% | NOISY |
| firefox | single-row-10000 | 60 | 51.000 [47.000, 52.000] | 9.000 [7.000, 10.000] | 9.3% / 31.6% | NOISY |
| firefox | single-row-50000 | 60 | 108.000 [105.000, 116.000] | 6.000 [4.000, 15.000] | 5.5% / 360.8% | NOISY |
| firefox | startup-working-set-10000 | 60 | 646.000 [602.000, 674.000] | 525.000 [483.000, 542.000] | 28.1% / 28.5% | NOISY |
| firefox | startup-working-set-50000 | 20 | 3257.000 [3061.000, 3363.000] | 2768.000 [2429.000, 2772.000] | 8.6% / 11.8% | OK |
| firefox | delete-100-windows-10000 | 60 | 125.000 [106.000, 138.000] | 267.000 [225.000, 308.000] | 15.8% / 41.2% | NOISY |
| firefox | delete-100-windows-50000 | 60 | 167.000 [144.000, 226.000] | 146.000 [131.000, 160.000] | 43.1% / 26.7% | NOISY |
| firefox | outbox-500-independent | 20 | 1955.000 [1800.000, 2077.000] | 1856.000 [1809.000, 1861.000] | 9.5% / 5.9% | OK |
| webkit | single-row-10000 | 60 | 35.000 [35.000, 38.000] | 16.000 [11.000, 18.000] | 19.3% / 23.5% | NOISY |
| webkit | single-row-50000 | 60 | 129.000 [127.000, 134.000] | 18.000 [11.000, 20.000] | 9.8% / 28.7% | NOISY |
| webkit | startup-working-set-10000 | 20 | 258.000 [248.000, 261.000] | 198.000 [195.000, 202.000] | 2.7% / 2.6% | OK |
| webkit | startup-working-set-50000 | 20 | 1200.000 [1172.000, 1202.000] | 890.000 [881.000, 892.000] | 2.0% / 1.3% | OK |
| webkit | delete-100-windows-10000 | 40 | 74.000 [67.000, 98.000] | 137.000 [133.000, 137.000] | 14.5% / 12.3% | OK |
| webkit | delete-100-windows-50000 | 40 | 149.000 [141.000, 159.000] | 149.000 [147.000, 151.000] | 5.3% / 14.8% | OK |
| webkit | outbox-500-independent | 20 | 13355.000 [12754.000, 13355.000] | 13286.000 [12942.000, 13327.000] | 3.2% / 2.9% | OK |

These are the unmodified gates.mjs verdicts. Chromium had an externally observed load breach that its sparse runner readings missed, so its nominal PASS verdicts do not certify a continuously quiet host. Firefox gates are NOISY because of observed non-quiescence.

| Browser | Gate | 95% CI | Limit | Verdict | Reason |
| --- | --- | ---: | ---: | --- | --- |
| chromium | single-row durable apply p95 <= 10 ms | [2.400, 4.800] | 10 | NOISY | required adapter CV exceeds threshold |
| chromium | 50k/10k engine p95 <= 1.25x | [0.800, 1.920] | 1.25 | NOISY | required adapter CV exceeds threshold |
| chromium | startup-working-set-10000 engine/legacy p95 <= 1x | [0.747, 0.825] | 1 | PASS | 95% bootstrap interval |
| chromium | startup-working-set-50000 engine/legacy p95 <= 1x | [0.887, 0.952] | 1 | PASS | 95% bootstrap interval |
| chromium | delete-100-windows-10000 engine/legacy p95 <= 1x | [0.854, 1.053] | 1 | NOISY | 95% bootstrap interval |
| chromium | delete-100-windows-50000 engine/legacy p95 <= 1x | [0.499, 0.754] | 1 | PASS | 95% bootstrap interval |
| firefox | single-row durable apply p95 <= 10 ms | [4.000, 15.000] | 10 | NOISY | not quiescent |
| firefox | 50k/10k engine p95 <= 1.25x | [0.400, 2.143] | 1.25 | NOISY | not quiescent |
| firefox | startup-working-set-10000 engine/legacy p95 <= 1x | [0.748, 0.870] | 1 | NOISY | not quiescent |
| firefox | startup-working-set-50000 engine/legacy p95 <= 1x | [0.746, 0.893] | 1 | NOISY | not quiescent |
| firefox | delete-100-windows-10000 engine/legacy p95 <= 1x | [1.681, 2.775] | 1 | NOISY | not quiescent |
| firefox | delete-100-windows-50000 engine/legacy p95 <= 1x | [0.619, 1.111] | 1 | NOISY | not quiescent |
| webkit | single-row durable apply p95 <= 10 ms | [11.000, 20.000] | 10 | NOISY | required adapter CV exceeds threshold |
| webkit | 50k/10k engine p95 <= 1.25x | [0.667, 1.727] | 1.25 | NOISY | required adapter CV exceeds threshold |
| webkit | startup-working-set-10000 engine/legacy p95 <= 1x | [0.751, 0.815] | 1 | PASS | 95% bootstrap interval |
| webkit | startup-working-set-50000 engine/legacy p95 <= 1x | [0.733, 0.755] | 1 | PASS | 95% bootstrap interval |
| webkit | delete-100-windows-10000 engine/legacy p95 <= 1x | [1.388, 2.045] | 1 | FAIL | 95% bootstrap interval |
| webkit | delete-100-windows-50000 engine/legacy p95 <= 1x | [0.931, 1.064] | 1 | NOISY | 95% bootstrap interval |

Gate totals: 5 PASS, 1 FAIL, 12 NOISY. NOISY is not acceptance. The outbox case is a storage control using the same durable outbox in both modes; it has no network-drain gate.

Validation: protocol, module-sdk and local-runtime builds passed; client typecheck and build passed; all four bench:sync-engine:check tests passed. The complete native matrix ran all 21 browser/case combinations with default sample settings. The runner and the separate gates.mjs invocation exited 1 because of FAIL verdicts. No full tests, cargo tests or source edits were needed for this measurement-only task.

No Whagons app code change is required. Keep the engine opt-in. No protocol, package version or client-contract change, npm publication, PR merge or speculative performance fix was made. A reserved-machine rerun is still needed for continuous quiet-host acceptance, and any FAIL verdict needs a separate focused investigation. These storage measurements do not certify server replay, network, first paint, physical Safari/iOS, memory or battery behavior.
