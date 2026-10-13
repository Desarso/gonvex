# Sync engine storage measurements

Phase 0/1 storage fixtures only. No runtime, worker, DOM, subscriber delivery, wire, energy or full-client latency measurement.

Hardware: AMD Ryzen 7 5800X 8-Core Processor, 31.23 GiB RAM, Linux 7.2.4-arch1-2. Node v26.8.2; Chromium 149.0.7827.55. The shared workstation had other development processes, so these are not quiescent reference-hardware certifications.

Seed `1196840961`, artifact `ef83e92b1ed10c04d95d262a95e99156178f8719ce1486b6cce2903c2ff2bcc0`. Task payloads contain 50 mixed columns, four indexed scalar columns, 20,524,480 encoded bytes at 10k rows and 102,620,058 at 50k rows, averaging about 2,052 bytes per row. The Node fixture also seeds five related tables, supplies 250 visible IDs and 1,000 exact IDs, and uses a 200 MiB/unlimited-row working-set budget. Those ID fixtures are not mounted subscriptions. UTF-16 residency accounting can make the 50k working set partial; omitted rows remain retrievable from disk.

Five warmups and 20 measured samples, serially alternating adapter order. Input preparation/hashing and seeding are outside timed calls. See the raw JSON files for every sample and exact source commit.

## Node and fake-indexeddb

Source `c5a6cba395d555edbe98041dae41882f2095e658`. The legacy adapter source is unchanged from the settled base. This records the requested baseline and candidate storage costs.

| Storage metric | Legacy median / p95, ms | Engine median / p95, ms |
| --- | ---: | ---: |
| single-row-10000 | 268.751 / 285.650 | 376.789 / 405.506 |
| startup-working-set-10000 | 291.784 / 330.860 | 887.556 / 933.259 |
| delete-100-windows-10000 | 334.518 / 828.441 | 1463.426 / 3506.492 |
| single-row-50000 | 3016.548 / 4422.360 | 4458.220 / 5379.285 |
| startup-working-set-50000 | 2277.655 / 2611.955 | 6438.144 / 7946.465 |
| delete-100-windows-50000 | 1779.272 / 1927.339 | 7652.076 / 8229.634 |

These emulator timings regress and fail the scaling gate. fake-indexeddb 6.2.5 scans every secondary index entry on overwrites through `RecordStore.deleteByValue` and `ObjectStore.storeRecord`. The additional membership indexes therefore add corpus-sized emulator scans despite constant SDK write counts. No emulator patch or index removal was used to hide that cost.

## Native IndexedDB with isolated heaps

Source `fac4363fb0091836e979868032cc13e519c874ce`. One Chromium browser, two isolated non-persistent BrowserContexts, one adapter per heap, and serial operations. This measures native IndexedDB transactions, not persistence after browser/profile destruction or file-system flush guarantees. The decoded seed corpus is released after seeding; one preallocated fixture hash map prepares certificates with the unchanged XOR algorithm.

| Storage metric | Legacy median / p95, ms | Engine median / p95, ms |
| --- | ---: | ---: |
| single-row-10000 | 17.400 / 21.900 | 1.600 / 2.000 |
| single-row-50000 | 101.100 / 148.600 | 4.900 / 7.400 |

At 50k rows the engine p95 is 20.08x faster than legacy and below the 10 ms storage target. The 50k/10k engine p95 ratio is **3.70x**, exceeding the **1.25x acceptance gate**. Median scaling also misses the gate. Keep the store opt-in. No rollout, visible-delivery, browser cold-paint or battery claim follows from these numbers.

## Original shared-page check

Source `c9e4ee7cf48f2075dff4cc9027559e62803883b3`. This earlier run shared one page/heap between both adapters and retained the decoded seed corpus. Its samples remain available instead of being discarded after changing the fixture isolation.

| Storage metric | Legacy median / p95, ms | Engine median / p95, ms |
| --- | ---: | ---: |
| single-row-10000 | 16.400 / 18.100 | 2.100 / 3.300 |
| single-row-50000 | 139.300 / 201.400 | 6.700 / 26.700 |

## Correctness and remaining gates

Focused tests cover late cross-entity/checkpoint failures, atomic rollback, per-field authority, same-epoch tombstones, exclusion/shared owners, epoch resets, bounded partial reads, order/duplicate preservation, bounded retirement/rebalancing, legacy coexistence, peer gaps and cursor/digest resume. A normalized delete retains its old projected hash without ownership until that collection advances its checkpoint, so reconnect replay can verify the exact old certificate.

The performance snapshots above precede the final certificate-retention fix; that fix is verified functionally and its timing has not been remeasured. Re-run on the final revision with persistent profiles and a quiescent machine before closing the scaling gate, then add Firefox/Safari/device parity before rollout. Existing scalar indexing and LocalReplica RAM materialization remain unchanged; their costs belong to later phases. Automatic import/export of populated stores is not provided, so opt in only in fresh isolated namespaces and retain the engine database for recovery.
