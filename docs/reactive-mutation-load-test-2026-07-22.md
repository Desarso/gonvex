# Gonvex 100-user mutation and invalidation load test — 2026-07-22

## Result

The local runtime completed a matched control run and a mutation run with 100
distinct synthetic users across 10 disposable tenant databases. Each user held
one WebSocket connection and 50 subscriptions. The mutation run sent 100
aggregate mutations per second for 20 seconds, or approximately one mutation
per user per second.

All 5,000 initial subscriptions and all 2,000 mutations completed without an
error or unexpected connection close. The runtime did not run out of CPU,
memory, or network capacity, but the write workload saturated the process-wide
PostgreSQL connection budget and produced severe mutation acknowledgement
queueing.

| Measure | Control | 100 mutations/s |
| --- | ---: | ---: |
| Connections | 100 / 100 | 100 / 100 |
| Initial subscription results | 5,000 / 5,000 | 5,000 / 5,000 |
| Mutations | 0 | 2,000 / 2,000 |
| Operation errors | 0 | 0 |
| Run duration | 30.25 s | 36.53 s |
| Initial-result average | 168.56 ms | 196.25 ms |
| Initial-result p95 bucket | 1 s | 1 s |

The mutation workload stopped sending at 30 seconds, but the runtime needed
another 6.53 seconds to acknowledge the backlog and finish reactive delivery.

## Mutation and reactive latency

| Phase | Average | p50 bucket | p95 bucket | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Mutation round trip | 3.05 s | 2 s | 10 s | 15.09 s |
| Mutation server duration | 858.70 ms | 500 ms | 5 s | 6.43 s |
| Change commit to client | 616.26 ms | 200 ms | 5 s | 6.56 s |
| Reactive query execution | 487.12 ms | 100 ms | 5 s | 5.95 s |

The runner uses bounded histograms, so percentile values are bucket upper
bounds rather than interpolated exact percentiles.

The 2,000 mutations produced 8,795 client-visible invalidation messages:

- 3,456 full query results;
- 5,339 unchanged-result progress messages;
- 0 keyed patches;
- 92.64 MB of logical invalidation payload before WebSocket compression.

The runtime coalesced 22,103 redundant rerun requests, but still executed
13,805 reactive query reruns. The mutation target was
`analytics.createSessionLog`; one profile subscription was changed from
`tenants.getByDomain` to `analytics.listSessionLogs` so every write had an
observable subscribed dependency while the total stayed at 50 subscriptions
per user.

## CPU, memory, and network

| Resource | Control | Mutation run |
| --- | ---: | ---: |
| Runtime peak RSS, full run | 547.33 MiB | 586.48 MiB |
| Runtime CPU, idle hold average | 0.003 cores | — |
| Runtime CPU, mutation/drain average | — | 0.83 cores |
| Runtime CPU, mutation/drain peak | — | 2.11 cores |
| Runtime CPU, full-run peak | 8.58 cores | 8.44 cores |
| Server-to-client wire bytes | 8.18 MB | 15.11 MB |
| Server-to-client wire rate, mutation/drain average | — | 2.21 Mbps |
| Server-to-client wire rate, mutation/drain peak | — | 5.88 Mbps |

The mutation run added 6.93 MB of compressed server-to-client traffic over the
control. Network, CPU, and memory all retained substantial headroom at this
100-user scale. Cold subscription hydration, not the mutation phase, produced
the full-run CPU peak.

## Bottleneck found

Runtime metrics recorded 29,588 PostgreSQL pool waits and 1,964 seconds of
cumulative wait time. Gonvex deliberately enforces a hard process-wide budget
of 20 physical PostgreSQL connections across the landlord and all tenant
pools. With 10 active tenants, concurrent mutation commits, and reactive query
reruns, work queued behind that budget:

| Runtime metric | Result |
| --- | ---: |
| `analytics.createSessionLog` calls | 2,000 |
| Average mutation function duration | 841.08 ms |
| `analytics.listSessionLogs` calls | 4,400 |
| Average list function duration | 496.80 ms |
| Reactive database queries | 13,312 |
| Aggregate reactive database-query time | 5,219.58 s |
| Reactive reruns coalesced | 22,103 |
| Shared subscriptions | 0 |

The test therefore found a database concurrency and invalidation-amplification
limit, not a WebSocket or host-resource limit. The chosen list query also grows
with every inserted log and is intentionally harsher than a bounded or highly
selective production query.

The next optimization pass should first bound or paginate the invalidated list,
then evaluate permission-safe query sharing for identical tenant results and
reduce per-mutation rerun fan-out. Raising the 20-connection safety ceiling
should only be considered after measuring PostgreSQL headroom; doing so alone
would move queueing into the database rather than remove the amplification.

## Artifacts and caveats

- Control report: `tmp/load-reports/reactive-mutation-control-100.json`
- Mutation report: `tmp/load-reports/reactive-mutation-100users-100rps.json`
- The runtime, load generator, PostgreSQL, and all tenant databases ran on one
  local machine.
- Synthetic users had distinct subjects and explicit memberships in the local
  disposable fixture. External identity-provider latency was not included.
- The profile retained 50 subscriptions per user, but replaced one query with
  the mutation-observer query described above.
- Session-log rows created by the test remain only in the disposable local
  tenant databases.
