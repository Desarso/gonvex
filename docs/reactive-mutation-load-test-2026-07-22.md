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

## Fixed-row, multi-tenant connection-budget follow-up

A second workload replaced the growing session-log query with
`userLiveLocations.upsert`. Each of the 100 users repeatedly updated one fixed
row, and the existing 50-query Whagons profile already subscribed to
`userLiveLocations.list`. This removed unbounded test-data growth and exercised
the normal mutation-to-subscription invalidation path across the same 10 tenant
databases.

All four connection-budget runs established 100 WebSockets, returned all 5,000
initial results, and successfully acknowledged all 2,001 attempted mutations.
There were no query errors, mutation errors, or unexpected socket closes.

| Total connection budget | Initial avg / p95 | Mutation avg / p95 bucket | Invalidation avg / p95 bucket | Full updates | Runtime peak RSS | Steady CPU |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20 | 249 ms / 2 s | 3.59 s / 30 s | 1.12 s / 10 s | 1,841 | 623 MiB | 0.75 cores |
| 32 | 1.85 s / 10 s | 2.89 s / 30 s | 770 ms / 5 s | 2,582 | 3,578 MiB | 1.82 cores |
| 48 | 336 ms / 1 s | 2.64 s / 10 s | 589 ms / 5 s | 3,418 | 1,206 MiB | 1.24 cores |
| 64 | 1.37 s / 5 s | 2.57 s / 10 s | 589 ms / 5 s | 4,221 | 2,946 MiB | 2.16 cores |

These are single local runs on a shared workstation, so the non-monotonic cold
startup and RSS measurements are noisy. The stable conclusion is that 48–64
connections improve reactive drain throughput but do not materially improve
mutation server time, while they can greatly increase memory and PostgreSQL
pressure. A higher pool alone is therefore not the reliability fix.

The production-shaped pool was also tested separately at 20 total connections,
2 per tenant database, and 1 idle connection. It completed 5,000 subscriptions
and 2,000 mutations with zero errors:

| Measure | Production-shaped result |
| --- | ---: |
| Initial result average / p95 | 338 ms / 1 s |
| Mutation round trip average / p95 bucket | 3.48 s / 10 s |
| Mutation server average / p95 bucket | 1.02 s / 5 s |
| Change commit to client average / p95 bucket | 812 ms / 5 s |
| Runtime steady RSS / peak RSS | 306 MiB / 1,047 MiB |
| Runtime steady CPU | 1.29 cores |
| Server-to-client bytes | 10.43 MB |
| Steady server-to-client rate / peak | 0.12 MB/s / 1.00 MB/s |

The final build, including process-wide budget telemetry, repeated this result:
5,000 of 5,000 subscriptions and 2,001 of 2,001 mutations completed with zero
errors. It measured 150 waits at the global 20-connection gate totaling 115.5
seconds, compared with 74,202 `database/sql` pool waits totaling 5,542.8
seconds. This separates cross-database budget pressure from the much larger
per-pool/query-contention signal and confirms that simply raising the global
gate cannot remove the dominant queueing.

An experimental one-reactive-query-per-tenant limiter was rejected after it
failed the same workload: only 1,147 of 2,000 mutations completed before the
result timeout, 31 mutation errors were observed, and invalidation latency rose
to 1.67 seconds. Serializing reruns created a backlog instead of protecting
writes, so that change is intentionally not part of the runtime.

## Production interpretation

A runtime replica is one Gonvex server process or container. Every replica has
its own in-memory connection budget, so two replicas configured for 20 can open
up to 40 PostgreSQL connections. Tenant databases provide table, migration,
lock, and backup isolation, but databases on the same PostgreSQL instance still
share CPU, RAM, disk I/O, WAL, network, and the instance-wide
`max_connections` limit.

Keep the production default at 20 total connections per replica and 2 per
tenant database until the production PostgreSQL allocation is measured. For
`R` replicas, use:

`per-replica budget <= floor((PostgreSQL max connections - reserved/admin - other services - safety headroom) / R)`

The runtime now accepts an explicit total budget up to a hard ceiling of 64,
but production remains at 20. This makes a measured deployment allocation
possible without allowing a typo to create an unbounded pool. Scaling replicas
requires dividing the database allocation between them; the runtime processes
cannot coordinate that limit themselves.

At the observed 100-user workload, a naive 100x linear network projection for
10,000 equally active users is about 12 MB/s (96 Mbps), or roughly 43 GB/hour,
with a cold-hydration peak near 100 MB/s (800 Mbps). This is a planning bound,
not a capacity claim: query result sizes, compression, change frequency, and
subscription sharing determine the real number. More importantly, 10,000 users
performing one mutation per second would mean 10,000 mutations/s, while this
single local PostgreSQL instance already showed multi-second write queueing at
100 mutations/s.

The next scaling fix is to reduce invalidation amplification. In this workload,
`userLiveLocations.list` returns a tenant-wide result but is identity-scoped, so
one tenant mutation can rerun the same logical list for every connected user in
that tenant. After a handler-by-handler security audit, Whagons queries whose
results depend only on tenant and permission set should opt into
permission-safe shared subscriptions. That application manifest change can
reduce database work by an order of magnitude without weakening the runtime's
default user isolation.

Additional reports:

- `tmp/load-reports/reactive-location-100users-budget20.json`
- `tmp/load-reports/reactive-location-100users-budget32.json`
- `tmp/load-reports/reactive-location-100users-budget48.json`
- `tmp/load-reports/reactive-location-100users-budget64.json`
- `tmp/load-reports/reactive-location-100users-production-pool.json`
- `tmp/load-reports/reactive-location-100users-production-final.json`
- Rejected limiter experiment:
  `tmp/load-reports/reactive-location-100users-production-limited.json`
