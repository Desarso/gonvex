# Gonvex multi-tenant reactive load test — 2026-07-22

## Result

Gonvex sustained 10,000 distinct synthetic users across 10 physical tenant databases. Every user held one WebSocket connection and 50 Whagons workspace subscriptions.

| Measure | Result |
| --- | ---: |
| WebSocket connections | 10,000 / 10,000 |
| Subscriptions | 500,000 / 500,000 |
| Subscription errors | 0 |
| Setup errors | 0 |
| Unexpected closes | 0 |
| Users per tenant | 1,000 |
| Subscriptions per tenant | 50,000 |
| Ramp / hold | 20 minutes / 30 seconds |
| Total run time | 20m 38.9s |

Each tenant completed all 50,000 initial results without error. Tenant-level p95 first-result latency was 500 ms for all 10 tenants.

## Latency

| Phase | Average | p50 | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| Connect | 0.86 ms | 1 ms | 5 ms | 10 ms | 27.83 ms |
| Synthetic auth | 6.69 ms | 5 ms | 20 ms | 50 ms | 87.23 ms |
| First result, end to end | 74.02 ms | 50 ms | 500 ms | 500 ms | 2.83 s |
| Server query | 71.66 ms | 50 ms | 500 ms | 500 ms | 2.82 s |

The close match between server-query and end-to-end latency shows that the corrected subscription scheduler added little queueing at the tested arrival rate.

## Runtime and host resources

The test ran locally on an AMD Ryzen 7 5800X with 14 logical CPUs available to the environment and 31.23 GiB RAM.

| Resource | Peak / minimum |
| --- | ---: |
| Gonvex RSS | 4.15 GiB peak |
| Gonvex CPU | 9.92 cores peak |
| Gonvex file descriptors | 10,196 peak |
| Gonvex OS threads | 29 peak |
| Load-generator RSS | 0.83 GiB peak |
| Load-generator CPU | 0.40 cores peak |
| Load-generator file descriptors | 10,007 peak |
| Host available memory | 10.64 GiB minimum |

At this workload, plan approximately 0.45 MiB of Gonvex RSS per connected user including the runtime baseline, or 9 KiB per active subscription. This is an observed sizing ratio for the Whagons 50-subscription profile, not a universal per-subscription constant.

## Network and data throughput

WebSocket compression was enabled.

| Direction / measure | Result |
| --- | ---: |
| Server to clients, wire bytes | 0.874 GB (0.814 GiB) |
| Server to clients, logical bytes | 5.127 GB (4.775 GiB) |
| Read compression ratio | 5.86:1 |
| Clients to server, wire bytes | 63.9 MB (60.9 MiB) |
| Average server egress | 5.65 Mbps |
| Peak server egress | 12.19 Mbps |
| Average client ingress to server | 0.41 Mbps |
| Peak client ingress to server | 0.46 Mbps |
| Peak initial-result rate | 519 results/second |

The cold-start cohort used about 87.4 KB of server egress per user, or 1.75 KB per initial subscription on the wire. Logical result data averaged 512.7 KB per user.

Capacity examples for the same data shape:

- One complete 10,000-user cold start transfers about 0.87 GB of application WebSocket payload. Allow roughly 1.1 GB after adding a 25% transport/TLS planning margin.
- One complete cold start per day is about 26.2 GB/month measured, or roughly 32.8 GB/month with that margin.
- One complete cold start per hour is about 629 GB/month measured, or roughly 787 GB/month with that margin.
- Delivering the same cohort in 60 seconds instead of the tested 20-minute ramp would require about 117 Mbps average server egress before transport margin.

Steady-state bandwidth depends on invalidation selectivity. A worst-case full-result refresh of all 500,000 subscriptions has approximately the same 0.87 GB payload cost as the cold start. Unchanged-result progress messages and keyed patches should make normal selective updates materially smaller, but this run did not apply a write storm during the final hold.

## Limits found and fixed during the test

### PostgreSQL connection churn

Budgeted database pools retained zero idle connections, causing a fresh PostgreSQL TCP/TLS session for nearly every query. At 273–500 users, the local Docker-published PostgreSQL path began resetting connections.

The runtime now retains at most one warm connection per database with a one-second idle expiry. The process-wide 20-connection safety ceiling remains in force.

### Explicit tenant routing lost during registry hydration

Registry hydration replaced explicit environment tenant URLs with older persisted URLs. This prevented the isolated test runtime from using the intended direct local PostgreSQL route.

The server now keeps an immutable copy of deployment-provided tenant routes and gives those routes precedence over discovered registry metadata.

### Quadratic subscription accounting

Every subscription attach and detach recomputed the listener count by scanning and locking every existing group. With distinct users this made startup O(n²). Before the fix, the 2,500-user run accepted all 125,000 subscriptions but only completed 122,093 results before the five-minute deadline; 585 sockets timed out.

The manager now maintains an exact listener counter under its existing lock. The corrected 2,500-user run completed 125,000 / 125,000 results with zero errors and p95 latency of 200 ms.

### Per-user retained result snapshots

Identity-scoped groups retained a complete JSON result even when they had only one listener. At 1,000 users, runtime RSS peaked at 1.52 GiB.

One-listener groups now retain the result hash, row IDs, and last delivered listener token, but not the full JSON payload. Multi-listener shared groups still retain snapshots for immediate replay and keyed patches. A reconnecting listener is tracked separately and receives a full result even when its query hash is unchanged.

At the same 1,000-user workload, peak RSS fell to 653 MiB, a 58% reduction. The final 10,000-user run peaked at 4.15 GiB.

## Test topology and caveats

- The runtime, generator, PostgreSQL, and tenant databases were local to one machine.
- Ten disposable tenant databases were cloned from the same Whagons load-test database. They had distinct database state containers but identical starting data shapes.
- Each WebSocket used a distinct synthetic user ID. Synthetic users shared the same permission shape; this test did not include external identity-provider or JWT network latency.
- The workload used the current Whagons 50-subscription workspace profile and measured cold subscription startup plus a 30-second connected hold.
- No browser windows were created; the Go generator held protocol-level WebSocket clients.
- No write/invalidation storm was applied during the final 10,000-user hold. That should be measured separately for change-to-client latency, patch efficiency, and worst-case database rerun throughput.
- Raw report: `tmp/load-reports/reactive-distinct-multitenant-stage-10000-memory-fixed-final.json`.
