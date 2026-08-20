# Gonvex Architecture

Gonvex is a transactional Postgres-backed realtime Local Replica system with
an indexed server-query escape hatch for datasets that are too large to
replicate. This document is the architecture contract, not a future wishlist.

## Constitution

1. Postgres commits are the only realtime truth.
2. Reducers are the only normal durable write path.
3. Actions cannot access application database handles; they call Reducers.
4. One Reducer represents one atomic business intent.
5. Declared writes and manual invalidation do not exist.
6. Bounded data uses Replica Collections.
7. Unbounded data uses indexed Live Queries.
8. Live Queries require a structured, inspectable query plan.
9. One server transaction is applied atomically to the Local Replica before UI notification.
10. The Local Replica is the only server-state store exposed to UI bindings.
11. Interactive Reducers require an optimistic contract or an explicit reviewed exception.
12. Capacity limits protect execution but never determine freshness.

## Executable functions

Application code has exactly three executable function kinds.

### Query

A Query is read-only and one-shot. Gonvex executes it in a repeatable-read,
read-only Postgres transaction. It is called with `useQuery` or `client.query`.

### Reducer

A Reducer is the only public durable state transition. Gonvex begins one
Postgres transaction and exposes only `ctx.Tx` to application code. Raw pools
are hidden so an operation cannot accidentally commit outside the business
transaction. Failure rolls the complete intent back.

```go
app.Reducer(
  "tasks.rename",
  RenameTask,
  gonvex.OptimisticReducer("tasks").RowIDArg("taskId").FieldsArg("patch"),
)
```

`Writes(...)`, manual notifications, and refetch instructions are not part of
the API. `_gonvex_sync_changes` records what actually committed.

### Action

An Action performs external or long-running work such as HTTP, email, AI,
push, and files. Its database handles are unavailable. Durable state must
re-enter through:

```go
result, err := ctx.Reducers.Call(ctx, "tasks.markDelivered", args)
```

Business rows and external-work requests should be committed together through
a durable outbox. The Action worker processes the outbox idempotently.

## Authoritative change feed

Every application table has a row-level trigger that appends to
`_gonvex_sync_changes`. Each committed transaction has one monotonically
increasing revision and ordered row changes:

```text
revision / ordinal / origin command / table / row id
operation / old value / new value / changed columns
```

Postgres sends a small `gonvex_change_feed` notification only to announce that
a revision exists. Gonvex reads the durable revision before routing it. The
former `gonvex_table_change` statement triggers are removed during migration
and have no installer or runtime fallback.

## Delivery modes

Delivery is separate from executable function kind.

### Replica Collection

A Replica Collection is a bounded, entity-shaped set with explicit row and
byte budgets. A complete collection supports exact offline evaluation; a
truncated collection reports `partial` rather than pretending to represent the
database.

```go
app.ReplicaCollection(
  "tasks.recent",
  RecentTasks,
  gonvex.ReplicaTable("tasks").
    Columns("id", "title", "status", "updated_at").
    OrderBy("updated_at", "desc").
    Budget(10_000, 50<<20),
)
```

### Live Query

A Live Query maintains an exact server-computed result window for a large
dataset. It is not arbitrary reactive SQL. Registration requires
`gonvex.LivePlan(...)`, which declares searchable/filterable/sortable fields,
window budgets, and offline capability.

```go
app.LiveQuery("tasks.grid", TasksGrid, gonvex.LivePlan(
  gonvex.LiveTable("tasks").
    Filter(gonvex.Eq("workspace_id", gonvex.Arg("workspaceId"))).
    SearchArg("search", "title", "description").
    SortArgs("sort", "direction", "deadline", "asc", "deadline", "created_at").
    WindowArgs("offset", "limit", 100, 150),
))
```

The plan compiles to parameterized PostgreSQL. Identifiers are allowlisted by
the plan. Portable operators—equality, inequality, ranges, membership,
contains, case-insensitive contains, boolean composition, sort, and window—can
also run against cached replica rows. A plan can explicitly mark a capability
online-only.

Canonical groups share computation only when query arguments and visibility
fingerprints are equivalent. A title-only change can patch an entity directly.
A change to filter/order/window semantics reruns one canonical SQL query,
diffs IDs, and fans out the small delta.

## Revision-only correctness

A Live Query group has two freshness values:

```text
requiredRevision
computedRevision
```

It is authoritative exactly when:

```text
computedRevision >= requiredRevision
```

If revision 993 arrives during the execution for 992, completing 992 cannot
mark the group current. Retained snapshots and grace timers only control
memory; they carry no correctness meaning. Unknown-dependency and broad
invalidation fallbacks do not exist.

## Local Replica

The Local Replica contains normalized authoritative entities, optimistic
overlays, Live Query ID memberships, completeness/freshness, pending commands,
and the server revision. Live Queries store ordering and membership IDs—not
duplicate entities.

The server emits one `replica.transaction` per visible Postgres transaction:

```text
cursor: database epoch + revision
originCommandId
ordered entity changes
query membership changes
```

The client serializes transaction application. Persistence completes before
one in-memory state swap and one subscriber notification. Back-to-back frames
cannot interleave.

- Web uses transactional IndexedDB storage.
- Expo/mobile uses `@gonvex/expo-sqlite`, with normalized entity/query tables
  and the cursor updated inside one SQLite transaction.

React bindings integrate the replica as an external store. Signals are
selectors over the replica, never another server-state owner.

## Optimistic commands

Optimistic state is an overlay, not fabricated authoritative state. A command
is identified by `originCommandId`. Reducer success includes its committed
revision; the overlay remains until that revision is applied locally. This
gives a continuous transition from optimistic to authoritative state without a
blank or rollback frame.

Every public Reducer must provide optimistic metadata. A genuinely unsuitable
operation must use `OnlineOnlyNonOptimistic("reviewed reason")`, making the
exception visible in generated manifests and the dashboard.

## Visibility

Routing evaluates old and new visibility. A row moving out of a caller's
scope becomes a delete; a row moving in becomes an upsert. Live Query sharing
includes the visibility fingerprint. Equal SQL arguments alone never prove
equal results.

## Offline contract

Hooks expose:

```text
source:       server | cache
completeness: complete | partial
freshness:    current | verifying | offline
```

Portable plans run against cached entities while offline. Partial caches must
be labeled as partial. Server-only operators require a connection.

## Operational protection

Initial Queries, snapshots, and Live Query recomputations share one bounded
admission controller. Admission controls concurrency and latency; it never
changes which revision is required or whether a result is authoritative.
