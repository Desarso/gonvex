# Durable Sync Collections

Gonvex syncs are explicit, authorized, single-table collections that render
from IndexedDB and resume from a durable Postgres cursor. They complement live
queries; they do not replace computed queries, search, aggregates, or arbitrary
joins.

## Function declaration

```go
app.Sync(
    "tasks.recent",
    RecentTasks,
    gonvex.SyncTable("tasks").
        Key("_id").
        Columns("_id", "tenantId", "workspaceId", "title", "updatedAt", "deletedAt").
        EqualArg("tenantId").
        EqualArg("workspaceId").
        ExcludeWhenSet("deletedAt").
        VisibilityDependsOn("userTeams", "workspaces").
        OrderBy("updatedAt", "desc").
        Progressive().
        Budget(100, 4_194_304),
)
```

The handler returns the initial authorized rows. The declaration tells the
runtime which projected columns may enter the durable log and how later row
changes enter or leave the collection.

Use `Eager` when the complete collection fits its row and byte budgets. Use
`Progressive` for a bounded ordered window. Progressive streams reconcile the
current top-N keys after relevant transactions, so inserts, deletes, and
reorders correctly cross the window boundary while only changed rows travel to
the browser.

## Commit and resume model

1. Per-table row triggers stage projected old/new row values.
2. A deferred constraint trigger assigns one monotonically increasing revision
   to every row change in the transaction.
3. `NOTIFY` is only a wake-up hint. The durable `_gonvex_sync_changes` table is
   the source of truth.
4. The browser persists normalized collections plus `{epoch, revision}`.
5. Reload sends the cursor and cached keys. If retained, the runtime returns
   only later deltas; otherwise it sends a fresh authorized snapshot.

The runtime prunes the log no more than hourly. Default retention is seven
days; a sync may request a longer window with `RetainFor`.

## Auth and visibility

IndexedDB collections are scoped to the server-issued cache scope. The client
also remembers the last scope by project, tenant, JWT issuer, and JWT subject,
which allows a warm snapshot to render while server auth is revalidated.
Server auth, definition, or visibility changes reset the collection.

`VisibilityDependsOn` must name every table that can change which rows the
handler returns without changing the source row. A change to one of those
tables resets and reauthorizes the collection.

Only declared columns are captured. Soft-delete markers must be declared with
`ExcludeWhenSet`, matching the snapshot handler.

## Client storage

`GonvexClientOptions.sync` configures the normalized Dexie store:

```ts
new ConvexReactClient(url, {
  sync: {
    databaseName: "my-app-sync",
    maxBytes: 150 * 1024 * 1024,
  },
});
```

Each collection enforces its own server-declared row/byte budget. The global
budget evicts least-recently-used collections. Deltas update only affected
IndexedDB rows.

React exposes `useSync` and `useSyncSelector`. Prefer selectors for consumers
that only need a derived subset so unrelated row changes do not rerender them.

## Deliberate v1 boundaries

- One source table per sync.
- Equality arguments and null/non-null exclusion predicates.
- Handler-defined authorization and snapshot shaping.
- Server queries remain the path for search, scroll past a progressive window,
  aggregates, joins, and cache misses.
- Offline writes and mutation rebasing are not enabled yet. Mutation IDs are
  carried through the log so an optimistic queue can be added without changing
  the wire identity model.
