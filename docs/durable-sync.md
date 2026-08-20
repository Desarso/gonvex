# Replica Collections and Local Replica

The public guide lives in
[`apps/docs/content/docs/durable-sync.mdx`](../apps/docs/content/docs/durable-sync.mdx).
This file records the contributor-level contract.

```go
app.ReplicaCollection(
    "tasks.recent",
    RecentTasks,
    gonvex.ReplicaTable("tasks").
        Key("id").
        Columns("id", "workspace_id", "title", "updated_at").
        EqualArg("workspace_id", "workspaceId").
        OrderBy("updated_at", "desc").
        Progressive().
        Budget(10_000, 50<<20),
)
```

Replica Collections are authorized, bounded entity sets. They complement
structured Live Queries, which own exact PostgreSQL-computed membership and
ordering for datasets too large to replicate.

## Commit and resume model

1. Row triggers stage full old/new values in `_gonvex_sync_changes`.
2. One deferred commit trigger gives every row in the transaction one revision.
3. `NOTIFY` is a wake-up hint; the committed log is the only source of truth.
4. Gonvex routes one `replica.transaction` containing all visible row changes.
5. The client persists entities, memberships, and cursor atomically, then
   notifies subscribers once.
6. Reload resumes from `{epoch, revision}` or receives a fresh authorized
   snapshot when retention or visibility requires it.

There is no declared write set, table-change callback, or broad invalidation
path. A transaction carries its `originCommandId`, allowing the Local Replica
to replace an optimistic overlay only after the authoritative revision arrives.

This is the v2 delivery contract: public reads are Queries delivered as Live
Queries or Replica Collections, and public writes are Reducers. The former
declared `Writes` metadata, manual invalidation API, and generic reactive-query
API are not part of v2. This document describes the implemented sync path; the
broader identity/control-plane migration remains a compatibility cutover for
deployments that still use legacy landlord names.

## One normalized store

The Local Replica owns authoritative entities, optimistic overlays, ordered
Live Query ID memberships, completeness, freshness, pending commands, and the
applied cursor. IndexedDB persists web state; `@gonvex/expo-sqlite` applies the
same transaction to normalized SQLite tables.

React bindings use `useSyncExternalStore`. `useEntity` reads one normalized
entity, `useReplicaCollection` reads a bounded collection, and
`useLiveQueryState` exposes rows plus `source`, `completeness`, and `freshness`.

## Deliberate boundaries

- Replica Collections have one source table and explicit row/byte budgets.
- Equality arguments and null/non-null exclusions are portable collection rules.
- Search, arbitrary sorting, joins, aggregates, and unbounded pagination stay
  in indexed structured Live Queries.
- A partial cache is always labeled partial offline.
- PostgreSQL-specific Live Query expressions are explicitly online-only.
