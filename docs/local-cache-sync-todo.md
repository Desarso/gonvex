# Gonvex Local Cache + Shape Sync TODO

> Future-design note: this document describes a possible row-shape/local-SQL
> system. It is not the browser cache used by `useQuery`. The implemented cache
> stores exact query-result snapshots in IndexedDB; see
> `apps/docs/content/docs/browser-query-cache.mdx`.

Goal: keep normal app code on `useQuery`, while Gonvex automatically uses a persistent browser SQL cache only when it is safe, complete, and not corrupt. Server/Postgres remains authoritative.

## Non-Negotiable Invariants

- Local cache is an optimization, never the source of truth.
- Backend visibility rules define what rows can be cached locally.
- Frontend/UI visibility may hide/filter already-authorized rows, but cannot grant access.
- Initial page load never waits for a full shape clone.
- If the cache is incomplete, stale, too large, corrupt, or not owned by a single DB owner, route to server.
- If uncertain, route to server and/or mark shapes stale.
- If local DB integrity is suspect, delete/rebuild local cache from server.
- Only one browser execution context may write to the local SQLite/PGlite database at a time.

## Lessons From Notion's WASM SQLite Work

- The main architecture lesson is the SharedWorker-mediated active-tab coordinator.
- Each browser tab may have its own dedicated Worker capable of talking to SQLite/PGlite, but only one tab's dedicated Worker is active at a time.
- A SharedWorker chooses and tracks the active tab. All tabs send local DB queries to the SharedWorker; the SharedWorker forwards them to the active tab's dedicated Worker; results flow back through the SharedWorker to the requesting tab.
- Use an always-held Web Lock per tab as a liveness signal. When the active tab closes, its lock disappears and the SharedWorker elects another active tab.
- Tabs do not independently open/write the same browser SQLite/PGlite database at the same time.
- Active ownership is not just "tab exists"; the active tab's dedicated Worker must have completed async WASM/storage startup and an integrity check before any local result is trusted.
- Prefer a storage mode that does not require cross-origin isolation if possible. Notion shipped OPFS SyncAccessHandle Pool VFS because it avoided the full COOP/COEP and third-party script/iframe rollout problem.
- Cache startup must be async. WASM/storage loading must not block first page render; if storage is not ready, normal server queries continue and the cache may attach later.
- Cache should race/fallback to server, never slow the first useful render. Some devices read disk slowly, so local cache is not always the fastest path. A slow local read loses to the server response and must not hold React suspense or the first paint hostage.
- Treat browser SQL cache as disposable: corruption, checkpoint mismatch, schema mismatch, or storage errors trigger reset/resync.
- Instrument corruption and consistency errors. No local cache rollout is acceptable until we can prove those errors are absent or self-healing in tests and telemetry.

## Required Browser Cache Architecture

The browser cache worker architecture must follow this shape:

```txt
Tab A main thread ─┐
Tab B main thread ─┼─> SharedWorker coordinator ─> active tab dedicated Worker ─> SQLite/PGlite OPFS DB
Tab C main thread ─┘
```

Responsibilities:

- Main thread:
  - calls normal Gonvex APIs (`useQuery`, shape declarations, mutations)
  - never directly opens the SQLite/PGlite database
  - sends cache reads/writes to the coordinator
- SharedWorker coordinator:
  - owns tab registration
  - elects the active tab
  - tracks tab liveness through Web Locks
  - routes every DB request to the active tab worker
  - rejects or queues writes when no safe active owner exists
  - routes to server while the active worker is booting, after owner loss, or before post-handoff integrity verification
- Active tab dedicated Worker:
  - loads WASM SQLite/PGlite
  - opens the OPFS database
  - executes reads/writes/transactions
  - runs integrity checks and reports corruption signals
- Non-active tabs:
  - can make cache requests
  - receive responses through the SharedWorker
  - do not open the DB for writes

Fallbacks:

- If SharedWorker is unavailable, use a stricter fallback: one active tab via Web Locks + BroadcastChannel. If this cannot be proven safe, disable persistent local SQL cache and route to server.
- If OPFS or the selected VFS/storage backend is unavailable, disable persistent local SQL cache and route to server.
- If DB initialization is slow, keep using server queries; local cache can attach later.
- If the local request and server request race, server is allowed to win. Server results may warm cache metadata, but only completed shape sync marks a shape locally complete.

Hard failure behavior:

- Corruption/integrity/checkpoint/schema mismatch -> close DB, clear local cache namespace, mark all shapes stale, route to server, then resync.
- Active owner lost mid-transaction -> transaction is considered failed, route callers to server, elect new active owner, verify DB before accepting local results.
- Permission/auth/tenant scope changes -> stop local routing immediately for old scope and clear or quarantine old scoped rows.
- Auth isolation is scoped by project id, tenant id, user id, auth scope, schema epoch, and permission epoch. A match on only query name or shape id is never enough for local routing.

## Storage Engine Decision

Evaluate both:

- PGlite: heavier, more Postgres-like, likely better long-term if we want local SQL and shape tables.
- WASM SQLite: lighter, Notion-proven pattern, likely enough for cache/query planning if we do not need Postgres-specific local features.

Decision criteria:

- Worker/SharedWorker support.
- OPFS persistence behavior.
- Multi-tab single-writer story.
- Bundle/WASM size.
- Query performance on 20k/100k visible rows.
- Recovery behavior after forced crash/corrupt cache.
- Schema migration ergonomics.

## Phase 1: Cache Planner

- Add a cache planner that decides `local` vs `server`.
- Inputs:
  - query id/path
  - required shape id
  - project/tenant/user/auth scope
  - shape sync state
  - local DB health/ownership state
  - query local-plan availability
- Output:
  - `local` only when every condition is safe
  - `server` otherwise, with a reason

Rules:

- Shape must be `ready`.
- Shape must not be `tooLarge`.
- Shape auth scope must match query auth scope.
- Shape schema/cache epoch must match current runtime/schema epoch.
- Query must have a local execution plan.
- Local DB must be healthy.
- Local DB must have a single active owner/writer.

## Phase 2: Shape Metadata

- Add backend shape definitions.
- Shape examples are generic, not task-specific:
  - `<table>.visible`
  - `<table>.tenantVisible`
  - custom app shapes
- Shape metadata:
  - shape id
  - table set
  - visibility dependencies
  - max rows / max bytes
  - schema version
  - permission/auth scope version
  - checkpoint

## Phase 3: Frontend DB Owner

- Add a cache worker package.
- Implement the Notion-style topology:
  - SharedWorker coordinator.
  - one dedicated Worker per tab.
  - SharedWorker elects exactly one active tab.
  - active tab's dedicated Worker is the only worker allowed to open/write the DB.
  - all tabs route DB queries through the SharedWorker.
- Use Web Locks for tab liveness and active-owner election.
- Add explicit owner state:
  - `no_owner`
  - `electing`
  - `active_owner_ready`
  - `owner_lost`
  - `unsafe_fallback_disabled`
- Fall back to one active tab + BroadcastChannel coordination only if tests prove safety. Otherwise disable persistent local SQL cache for unsupported browsers.
- No direct DB access from React components.
- Tests must simulate two tabs and assert:
  - both tabs can request DB reads.
  - all DB requests route to one active tab worker.
  - closing active tab elects a new owner.
  - no two workers write at the same time.
  - owner loss causes server fallback until DB integrity is verified.
  - local routing stays disabled while WASM/storage startup is still pending.
  - corruption signals produce reset + server fallback without trusting partial local results.
  - project/tenant/user/auth-scope changes cannot reuse another scope's rows.

## Phase 4: Persistent Query/Shape Store

- Metadata tables:
  - `gonvex_cache_meta`
  - `gonvex_shapes`
  - `gonvex_shape_checkpoints`
  - `gonvex_mutation_log`
- Row tables generated from Gonvex schema for visible shapes.
- Cache stores rows plus shape readiness, not just random query windows.

## Phase 5: Initial Window + Background Shape Sync

- `useQuery` first does the fast thing:
  - if safe local ready: local
  - otherwise server query
- Shape sync starts/continues in background.
- When shape becomes ready, later compatible queries can route local.
- If shape crosses size limits, mark `tooLarge` and keep server mode.

## Phase 6: Real-Time Shape Patches

- Add shape sync stream:
  - initial snapshot
  - insert/update/delete patches
  - checkpoint
  - stale/resync messages
- Patch application is idempotent.
- Out-of-order or missing checkpoint forces resync.
- Permission/role/membership changes mark affected shapes stale.

## Phase 7: Query Routing

- `useQuery` remains the public API.
- Queries can declare an optional backing shape and local execution plan.
- Planner routes:
  - local if shape covers query and is ready
  - server otherwise
- Server result can warm local cache but does not mark a shape complete.

## Phase 8: Optimistic Mutations

- Mutations may apply local optimistic patches through the DB owner.
- Mutation log stores rollback patch.
- Server success confirms; server failure rolls back.
- Shape stream remains authoritative and reconciles final state.

## Phase 9: Browser/E2E Testing

- Use Playwright to test:
  - reload uses persistent cache when shape is ready
  - server fallback when shape is stale/incomplete
  - two tabs route DB requests through the SharedWorker coordinator
  - only active tab's dedicated Worker opens/writes the DB
  - active tab close elects a replacement active tab
  - no two workers own writes at the same time
  - cache corruption causes reset + server fallback
  - auth/tenant switch never shows previous user's data
  - shape too-large state disables local routing
  - slow cache read loses race to server and does not delay render
  - slow WASM init does not block initial page load

## Ralph Loop Execution Plan

Use this as the autonomous implementation loop:

1. Implement only the cache planner and coordinator state machine first.
   - No real SQLite/PGlite dependency yet.
   - Unit test every route-to-server safety branch.
2. Build a fake browser DB coordinator.
   - Simulate SharedWorker, tab workers, Web Locks, active owner election, owner loss.
   - Test with Vitest and Playwright.
3. Add the real SharedWorker coordinator.
   - Main thread API remains tiny: `requestLocalRead`, `requestLocalWrite`, `getCacheHealth`.
   - No React hook touches the DB directly.
4. Add a storage adapter behind the coordinator.
   - First adapter may be fake/in-memory for tests.
   - Then evaluate WASM SQLite vs PGlite using the same adapter contract.
5. Add local metadata tables only.
   - shape state
   - checkpoints
   - cache health
   - mutation log
6. Add one generated row table and one shape.
   - Keep it generic; do not hardcode task assumptions.
7. Add snapshot sync.
   - Server first page remains foreground.
   - Shape snapshot hydrates in background.
8. Add patch sync.
   - idempotent insert/update/delete
   - checkpoint validation
   - stale/resync fallback
9. Add `useQuery` routing.
   - server by default
   - local only if planner says `local-ready`
10. Add optimistic mutations only after shape sync is reliable.
11. Run full gates after each loop:
   - unit tests
   - Playwright multi-tab tests
   - corruption/reset tests
   - auth/tenant leak tests
   - performance smoke: first render not delayed by cache init

Do not proceed to the next step if any correctness or corruption test is red.

## Current Implementation Status

Implemented in `packages/client/src`:

- Cache planner types and routing guardrails in `cache.ts`.
  - Requires a local query plan.
  - Requires a declared backing shape.
  - Requires explicit shape coverage for the query.
  - Checks project, tenant, user, auth scope, schema epoch, and permission epoch.
  - Rejects missing, syncing, stale, too-large, over-limit, unhealthy, or unsafe-owner states.
- Coordinator model in `cache-coordinator.ts`.
  - Tracks tab registration, liveness lock state, active owner election, owner loss, and unsafe fallback disablement.
  - Requires the active owner to complete dedicated-worker readiness, storage startup, and integrity verification before local routing.
- Fake browser DB coordinator harness in `browser-cache.ts`.
  - Simulates the SharedWorker -> active tab dedicated Worker topology without opening a real SQL engine.
  - Routes non-active tab requests through the active owner worker.
  - Converts local corruption/integrity failures into reset plus server fallback.
- Persistent cache controller in `persistent-cache.ts`.
  - Keeps first query decisions server-safe while shape hydration runs in the background.
  - Marks old scoped shape state stale on auth/project/tenant/user scope changes.
- Browser cache client scaffold in `browser-cache-client.ts`.
  - Exposes the future main-thread API: `requestLocalRead`, `requestLocalWrite`, and `getCacheHealth`.
  - Uses the fake SharedWorker/coordinator harness in tests so callers cannot bypass the active owner.
- Browser capability gate in `browser-capabilities.ts`.
  - Requires SharedWorker, Web Locks, dedicated Worker, and OPFS before enabling persistent SQL cache.
  - Disables persistent local routing when those primitives cannot prove safe ownership.

Tests added or extended:

- `packages/client/src/cache.test.ts`
- `packages/client/src/cache-coordinator.test.ts`
- `packages/client/src/browser-cache.test.ts`
- `packages/client/src/browser-cache-client.test.ts`
- `packages/client/src/browser-capabilities.test.ts`
- `packages/client/src/persistent-cache.test.ts`

The former localStorage-only `tests/e2e/cache-routing.spec.ts` placeholder was
removed when the production query-snapshot cache gained a real browser fixture.

Still intentionally not implemented:

- Real SharedWorker and dedicated Worker bundle files.
- SQLite/PGlite storage adapter.
- Actual OPFS persistence, metadata SQL tables, and row table generation.
- Production `useQuery` local execution path.
- Server shape sync stream and patch protocol.

## Phase 10: Pilot

- Start with one low-risk table shape in dashboard or a fixture app.
- Then pilot a real Whagons table.
- Do not ship broad app data caching until corruption and multi-tab tests are reliable.
