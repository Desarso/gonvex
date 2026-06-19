# Gonvex — Implementation Prompt (remaining work to a usable Convex-on-Postgres)

You are implementing the remaining pieces of **Gonvex**, a Convex-style backend built in **Go + Postgres** with realtime React subscriptions. Backend functions are authored in Go; the frontend gets generated TypeScript bindings and live `useQuery` subscriptions over WebSocket. Postgres is the source of truth; reactivity is driven by `LISTEN/NOTIFY` triggers.

The realtime-SQL engine (WebSocket subscriptions, debounced rerun, SQL reads with `COUNT(*)`/`reltuples` estimate, pagination/search/sort/filter) is **working for one hardcoded table (`tasks`)**. The job is to turn that prototype into a real, multi-tenant, programmable backend.

## Current state (read before starting)

- **Function authoring exists, runtime dispatch does not.** `apps/.../gonvex/*.go` defines `Register(app)` with `app.Query("tasks.list", ListTasks)` etc., and handlers are real Go funcs — but `pkg/gonvex.App` is `struct{}` and `app.Query()` is a no-op; the handler bodies return empty values. The runtime binary (`server/cmd/gonvex-runtime`) does **not** import user app packages. Real execution is a hardcoded `switch` in `server/internal/server/ws.go` (`executeQuery`/`executeMutation`). The CLI (`cmd/gonvex`) parses Go source statically to build a manifest + TS bindings and syncs the manifest to the runtime over HTTP.
- **Reactivity** = per-statement Postgres triggers → `pg_notify('gonvex_table_change', …)` → Go `LISTEN` → 75ms-debounced rerun of matching subscriptions (`server/internal/server/notify.go`, `ws.go`). Hardcoded to the `tasks` table. Invalidation is coarse (table-level) with a row-ID intersection heuristic for updates.
- **No auth, no tenancy, no access control** in the query path — client args (`Filters`, `Limit`) are passed straight to SQL.
- Fan-out reruns subscriptions in a **serial loop** with no concurrency bound.

## Target architecture — multi-tenancy (REQUIRED design)

- **Landlord DB** (one, global): `users` (global identity — a user can belong to many tenants), `tenants` registry, `memberships` (user ↔ tenant), auth/sessions, billing. The landlord is the source of truth for identity and "which tenants can I access."
- **Per-tenant data store** (isolated per tenant): all tenant business data (tasks, workspaces, spots, …) **and** a tenant-local `members`/`permissions` table keyed by the global `user_id`. Permissions live **inside the tenant** so visibility filtering is a local query.
- **Isolation choice — DB-per-tenant is the primary design.** Each tenant gets its own Postgres database (its data + a tenant-local `members`/`permissions` table keyed by the global `user_id`); the landlord is a separate database. Tenants are strongly isolated; the only cross-tenant relationship is a user belonging to several tenants, resolved at session setup (the tenant-switcher menu is a landlord-only "list my memberships" query — never a join into tenant data). Abstract all DB access behind a `TenantStore(tenantId)` resolver so the connection target is swappable; schema-per-tenant-in-one-cluster is a valid fallback for very high tenant counts but is **not** the default here.
- **The one hard part this creates — per-tenant `LISTEN` connections (treat as first-class, see item 4).** `LISTEN/NOTIFY` cannot be connection-pooled, so each tenant database with a live subscriber needs its own long-lived listener connection. This is bounded by *concurrently active* tenants (open lazily on first subscriber, tear down on idle), not total tenants — design that lifecycle deliberately, it's the main scaling risk of DB-per-tenant + reactivity.
- **Session flow:** authenticate against landlord → list memberships → on tenant selection, load the tenant-local member + permission context → cache on the session → **every reactive query runs against that tenant's store with that permission context injected server-side.** Landlord is touched only at session setup; reactive queries stay tenant-local (so invalidation only ever watches one store).

## Principles / constraints

- **Never trust the client for tenancy or visibility.** Tenant scope and visibility predicates are injected server-side from the authenticated session, not from client args.
- **Keep reactive queries within a single tenant store.** No subscribed query spans landlord + tenant.
- **Parameterize all SQL values; allow-list all identifiers** (tables, columns, sort keys, filter operators).
- Prefer Postgres-native mechanisms (indexes, `COUNT`, RLS, `LISTEN/NOTIFY`) over app-layer reimplementation.
- Each work item must ship with tests and not regress the working `tasks` demo until its generic replacement is proven.

---

## Work items (roughly in dependency order)

### 1. Function registry + runtime dispatch  ← unblocks everything
- Make `pkg/gonvex.App` a real registry: store `{path → {kind, handler, argType, resultType}}` for `Query`/`Mutation`/`Action`/`InternalMutation`/`LiveGrid`/`HTTP`.
- Decide and implement the build/link model. **Recommended:** generate a per-project runtime `main` that imports the user's `gonvex/` package, calls `Register(app)`, and serves with the gonvex server library — so user Go handlers are compiled into the runtime binary (avoid Go `plugin`; it's fragile). Document the choice.
- Implement reflection-based dispatch: decode JSON args into the handler's arg struct, invoke with a real ctx, encode the result. Validate arg shape; return typed errors.
- Replace the hardcoded `executeQuery`/`executeMutation`/`executeAction` switches with registry dispatch.
- Real ctx types (`QueryCtx`, `MutationCtx`, `ActionCtx`) exposing: tenant-scoped DB handle, authenticated user + permissions, tenant id, structured logging.
- **Acceptance:** a newly authored Go query/mutation (not in any switch) executes end-to-end from React `useQuery`/`useMutation` and reads/writes real Postgres data.

### 2. Tenancy & DB routing (DB-per-tenant)
- **Landlord database** + migrations: `users` (global identity), `tenants`, `memberships`, `sessions`.
- `TenantStore(tenantId)` resolver returning a connection pool for that tenant's **own database**; per-tenant pools with sane size limits; lazy creation; idle teardown. Keep the abstraction clean enough that a tenant could be relocated to another cluster (or collapsed to a schema) without touching call sites.
- Tenant provisioning: `CREATE DATABASE` + run all tenant migrations + seed defaults; fast, idempotent, automated on tenant creation.
- **Acceptance:** two tenants are physically separate databases; a query in tenant A can never read tenant B; a landlord user maps to many tenants and switching tenants re-targets the store.

### 3. Auth, sessions, access control, visibility
- Auth integration against landlord; session tokens; **WebSocket auth handshake** (authenticate the socket before any subscribe).
- On tenant entry, load tenant-local permissions into the session.
- **Visibility predicate injection:** a server-side layer that, per query, derives the user's visible scope (workspaces/spots/teams) from tenant-local permissions and injects it as a parameterized `WHERE` (or applies Postgres **RLS** per tenant as defense-in-depth). This is where the count/visibility problem is solved cleanly — counts become a local `COUNT(*)` over a bounded, visibility-scoped set.
- **Acceptance:** a spot-restricted user's list and counts reflect only what they may see; bypassing via client args is impossible.

### 4. Generic reactivity (de-hardcode + scale)
- Auto-install per-table `NOTIFY` triggers for **every** table on migrate (generalize `notify.go` beyond `tasks`); installed in each tenant database; payload carries `{table, broad, ids}` (the tenant is implicit in which DB emitted it).
- **Per-tenant listener lifecycle (first-class):** one `LISTEN` connection per tenant **database** that has ≥1 live subscriber — opened lazily on first subscriber, torn down on idle, health-checked, with a cap + backpressure. This is the main scaling risk of DB-per-tenant + reactivity; bound it by *concurrently active* tenants. Don't open listeners for tenants nobody is watching.
- **Dependency-aware invalidation:** capture per-query dependencies (tables + filter predicates, ideally the read set) and only rerun subscriptions whose dependencies intersect a change — replacing the current "rerun every subscription on this table." At minimum scope invalidation by `(tenant, table)`.
- **Bounded fan-out:** replace the serial rerun loop with a concurrency-limited worker pool; keep per-`(tenant, table)` debounce/coalescing.
- **Acceptance:** a write in tenant A reruns only affected subscriptions in tenant A; 10k idle subscriptions don't rerun on an unrelated change; a hot table doesn't serialize the world.

### 5. Mutations & transactions
- Run mutation handlers inside a DB transaction against the tenant store; commit → trigger invalidation.
- Arg validation; `InternalMutation` (server-only) support.
- Client **optimistic updates** (predicted result, reconcile on authoritative push).
- **Acceptance:** a mutation is atomic, its effects appear reactively, and an optimistic UI update reconciles correctly.

### 6. Actions
- Execute registered actions (no DB tx guarantees; may call mutations, external APIs, storage).
- **Acceptance:** an action can call an external API and then a mutation, with results streamed back.

### 7. Schema & migrations across tenants
- Schema DSL → real migration generation; apply across landlord + all tenant stores with per-store version tracking and partial-failure handling; auto-install NOTIFY triggers + indexes.
- **Acceptance:** one schema change rolls out to all tenants with a clear migrated/failed report and no drift.

### 8. Counts / aggregates as a first-class, reactive, visibility-scoped primitive
- Exact `COUNT(*)` with injected visibility `WHERE`; `reltuples` estimate fallback for huge sets; reactive rerun on relevant changes.
- (Optional, for very large tenants) maintained rollups / incremental aggregates — note the per-user-visibility vs unbounded-scale tradeoff; default to exact-local since per-tenant data is bounded.
- **Acceptance:** sidebar/workspace/status-pill counts are the real visibility-scoped totals, update live, and don't depend on what's loaded in the client.

### 9. Platform completeness
- Scheduled functions / cron; file storage (S3) wired to ctx; full-text search (Postgres FTS / `pg_trgm`) beyond `ILIKE`; complete generated TS bindings + typed hooks (`useQuery`, `useMutation`, `usePaginatedQuery`, `useLiveGrid`).

### 10. Security & ops hardening
- SQL-injection audit (parameterize values, allow-list identifiers, validate filter operators); per-tenant RLS; query timeouts; connection/rate limits; tenant-isolation + auth test suites; per-tenant metrics, slow-query and listener-health observability.

---

## Suggested order

1 (dispatch) → 2 (tenancy) → 3 (auth/visibility) → 4 (generic reactivity) → 5 (mutations) → 8 (counts) → 6, 7, 9, 10.

Ship each behind tests; keep the `tasks` demo working until its generic replacement passes.
