# Plan: Eliminate the tenant-wide `sync.ready` fan-out storm

## Problem (verified 2026-08-07)

One task update makes every open sync subscription for the tenant re-deliver,
even though only one table changed. With Whagons open (34 reference syncs +
one `sync.recentWorkspaceTasks` per workspace, ~160 workspaces on large
tenants), a single `tasks.update` produces ~194 individual `sync.ready`
frames (~54 KB uncompressed) plus hidden CPU and DB cost.

Verified mechanics (all paths absolute; `gonvex` repo =
`/home/gabriel/Desktop/coding/gonvex`, client app =
`/home/gabriel/Desktop/coding/whagons/whagons5-client`):

1. The Postgres trigger `gonvex_sync_finalize_transaction` advances one
   tenant-global revision and emits `pg_notify('gonvex_sync_change',
   {epoch, revision})` — **no table names in the payload**
   (`server/internal/schema/sync.go:239`).
2. The tenant listener calls `notifySyncRevision(project, tenant)` which
   schedules a delivery goroutine for **every** subscription on **every**
   connection of the tenant, with no table filtering
   (`server/internal/server/tenant_listeners.go:256`,
   `server/internal/server/sync.go:1171`).
3. Each `deliverSync` independently runs `currentSyncCursor` (a
   `_gonvex_sync_clock` SELECT) and `readSyncChanges` (a `_gonvex_sync_changes`
   SELECT filtered to its tables) — so ~194 subscriptions ≈ ~388 DB queries
   per revision per connected client (`server/internal/server/sync.go:552-611`).
4. Unchanged subscriptions still advance their cursor and emit an individual
   `sync.ready` frame via `wsConn.write` → one `WriteJSON` per frame
   (`sync.go:607-610`, `sync.go:685-692`, `server/internal/server/ws.go:805`).
5. The protocol and JS client already support a batched
   `sync.readyMany` frame (`packages/protocol/src/index.ts:253`,
   `packages/client/src/index.ts:586`) — **the server never emits it**.
6. On the client, **every** `sync.ready` triggers a full
   `syncRowsHashes(subscription.rows)` integrity pass, even when
   `subscription.integrityRows === subscription.rows` (rows untouched since the
   last verified digest) (`packages/client/src/index.ts:1120-1164`). It also
   enqueues an IndexedDB persist per ready (cursor-only writes are already
   cheap via `rowsUnchanged`, `index.ts:1427`).
7. Whagons opens 34 reference syncs
   (`src/providers/WhagonsSyncProvider.tsx:25`) and keeps
   `sync.recentWorkspaceTasks` open for **every** authorized workspace, not
   just the visible one (`WhagonsSyncProvider.tsx:620-656`).

The behavior is logically correct; the transport is O(subscriptions) per
revision in frames, DB queries, and client hashing. Fix plan below, split
into independent work packages.

---

## WP1 — Whagons: bound live workspace-task syncs (active + LRU)

**Repo:** whagons5-client. No protocol changes. Biggest single multiplier
(160 → ~9 subscriptions on large tenants).

**Where:** `src/providers/WhagonsSyncProvider.tsx`, the `useLayoutEffect` at
line ~624 that opens `api.sync.recentWorkspaceTasks` for every workspace in
`collections.workspaces`.

**Change:**
- Replace "all workspaces" with: the currently routed workspace
  (`activeWorkspaceIdFromLocation()`, already in the file at ~line 342) plus a
  bounded LRU of recently visited workspaces. Add a constant, e.g.
  `MAX_WARM_WORKSPACE_TASK_SYNCS = 8`.
- Reuse the existing localStorage manifest
  (`readWorkspaceTaskSyncManifest` / `writeWorkspaceTaskSyncManifest`,
  `MAX_PERSISTED_WORKSPACE_SYNC_IDS`) as the LRU's persisted form: most
  recently visited first, truncated to the cap. Update it on route change.
- Never evict the currently routed workspace. Evict LRU tail only when over
  cap; keep the existing eviction cleanup
  (`unsubscribe()` + `workspaceTaskWindowStore.remove(workspaceId)`).
- Listen for route changes (the provider already derives the active workspace
  at mount; it must now react to navigation — hook into the router or a
  `popstate`/location observer consistent with how the app already does this).

**Known intent being changed — read before coding:** the comment at
`WhagonsSyncProvider.tsx:620-623` says changing `/workspace/:id` must never
open/close a sync or fall back to `bulk.tasksByWorkspace`. This plan
deliberately relaxes that for *cold* workspaces (outside the LRU). Mitigations
that make this acceptable:
- `@gonvex/client` persists sync snapshots in IndexedDB, so re-opening a
  previously warm workspace paints from disk and reconciles via delta — not a
  full re-download.
- `src/cache/taskQueryPlanner.ts` already routes queries to
  `bulk.tasksByWorkspace` whenever the local window is absent; verify a cold
  workspace renders correctly through that path (it must, since brand-new
  workspaces exercise it today).

**Must verify (agent):**
- `/workspace/all` and shared views: check whether any aggregate view iterates
  all workspace windows (`workspaceTaskWindowStore`) and assumes each is warm.
  If so, it must tolerate missing windows (planner fallback), not silently
  show partial data.
- E2E timing hooks (`__whWorkspaceTaskSyncTiming`, line ~659) keep working for
  warm workspaces.
- The offline path: task windows for LRU workspaces still hydrate from
  IndexedDB when offline; cold workspaces show the existing
  offline-unavailable behavior, not a crash.

**Acceptance:** with N workspaces (> cap), a task update on the visible
workspace results in at most `34 + cap + 1` subscription deliveries, and
navigating between two recently visited workspaces triggers zero
`bulk.tasksByWorkspace` calls. Add/adjust unit tests near existing provider
tests; run the sync-related e2e specs.

---

## WP2 — Client package: skip integrity re-hash when rows are unchanged

**Repo:** gonvex, `packages/client/src/index.ts`. Independent, small, big CPU
win. Ship in the next `@gonvex/client` patch release.

**Where:** the `sync.ready` handler, `index.ts:1120-1164`.

**Change:** before scheduling `syncRowsHashes`, add a memoized fast path:

```
if (
  !subscription.forceFullIntegrity
  && subscription.integrityRows === subscription.rows
  && subscription.integrityDigest
) {
  if (message.digest && subscription.integrityDigest !== message.digest) {
    // fall through to full re-hash → reset-on-mismatch as today
  } else {
    this.acceptSyncReady(subscription, message, subscription.integrityDigest);
    return;
  }
}
```

Constraints:
- `subscription.integrityRows` is already maintained (`index.ts:1153,1187`);
  the identity comparison is the correctness guard — any row mutation replaces
  the `rows` array reference (verify this invariant holds everywhere rows are
  written; if any code mutates `subscription.rows` in place, fix that first).
- Keep bumping `verificationGeneration` semantics intact for the slow path.
- `forceFullIntegrity` (integrity-reconciling flow, `index.ts:1110`) must
  always take the full re-hash path.
- On digest mismatch in the fast path, do **not** immediately reset — run the
  full re-hash once (protects against a stale memo) and let the existing
  mismatch → `sync.reset` logic decide.

**Tests:** extend `packages/client/src/sync-integration.test.ts` — assert that
two consecutive `sync.ready` frames with identical rows hash only once (spy on
`syncRowsHashes` or count digest computations), and that a mismatching digest
still resets.

---

## WP3 — Server: coalesce per-connection `sync.ready` into `sync.readyMany`

**Repo:** gonvex server. The client already consumes `sync.readyMany`
(`packages/client/src/index.ts:586` fans it out to the per-subscription
handler). The server must start emitting it, gated by client capability.

**Design — connection-level write coalescer** (minimal-risk; avoids
restructuring the per-subscription delivery goroutines):
- In `wsConn` (`server/internal/server/ws.go`), add a small pending-ready
  buffer: `pendingReady []serverMessage` + a flush timer (10–25 ms).
- Route `writeSyncReady` (`sync.go:685`) through the coalescer instead of
  `conn.write` directly. Any **other** frame written to the connection
  (`sync.delta`, `sync.snapshot`, `sync.reset`, `sync.syncing`, query results,
  mutation acks) must **flush pending readies first** — this preserves the
  per-subscription ordering invariant (a ready is only ever delayed, never
  reordered across a later frame for the same subscription; flushing before
  every non-ready write makes that hold trivially for all subscriptions).
- On flush: 1 buffered ready → plain `sync.ready` frame (wire-compatible);
  ≥2 → one `sync.readyMany` frame with the array in the shape
  `packages/protocol/src/index.ts:253` defines (`{type, ready: SyncReady[]}`).
  Match the exact field casing the client parses at `index.ts:586-588` (it
  spreads each entry into `{type: "sync.ready", ...ready}` — so each array
  entry carries `id`, `path`, `cursor`, `mode`, `digest`, `truncated`).
- Flush also on connection close and before the pong/heartbeat if one exists.

**Capability gating:** today capabilities only flow server→client
(`ws.go:326`, `connected` frame). Old pinned clients (mobile builds) may
predate `readyMany` handling, so:
- Add optional `capabilities` to the client `auth` message
  (`packages/client/src/index.ts:2232`, protocol type in
  `packages/protocol/src/index.ts`), e.g. `{ syncReadyMany: 1 }`; server
  stores it on `wsConn` (extend `clientMessage`, `ws.go:24`).
- Server coalesces only when the connection advertised `syncReadyMany: 1`;
  otherwise writes individual frames exactly as today.

**Tests:** server-side Go test: open N subscriptions, commit one revision
touching one table, assert the connection receives exactly one delta + one
`sync.readyMany` covering the rest (capability on) vs N ready frames
(capability off). Check existing sync server tests for the harness to extend
(`server/internal/server/*_test.go`, grep `sync.ready`).

---

## WP4 — Server: inspect changed tables once per revision, not per subscription

**Repo:** gonvex server. Removes the O(subscriptions) DB-query fan-out.

**Design:**
1. **Carry table names in the notify payload.** Extend the trigger in
   `server/internal/schema/sync.go` (`gonvex_sync_finalize_transaction`) to
   aggregate `array_agg(DISTINCT table_name)` from the finalized
   `_gonvex_sync_changes` rows of this transaction and include it in the
   `pg_notify` JSON. Payload cap is 8000 bytes — table-name lists are tiny;
   still, if the JSON would exceed ~7 KB, fall back to omitting the list
   (treat as "all tables"). Bump whatever schema-install versioning gates
   trigger redefinition (`installTenantSyncLog`, `sync.go:183`).
2. **Filter in `notifySyncRevision`.** Parse the payload in the listener
   (`tenant_listeners.go:255` currently ignores it), pass
   `changedTables []string` through. For each subscription, compute
   `relevant = intersects(changedTables, {definition.Table} ∪ definition.VisibilityTables)`.
   - Relevant → `scheduleSyncDelivery` as today.
   - Not relevant **and** subscription is `verified` → advance
     `subscription.cursor.Revision` in memory (under the subscription lock,
     only if `latest.Epoch` unchanged — reuse the cheap path) and emit a
     ready through the WP3 coalescer **without** running `readSyncChanges` or
     `currentSyncCursor` per subscription. One `currentSyncClock` call per
     tenant-notification (shared across all its connections/subscriptions)
     replaces ~194.
   - Unverified subscriptions keep the full `deliverSync` path.
3. **Empty/absent table list** (reconnect replay at
   `tenant_listeners.go:212`, or oversized payload) → current behavior:
   schedule everything. That path is the correctness backstop; the filter is
   an optimization only.

**Correctness constraints:**
- **Cursor-lag vs retention:** if you choose (or a later refactor chooses) to
  *not* advance cursors of unaffected subscriptions, note that
  `pruneSyncLog` advances `retained_revision` (`sync.go:410-448`) and a
  cursor older than retention forces reset + full resnapshot
  (`errSyncCursorExpired`, `sync.go:599`). The design above avoids this by
  always advancing cursors — keep that property.
- Notifications can coalesce/drop (edge-triggered LISTEN). The skip path must
  tolerate a stale in-memory cursor: `deliverSync` already re-reads the real
  range on the next relevant change, and `readSyncChanges` only reads that
  subscription's tables — so a skipped-then-relevant subscription catches up
  correctly. Add a test for exactly this sequence (change table A, skip sub-B;
  then change table B; assert sub-B receives the delta for the B change with
  the correct cursor span).
- `syncValueMatches`/args-filtered subs (e.g. per-workspace task syncs) are
  table-relevant for every task change; that's fine — table-level filtering is
  the win for the 34 reference syncs, and per-arg filtering already happens
  inside `deliverSync`.

**Tests:** Go tests asserting DB query counts (or at minimum: one revision
touching `tasks` schedules full delivery only for task-table subscriptions;
others get cursor-advance + coalesced ready with zero `_gonvex_sync_changes`
queries).

---

## WP5 — Protocol: connection-level revision watermark (supersedes per-sub ready for the unchanged case)

**Repo:** gonvex (protocol + client + server). Do after WP3/WP4; it removes
even the batched acks and the client's per-collection processing.

**Design:**
- New server frame: `{type: "sync.watermark", revision: number}` per
  connection, meaning: *every currently-open, verified, up-to-date
  subscription on this connection whose tables did not change is current
  through `revision`*. No epochs in the frame — each subscription keeps its
  own epoch (epoch is per-definition, `sync.go:533-541`); the watermark only
  bumps `cursor.revision`.
- Server: in the WP4 skip path, instead of enqueueing per-subscription readies,
  track the set of skipped-but-advanced subscriptions and emit one watermark
  per connection per notification (through the WP3 flush machinery so ordering
  vs deltas holds). Subscriptions that received a delta/ready in the same pass
  are excluded — their own ready already carries the newer cursor; the client
  applies the watermark only to subs whose `cursor.revision < watermark` and
  which are `isUpToDate && !opening && !forceFullIntegrity`.
- Client (`packages/client/src/index.ts`): on watermark, for each qualifying
  subscription: set `cursor.revision`, persist via the existing
  cursor-only-persist path (`persistSyncSnapshot` with `rowsUnchanged` — but
  **coalesce**: one storage transaction for all affected subs, or throttle
  cursor-only persists to e.g. 1/s per collection; per-ready `store.replace`
  churn is part of the cost being removed). **No integrity hashing, no
  listener emission** — app code must not observe watermarks (Whagons' ready
  handler at `WhagonsSyncProvider.tsx:678` reads cursors from ready frames;
  watermark advancement is internal to the client package).
- Capability: `syncWatermark: 1` in both directions (server advertises in
  `connected`, client requests in `auth`). When both present, server stops
  sending per-sub readies for unchanged collections entirely; WP3's readyMany
  remains for reconnect floods and multi-collection reconciles.
- Reconnect: unchanged — client resumes each sub with its persisted cursor +
  digest (`index.ts:1361-1389`); a watermark-advanced cursor is
  indistinguishable from a ready-advanced one to the resume path. Verify the
  server accepts a resume cursor at a revision for which that sub never
  received an explicit ready (it must — `openSyncWithClock` only compares
  numbers, `sync.go:289`).

**Tests:** client: watermark advances cursors without hashing or listener
calls; resume-after-watermark round-trips. Server: one revision → exactly
`1 delta + 1 ready (changed sub) + 1 watermark` per connection.

---

## Safety invariants (every WP must preserve these — add tests where noted)

The overriding rule: **no optimization may ever cause a subscription to skip a
revision that changed its visible rows.** Concretely:

1. **Skip-equivalence (WP4/WP5):** a subscription may only be skipped (cursor
   advanced without `readSyncChanges`) when the changed-table set for the
   revision range has empty intersection with
   `{definition.Table} ∪ definition.VisibilityTables`. That set must come from
   the same `_gonvex_sync_changes` rows the delivery path reads (the trigger
   aggregates them in the same transaction), so a skip is provably equivalent
   to today's "readSyncChanges returned zero rows → advance cursor + ready"
   path (`sync.go:607-610`). Test: change table A, confirm a B-only sub is
   skipped; then change table B and assert the B sub receives the delta with a
   cursor spanning both revisions.
2. **Lossy-notification backstops stay:** LISTEN reconnect replay
   (`tenant_listeners.go:212`) and any absent/oversized table payload must
   schedule FULL delivery for every subscription (no filtering). Filtering is
   an optimization on top of a correct baseline, never the correctness
   mechanism.
3. **Digest verification is never weakened on the changed path:** every
   `sync.delta` and every explicit `sync.ready` keeps its digest, and the
   client keeps verifying it. WP2's memoization only reuses a digest computed
   from the exact same rows array reference that was already verified; a
   mismatch or `forceFullIntegrity` always falls back to the full re-hash.
   WP5's watermark applies only to verified, up-to-date subscriptions whose
   tables did not change — their rows and digest are unchanged by definition;
   integrity is still re-verified on the next delta, explicit ready, or
   digest-checked reconnect resume (`index.ts:1361-1389`).
4. **Cursor monotonicity vs retention:** unchanged subscriptions must keep
   having their cursors advanced (in-memory server-side; via ready/watermark
   client-side) so no cursor falls behind `retained_revision` and forces
   spurious resets — but note a cursor-expired resume is itself SAFE (it
   resets to a full resnapshot, never serves stale data); it is a cost, not a
   correctness failure. WP1's evicted workspaces rely on exactly this:
   resume-with-digest catch-up when warm, full resnapshot when past retention.
5. **Ordering:** a `sync.ready`/`readyMany`/watermark for a subscription must
   never be delivered before a delta/snapshot/reset it acknowledges. The WP3
   coalescer flushes all pending readies before writing ANY other frame on the
   connection.
6. **Local windows never serve closed syncs (WP1):** when a workspace sync is
   evicted, its in-memory window is removed (`workspaceTaskWindowStore.remove`)
   so the planner falls back to live server queries — the persisted IndexedDB
   snapshot is only ever used as paint-before-reconcile under a live,
   digest-verified subscription.

## Sequencing, rollout, and measurement

Order: **WP1 ∥ WP2** (independent, ship first) → **WP3** → **WP4** → **WP5**.
Each is safe alone; each later WP builds on the previous one's machinery.

Cross-repo mechanics (traps that have bitten before):
- gonvex package changes require a version bump, `pnpm` install in
  whagons5-client, **and a dev-server restart** — stale `node_modules` has
  previously shipped bad manifests.
- Changing a sync *definition* resnapshots that collection; none of these WPs
  change definitions, so no mass resnapshot is expected on deploy. Sync
  collections survive deploys by design (syncScope + per-definition epochs).
- Local dev has previously exhibited a dead sync push chain (revision
  advances, no delta delivered). **Before** starting server work, verify the
  local repro actually delivers deltas (open the app, update a task, watch WS
  frames); if the chain is dead locally, fix/diagnose that first or verify
  against an environment where push works — otherwise every WP's acceptance
  test is unmeasurable.
- Whagons uses `pnpm` (never npm); runtime on :8080, Vite on :5173.

Measurement (do once before WP1 and after each WP, same tenant/workspace
count): in the browser Network tab (WS frames) or a small client-side counter,
update one task and record (a) number of `sync.*` frames received, (b) total
bytes, (c) time spent in `syncRowsHashes` (Performance profile). Baseline is
~194 frames / ~54 KB. Targets: WP1 → ~43 frames; WP3 → ~3 frames
(delta + ready + readyMany); WP5 → 3 frames with no hashing for unchanged
collections; WP4 → server-side: 2 DB queries per revision per tenant instead
of ~2×subscriptions per revision per connection.
