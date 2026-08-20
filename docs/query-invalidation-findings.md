# Query invalidation findings

> Historical note: this report describes the pre-v2 generic reactive-query
> invalidation path and declared `Writes` metadata. Those APIs are removed from
> the v2 public model; current reads use Live Queries or Replica Collections.

Investigation date: 2026-08-07  
App: `/home/gabriel/Desktop/coding/whagons/whagons5-client` (`gonvex`)  
Framework: `/home/gabriel/Desktop/coding/gonvex` (`main`)

Path abbreviations used below:

- `APP` = `/home/gabriel/Desktop/coding/whagons/whagons5-client`
- `GVX` = `/home/gabriel/Desktop/coding/gonvex`

## Executive summary

1. **The normal desktop Glide grid is unconditionally forced onto `bulk.tasksByWorkspace`.** The generic planner would accept a complete, current, authoritative local workspace corpus, but `decideGlideTaskQueryExecution` overwrites every input with `requireLiveServerProjection: true`; the planner therefore returns `server / live-server-required` before considering the local window (`APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:153-157`; `APP/src/cache/taskQueryPlanner.ts:49-55`). The local sync is nevertheless opened and passed all the way to the grid (`APP/src/pages/spaces/Workspace.tsx:838-866`; `APP/src/pages/spaces/workspace/utils/workspaceTabs.tsx:347-401`).

2. **The stated “few tasks, no search” measurement premise is only half true.** The spec seeds three tasks in a new workspace, but the actor's `openTaskByName` calls `searchFor`, and the observer explicitly calls `searchFor`; neither clears the search before measurement (`APP/tests/e2e/realtime/ws-traffic-budget.spec.ts:291-309`; `APP/tests/e2e/helpers/tasks.ts:199-203`; `APP/tests/e2e/realtime/ws-traffic-budget.spec.ts:327-336`). A fix that keeps search server-backed will therefore not remove the grid query from this exact benchmark until the spec clears the search after locating the task.

3. **One priority edit is one application transaction and one database sync revision, not two or three commits.** `tasks.update` requires the framework transaction; the runtime begins it, runs the handler, commits once, and only then publishes scheduled work (`APP/gonvex/tasks.go:1200-1205`; `GVX/server/internal/server/ws.go:1809-1849`). PostgreSQL stages all row changes in that transaction and allocates one revision at the deferred finalize trigger (`GVX/server/internal/schema/sync.go:131-148`; `GVX/server/internal/schema/sync.go:212-263`).

4. **The 2–3 reactive rounds are duplicate invalidation paths, not duplicate database revisions.** After commit, the WebSocket mutation path schedules one broad invalidation from the entire declared `Writes` set (`GVX/server/internal/server/ws.go:789-806`; `GVX/server/internal/server/ws.go:1183-1207`). Separately, statement-level PostgreSQL triggers send precise table/operation/row/column notifications for the physical writes (`GVX/server/internal/schema/notify.go:150-173`; `GVX/server/internal/schema/notify.go:224-245`), which the tenant listener schedules again (`GVX/server/internal/server/tenant_listeners.go:255-281`). The 75 ms debounce is keyed by the *exact table set*, so the large declared set and each single-table event do not merge (`GVX/server/internal/server/ws.go:1226-1269`).

5. **Identical-result suppression already exists, but `bulk.tasksByWorkspace` defeats it.** Shared subscriptions SHA-256 the JSON payload and send `query.progress` when the hash is unchanged (`GVX/server/internal/server/subscriptions.go:589-635`). `TasksByWorkspace`, however, returns elapsed-time fields such as `serverFunctionDurationMs`, `visibilityDurationMs`, and other per-execution timings inside `perf`, so semantically identical task pages normally hash differently (`APP/gonvex/bulk.go:88-94`; `APP/gonvex/bulk.go:443-473`; `APP/gonvex/bulk.go:554-570`). That is why duplicate rounds still become full `query.result` pages.

6. **The biggest byte win is to stop subscribing in the local-capable default view; the safest cross-cutting win is to make the existing hash suppression effective.** The measured actor page traffic is about 50 KB in six task-page results, while the observer is commonly about 29 KB in four results (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:3-8`). Removing the grid's forced server mode attacks the source. Removing volatile timing data from the semantic result (or excluding it from the content hash) should suppress approximately the duplicate half of unchanged reruns—roughly 25 KB actor / 14 KB observer in the common two-round case—without a protocol change.

## 1. Why `bulk.tasksByWorkspace` is subscribed in the workspace

### Intended local path

The client already has the intended architecture:

- `WhagonsSyncProvider` keeps the active workspace's `sync.recentWorkspaceTasks` collection open and stores snapshots/readiness in `workspaceTaskWindowStore` (`APP/src/providers/WhagonsSyncProvider.tsx:761-805`; `APP/src/providers/WhagonsSyncProvider.tsx:825-865`).
- `useRecentWorkspaceTasksSync` exposes those rows and only calls them up to date when the task-window revision exactly matches the reference-data revision (`APP/src/providers/WhagonsSyncProvider.tsx:990-1020`).
- `Workspace` builds `localTaskCorpus` from that hook; its general `useGonvexTasks` server hook is disabled on desktop because it is enabled only for the mobile grid (`APP/src/pages/spaces/Workspace.tsx:838-866`).
- The grid tab passes `localTaskCorpus` through `WorkspaceTable` to Glide (`APP/src/pages/spaces/workspace/utils/workspaceTabs.tsx:347-401`; `APP/src/pages/spaces/components/WorkspaceTable.tsx:3018-3050`).
- The planner returns local for a concrete workspace whose corpus is present, current, authoritative, complete, and below the local row limit; it also supports a resident first window when explicitly allowed (`APP/src/cache/taskQueryPlanner.ts:56-109`).

### Exact condition that forces the server

The Glide wrapper makes the planner's local checks unreachable:

```ts
export function decideGlideTaskQueryExecution(input) {
  return decideTaskQueryExecution({
    ...input,
    requireLiveServerProjection: true,
  });
}
```

That override is at `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:153-157`; the planner's first substantive branch returns `server / live-server-required` at `APP/src/cache/taskQueryPlanner.ts:52-55`. Glide computes this decision at `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:2933-2983`, opens `bulk.tasksByWorkspace` at `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:3050-3074`, and calls it whenever the decision is server at `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:3127-3194`.

The grid already computes a useful default-window eligibility predicate—no search, no filters, no exclusions, `all` mode, compatible sort—at `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:2948-2960`. The forced flag, not a failure of those checks, is the immediate cause.

### Measurement-state correction

The spec seeds exactly three tasks (`APP/tests/e2e/realtime/ws-traffic-budget.spec.ts:291-309`), but both pages retain a task-name search:

- `openTaskByName` visits the workspace, selects Grid, calls `searchFor(name)`, and then opens the row (`APP/tests/e2e/helpers/tasks.ts:199-210`).
- `searchFor` fills the header search, stores it in local storage, and dispatches `wh:searchTextChanged` (`APP/tests/e2e/helpers/tasks.ts:149-173`).
- The observer does the same explicitly (`APP/tests/e2e/realtime/ws-traffic-budget.spec.ts:331-336`).

Thus this is “three tasks, active search,” not “three tasks, no search.” Today that distinction is masked by the unconditional server flag. Once the flag is removed, the product decision must be explicit: either complete local corpora may run local search, or search must be added as a planner input that selects server mode. If the intended contract is “search/filter/sort/overflow only use the server,” this benchmark must clear search before `resetTraffic` to measure the default view (`APP/tests/e2e/realtime/ws-traffic-budget.spec.ts:344-353`).

### What the six actor frames mean

They do **not** mean six server pages for the Glide viewport. Glide keeps one window subscription and unsubscribes the previous window before replacing it (`APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:3050-3059`; `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:3124-3137`). The framework's current `usePaginatedQuery` implementation also issues one first-page query and has a no-op `loadMore`, so the hook itself does not currently create server page fan-out (`GVX/packages/react/src/index.tsx:1010-1019`).

There are, however, multiple task-query owners on the actor:

- the Glide grid's window subscription (`APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:3050-3074`);
- the task preview's `useGonvexTasks`, scoped to the task workspace and configured for up to 2,000 rows (`APP/src/pages/spaces/components/taskPreview/TaskPreviewPanel.tsx:951-963`). The preview remains mounted after the user clicks Edit: its host stays mounted once used, and the sheet deliberately keeps its last content tree warm while closed (`APP/src/pages/spaces/workspace/components/WorkspaceTaskPreviewHost.tsx:19-57`; `APP/src/pages/spaces/components/TaskPreviewSheet.tsx:41-45`; `APP/src/pages/spaces/components/TaskPreviewSheet.tsx:82-87`).

The server telemetry proves repeated rounds for the same subscription ID: the first measured mutation commits once at `GVX/tmp/gonvex-telemetry.jsonl:2948022`, while the same `bulk.tasksByWorkspace` operation ID executes at least twice for that edit at `GVX/tmp/gonvex-telemetry.jsonl:2948036` and `GVX/tmp/gonvex-telemetry.jsonl:2948083`. The aggregate browser metric cannot map each frame back to a React owner or subscription ID, so it cannot safely apportion all six between grid and retained preview; it *can* establish that the count is a combination of multiple live owners and repeated invalidation rounds, not six independent database revisions. The stable six actor results versus two-to-four observer results (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:3-8`) is consistent with the actor-only retained preview plus timing-dependent coalescing/suppression.

One additional client inefficiency is latent rather than causal while server-forced: `executionQueryKey` includes a serialized local corpus, but `normalizedLocalCorpus` is empty in server mode, so sync-row changes do not currently resubscribe the forced server query (`APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:3024-3035`).

## 2. Priority-change mutation and revision chain

### Client call

The dialog sends one `tasks.update` mutation. It submits the whole ordinary form snapshot—not only `priority_id`—and applies an optimistic patch to `sync.recentWorkspaceTasks` (`APP/src/pages/spaces/components/taskDialog/hooks/useTaskSubmit.ts:112-163`). It then invokes custom-field synchronization, but that helper exits without mutations when the category has no fields (`APP/src/pages/spaces/components/taskDialog/hooks/useTaskSubmit.ts:165-165`; `APP/src/pages/spaces/components/taskDialog/hooks/useCustomFieldSync.ts:20-28`). The E2E seed creates a fresh category/template but no custom-field definitions (`APP/gonvex/testing_workflows.go:992-1058`), so the measured flow has no follow-up `customFields.setTaskValue` calls.

### Registered mutation and declared write set

`tasks.update` is registered with this static declared `Writes` set (`APP/gonvex/tasks.go:99-122`):

`workflowRuns`, `workflowRunLogs`, `emailDeliveries`, `tasks`, `taskUsers`, `taskTags`, `taskRelations`, `taskNotes`, `taskAttachments`, `taskCustomFieldValues`, `taskLogs`, `taskApprovalInstances`, `taskAcks`, `taskAckReads`, `taskWorkspaceContexts`, `workspaceChat`, `notifications`, `gamificationPointTransactions`, `gamificationUserPoints`, `gamificationUserActionStats`, and `gamificationUserBadges`.

That declaration describes the union of every possible `tasks.update` path. It is not the physical write set of a priority-only edit.

### Physical writes in the one main transaction

For this priority edit, the handler performs the following work before the single commit:

1. **`tasks` update.** `updateTaskRow` writes the submitted physical task columns and always advances `updatedAt` (`APP/gonvex/tasks.go:2001-2059`; `APP/gonvex/tasks.go:2078-2100`). Because the client submits several unchanged fields as well as the changed priority, PostgreSQL still sees one task `UPDATE` statement, but its change-notify trigger computes actually changed columns by comparing old/new values (`APP/src/pages/spaces/components/taskDialog/hooks/useTaskSubmit.ts:118-145`; `GVX/server/internal/schema/notify.go:197-214`).
2. **`taskLogs` insert.** Priority is a tracked field, so the handler inserts one `TICKET_UPDATED` audit row containing the old/new priority (`APP/gonvex/tasks.go:1637-1667`).
3. **`taskSearchDocuments` upsert.** The handler explicitly refreshes the search projection (`APP/gonvex/tasks.go:1349-1358`; `APP/gonvex/tasks.go:1538-1589`). The projection contains priority and a freshly generated `updatedAt` (`APP/gonvex/task_search.go:197-240`).
4. **`taskGridRows` upsert.** The handler explicitly refreshes the grid projection (`APP/gonvex/tasks.go:1356-1361`; `APP/gonvex/tasks.go:1538-1589`). That projection includes priority fields and a freshly generated `projectionUpdatedAt` (`APP/gonvex/task_grid.go:227-303`). If lazy infrastructure has installed the task triggers, the task update also invokes projection refresh triggers; those triggers are registered on updates including `priorityId` (`APP/gonvex/task_search.go:163-174`; `APP/gonvex/task_grid.go:137-155`). Repeated upserts remain inside the same transaction/revision.
5. **`notifications` insert(s), conditional on recipients.** A priority difference produces the `task_updated` notification kind; the actor is excluded and assignees/creator are recipients (`APP/gonvex/notification_trigger_parity.go:72-93`; `APP/gonvex/notification_trigger_parity.go:122-134`; `APP/gonvex/notification_trigger_parity.go:361-393`). `insertNotificationRows` inserts one row per recipient and schedules push delivery (`APP/gonvex/boards_broadcasts.go:590-617`). The growing observer notification result in the three metric rows confirms one new visible notification per edit in this scenario (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:4`; `APP/.whagons-monitor/ws-traffic-metrics.jsonl:6`; `APP/.whagons-monitor/ws-traffic-metrics.jsonl:8`).

No `taskUsers` or `taskTags` row changes for this edit: assignee sync only runs when the submitted user IDs differ, and no tag-sync side effect is present in the measured handler path (`APP/gonvex/tasks.go:1270-1287`; `APP/gonvex/tasks.go:1341-1347`). Automatic assignment returns immediately because status did not change (`APP/gonvex/tasks.go:1743-1753`). Approval/acknowledgment, gamification, and workflow work are all guarded by `statusChanged`, which is false for a priority-only edit (`APP/gonvex/tasks.go:1363-1407`; `APP/gonvex/tasks.go:1421-1423`).

`taskUpdateChangedTables(false)` names `tasks`, `taskUsers`, `taskTags`, `taskLogs`, and `notifications`, but this handler callback does not publish an extra pre-commit invalidation: registered mutation contexts deliberately clear `NotifyTableChange` (`APP/gonvex/tasks.go:1425-1451`; `GVX/server/internal/server/ws.go:1934-1941`). The runtime instead uses the manifest's declared write set after commit.

### Revisions and scheduled follow-up

The main mutation is one transaction and therefore one `_gonvex_sync_clock` revision covering every tracked physical row change above. Sync row events are staged with transaction ID, then the deferred constraint trigger assigns one revision to every unassigned event in that transaction (`GVX/server/internal/schema/sync.go:101-148`; `GVX/server/internal/schema/sync.go:212-245`). The recorded mutation likewise has one commit timestamp (`GVX/tmp/gonvex-telemetry.jsonl:2948022`).

For each notification recipient, a zero-delay `pushNotificationHelpers.deliverScheduled` internal mutation is queued only after the main commit (`APP/gonvex/push_notifications.go:69-95`; `GVX/server/internal/server/deferred_scheduler.go:12-15`; `GVX/server/internal/server/deferred_scheduler.go:58-68`). Its declaration is `Reads("fcmTokens", "users")` and `Writes("fcmTokens")` (`APP/gonvex/push_notifications.go:60-67`). It is a separate scheduled transaction. If the recipient has no enabled FCM token it returns before any write and therefore creates no sync revision; if delivery succeeds or finds a stale token, it updates `fcmTokens` and creates a later, separate revision (`APP/gonvex/push_notifications.go:145-163`; `APP/gonvex/push_notifications.go:182-219`). Scheduled internal mutations publish their own declared-write invalidation after success (`GVX/server/internal/server/server.go:150-180`). This conditional `fcmTokens` transaction does not explain the measured task-query rounds because those queries do not depend on `fcmTokens`.

### Why two or three query rounds emerge from one revision

For a subscription that reads `tasks`, the main commit generates at least two independent requests:

1. a broad, multi-table request from `tasks.update`'s declared `Writes` (`GVX/server/internal/server/ws.go:1183-1207`);
2. a precise single-table `tasks` update request from the PostgreSQL notification trigger (`GVX/server/internal/schema/notify.go:161-168`; `GVX/server/internal/server/tenant_listeners.go:266-281`).

A query that also reads a physically written second table—such as `notifications` or `taskLogs`—can receive a third request under another debounce key. The manager immediately starts the first execution; requests arriving while it runs set `dirty`, and the loop performs one trailing execution after the first completes (`GVX/server/internal/server/subscriptions.go:491-552`). Multiple arrivals during the same run collapse to that one trailing execution, which is why the observed count is commonly two or three rather than one execution per physical statement.

## 3. Framework invalidation behavior

### Matching model

The live-query manager derives dependencies from manifest `Reads`; if none are declared, it applies the legacy `subscriptionTables` mapping, and if that still yields nothing it indexes the query as broad (`GVX/server/internal/server/subscriptions.go:353-393`; `GVX/server/internal/server/subscriptions.go:407-422`). A change first selects broad groups plus groups indexed under any changed table (`GVX/server/internal/server/subscriptions.go:301-328`).

Matching is therefore table-level first. For a precise non-broad `UPDATE`, it can then skip a query when the changed columns do not intersect that query's declared read columns/filters/order fields, and it can use result row IDs for updates; broad changes, inserts, deletes, missing row IDs, or result shapes without extractable row IDs match conservatively (`GVX/server/internal/server/subscriptions.go:440-468`). `PageResult` is an object rather than a top-level row array, so `resultRowIDs` cannot extract its nested page IDs and returns nil, disabling row-ID pruning for `bulk.tasksByWorkspace` (`GVX/server/internal/server/ws.go:1457-1486`; `APP/gonvex/bulk.go:88-94`).

### Mapping from commit/revision to rerun

Database sync revisions and query subscription revisions are different counters:

- PostgreSQL allocates one durable database revision per transaction that wrote tracked rows (`GVX/server/internal/schema/sync.go:212-263`). The sync listener uses the revision's changed-table list to deliver only intersecting sync collections and fast-advance the rest (`GVX/server/internal/server/sync.go:1172-1236`).
- Live queries react to table-change events, not directly to that database revision. The listener receives both the durable sync notification and separate table notifications; only the latter call `scheduleTableChange` for query invalidation (`GVX/server/internal/server/tenant_listeners.go:194-222`; `GVX/server/internal/server/tenant_listeners.go:255-281`).
- Each completed shared-subscription execution receives a new in-memory subscription sequence (`GVX/server/internal/server/subscriptions.go:595-626`). Thus “two query revisions” can be two executions caused by one database revision.

### Existing coalescing

There are two limited forms of coalescing:

1. **Table-change debounce:** 75 ms, but keyed by `project + tenant + exact sorted table set` (`GVX/server/internal/server/ws.go:202`; `GVX/server/internal/server/ws.go:1226-1269`). It coalesces repeated `tasks` events with `tasks`, but it cannot combine a 21-table declared batch with a one-table `tasks` event or a one-table `notifications` event.
2. **In-flight shared-group coalescing:** if a request arrives while the query is running, the group marks itself dirty and makes one trailing pass (`GVX/server/internal/server/subscriptions.go:491-552`). It has no pre-execution wait window and therefore cannot merge closely spaced requests that straddle the first execution.

There is no per-connection invalidation debounce for query reruns. Shared groups are already keyed by project, tenant, path, canonical args, permissions/user, bundle, and cache scope, so identical eligible subscriptions can share an execution across listeners (`GVX/server/internal/server/subscriptions.go:353-393`). Coalescing belongs most naturally before `group.run`, or by merging table-change batches per tenant before `requestChange`, rather than independently on every WebSocket connection.

### Existing result suppression and patching

An identical-result hash short-circuit **already exists**. `completeResult` hashes the exact marshaled payload; a repeated hash sends `query.progress` rather than `query.result` (`GVX/server/internal/server/subscriptions.go:589-635`). The cache revision also embeds that content hash so a listener already holding the same payload can be advanced with progress (`GVX/server/internal/server/query_cache.go:122-137`; `GVX/server/internal/server/subscriptions.go:683-718`).

There is also a `query.patch` path for results of at least 4 KB, but `keyedRows` only accepts a top-level JSON array of rows with string `id` fields (`GVX/server/internal/server/subscriptions.go:642-655`; `GVX/server/internal/server/subscriptions.go:811-879`). `bulk.tasksByWorkspace` returns a `PageResult` object with nested `page`, so it is ineligible. Extending the existing patch representation to a known page envelope could avoid sending unchanged rows without introducing a new message type, but clients would need compatible page-envelope application semantics.

The immediate suppression bug is application payload volatility: all `TasksByWorkspace` branches include execution timings in `perf`, including `serverFunctionDurationMs` (`APP/gonvex/bulk.go:443-473`; `APP/gonvex/bulk.go:554-570`; `APP/gonvex/bulk.go:1111-1125`). Hashing the exact JSON correctly treats those payloads as different even when the page rows are identical.

The server query cache does not prevent these reruns. Every scheduled table change immediately increments per-table cache generations (`GVX/server/internal/server/ws.go:1226-1231`; `GVX/server/internal/server/cache.go:129-142`), and invalidation-triggered executions deliberately bypass cached results to avoid replaying stale data (`GVX/server/internal/server/ws.go:1501-1531`).

### Where proposed mechanisms fit

- **Per-subscription coalescing:** add a short pending window to `sharedSubscription.request` before starting `run`, or merge all tenant table sets received for a mutation/revision before `requestChange` (`GVX/server/internal/server/subscriptions.go:491-507`; `GVX/server/internal/server/ws.go:1226-1280`). No wire-protocol change; it adds bounded freshness latency.
- **Identical-result suppression:** keep the existing mechanism, but hash a semantic payload or remove volatile `perf` values from query results (`GVX/server/internal/server/subscriptions.go:589-635`; `APP/gonvex/bulk.go:443-473`). No protocol change.
- **Changed-row diffing:** extend `keyedResultPatch` to understand `{page, ...metadata}` or expose a top-level keyed-row result. `query.patch` already exists, but page-envelope patch semantics and client support must be verified (`GVX/server/internal/server/subscriptions.go:642-655`; `GVX/server/internal/server/subscriptions.go:811-879`).
- **Narrower dependencies:** add precise `Reads(...).Columns(...).Filters(...)` in the app registrations and avoid broad post-commit invalidations that discard those column filters (`GVX/server/internal/server/subscriptions.go:440-468`; `GVX/server/internal/server/ws.go:1188-1207`). A static narrower `Writes` union helps only for tables the mutation does not always write; it cannot express per-argument physical writes without split mutations or runtime-supplied change metadata.

## 4. Reads of the top offenders versus a priority edit

The actual priority edit physically writes `tasks`, `taskLogs`, `taskSearchDocuments`, `taskGridRows`, and—when recipients exist—`notifications`, all in the main transaction; it does not write pivot, finding, workspace-context, workflow, or gamification tables for this path (`APP/gonvex/tasks.go:1322-1423`; `APP/gonvex/tasks.go:1637-1667`; `APP/gonvex/notification_trigger_parity.go:361-393`).

| Query | Declared/fallback Reads | Priority relevance | Would merely narrowing `tasks.update` Writes stop it? |
|---|---|---|---|
| `bulk.tasksByWorkspace` | Explicitly declares 26 tables including `tasks`, task pivots, contexts, references, and users (`APP/gonvex/bulk.go:134-146`; generated manifest at `APP/gonvex/_generated/manifest.json:1542-1626`). It does not declare the `taskSearchDocuments` projection used by the active-search SQL path (`APP/gonvex/bulk.go:460-473`). | **Genuine.** The returned row includes `priorityId`, so a priority change must update the page. The second execution for the same commit is redundant. | **No.** `tasks` must remain in the write set and read set. Fix local sourcing, round coalescing, semantic hash suppression, or page diffing. |
| `bulk.taskPivotData` | No manifest Reads (`APP/gonvex/bulk.go:148-148`; `APP/gonvex/_generated/manifest.json:1634-1638`). Legacy fallback incorrectly adds `taskUsers`, `taskTags`, `taskCustomFieldValues`, `taskApprovalInstances`, and `tasks` (`GVX/server/internal/server/ws.go:2243-2246`). Handler actually reads only the first three pivot tables (`APP/gonvex/bulk.go:2513-2538`). | **False positive.** Priority changes none of those three tables. | **No, not alone.** Because fallback includes `tasks`, any correct update write set still selects it. Add explicit Reads for only the three actual tables. This should eliminate the measured six actor progress frames / about 2.2 KB and their DB work (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:3`; `APP/.whagons-monitor/ws-traffic-metrics.jsonl:5`; `APP/.whagons-monitor/ws-traffic-metrics.jsonl:7`). |
| `taskFindings.routesByTenant` | No manifest Reads (`APP/gonvex/fallbacks.go:73-74`; `APP/gonvex/_generated/manifest.json:4095-4103`). Prefix fallback adds `taskFindings`, `findingNotes`, `taskLogs`, `taskWorkspaceContexts`, and `tasks` (`GVX/server/internal/server/ws.go:2272-2276`). Handler actually reads only `taskWorkspaceContexts` and filters `kind=finding` (`APP/gonvex/remaining_compatibility.go:2066-2071`). | **False positive.** Priority changes neither contexts nor route kind. | **No, not alone.** Fallback `tasks` still matches. Declare `Reads("taskWorkspaceContexts")` with relevant columns/filters. |
| `taskFindings.listByLinkedTaskId` | Same missing-Reads prefix fallback as above (`APP/gonvex/fallbacks.go:73-74`; `GVX/server/internal/server/ws.go:2272-2276`). It actually reads the linked task, `taskFindings`, and optionally the source task's name/deleted state (`APP/gonvex/remaining_compatibility.go:2079-2158`). | **Table dependency genuine, field dependency false for priority.** Priority is not returned or filtered; only task identity/name/deleted state matters. | **No, not alone.** Add explicit task read columns and stop broad mutation invalidation from bypassing column matching. A precise physical `tasks` update containing only `priorityId`/`updatedAt` could then be skipped. |
| Other explicitly registered `taskFindings.*` aggregates | `list`, `countsByTenant`, and `linkedTasksByTenant` declare task columns such as workspace/status/name/deleted fields, but not priority (`APP/gonvex/fallbacks.go:47-71`). | **False for priority result content.** Their task columns do not intersect a priority-only physical change. | **A narrower table list alone is insufficient** because the broad declared `tasks` event bypasses column matching. Suppressing/replacing that broad event would let the existing column declarations skip the physical priority update (`GVX/server/internal/server/subscriptions.go:440-460`). |
| `settings.listNotifications` | Precisely declares `notifications`, selected columns, and tenant/user filters (`APP/gonvex/fallbacks.go:41-45`; generated manifest at `APP/gonvex/_generated/manifest.json:3854-3879`). | **Genuine for the observer.** This scenario inserts one `task_updated` notification for the non-actor recipient. It should deliver the new row once. The actor's unchanged reruns should collapse to progress. | **No when a notification is inserted.** Removing `notifications` from the broad declaration would remove one redundant request, but the physical insert still correctly invalidates it. If there are no recipients, dynamic writes or reliance on physical notifications would avoid the false broad round. |
| `bulk.workspaceStats` | No manifest Reads (`APP/gonvex/bulk.go:152-152`; `APP/gonvex/_generated/manifest.json:1654-1658`). Its legacy fallback becomes the meaningless prefix table `bulk` (`GVX/server/internal/server/ws.go:2164-2169`; `GVX/server/internal/server/ws.go:2280-2283`). | **Genuine.** The handler reads `tasks` and groups by `priorityId`, so the `priorityCounts` result changes (`APP/gonvex/bulk.go:2812-2859`; `APP/gonvex/bulk.go:2883-2905`). | The measured result is not normal dependency invalidation: the client listens for any `bulk.tasksByWorkspace` invalidation and imperatively re-queries stats after 200 ms because the subscription is not correctly invalidated (`APP/src/pages/spaces/workspace/hooks/useWorkspaceStats.ts:235-286`). Add explicit `Reads("tasks")` with the real columns and remove the telemetry-triggered refetch. |

### Effect of narrowing `tasks.update` Writes

Narrowing the static write declaration from the 21-table union to the priority path's actual tables would stop false reruns for queries whose only overlap is `workspaceChat`, gamification tables, workflow tables, approvals/acks, attachments, notes, relations, or pivots. It would **not** stop the five top offenders above by itself: `tasksByWorkspace` and `workspaceStats` genuinely depend on `tasks`; `taskPivotData` and the two unregistered finding queries incorrectly inherit `tasks`; notifications genuinely change for the observer. The measured chat unread summary also depends on notifications, so it is genuine when the observer receives a row, whereas direct workspace-chat and gamification queries are declaration-induced false positives for a priority edit (`GVX/server/internal/server/ws.go:2199-2202`; `APP/gonvex/tasks.go:114-122`).

Because one `tasks.update` entry handles priority, status, assignee, form, custom-field, approval/ack, workflow, and gamification paths, blindly deleting tables from its static declaration risks stale results on other update shapes (`APP/gonvex/tasks.go:1243-1423`). Safer designs are separate field-specific mutations (for example `tasks.updatePriority`), conditional/dynamic write metadata from the committed handler, or eliminating the broad manifest broadcast when precise PostgreSQL notifications are known to be active.

## 5. Ranked quick-win inventory

Impact estimates use the requested metric rows 3–8: actor roughly 66 frames / 78 KB with `bulk.tasksByWorkspace` about 50 KB; observer roughly 36 frames / 40 KB, commonly about 29 KB from the task page (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:3-8`). They are directional, not promises; the exact test has active search and an actor-only retained preview.

### 1. Remove volatile `perf` values from the semantic task-page payload

- **Repo:** app (`APP/gonvex/bulk.go`), optionally framework if the hash gains an explicit non-semantic metadata exclusion.
- **Change:** keep timing in server telemetry/logs, not in the query result; alternatively hash/send a canonical result that excludes `perf` while preserving it only out of band.
- **Estimated impact:** in the common two-round case, the first rerun contains the real priority change and the second is semantically identical. Turning the latter into `query.progress` saves approximately half the task-page bytes: about **25 KB actor and 14 KB observer per edit**, plus it enables the already-present suppression for every other unchanged task-page rerun. Frame count is unchanged unless progress events are also coalesced, but bytes and client JSON/render work fall sharply.
- **Risk/effort:** low. Confirm no UI consumes `perf`; the fields are explicitly execution timings (`APP/gonvex/bulk.go:443-473`; `APP/gonvex/bulk.go:554-570`).
- **Protocol:** none; `query.progress` already exists (`GVX/server/internal/server/subscriptions.go:626-635`).

### 2. Let the default Glide view use the local synchronized window

- **Repo:** app (`taskQueryPlanner.ts`, `GlideWorkspaceTaskGrid.tsx`, and coverage).
- **Change:** remove unconditional `requireLiveServerProjection: true`; select server mode only for the intended search/filter/sort/group/deleted/overflow cases. Keep the exact planner readiness/authority gates (`APP/src/cache/taskQueryPlanner.ts:56-109`; `APP/src/pages/spaces/components/workspaceTable/glide/GlideWorkspaceTaskGrid.tsx:2948-2971`). Also migrate the retained task preview's related-task lookup away from its broad `useGonvexTasks` subscription where the local workspace window is sufficient (`APP/src/pages/spaces/components/taskPreview/TaskPreviewPanel.tsx:951-963`).
- **Estimated impact:** in a true default no-search view, removing the Glide subscription eliminates the observer's task-page traffic—commonly **about 29 KB and 2–4 frames per edit**—and a comparable grid-owned portion of the actor's roughly 50 KB. Removing/replacing the retained preview subscription captures the remainder on the actor. Against this *exact* benchmark, a policy that keeps search server-backed saves zero until the spec clears its search.
- **Risk/effort:** medium. Validate computed context membership, completeness/truncation, exact revision equality, large-workspace overflow, and filters/sorts. Those safety signals already exist (`APP/src/providers/WhagonsSyncProvider.tsx:1013-1020`; `APP/src/cache/taskQueryPlanner.ts:84-109`).
- **Protocol:** none.

### 3. Add correct Reads for pivot, findings, and workspace stats

- **Repo:** app Go registrations and generated manifest; remove the stats workaround in the client after server behavior is covered.
- **Change:**
  - `bulk.taskPivotData`: `taskUsers`, `taskTags`, `taskCustomFieldValues` only (`APP/gonvex/bulk.go:2513-2538`).
  - `taskFindings.routesByTenant`: `taskWorkspaceContexts` only, with finding-relevant columns/filters (`APP/gonvex/remaining_compatibility.go:2066-2071`).
  - `taskFindings.listByLinkedTaskId`: `taskFindings` plus narrow `tasks` name/identity/deleted columns (`APP/gonvex/remaining_compatibility.go:2079-2158`).
  - `bulk.workspaceStats`: explicit task columns including priority/status/workspace/timestamps/visibility inputs, then remove the telemetry-driven imperative re-query (`APP/src/pages/spaces/workspace/hooks/useWorkspaceStats.ts:235-286`).
- **Estimated impact:** immediately removes the actor's **six pivot progress frames (~2.2 KB)** and **six measured finding progress frames (~2.3 KB)** when combined with precise rather than broad task invalidation; it also removes equivalent observer noise and substantial database CPU. Correct stats dependency removes one redundant one-shot subscribe/result cycle of roughly **1.1 KB** per refresh and fixes correctness structurally (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:3-8`).
- **Risk/effort:** low to medium; add invalidation tests for every table/column actually read.
- **Protocol:** none.

### 4. Coalesce declared and physical invalidation rounds

- **Repo:** framework (`server/internal/server/ws.go`, `subscriptions.go`, tenant listener).
- **Change options, in preference order:**
  1. associate the broad post-commit declaration and PostgreSQL notifications with the same mutation ID/revision and merge their table sets;
  2. when a healthy tenant listener is authoritative, use the declared set only as a cache-correctness fallback and let precise committed table notifications drive live subscriptions;
  3. add a short 25–100 ms pending debounce per shared subscription before the first execution.
- **Estimated impact:** collapses the duplicate task execution and much of the long tail. With semantic hash fixed, it mainly saves DB CPU and roughly **20–30 progress frames / 7–11 KB actor** and **8–15 frames / 3–6 KB observer**. Without the hash fix, it also avoids approximately the duplicated **25 KB actor / 14 KB observer** task result.
- **Risk/effort:** medium. Preserve low latency, cross-process notification correctness, cache-generation ordering, reconnect recovery, and mutations on databases without installed triggers. The current two-stage behavior is visible at `GVX/server/internal/server/ws.go:1183-1280` and `GVX/server/internal/server/subscriptions.go:491-552`.
- **Protocol:** none for debounce/merging. Latency semantics change by the chosen window.

### 5. Split or dynamically narrow `tasks.update` Writes

- **Repo:** primarily app; framework support is needed for conditional committed write metadata if the mutation is not split.
- **Change:** introduce field-specific mutations such as `tasks.updatePriority`, or let a successful mutation return/record its actual committed tables and columns. A priority update's broad set should not include workflow, gamification, chat, approval/ack, attachment/note, relation, or pivot tables (`APP/gonvex/tasks.go:114-122`; actual guards at `APP/gonvex/tasks.go:1341-1423`).
- **Estimated impact:** removes most false-positive long-tail reruns (gamification, direct chat, approval/ack, unrelated resources), plausibly **10–25 frames and 4–10 KB per client per edit** in this metric, with larger server-CPU savings than byte savings because existing hashes turn many results into progress.
- **Risk/effort:** medium-high for a split, high for framework dynamic dependencies. A naive static narrowing is unsafe for status/assignee/custom-field updates and does little for top queries that incorrectly include `tasks`.
- **Protocol:** none for a new mutation path if old clients retain `tasks.update`; a dynamic dependency API is an internal manifest/runtime contract change.

### 6. Extend row patching to page envelopes

- **Repo:** framework plus client packages, with app result-shape tests.
- **Change:** teach `keyedResultPatch` and clients to patch `{page, total, isDone, continueCursor}` while sending changed metadata separately, or expose a top-level keyed collection.
- **Estimated impact:** for the first genuine priority result, send one changed task row instead of the whole page. On the measured tiny workspace this could reduce the remaining first result by several KB; on real 250-row pages it is the only option here whose byte savings scale with page size.
- **Risk/effort:** medium-high because order, pagination metadata, inserts/deletes, and cache revisions must stay atomic.
- **Protocol:** can reuse `query.patch`, but page-envelope patch semantics require coordinated server/client compatibility (`GVX/server/internal/server/subscriptions.go:642-655`; `GVX/server/internal/server/subscriptions.go:811-879`).

### 7. Demote only residual chatty counters to polling/coarser dependencies

- **Repo:** app.
- **Change:** after dependency declarations and round coalescing are fixed, move non-critical badge/counter queries to visibility-aware polling or event-specific dependencies. Do not use polling to mask genuinely live task pages, notifications, or workspace priority stats.
- **Estimated impact:** each residual query is only about 0.35–1.1 KB per rerun in the metric, but removing a dozen false progress events can save **4–8 KB and 10–20 frames** per edit (`APP/.whagons-monitor/ws-traffic-metrics.jsonl:3-8`).
- **Risk/effort:** low implementation effort but a real freshness/UX tradeoff; lower value than fixing dependency correctness.
- **Protocol:** none.

## Recommended sequence and verification

1. First remove `perf` volatility and add the missing Reads. These are small changes that make existing framework mechanisms work as designed.
2. Correct the benchmark so one scenario truly has no search; keep a second explicit active-search scenario. The current helper leaves search active (`APP/tests/e2e/helpers/tasks.ts:164-173`; `APP/tests/e2e/realtime/ws-traffic-budget.spec.ts:327-336`).
3. Enable the planner's local default path and separately remove the retained preview's broad task subscription.
4. Implement framework invalidation coalescing, using server telemetry to assert one query execution per relevant shared group per application commit.
5. Only then evaluate dynamic Writes and page-envelope patches; their remaining benefit will be measurable without the current duplicate noise.

Success criteria for a default-view priority edit should be: one `tasks.update` mutation result, one `sync.recentWorkspaceTasks` delta plus readiness/watermark bookkeeping, zero `bulk.tasksByWorkspace` frames for clients using the authoritative local window, one notification result only for actual recipients, one workspace-stats update, and no pivot/finding/gamification/workspace-chat reruns unless their physical inputs changed. The sync fan-out is already table-filtered by changed-table intersection (`GVX/server/internal/server/sync.go:1172-1236`); the next work is to bring query invalidation to the same level of precision.
