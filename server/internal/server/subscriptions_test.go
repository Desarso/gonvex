package server

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestSubscriptionTokensAreDistinctMapKeys(t *testing.T) {
	tokens := make(map[*subscriptionToken]struct{}, 10_000)
	for range 10_000 {
		tokens[newSubscriptionToken()] = struct{}{}
	}
	if len(tokens) != 10_000 {
		t.Fatalf("distinct subscription tokens = %d, want 10000", len(tokens))
	}
}

func TestResultRowIDsAcceptsConvexStyleIDs(t *testing.T) {
	ids := resultRowIDs([]map[string]any{
		{"id": "postgres-id"},
		{"_id": "convex-id"},
		{"id": "preferred-id", "_id": "fallback-id"},
	})
	for _, id := range []string{"postgres-id", "convex-id", "preferred-id"} {
		if !ids[id] {
			t.Fatalf("missing result row id %q from %#v", id, ids)
		}
	}
	if ids["fallback-id"] {
		t.Fatalf("used _id even though id was available: %#v", ids)
	}
}

func TestResultRowIDsAcceptsPageEnvelopes(t *testing.T) {
	ids := resultRowIDs(map[string]any{
		"rows": []any{
			map[string]any{"id": "task-1"},
			map[string]any{"_id": "task-2"},
		},
	})
	for _, id := range []string{"task-1", "task-2"} {
		if !ids[id] {
			t.Fatalf("missing page result row id %q from %#v", id, ids)
		}
	}
	itemIDs := resultRowIDs(map[string]any{"items": []map[string]any{{"id": "item-1"}}})
	if !itemIDs["item-1"] {
		t.Fatalf("missing items envelope id from %#v", itemIDs)
	}
}

func installTestTenantListener(manager *tenantListenerManager, project, tenant string, connected bool) {
	manager.mu.Lock()
	manager.active[tenantListenerKey{project: project, tenant: tenant}] = &tenantListener{
		key: tenantListenerKey{project: project, tenant: tenant}, connected: connected,
	}
	manager.mu.Unlock()
}

func indexedTestGroup(manager *subscriptionManager, key, table string, executions *atomic.Int32) *sharedSubscription {
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: key, project: "project-a", tenant: "tenant-a", path: key,
		ctx: context.Background(), reads: []manifest.ReadDependency{{Table: table}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	manager.mu.Lock()
	manager.groups[key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()
	return group
}

func TestPreciseTriggerTablesOverrideDeclaredWritesForSubscriptions(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	installTestTenantListener(manager.listeners, "project-a", "tenant-a", true)
	var taskExecutions atomic.Int32
	var logExecutions atomic.Int32
	manager.execute = func(_ context.Context, group *sharedSubscription, _ querySubscription, _ string, _ float64) (any, error) {
		if group.key == "tasks.list" {
			taskExecutions.Add(1)
		} else {
			logExecutions.Add(1)
		}
		return []map[string]any{{"id": "task-1"}}, nil
	}
	indexedTestGroup(manager, "tasks.list", "tasks", &taskExecutions)
	indexedTestGroup(manager, "taskLogs.list", "task_logs", &logExecutions)

	const commitID = "precise-commit"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, broad: true,
		tables: map[string]bool{"tasks": true, "task_logs": true}, changedAtMS: 10,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		operation: "update", changedColumns: []string{"title"}, rowIDs: map[string]bool{"task-1": true},
		triggerObserved: true, changedAtMS: 11,
	})

	eventually(t, time.Second, func() bool { return taskExecutions.Load() == 1 })
	time.Sleep(subscriptionRerunCooldown + 25*time.Millisecond)
	if got := logExecutions.Load(); got != 0 {
		t.Fatalf("task_logs executions = %d, want 0 for tasks-only observed commit", got)
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionsSkippedByTable; got == 0 {
		t.Fatal("subscriptions skipped by table = 0, want the declared-only task_logs dependency counted")
	}
}

func TestUnhealthyListenerFallsBackToDeclaredWrites(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	installTestTenantListener(manager.listeners, "project-a", "tenant-a", false)
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	indexedTestGroup(manager, "tasks.list", "tasks", &executions)
	indexedTestGroup(manager, "taskLogs.list", "task_logs", &executions)

	const commitID = "fallback-commit"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, broad: true,
		tables: map[string]bool{"tasks": true, "task_logs": true}, changedAtMS: 10,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		operation: "update", triggerObserved: true, changedAtMS: 11,
	})

	eventually(t, time.Second, func() bool { return executions.Load() == 2 })
}

func TestLateAdditionalTableForPreciseCommitIsNotDeduplicated(t *testing.T) {
	oldCooldown := subscriptionRerunCooldown
	subscriptionRerunCooldown = 10 * time.Millisecond
	t.Cleanup(func() { subscriptionRerunCooldown = oldCooldown })

	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: "combined.list", project: "project-a", tenant: "tenant-a", path: "combined.list",
		ctx: context.Background(), reads: []manifest.ReadDependency{{Table: "tasks"}, {Table: "task_logs"}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	manager.mu.Lock()
	manager.groups[group.key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()

	const commitID = "late-table-commit"
	manager.requestChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		tables: map[string]bool{"tasks": true}, details: map[string]tableChangeDetail{"tasks": {precise: true, broad: true}},
	})
	eventually(t, time.Second, func() bool { return executions.Load() == 1 })
	manager.requestChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "task_logs",
		tables: map[string]bool{"task_logs": true}, details: map[string]tableChangeDetail{"task_logs": {precise: true, broad: true}},
	})
	eventually(t, time.Second, func() bool { return executions.Load() == 2 })
}

func TestSubscriptionCountsDoNotTraverseGroups(t *testing.T) {
	blocked := &sharedSubscription{}
	manager := &subscriptionManager{
		groups:        map[string]*sharedSubscription{"blocked": blocked},
		listenerCount: 42,
	}
	blocked.mu.Lock()
	type counts struct{ groups, listeners int }
	done := make(chan counts, 1)
	go func() {
		manager.mu.Lock()
		groups, listeners := manager.countsLocked()
		manager.mu.Unlock()
		done <- counts{groups: groups, listeners: listeners}
	}()

	select {
	case got := <-done:
		blocked.mu.Unlock()
		if got.groups != 1 || got.listeners != 42 {
			t.Fatalf("counts = %+v, want groups=1 listeners=42", got)
		}
	case <-time.After(50 * time.Millisecond):
		blocked.mu.Unlock()
		<-done
		t.Fatal("counting subscriptions blocked on an individual group")
	}
}

func TestSubscriptionRunnerSerializesAndCoalescesBurst(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var running atomic.Int32
	var maximum atomic.Int32
	var executions atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	type executionChange struct {
		reason      string
		changedAtMS float64
	}
	changes := make(chan executionChange, 2)
	manager.execute = func(_ context.Context, _ *sharedSubscription, _ querySubscription, reason string, changedAtMS float64) (any, error) {
		changes <- executionChange{reason: reason, changedAtMS: changedAtMS}
		current := running.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		call := executions.Add(1)
		if call == 1 {
			close(started)
			<-release
		}
		running.Add(-1)
		return []map[string]any{{"id": "task-1", "title": "same"}}, nil
	}
	group := &sharedSubscription{
		manager: manager, project: "project-a", tenant: "tenant-a", path: "tasks.list",
		ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{},
	}
	token := newSubscriptionToken()
	group.listeners[token] = querySubscription{token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}}

	group.request("initial", 0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first execution did not start")
	}
	for index := 0; index < 20; index++ {
		group.request("invalidate", float64(index+1))
	}
	close(release)
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 2
	})
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent executions = %d, want 1", maximum.Load())
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.RerunsCoalesced; got != 20 {
		t.Fatalf("reruns coalesced = %d, want 20", got)
	}
	first, second := <-changes, <-changes
	if first.reason != "initial" || first.changedAtMS != 0 {
		t.Fatalf("first execution change = %#v", first)
	}
	if second.reason != "invalidate" || second.changedAtMS != 20 {
		t.Fatalf("coalesced execution change = %#v, want latest revision 20", second)
	}
}

func TestSubscriptionCooldownBoundsDistinctCommitBurstAndDeliversFinalState(t *testing.T) {
	oldCooldown := subscriptionRerunCooldown
	subscriptionRerunCooldown = 25 * time.Millisecond
	t.Cleanup(func() { subscriptionRerunCooldown = oldCooldown })

	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var state atomic.Int32
	var executions atomic.Int32
	var delivered atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		value := state.Load()
		delivered.Store(value)
		return map[string]any{"value": value}, nil
	}
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: "tasks.latest", project: "project-a", tenant: "tenant-a", path: "tasks.latest",
		ctx: context.Background(), reads: []manifest.ReadDependency{{Table: "tasks"}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	group.request("initial", 0)
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 1
	})

	for index := 1; index <= 20; index++ {
		state.Store(int32(index))
		group.requestForCommit("invalidate", float64(index), "commit-"+strconv.Itoa(index))
	}
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && delivered.Load() == 20
	})
	if reruns := executions.Load() - 1; reruns > 3 {
		t.Fatalf("executions for 20-commit cooldown burst = %d, want at most 3", reruns)
	}
}

func TestSubscriptionRerunDispatcherBoundsConcurrency(t *testing.T) {
	server := New(config.Config{
		TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20, SubscriptionRerunConcurrency: 2,
	})
	manager := server.subscriptions
	var running atomic.Int32
	var maximum atomic.Int32
	var executions atomic.Int32
	release := make(chan struct{})
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		current := running.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		<-release
		running.Add(-1)
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	groups := make([]*sharedSubscription, 0, 10)
	for index := 0; index < 10; index++ {
		group := indexedTestGroup(manager, "tasks."+strconv.Itoa(index), "tasks", &executions)
		groups = append(groups, group)
		group.request("recover", 1)
	}
	eventually(t, time.Second, func() bool { return running.Load() == 2 })
	eventually(t, time.Second, func() bool {
		return server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionRerunQueueDepth == 8
	})
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionRerunQueueDepth; got != 8 {
		t.Fatalf("rerun queue depth = %d, want 8", got)
	}
	close(release)
	eventually(t, time.Second, func() bool { return executions.Load() == 10 })
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent reruns = %d, want 2", got)
	}
	for _, group := range groups {
		group.mu.Lock()
		running := group.running
		group.mu.Unlock()
		if running {
			t.Fatal("rerun group remained active after dispatcher drained")
		}
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionRerunQueueDepth; got != 0 {
		t.Fatalf("final rerun queue depth = %d, want 0", got)
	}
}

func TestDeclaredAndPhysicalInvalidationsForCommitExecuteSubscriptionOnce(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1", "title": "latest"}}, nil
	}
	groupCtx, groupCancel := context.WithCancel(context.Background())
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: "tasks-group", project: "project-a", tenant: "tenant-a", path: "tasks.list",
		ctx: groupCtx, cancel: groupCancel, reads: []manifest.ReadDependency{{Table: "tasks"}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	manager.mu.Lock()
	manager.groups[group.key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()

	const commitID = "mutation-one"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", tables: map[string]bool{"tasks": true, "taskLogs": true},
		broad: true, changedAtMS: 10, commitID: commitID,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", table: "tasks", operation: "update",
		changedColumns: []string{"title"}, rowIDs: map[string]bool{"task-1": true}, changedAtMS: 11, commitID: commitID,
	})
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 1
	})

	// A delayed physical notification for an already executed commit is still
	// harmless: commit-aware subscription deduplication prevents a second run.
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", table: "tasks", operation: "update",
		changedColumns: []string{"title"}, changedAtMS: 12, commitID: commitID,
	})
	time.Sleep(tableChangeDebounce + subscriptionInvalidationCoalesce + 25*time.Millisecond)
	if got := executions.Load(); got != 1 {
		t.Fatalf("subscription executions for one declared+physical commit = %d, want 1", got)
	}
	reactive := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.SubscriptionCommitsObserved != 1 || reactive.CommitQueryExecutions != 1 ||
		reactive.ExecutionsPerSubscriptionCommit != 1 || reactive.MaxExecutionsPerSubscriptionCommit != 1 ||
		reactive.DuplicateCommitQueryExecutions != 0 {
		t.Fatalf("commit execution telemetry = %+v, want exactly one execution for one subscription/commit", reactive)
	}
}

func TestRapidCommitsCoalesceToLatestResultAndAdvanceRevision(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	connection, peer := newSyncReadyTestConnection(t, false)
	connection.server = server
	connection.project = "project-a"
	connection.tenant = "tenant-a"

	manager := server.subscriptions
	var state atomic.Int32
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return map[string]any{"value": state.Load()}, nil
	}
	groupCtx, groupCancel := context.WithCancel(context.Background())
	token := newSubscriptionToken()
	sub := querySubscription{
		conn: connection, id: "query-1", project: "project-a", tenant: "tenant-a", path: "tasks.latest",
		token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}},
	}
	connection.mu.Lock()
	connection.subs[sub.id] = sub
	connection.mu.Unlock()
	group := &sharedSubscription{
		manager: manager, key: "latest-group", project: sub.project, tenant: sub.tenant, path: sub.path,
		ctx: groupCtx, cancel: groupCancel, reads: []manifest.ReadDependency{{Table: "tasks"}},
		listeners: map[*subscriptionToken]querySubscription{token: sub},
	}
	manager.mu.Lock()
	manager.groups[group.key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()

	group.request("initial", 0)
	initial := readSyncTestFrames(t, peer, 1)[0]
	if initial.Type != "query.result" || initial.SubscriptionRevision == nil || initial.SubscriptionRevision.Sequence != 1 {
		t.Fatalf("initial query frame = %+v, want result at revision 1", initial)
	}

	state.Store(1)
	manager.requestChange(tableChange{project: "project-a", tenant: "tenant-a", table: "tasks", broad: true, changedAtMS: 20, commitID: "commit-one"})
	state.Store(2)
	manager.requestChange(tableChange{project: "project-a", tenant: "tenant-a", table: "tasks", broad: true, changedAtMS: 21, commitID: "commit-two"})
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 2
	})

	latest := readSyncTestFrames(t, peer, 1)[0]
	if latest.Type != "query.result" || latest.SubscriptionRevision == nil || latest.SubscriptionRevision.Sequence != 2 {
		t.Fatalf("coalesced query frame = %+v, want result at revision 2", latest)
	}
	var payload struct {
		Value int32 `json:"value"`
	}
	encodedResult, err := json.Marshal(latest.Result)
	if err != nil {
		t.Fatalf("encode latest result: %v", err)
	}
	if err := json.Unmarshal(encodedResult, &payload); err != nil {
		t.Fatalf("decode latest result: %v", err)
	}
	if payload.Value != 2 {
		t.Fatalf("coalesced query result value = %d, want final state 2", payload.Value)
	}
	group.mu.Lock()
	revision := group.revision
	group.mu.Unlock()
	if revision != 2 {
		t.Fatalf("subscription revision = %d, want initial + one coalesced rerun = 2", revision)
	}
	reactive := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.SubscriptionCommitsObserved != 2 || reactive.CommitQueryExecutions != 1 ||
		reactive.ExecutionsPerSubscriptionCommit != 0.5 || reactive.MaxExecutionsPerSubscriptionCommit != 1 ||
		reactive.DuplicateCommitQueryExecutions != 0 {
		t.Fatalf("rapid-commit execution telemetry = %+v, want both commits covered by one latest-state execution", reactive)
	}
}

func TestSingleListenerGroupKeepsHashWithoutRetainingResultPayload(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: server.subscriptions,
		path:    "tasks.list",
		ctx:     context.Background(),
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background()},
		},
	}
	result := []map[string]any{{"id": "task-1", "title": "same"}}

	group.completeResult(result, "initial", 0, time.Now())
	if !group.hasHash {
		t.Fatal("single-listener group did not retain the result hash")
	}
	if len(group.lastResult) != 0 {
		t.Fatalf("single-listener group retained %d result bytes", len(group.lastResult))
	}

	group.completeResult(result, "invalidate", 0, time.Now())
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.UnchangedResultsSuppressed; got != 1 {
		t.Fatalf("unchanged results suppressed = %d, want 1", got)
	}

	group.listeners[newSubscriptionToken()] = querySubscription{ctx: context.Background()}
	group.completeResult([]map[string]any{{"id": "task-1", "title": "changed"}}, "invalidate", 0, time.Now())
	if len(group.lastResult) == 0 {
		t.Fatal("shared group did not retain a replayable result")
	}
}

func TestSubscriptionResultSuppressionIgnoresTopLevelPerformanceMetadata(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: server.subscriptions,
		path:    "bulk.tasksByWorkspace",
		ctx:     context.Background(),
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background()},
		},
	}
	result := func(title string, durationMS float64) map[string]any {
		return map[string]any{
			"page":  []map[string]any{{"id": "task-1", "title": title}},
			"total": 1,
			"perf": map[string]any{
				"source":                   "tasksSQL",
				"serverFunctionDurationMs": durationMS,
			},
		}
	}

	group.completeResult(result("same", 1.25), "initial", 0, time.Now())
	group.completeResult(result("same", 8.75), "invalidate", 0, time.Now())

	reactive := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.FullResults != 1 || reactive.UnchangedResultsSuppressed != 1 || reactive.ProgressMessages != 1 {
		t.Fatalf("volatile-only rerun metrics = %+v, want one full result followed by one progress suppression", reactive)
	}

	group.completeResult(result("changed", 3.5), "invalidate", 0, time.Now())
	reactive = server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.FullResults != 2 || reactive.UnchangedResultsSuppressed != 1 {
		t.Fatalf("semantic-change metrics = %+v, want a second full result", reactive)
	}
}

func TestDependencyIndexSelectsOnlyMatchingTenantTableAndColumns(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	manager := server.subscriptions
	tasks := &sharedSubscription{manager: manager, project: "p", tenant: "a", path: "tasks.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "tasks", Columns: []string{"title"}}}}
	users := &sharedSubscription{manager: manager, project: "p", tenant: "a", path: "users.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "users"}}}
	otherTenant := &sharedSubscription{manager: manager, project: "p", tenant: "b", path: "tasks.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "tasks"}}}
	manager.mu.Lock()
	manager.indexGroupLocked(tasks)
	manager.indexGroupLocked(users)
	manager.indexGroupLocked(otherTenant)
	manager.mu.Unlock()

	manager.requestChange(tableChange{project: "p", tenant: "a", table: "tasks", operation: "update", changedColumns: []string{"description"}})
	if tasks.requested != 0 || users.requested != 0 || otherTenant.requested != 0 {
		t.Fatalf("irrelevant column selected a subscription: tasks=%d users=%d other=%d", tasks.requested, users.requested, otherTenant.requested)
	}
	manager.requestChange(tableChange{project: "p", tenant: "a", table: "tasks", operation: "update", changedColumns: []string{"title"}})
	if tasks.requested != 1 || users.requested != 0 || otherTenant.requested != 0 {
		t.Fatalf("dependency selection mismatch: tasks=%d users=%d other=%d", tasks.requested, users.requested, otherTenant.requested)
	}
}

func TestSharedKeyRequiresExplicitPermissionSharing(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	server.runtime.SyncManifest(manifest.Manifest{Project: "p", Functions: map[string]manifest.FunctionEntry{
		"tasks.list": {Kind: manifest.FunctionKindQuery, Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{{Table: "tasks"}}, ShareByPermissions: true}},
	}, Schema: manifest.EmptySchema()})
	base := querySubscription{project: "p", tenant: "a", path: "tasks.list", args: json.RawMessage(`{"status":"open"}`), caller: callerContext{user: &gonvex.User{ID: "one"}, permissions: map[string]any{"role": "member"}}}
	other := base
	other.caller.user = &gonvex.User{ID: "two"}
	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(other)
	if firstKey != secondKey {
		t.Fatal("same permission scope should share when explicitly enabled")
	}
	other.tenant = "b"
	thirdKey, _, _ := server.subscriptions.groupKeyAndDependencies(other)
	if firstKey == thirdKey {
		t.Fatal("different tenants must never share")
	}
}

func TestSharedKeySeparatesUsersAndBundleVersionsByDefault(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	current := manifest.Manifest{
		Project: "p",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {Kind: manifest.FunctionKindQuery, Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{{Table: "tasks"}}}},
		},
		Schema: manifest.EmptySchema(),
		Bundle: &manifest.SourceBundle{Hash: "bundle-a"},
	}
	if err := server.runtime.SyncManifest(current); err != nil {
		t.Fatal(err)
	}
	base := querySubscription{project: "p", tenant: "a", path: "tasks.list", args: json.RawMessage(`{"status":"open"}`), caller: callerContext{user: &gonvex.User{ID: "one"}, permissions: map[string]any{"role": "member"}}}
	otherUser := base
	otherUser.caller.user = &gonvex.User{ID: "two"}
	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(otherUser)
	if firstKey == secondKey {
		t.Fatal("different users must not share unless permission-only sharing is explicit")
	}

	current.Bundle.Hash = "bundle-b"
	if err := server.runtime.SyncManifest(current); err != nil {
		t.Fatal(err)
	}
	afterDeploy, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	if firstKey == afterDeploy {
		t.Fatal("bundle deployment must create a distinct shared key")
	}
}

func TestSharedKeySeparatesQueryCacheScopes(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: "p",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {
				Kind: manifest.FunctionKindQuery,
				Dependencies: manifest.FunctionDependencies{
					Reads: []manifest.ReadDependency{{Table: "tasks"}},
				},
			},
		},
		Schema: manifest.EmptySchema(),
		Bundle: &manifest.SourceBundle{Hash: "same-bundle"},
	}); err != nil {
		t.Fatal(err)
	}

	first := querySubscription{
		project: "p", tenant: "a", path: "tasks.list",
		args: json.RawMessage(`{"status":"open"}`),
		caller: callerContext{
			user:        &gonvex.User{ID: "one"},
			permissions: map[string]any{"role": "member"},
		},
		cacheScope: "scope-before-manifest-change",
	}
	second := first
	second.cacheScope = "scope-after-manifest-change"

	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(first)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(second)
	if firstKey == secondKey {
		t.Fatal("different query cache scopes must not share a subscription group")
	}
}

func TestKeyedResultPatch(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`[{"id":"a","title":"old"},{"id":"b","title":"keep"}]`),
		json.RawMessage(`[{"id":"b","title":"keep"},{"id":"a","title":"new"},{"id":"c","title":"added"}]`),
	)
	if !ok || len(patch.Inserted) != 1 || len(patch.Updated) != 1 || len(patch.Deleted) != 0 {
		t.Fatalf("unexpected patch: %#v", patch)
	}
	if got := patch.Order; len(got) != 3 || got[0] != "b" || got[2] != "c" {
		t.Fatalf("unexpected order: %v", got)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied")
}
