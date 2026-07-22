package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
)

type memoryMutationLogStore struct {
	mu       sync.Mutex
	entries  []runtimeLogEntry
	appended chan runtimeLogEntry
}

func (s *memoryMutationLogStore) LoadRecent(_ context.Context, limit int) ([]runtimeLogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := len(s.entries) - limit
	if start < 0 {
		start = 0
	}
	return append([]runtimeLogEntry(nil), s.entries[start:]...), nil
}

func (s *memoryMutationLogStore) Append(_ context.Context, entry runtimeLogEntry) error {
	s.mu.Lock()
	s.entries = append(s.entries, entry)
	s.mu.Unlock()
	if s.appended != nil {
		s.appended <- entry
	}
	return nil
}

func TestRuntimeMetricsPersistsOnlyMutations(t *testing.T) {
	store := &memoryMutationLogStore{appended: make(chan runtimeLogEntry, 2)}
	metrics := newRuntimeMetrics()
	metrics.startMutationLogPersistence(store)
	now := time.Now().UTC()
	request := sanitizeRuntimeLogRequest(json.RawMessage(`{"title":"Test","accessToken":"secret"}`))
	metrics.recordRuntimeLog(runtimeLogEntry{Time: now.Format(time.RFC3339Nano), Project: "project-a", Path: "tasks.create", Kind: "mutation", Outcome: "ok", Request: request}, now)
	metrics.recordRuntimeLog(runtimeLogEntry{Time: now.Add(time.Millisecond).Format(time.RFC3339Nano), Project: "project-a", Path: "tasks.cleanup", Kind: "internalMutation", Outcome: "ok"}, now)
	metrics.recordRuntimeLog(runtimeLogEntry{Time: now.Add(2 * time.Millisecond).Format(time.RFC3339Nano), Project: "project-a", Path: "tasks.list", Kind: "query", Outcome: "ok"}, now)

	for _, wantKind := range []string{"mutation", "internalMutation"} {
		select {
		case entry := <-store.appended:
			if entry.Kind != wantKind {
				t.Fatalf("persisted kind = %q, want %q", entry.Kind, wantKind)
			}
			if entry.Kind == "mutation" && string(entry.Request) != `{"accessToken":"[REDACTED]","title":"Test"}` {
				t.Fatalf("persisted request = %s", entry.Request)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s persistence", wantKind)
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.entries) != 2 {
		t.Fatalf("persisted entries = %d, want only two mutations", len(store.entries))
	}
}

func TestProjectRegistrySchemaIncludesRuntimeMutationLogs(t *testing.T) {
	statements := []string{}
	db := &recordingDB{exec: func(query string, args ...any) {
		statements = append(statements, query)
	}}
	if err := ensureProjectRegistry(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS gonvex_runtime_mutation_logs") {
		t.Fatalf("expected runtime mutation log table in control-plane migration")
	}
	if !strings.Contains(joined, "entry JSONB NOT NULL") {
		t.Fatalf("expected complete runtime log JSON in control-plane migration")
	}
}

func TestRuntimeMetricsRestoresLatestMutationLogsInOrder(t *testing.T) {
	entries := make([]runtimeLogEntry, metricsLogLimit+5)
	start := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	for index := range entries {
		entries[index] = runtimeLogEntry{
			Time:        start.Add(time.Duration(index) * time.Second).Format(time.RFC3339Nano),
			ExecutionID: fmt.Sprintf("execution-%d", index),
			Project:     "project-a",
			Path:        fmt.Sprintf("tasks.mutation%d", index),
			Kind:        "mutation",
			Outcome:     "ok",
		}
	}
	store := &memoryMutationLogStore{entries: entries}
	restarted := newRuntimeMetrics()
	restarted.startMutationLogPersistence(store)

	deadline := time.Now().Add(time.Second)
	var snapshot runtimeMetricsSnapshot
	for time.Now().Before(deadline) {
		snapshot = restarted.snapshot(manifest.Manifest{}, 0, 0, "project-a")
		if len(snapshot.Logs) == metricsLogLimit {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(snapshot.Logs) != metricsLogLimit {
		t.Fatalf("restored logs = %d, want %d", len(snapshot.Logs), metricsLogLimit)
	}
	if snapshot.Logs[0].ExecutionID != "execution-1004" || snapshot.Logs[len(snapshot.Logs)-1].ExecutionID != "execution-5" {
		t.Fatalf("restored order/limit = first %q last %q", snapshot.Logs[0].ExecutionID, snapshot.Logs[len(snapshot.Logs)-1].ExecutionID)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.entries) != metricsLogLimit+5 {
		t.Fatalf("restored logs were persisted again: entries = %d", len(store.entries))
	}
}

func TestRuntimeLogRequestRedactsSensitiveValues(t *testing.T) {
	request := sanitizeRuntimeLogRequest(json.RawMessage(`{
		"workspaceId":"workspace-17",
		"accessToken":"top-secret",
		"nested":{"password":"hunter2","filter":"active"},
		"authorization":"Bearer private"
	}`))

	var decoded map[string]any
	if err := json.Unmarshal(request, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["workspaceId"] != "workspace-17" {
		t.Fatalf("workspaceId = %#v", decoded["workspaceId"])
	}
	if decoded["accessToken"] != "[REDACTED]" || decoded["authorization"] != "[REDACTED]" {
		t.Fatalf("top-level secrets were not redacted: %#v", decoded)
	}
	nested := decoded["nested"].(map[string]any)
	if nested["password"] != "[REDACTED]" || nested["filter"] != "active" {
		t.Fatalf("nested request was not safely preserved: %#v", nested)
	}
}

func TestRuntimeFunctionLogIncludesExecutionContext(t *testing.T) {
	metrics := newRuntimeMetrics()
	execution := newRuntimeFunctionLog(
		"project-a",
		"tenant-a",
		"bulk.tasksByWorkspace",
		"query",
		callerContext{user: &gonvex.User{ID: "user-a", Email: "user@example.test"}},
		json.RawMessage(`{"workspaceId":"workspace-a"}`),
	)
	metrics.recordFunctionExecution(execution, nil)

	snapshot := metrics.snapshot(manifest.Manifest{Functions: map[string]manifest.FunctionEntry{
		"bulk.tasksByWorkspace": {Kind: manifest.FunctionKindQuery},
	}}, 1, 1, "project-a")
	if len(snapshot.Logs) != 1 {
		t.Fatalf("logs = %d, want 1", len(snapshot.Logs))
	}
	entry := snapshot.Logs[0]
	if entry.ExecutionID == "" || entry.StartedAt == "" || entry.CompletedAt == "" {
		t.Fatalf("execution timestamps/id missing: %+v", entry)
	}
	if entry.Tenant != "tenant-a" || entry.UserID != "user-a" || entry.UserEmail != "user@example.test" {
		t.Fatalf("execution context missing: %+v", entry)
	}
	if string(entry.Request) != `{"workspaceId":"workspace-a"}` {
		t.Fatalf("request = %s", entry.Request)
	}
}

func TestRuntimeMetricsTracksDatabasePoolPressure(t *testing.T) {
	metrics := newRuntimeMetrics()
	metrics.recordDatabase("project-a", databasePoolStats{
		Pools:              2,
		OpenConnections:    8,
		InUse:              3,
		Idle:               5,
		MaxOpenConnections: 0,
		WaitCount:          4,
		WaitDuration:       125000000,
	})

	snapshot := metrics.snapshot(manifest.Manifest{}, 0, 0, "project-a")
	if snapshot.Database.Pools != 2 || snapshot.Database.OpenConnections != 8 || snapshot.Database.InUse != 3 || snapshot.Database.Idle != 5 {
		t.Fatalf("database snapshot = %+v", snapshot.Database)
	}
	if snapshot.Database.WaitCount != 4 || snapshot.Database.WaitDurationMS != 125 {
		t.Fatalf("database waits = %+v", snapshot.Database)
	}
	if len(snapshot.Database.Series) != 1 || snapshot.Database.Series[0].WaitCount != 4 || snapshot.Database.Series[0].WaitDurationMS != 125 {
		t.Fatalf("database series = %+v", snapshot.Database.Series)
	}
}

func TestWebsocketSnapshotScopesAndDescribesConnections(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	server := &Server{wsConns: map[*wsConn]bool{}}
	server.addWSConn(&wsConn{
		id: "conn-000001", project: "project-a", tenant: "tenant-a", auth: true,
		user: &gonvex.User{ID: "user-a", Email: "ada@example.test"}, connectedAt: now.Add(-time.Minute),
		lastActiveAt: now, lastActivity: "query.subscribe", lastPath: "tasks.list",
		device: clientDeviceInfo{BrowserName: "Chrome", BrowserVersion: "126", DeviceType: "desktop", Platform: "macOS"},
		subs:   map[string]querySubscription{"one": {path: "tasks.list"}, "two": {path: "notifications.list"}},
	})
	server.addWSConn(&wsConn{
		id: "conn-000002", project: "project-a", tenant: "tenant-b", auth: true,
		user: &gonvex.User{ID: "user-a", Email: "ada@example.test"}, connectedAt: now.Add(-2 * time.Minute),
		lastActiveAt: now.Add(-time.Second), lastActivity: "mutation.call", lastPath: "tasks.update",
		subs: map[string]querySubscription{},
	})
	server.addWSConn(&wsConn{
		id: "conn-000003", project: "project-b", tenant: "tenant-c", connectedAt: now,
		lastActiveAt: now, lastActivity: "connected", subs: map[string]querySubscription{},
	})

	snapshot := server.websocketSnapshot("project-a")
	if snapshot.Connections != 2 || snapshot.Subscriptions != 2 || snapshot.Users != 1 {
		t.Fatalf("unexpected websocket totals: %+v", snapshot)
	}
	if len(snapshot.Details) != 2 || snapshot.Details[0].ID != "conn-000001" {
		t.Fatalf("unexpected connection details/order: %+v", snapshot.Details)
	}
	first := snapshot.Details[0]
	if first.UserEmail != "ada@example.test" || first.Tenant != "tenant-a" || first.Browser != "Chrome 126" {
		t.Fatalf("connection identity/destination/device missing: %+v", first)
	}
	if strings.Join(first.Subscriptions, ",") != "notifications.list,tasks.list" {
		t.Fatalf("subscriptions = %v", first.Subscriptions)
	}
}
