package server

import (
	"encoding/json"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
)

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
