package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRuntimeMetricsPersistsFailuresOfEveryKind(t *testing.T) {
	store := &memoryReducerLogStore{appended: make(chan runtimeLogEntry, 4)}
	metrics := newRuntimeMetrics()
	metrics.startReducerLogPersistence(store)
	now := time.Now().UTC()

	// A failed action is the case that used to vanish with the in-memory ring.
	metrics.recordRuntimeLog(runtimeLogEntry{
		Time: now.Format(time.RFC3339Nano), Project: "project-a", Path: "assistant.processThread",
		Kind: "action", Outcome: "error", Error: "thread owner is not a member of this tenant",
	}, now)
	// Successful non-reducers stay memory-only.
	metrics.recordRuntimeLog(runtimeLogEntry{
		Time: now.Add(time.Millisecond).Format(time.RFC3339Nano), Project: "project-a",
		Path: "assistant.processThread", Kind: "action", Outcome: "ok",
	}, now)
	metrics.recordRuntimeLog(runtimeLogEntry{
		Time: now.Add(2 * time.Millisecond).Format(time.RFC3339Nano), Project: "project-a",
		Path: "tasks.list", Kind: "query", Outcome: "error", Error: "boom",
	}, now)

	for _, want := range []string{"assistant.processThread", "tasks.list"} {
		select {
		case entry := <-store.appended:
			if entry.Path != want || entry.Outcome != "error" {
				t.Fatalf("persisted %s/%s, want failed %s", entry.Path, entry.Outcome, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s to be persisted", want)
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.entries) != 2 {
		t.Fatalf("persisted entries = %d, want only the two failures", len(store.entries))
	}
}

func TestRuntimeMetricsRestoresPersistedFailuresIntoErrorCapture(t *testing.T) {
	metrics := newRuntimeMetrics()
	captured := make(chan runtimeLogEntry, 2)
	metrics.onFunctionError = func(entry runtimeLogEntry) { captured <- entry }

	metrics.restoreReducerLogs([]runtimeLogEntry{
		{Project: "project-a", Path: "tasks.update", Kind: "reducer", Outcome: "ok"},
		{Project: "project-a", Path: "dev.sync", Kind: "runtime", Outcome: "error", Error: "schema apply failed"},
	})

	select {
	case entry := <-captured:
		if entry.Path != "dev.sync" {
			t.Fatalf("restored %q, want the persisted failure", entry.Path)
		}
	case <-time.After(time.Second):
		t.Fatal("persisted failure was not restored into error capture")
	}
	select {
	case entry := <-captured:
		t.Fatalf("successful persisted log was restored into error capture: %#v", entry)
	default:
	}
}

func TestRuntimeMetricsForwardsFailedCallsToErrorCapture(t *testing.T) {
	metrics := newRuntimeMetrics()
	captured := make(chan runtimeLogEntry, 4)
	metrics.onFunctionError = func(entry runtimeLogEntry) { captured <- entry }
	now := time.Now().UTC()

	metrics.recordRuntimeLog(runtimeLogEntry{Project: "project-a", Path: "tasks.list", Kind: "query", Outcome: "ok"}, now)
	metrics.recordRuntimeLog(runtimeLogEntry{Project: "project-a", Path: "teams.create", Kind: "reducer", Outcome: "error", Error: "boom"}, now)
	metrics.recordRuntimeLog(runtimeLogEntry{Project: "project-a", Path: "dev.sync", Kind: "runtime", Outcome: "error", Error: "schema apply failed"}, now)

	for _, want := range []string{"teams.create", "dev.sync"} {
		select {
		case entry := <-captured:
			if entry.Path != want {
				t.Fatalf("captured %q, want failed log %q", entry.Path, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("failed log %q was never forwarded to error capture", want)
		}
	}
	select {
	case entry := <-captured:
		t.Fatalf("successful call was forwarded to error capture: %#v", entry)
	default:
	}
}

func TestRuntimeErrorEventCarriesTenantAccountAndCulprit(t *testing.T) {
	event, ok := runtimeErrorEvent(runtimeLogEntry{
		Time:             "2026-07-27T22:25:38.940Z",
		ExecutionID:      "ba97d237-c189-4f2c-b4e9-6cb503f93b63",
		Project:          "project-a",
		Tenant:           "el-rey-2",
		AccountID:        "account-1",
		AccountEmail:     "person@example.com",
		Path:             "assistant.processThread",
		Kind:             "action",
		Outcome:          "error",
		Error:            "assistant loop: thread owner is not a member of this tenant",
		CompletedAt:      "2026-07-27T22:25:39.040Z",
		DurationMS:       100,
		Request:          []byte(`{"threadId":"thread-1","token":"hidden"}`),
		RequestSizeBytes: 40,
	})
	if !ok {
		t.Fatal("failed action did not produce an error event")
	}
	if event.Tenant != "el-rey-2" || event.Account["id"] != "account-1" || event.Account["email"] != "person@example.com" {
		t.Fatalf("event lost its tenant/account attribution: %#v", event)
	}
	if event.Culprit != "action assistant.processThread" {
		t.Fatalf("culprit = %q, want kind + path so groups stay per function", event.Culprit)
	}
	if event.EventID != "runtime-ba97d237-c189-4f2c-b4e9-6cb503f93b63" {
		t.Fatalf("eventId = %q, want one event per execution", event.EventID)
	}
	if event.Level != "error" || event.Message == "" || event.Tags["source"] != "runtime" {
		t.Fatalf("event is not a tagged runtime error: %#v", event)
	}
	request, ok := event.Context["request"].(map[string]any)
	if !ok || request["threadId"] != "thread-1" || request["token"] != filteredErrorValue {
		t.Fatalf("event lost or failed to sanitize request context: %#v", event.Context)
	}
	if event.Context["completedAt"] != "2026-07-27T22:25:39.040Z" || event.Context["requestSizeBytes"] != "40" {
		t.Fatalf("event lost execution timing context: %#v", event.Context)
	}

	// Distinct functions must not collapse into one group.
	other, _ := runtimeErrorEvent(runtimeLogEntry{
		Time: "2026-07-27T22:25:38.940Z", ExecutionID: "other", Project: "project-a",
		Path: "teams.create", Kind: "reducer", Outcome: "error",
		Error: "assistant loop: thread owner is not a member of this tenant",
	})
	if fingerprint(event) == fingerprint(other) {
		t.Fatal("different functions produced the same fingerprint")
	}
}

func TestRuntimeErrorEventIgnoresNonFailures(t *testing.T) {
	if _, ok := runtimeErrorEvent(runtimeLogEntry{Project: "project-a", Path: "tasks.list", Kind: "query", Outcome: "ok"}); ok {
		t.Fatal("a successful call produced an error event")
	}
	if _, ok := runtimeErrorEvent(runtimeLogEntry{Project: "project-a", Path: "tasks.list", Outcome: "error"}); ok {
		t.Fatal("an error without a message produced an event")
	}
	if _, ok := runtimeErrorEvent(runtimeLogEntry{Path: "tasks.list", Outcome: "error", Error: "boom"}); ok {
		t.Fatal("an error without a project produced an event")
	}
}

// Existing deployments carry a kind CHECK from when only mutations were durable;
// with it in place every failed query/action insert is rejected by Postgres and
// the failure is lost exactly like before.
func TestProjectRegistryDropsRuntimeLogKindConstraint(t *testing.T) {
	statements := []string{}
	db := &recordingDB{exec: func(query string, args ...any) {
		statements = append(statements, query)
	}}
	if err := ensureProjectRegistry(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	if strings.Contains(joined, "CHECK (kind IN ('mutation', 'internalMutation'))") {
		t.Fatal("runtime log table still restricts kind to mutations")
	}
	if !strings.Contains(joined, "ALTER TABLE gonvex_runtime_mutation_logs RENAME TO gonvex_runtime_reducer_logs") {
		t.Fatal("existing runtime audit log is not migrated to Reducer terminology")
	}
	if !strings.Contains(joined, "WHERE kind IN ('mutation', 'internalMutation')") ||
		!strings.Contains(joined, "jsonb_set(entry, '{kind}', to_jsonb('reducer'::text), true)") {
		t.Fatal("existing runtime audit entries are not rewritten to reducer terminology")
	}
	if !strings.Contains(joined, "DROP CONSTRAINT IF EXISTS gonvex_runtime_mutation_logs_kind_check") {
		t.Fatal("existing deployments keep the old kind CHECK and reject failure logs")
	}
}
