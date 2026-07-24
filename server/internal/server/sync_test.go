package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestSyncValueMatchesEqualityArguments(t *testing.T) {
	definition := manifest.SyncDefinition{
		EqualFilters:   map[string]string{"workspaceId": "workspaceId"},
		ExcludeWhenSet: []string{"deletedAt"},
	}
	args := map[string]json.RawMessage{
		"workspaceId": json.RawMessage(`"workspace-a"`),
	}

	if !syncValueMatches(json.RawMessage(`{"id":"task-a","workspaceId":"workspace-a"}`), definition, args) {
		t.Fatal("expected matching workspace row")
	}
	if syncValueMatches(json.RawMessage(`{"id":"task-b","workspaceId":"workspace-b"}`), definition, args) {
		t.Fatal("did not expect another workspace row")
	}
	if syncValueMatches(json.RawMessage(`null`), definition, args) {
		t.Fatal("did not expect a deleted row to match")
	}
	if syncValueMatches(json.RawMessage(`{"id":"task-c","workspaceId":"workspace-a","deletedAt":1720000000000}`), definition, args) {
		t.Fatal("did not expect a soft-deleted row to match")
	}
}

func TestSyncSnapshotHonorsRowAndByteBudgets(t *testing.T) {
	result := []map[string]any{
		{"id": "a", "title": "small"},
		{"id": "b", "title": "also-small"},
		{"id": "c", "title": "third"},
	}
	rows, err := syncSnapshotRows(result, manifest.SyncDefinition{
		Key:      "id",
		MaxRows:  2,
		MaxBytes: 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected row budget to keep two rows, got %d", len(rows))
	}

	rows, err = syncSnapshotRows(result, manifest.SyncDefinition{
		Key:      "id",
		MaxBytes: int64(len(rows[0])),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected byte budget to keep one row, got %d", len(rows))
	}
}

func TestSyncRetentionUsesLongestDeclaredWindow(t *testing.T) {
	current := manifest.Manifest{Functions: map[string]manifest.FunctionEntry{
		"tasks.sync": {
			Kind: manifest.FunctionKindSync,
			Sync: &manifest.SyncDefinition{RetentionMilliseconds: (14 * 24 * time.Hour).Milliseconds()},
		},
		"statuses.sync": {
			Kind: manifest.FunctionKindSync,
			Sync: &manifest.SyncDefinition{RetentionMilliseconds: (2 * 24 * time.Hour).Milliseconds()},
		},
	}}

	if got := syncRetentionForManifest(current); got != 14*24*time.Hour {
		t.Fatalf("expected longest retention window, got %s", got)
	}
	if got := syncRetentionForManifest(manifest.Manifest{}); got != defaultSyncRetention {
		t.Fatalf("expected default retention window, got %s", got)
	}
}

func TestSyncCursorForClockSharesRevisionButIsolatesDefinitionsAndScopes(t *testing.T) {
	clock := syncClock{DatabaseEpoch: "database-a", Revision: 42, RetainedRevision: 7}
	tasks := manifest.SyncDefinition{Table: "tasks", Key: "id", Columns: []string{"id", "title"}}
	statuses := manifest.SyncDefinition{Table: "statuses", Key: "id", Columns: []string{"id", "name"}}

	taskCursor := syncCursorForClock(clock, tasks, "scope-a")
	statusCursor := syncCursorForClock(clock, statuses, "scope-a")
	otherScopeCursor := syncCursorForClock(clock, tasks, "scope-b")

	if taskCursor.Revision != clock.Revision || statusCursor.Revision != clock.Revision {
		t.Fatalf("expected every definition in a batch to share revision %d", clock.Revision)
	}
	if taskCursor.Epoch == statusCursor.Epoch {
		t.Fatal("different sync definitions must not share an epoch")
	}
	if taskCursor.Epoch == otherScopeCursor.Epoch {
		t.Fatal("different visibility scopes must not share an epoch")
	}
}

func TestSyncBatchProtocolPreservesIndependentResumeState(t *testing.T) {
	var message clientMessage
	if err := json.Unmarshal([]byte(`{
		"type":"sync.openMany",
		"opens":[
			{"id":"one","path":"sync.tasks","args":{"workspaceId":"a"},"cursor":{"epoch":"e1","revision":7}},
			{"id":"two","path":"sync.statuses","args":{},"cursor":{"epoch":"e2","revision":9},"keys":["s1","s2"]}
		]
	}`), &message); err != nil {
		t.Fatal(err)
	}
	if len(message.Opens) != 2 {
		t.Fatalf("expected two independent opens, got %d", len(message.Opens))
	}
	if message.Opens[0].Cursor == nil || message.Opens[0].Cursor.Revision != 7 {
		t.Fatalf("first cursor was not preserved: %#v", message.Opens[0].Cursor)
	}
	if got := len(message.Opens[1].Keys); got != 2 {
		t.Fatalf("expected progressive keys to survive batching, got %d", got)
	}
}

func TestSyncDeliveryStormCoalescesToOneRunningAndOnePendingPass(t *testing.T) {
	subscription := &syncSubscription{}
	if !beginSyncDelivery(subscription) {
		t.Fatal("first notification must start delivery")
	}

	for index := 0; index < 10_000; index++ {
		if beginSyncDelivery(subscription) {
			t.Fatalf("notification %d started a duplicate concurrent delivery", index)
		}
	}

	if !finishSyncDelivery(subscription) {
		t.Fatal("coalesced notifications must request exactly one follow-up delivery")
	}
	if finishSyncDelivery(subscription) {
		t.Fatal("follow-up delivery must drain the pending state")
	}

	subscription.mu.Lock()
	running := subscription.deliveryRunning
	pending := subscription.deliveryPending
	subscription.mu.Unlock()
	if running || pending {
		t.Fatalf("delivery state did not drain: running=%t pending=%t", running, pending)
	}
}

func TestSyncAuthRevocationCannotDeadlockDelivery(t *testing.T) {
	runtime := New(config.Config{RequireAuth: true})
	connection := &wsConn{
		server:  runtime,
		project: "project-a",
		tenant:  "tenant-a",
		syncs:   map[string]*syncSubscription{},
		subs:    map[string]querySubscription{},
	}
	subscription := &syncSubscription{
		conn: connection,
		id:   "sync-a",
		path: "tasks.sync",
	}
	connection.syncs[subscription.id] = subscription

	done := make(chan error, 1)
	go func() {
		done <- runtime.deliverSync(subscription)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sync delivery deadlocked while clearing revoked authentication")
	}

	connection.mu.Lock()
	remaining := len(connection.syncs)
	connection.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("revoked authentication left %d sync subscriptions attached", remaining)
	}
}
