package server

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
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

func TestSyncSnapshotRejectsDuplicateKeys(t *testing.T) {
	_, err := syncSnapshotRows([]map[string]any{
		{"id": "duplicate", "title": "first"},
		{"id": "duplicate", "title": "second"},
	}, manifest.SyncDefinition{Key: "id"})
	if err == nil {
		t.Fatal("duplicate keys would make the collection digest ambiguous")
	}
}

func TestSyncRowKeyMatchesJavaScriptScalarStringification(t *testing.T) {
	tests := map[string]string{
		`{"id":100000000000000000000}`: "100000000000000000000",
		`{"id":1e-7}`:                  "1e-7",
		`{"id":-0}`:                    "0",
		`{"id":true}`:                  "true",
	}
	for raw, want := range tests {
		if got := syncRowKey(json.RawMessage(raw), "id"); got != want {
			t.Fatalf("syncRowKey(%s) = %q, want %q", raw, got, want)
		}
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

func TestSyncDefinitionsForSchemaDurablyLogsVisibilityDependencies(t *testing.T) {
	definitions := map[string]manifest.SyncDefinition{
		"tasks": {
			Table:            "tasks",
			Key:              "id",
			Columns:          []string{"id", "title"},
			VisibilityTables: []string{"taskAcks"},
		},
	}
	schema := manifest.Schema{Tables: map[string]manifest.Table{
		"tasks": {Columns: map[string]manifest.Column{
			"id":    {Type: "id", PrimaryKey: true},
			"title": {Type: "string"},
		}},
		"taskAcks": {Columns: map[string]manifest.Column{
			"_id":    {Type: "id", PrimaryKey: true},
			"status": {Type: "string"},
		}},
	}}

	got, err := syncDefinitionsForSchema(definitions, schema)
	if err != nil {
		t.Fatal(err)
	}
	dependency, ok := got["taskAcks"]
	if !ok {
		t.Fatal("visibility dependency must have a durable sync-log trigger for offline reconnects")
	}
	if dependency.Key != "_id" || len(dependency.Columns) != 1 || dependency.Columns[0] != "_id" {
		t.Fatalf("unexpected visibility dependency definition: %#v", dependency)
	}
}

func TestSyncReadDependenciesAutomaticallyBecomeVisibilityDependencies(t *testing.T) {
	definition := effectiveSyncDefinition(manifest.FunctionEntry{
		Kind: manifest.FunctionKindSync,
		Sync: &manifest.SyncDefinition{
			Table: "tasks", Key: "id", VisibilityTables: []string{"taskAcks"},
		},
		Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{
			{Table: "tasks"},
			{Table: "taskApprovalInstances"},
			{Table: "taskAcks"},
		}},
	})
	if got, want := strings.Join(definition.VisibilityTables, ","), "taskAcks,taskApprovalInstances"; got != want {
		t.Fatalf("effective visibility dependencies = %q, want %q", got, want)
	}
}

func TestSyncVisibilityChangesAreIncludedInReconnectReconciliation(t *testing.T) {
	changes := []syncLogChange{
		{revision: 41, table: "tasks", rowID: "task-a"},
		{revision: 42, table: "taskAckReads", rowID: "read-a"},
	}
	if !syncVisibilityChanged(changes, "tasks") {
		t.Fatal("a dependency-table change must be reconciled before sync.ready")
	}
	if syncVisibilityChanged(changes[:1], "tasks") {
		t.Fatal("source-table changes must remain delta-replayable")
	}
}

func TestBoundedEagerSyncReconcilesItsAuthoritativeWindow(t *testing.T) {
	changes := []syncLogChange{{revision: 42, table: "statuses", rowID: "status-new"}}
	if !syncNeedsAuthoritativeReconcile(manifest.SyncDefinition{
		Table: "statuses", Mode: "eager", MaxRows: 100,
	}, changes) {
		t.Fatal("direct replay cannot determine which row follows a full bounded window")
	}
	if syncNeedsAuthoritativeReconcile(manifest.SyncDefinition{
		Table: "statuses", Mode: "eager",
	}, changes) {
		t.Fatal("an unbounded source-shaped eager collection should retain direct delta replay")
	}
}

func TestSyncValueMatchesAppliesExclusionsWithoutEqualityFilters(t *testing.T) {
	definition := manifest.SyncDefinition{ExcludeWhenSet: []string{"deletedAt"}}
	if syncValueMatches(json.RawMessage(`{"id":"deleted","deletedAt":123}`), definition, nil) {
		t.Fatal("a soft-deleted row must not reappear merely because the sync has no equality filters")
	}
	if !syncValueMatches(json.RawMessage(`{"id":"active","deletedAt":null}`), definition, nil) {
		t.Fatal("an active row should remain visible")
	}
}

func TestProgressiveSyncKeepsContextRoutedRowsInBothCollections(t *testing.T) {
	current := map[string]json.RawMessage{
		"task-a": json.RawMessage(`{"id":"task-a","workspaceId":"approval-workspace"}`),
	}
	visible := map[string]bool{"task-a": true}
	currentHashes := syncRowsHashes([]json.RawMessage{current["task-a"]}, "id")
	visibleHashes := map[string]string{"task-a": "stale-hash"}
	changes := []syncLogChange{{
		revision: 42,
		table:    "tasks",
		rowID:    "task-a",
		// The physical source task remains in its default workspace. The computed
		// handler includes it in the approval/acknowledgment workspace as well.
		newValue: json.RawMessage(`{"id":"task-a","workspaceId":"default-workspace"}`),
	}}

	upserts, deleted, _ := progressiveSyncDiff(current, currentHashes, visible, visibleHashes, changes)
	if _, ok := upserts["task-a"]; !ok {
		t.Fatal("handler-authorized context row must be updated in the action workspace")
	}
	if deleted["task-a"] {
		t.Fatal("context-routed task must not be removed merely because its physical workspace differs")
	}
}

func TestProgressiveSyncSendsOnlyRowsWhoseAuthoritativeHashChanged(t *testing.T) {
	rows := map[string]json.RawMessage{
		"same":    json.RawMessage(`{"id":"same","value":1}`),
		"changed": json.RawMessage(`{"id":"changed","value":2}`),
	}
	hashes := syncRowsHashes([]json.RawMessage{rows["same"], rows["changed"]}, "id")
	visible := map[string]bool{"same": true, "changed": true, "deleted": true}
	visibleHashes := map[string]string{
		"same":    hashes["same"],
		"changed": "old-hash",
		"deleted": "deleted-hash",
	}

	upserts, deleted, _ := progressiveSyncDiff(rows, hashes, visible, visibleHashes, nil)
	if len(upserts) != 1 || upserts["changed"] == nil {
		t.Fatalf("expected only changed row delta, got %#v", upserts)
	}
	if len(deleted) != 1 || !deleted["deleted"] {
		t.Fatalf("expected only missing row deletion, got %#v", deleted)
	}
}

func TestAuthoritativeSyncDiffConvergesAcrossRandomCorruption(t *testing.T) {
	random := rand.New(rand.NewSource(0x5eed))
	for iteration := 0; iteration < 2_000; iteration++ {
		currentRows := map[string]json.RawMessage{}
		currentHashes := map[string]string{}
		visibleKeys := map[string]bool{}
		visibleHashes := map[string]string{}
		for index := 0; index < 40; index++ {
			key := fmt.Sprintf("row-%d", index)
			if random.Intn(3) != 0 {
				row := json.RawMessage(fmt.Sprintf(`{"id":%q,"value":%d}`, key, random.Intn(20)))
				currentRows[key] = row
				currentHashes[key] = syncRowHash(row)
			}
			if random.Intn(3) != 0 {
				visibleKeys[key] = true
				switch random.Intn(4) {
				case 0:
					// Missing metadata is a valid corruption case.
				case 1:
					visibleHashes[key] = "corrupted"
				default:
					visibleHashes[key] = currentHashes[key]
				}
			}
		}

		upserts, deleted, _ := progressiveSyncDiff(
			currentRows,
			currentHashes,
			visibleKeys,
			visibleHashes,
			nil,
		)
		for key := range deleted {
			delete(visibleKeys, key)
			delete(visibleHashes, key)
		}
		for key := range upserts {
			visibleKeys[key] = true
			visibleHashes[key] = currentHashes[key]
		}

		if len(visibleKeys) != len(currentRows) || len(visibleHashes) != len(currentHashes) {
			t.Fatalf("iteration %d did not converge collection cardinality", iteration)
		}
		for key, hash := range currentHashes {
			if !visibleKeys[key] || visibleHashes[key] != hash {
				t.Fatalf("iteration %d left %q stale", iteration, key)
			}
		}
	}
}

func TestSyncHashesDigestMatchesClientCanonicalEncoding(t *testing.T) {
	const want = "db8992cf941b185a1c5bbcfbdc3f43204fe09d42b1a57ff584f6ad2e5e6dfdbd"
	if got := syncHashesDigest(map[string]string{"a": "b"}); got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
	if got := syncHashesDigest(map[string]string{}); got != "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945" {
		t.Fatalf("empty digest = %q", got)
	}
	const unicodeWant = "829a3fd7e2d0b8a181fc05f90729311dee80aa61e3a94723a1cc4553dd4dd7c9"
	if got := syncHashesDigest(map[string]string{"< ": "one", "": "two", "𐀀": "three"}); got != unicodeWant {
		t.Fatalf("unicode-key digest = %q, want %q", got, unicodeWant)
	}
}

func TestSyncRowHashMatchesClientCanonicalEncoding(t *testing.T) {
	row := json.RawMessage(`{
		"nested":{"z":1,"a":" ","key ":"value","":"bmp","𐀀":"supplementary"},
		"minusZero":-0,
		"id":"row-a",
		"amp":"<&>"
	}`)
	const want = "ba54453d2ecfb596b1cedeb64d8753675128ae801cf18da01a6cdb1005c5d71b"
	if got := syncRowHash(row); got != want {
		t.Fatalf("canonical row %s hash = %q, want %q", canonicalSyncJSON(row), got, want)
	}
}

func TestSyncBatchProtocolPreservesIndependentResumeState(t *testing.T) {
	var message clientMessage
	if err := json.Unmarshal([]byte(`{
		"type":"sync.openMany",
		"opens":[
			{"id":"one","path":"sync.tasks","args":{"workspaceId":"a"},"cursor":{"epoch":"e1","revision":7},"digest":"digest-one"},
			{"id":"two","path":"sync.statuses","args":{},"cursor":{"epoch":"e2","revision":9},"keys":["s1","s2"],"fullIntegrity":true}
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
	if message.Opens[0].Digest != "digest-one" || !message.Opens[1].FullIntegrity {
		t.Fatal("compact and expanded integrity resume metadata must survive batching")
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
