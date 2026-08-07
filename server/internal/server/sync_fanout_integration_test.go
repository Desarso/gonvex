package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSyncFanoutSkipThenRelevantChangePreservesCursorAndDelta(t *testing.T) {
	harness := newSyncFanoutHarness(t)
	harness.open(t, "sync-b", harness.pathB)
	harness.readFrames(t, 2)
	harness.callsB.Store(0)

	if _, err := harness.db.Exec(`INSERT INTO ` + quoteIdent(harness.tableA) + ` ("id", "value") VALUES ('a-1', 'first')`); err != nil {
		t.Fatal(err)
	}
	aClock := harness.clock(t)
	harness.runtime.notifySyncRevision(harness.project, harness.project, []string{harness.tableA}, aClock.DatabaseEpoch, aClock.Revision)

	ready := harness.readFrames(t, 1)[0]
	if ready.Type != "sync.ready" || ready.ID != "sync-b" || ready.Cursor == nil || ready.Cursor.Revision != aClock.Revision {
		t.Fatalf("unrelated revision frame = %#v, want cursor-advancing ready at revision %d", ready, aClock.Revision)
	}
	if got := harness.callsB.Load(); got != 0 {
		t.Fatalf("unrelated table change ran the B subscription handler %d times", got)
	}

	if _, err := harness.db.Exec(`INSERT INTO ` + quoteIdent(harness.tableB) + ` ("id", "value") VALUES ('b-1', 'second')`); err != nil {
		t.Fatal(err)
	}
	bClock := harness.clock(t)
	if bClock.Revision != aClock.Revision+1 {
		t.Fatalf("test revisions are not adjacent: A=%d B=%d", aClock.Revision, bClock.Revision)
	}
	harness.runtime.notifySyncRevision(harness.project, harness.project, []string{harness.tableB}, bClock.DatabaseEpoch, bClock.Revision)

	frames := harness.readFrames(t, 2)
	if frames[0].Type != "sync.delta" || frames[0].ID != "sync-b" || frames[0].Cursor == nil || frames[0].Cursor.Revision != bClock.Revision {
		t.Fatalf("relevant revision delta = %#v, want B delta through revision %d", frames[0], bClock.Revision)
	}
	if len(frames[0].Upserts) != 1 || syncRowKey(frames[0].Upserts[0], "id") != "b-1" {
		t.Fatalf("relevant revision upserts = %#v, want row b-1", frames[0].Upserts)
	}
	if frames[1].Type != "sync.ready" || frames[1].Cursor == nil || frames[1].Cursor.Revision != bClock.Revision {
		t.Fatalf("relevant revision ready = %#v, want revision %d", frames[1], bClock.Revision)
	}
}

func TestSyncFanoutWatermarkOrdersAfterChangedReadyAndResumes(t *testing.T) {
	harness := newSyncFanoutHarness(t)
	harness.connection.mu.Lock()
	harness.connection.syncWatermark = true
	harness.connection.syncReadyMany = true
	harness.connection.mu.Unlock()
	harness.open(t, "sync-a", harness.pathA)
	harness.open(t, "sync-b", harness.pathB)
	harness.readFrames(t, 4)

	if _, err := harness.db.Exec(`INSERT INTO ` + quoteIdent(harness.tableA) + ` ("id", "value") VALUES ('watermark-a', 'changed')`); err != nil {
		t.Fatal(err)
	}
	latest := harness.clock(t)
	harness.runtime.notifySyncRevision(
		harness.project,
		harness.project,
		[]string{harness.tableA},
		latest.DatabaseEpoch,
		latest.Revision,
	)

	frames := harness.readFrames(t, 3)
	if frames[0].Type != "sync.delta" || frames[0].ID != "sync-a" {
		t.Fatalf("first post-notify frame = %#v, want changed subscription delta", frames[0])
	}
	if frames[1].Type != "sync.ready" || frames[1].ID != "sync-a" {
		t.Fatalf("second post-notify frame = %#v, want changed subscription ready", frames[1])
	}
	if frames[2].Type != "sync.watermark" || frames[2].Revision != latest.Revision {
		t.Fatalf("third post-notify frame = %#v, want watermark revision %d", frames[2], latest.Revision)
	}

	// The B subscription never received an explicit ready at latest.Revision;
	// its server cursor moved only through the watermark skip path. Reopen from
	// that cursor and digest to prove numeric resume cursors are accepted.
	harness.connection.mu.Lock()
	watermarked := harness.connection.syncs["sync-b"]
	harness.connection.mu.Unlock()
	watermarked.mu.Lock()
	resumeCursor := watermarked.cursor
	resumeDigest := syncHashesDigest(watermarked.visibleHashes)
	watermarked.mu.Unlock()
	if resumeCursor.Revision != latest.Revision {
		t.Fatalf("watermarked server cursor = %d, want %d", resumeCursor.Revision, latest.Revision)
	}
	harness.connection.closeSync("sync-b")
	harness.connection.openSyncWithClock(harness.ctx, clientMessage{
		ID: "sync-b-resumed", Path: harness.pathB, Cursor: &resumeCursor, Digest: resumeDigest,
	}, harness.runtime.databaseURLForTenant(harness.project, harness.project), latest)

	resumed := harness.readFrames(t, 1)[0]
	if resumed.Type != "sync.ready" || resumed.ID != "sync-b-resumed" || resumed.Cursor == nil || resumed.Cursor.Revision != latest.Revision {
		t.Fatalf("watermark-cursor resume frame = %#v, want ready at revision %d", resumed, latest.Revision)
	}
}

func TestSyncFanoutWithoutWatermarkCapabilityKeepsPerSubscriptionReadies(t *testing.T) {
	harness := newSyncFanoutHarness(t)
	harness.connection.mu.Lock()
	harness.connection.syncWatermark = false
	harness.connection.syncReadyMany = false
	harness.connection.mu.Unlock()
	harness.open(t, "sync-a", harness.pathA)
	harness.open(t, "sync-b", harness.pathB)
	harness.readFrames(t, 4)

	if _, err := harness.db.Exec(`INSERT INTO ` + quoteIdent(harness.tableA) + ` ("id", "value") VALUES ('legacy-a', 'changed')`); err != nil {
		t.Fatal(err)
	}
	latest := harness.clock(t)
	harness.runtime.notifySyncRevision(
		harness.project,
		harness.project,
		[]string{harness.tableA},
		latest.DatabaseEpoch,
		latest.Revision,
	)

	frames := harness.readFrames(t, 3)
	deltas := 0
	readies := map[string]bool{}
	for _, frame := range frames {
		switch frame.Type {
		case "sync.delta":
			deltas++
		case "sync.ready":
			readies[frame.ID] = true
		default:
			t.Fatalf("legacy client received unexpected frame: %#v", frame)
		}
	}
	if deltas != 1 || !readies["sync-a"] || !readies["sync-b"] {
		t.Fatalf("legacy frames: deltas=%d readies=%#v, want one delta and both per-subscription readies", deltas, readies)
	}
}

func TestSyncFanoutReconnectReplayWithoutTablesDeliversEverything(t *testing.T) {
	harness := newSyncFanoutHarness(t)
	harness.open(t, "sync-a", harness.pathA)
	harness.open(t, "sync-b", harness.pathB)
	harness.readFrames(t, 4)
	harness.callsA.Store(0)
	harness.callsB.Store(0)

	harness.connection.mu.Lock()
	subscriptions := []*syncSubscription{
		harness.connection.syncs["sync-a"],
		harness.connection.syncs["sync-b"],
	}
	harness.connection.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.verified = false
		subscription.mu.Unlock()
	}

	if _, err := harness.db.Exec(`INSERT INTO ` + quoteIdent(harness.tableA) + ` ("id", "value") VALUES ('replay-a', 'changed')`); err != nil {
		t.Fatal(err)
	}
	latest := harness.clock(t)
	// Reconnect recovery deliberately carries no table list. It must retain the
	// full-delivery path even though only table A changed.
	harness.runtime.notifySyncRevision(harness.project, harness.project, nil, "", 0)

	covered := harness.readReadyCoverage(t, 2)
	if !covered["sync-a"] || !covered["sync-b"] {
		t.Fatalf("reconnect replay ready coverage = %#v, want both subscriptions", covered)
	}
	if harness.callsA.Load() != 1 || harness.callsB.Load() != 1 {
		t.Fatalf("reconnect replay handler calls = A:%d B:%d, want 1 each", harness.callsA.Load(), harness.callsB.Load())
	}
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		if !subscription.verified || subscription.cursor.Revision != latest.Revision {
			t.Errorf("subscription %s after replay: verified=%t revision=%d, want true/%d", subscription.id, subscription.verified, subscription.cursor.Revision, latest.Revision)
		}
		subscription.mu.Unlock()
	}
}

type syncFanoutHarness struct {
	ctx        context.Context
	db         *sql.DB
	runtime    *Server
	connection *wsConn
	peer       *websocket.Conn
	project    string
	tableA     string
	tableB     string
	pathA      string
	pathB      string
	callsA     atomic.Int32
	callsB     atomic.Int32
}

func newSyncFanoutHarness(t *testing.T) *syncFanoutHarness {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	harness := &syncFanoutHarness{ctx: context.Background()}
	suffix := time.Now().UnixNano()
	harness.project = fmt.Sprintf("wp4-project-%d", suffix)
	harness.tableA = fmt.Sprintf("wp4_a_%d", suffix)
	harness.tableB = fmt.Sprintf("wp4_b_%d", suffix)
	harness.pathA = "sync.wp4A"
	harness.pathB = "sync.wp4B"

	schema := manifest.Schema{Tables: map[string]manifest.Table{
		harness.tableA: {Columns: map[string]manifest.Column{
			"id": {Type: "id", PrimaryKey: true}, "value": {Type: "string"},
		}},
		harness.tableB: {Columns: map[string]manifest.Column{
			"id": {Type: "id", PrimaryKey: true}, "value": {Type: "string"},
		}},
	}}
	definitionA := manifest.SyncDefinition{Table: harness.tableA, Key: "id", Columns: []string{"id", "value"}, Mode: "eager"}
	definitionB := manifest.SyncDefinition{Table: harness.tableB, Key: "id", Columns: []string{"id", "value"}, Mode: "eager"}
	if _, err := schemasync.ApplyWithSync(harness.ctx, databaseURL, schema, map[string]manifest.SyncDefinition{
		harness.tableA: definitionA,
		harness.tableB: definitionB,
	}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	harness.db = db
	if err := harness.db.PingContext(harness.ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = harness.db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(harness.tableA))
		_, _ = harness.db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(harness.tableB))
		_, _ = harness.db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+harness.tableA+"_stage") + `()`)
		_, _ = harness.db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+harness.tableB+"_stage") + `()`)
		_ = harness.db.Close()
	})

	app := gonvex.NewApp()
	app.Sync(harness.pathA, func(*gonvex.QueryCtx, struct{}) ([]map[string]any, error) {
		harness.callsA.Add(1)
		return []map[string]any{}, nil
	}, gonvex.SyncTable(harness.tableA).Columns("id", "value"))
	app.Sync(harness.pathB, func(*gonvex.QueryCtx, struct{}) ([]map[string]any, error) {
		harness.callsB.Add(1)
		return []map[string]any{}, nil
	}, gonvex.SyncTable(harness.tableB).Columns("id", "value"))
	harness.runtime = NewWithApp(config.Config{
		ProjectDatabases:          map[string]string{harness.project: databaseURL},
		TenantListenerLimit:       1,
		TenantListenerIdleTimeout: time.Hour,
	}, app)
	if err := harness.runtime.runtime.SyncManifest(manifest.Manifest{
		Project: harness.project,
		Functions: map[string]manifest.FunctionEntry{
			harness.pathA: {Kind: manifest.FunctionKindSync, Sync: &definitionA},
			harness.pathB: {Kind: manifest.FunctionKindSync, Sync: &definitionB},
		},
		Schema: schema,
	}); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	listenerKey := tenantListenerKey{project: harness.project, tenant: harness.project}
	harness.runtime.subscriptions.listeners.active[listenerKey] = &tenantListener{
		key: listenerKey, databaseURL: databaseURL, ready: ready, connected: true, cancel: func() {},
	}
	t.Cleanup(func() {
		manager := harness.runtime.subscriptions.listeners
		manager.mu.Lock()
		listener := manager.active[listenerKey]
		delete(manager.active, listenerKey)
		if listener != nil && listener.idle != nil {
			listener.idle.Stop()
		}
		manager.mu.Unlock()
		harness.runtime.tenantStores.Close()
	})

	harness.connection, harness.peer = newSyncReadyTestConnection(t, true)
	harness.connection.server = harness.runtime
	harness.connection.project = harness.project
	harness.connection.tenant = harness.project
	harness.runtime.addWSConn(harness.connection)
	t.Cleanup(func() { harness.runtime.removeWSConn(harness.connection) })
	return harness
}

func (h *syncFanoutHarness) open(t *testing.T, id, path string) {
	t.Helper()
	h.openWithClock(t, id, path, h.clock(t))
}

func (h *syncFanoutHarness) openWithClock(t *testing.T, id, path string, clock syncClock) {
	t.Helper()
	h.connection.openSyncWithClock(h.ctx, clientMessage{ID: id, Path: path}, h.runtime.databaseURLForTenant(h.project, h.project), clock)
}

func (h *syncFanoutHarness) clock(t *testing.T) syncClock {
	t.Helper()
	clock, err := currentSyncClock(h.ctx, h.runtime.databaseURLForTenant(h.project, h.project))
	if err != nil {
		t.Fatal(err)
	}
	return clock
}

func (h *syncFanoutHarness) readFrames(t *testing.T, count int) []serverMessage {
	t.Helper()
	return readSyncTestFrames(t, h.peer, count)
}

func (h *syncFanoutHarness) readReadyCoverage(t *testing.T, count int) map[string]bool {
	t.Helper()
	if err := h.peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	covered := map[string]bool{}
	for len(covered) < count {
		var frame serverMessage
		if err := h.peer.ReadJSON(&frame); err != nil {
			t.Fatal(err)
		}
		switch frame.Type {
		case "sync.ready":
			covered[frame.ID] = true
		case "sync.readyMany":
			for _, ready := range frame.Ready {
				covered[ready.ID] = true
			}
		case "sync.delta":
			// A reconnect replay may reconcile changed rows before its ready.
		default:
			payload, _ := json.Marshal(frame)
			t.Fatalf("unexpected reconnect replay frame: %s", payload)
		}
	}
	return covered
}
