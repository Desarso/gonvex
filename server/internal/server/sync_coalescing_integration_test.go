package server

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSyncRevisionReadyFanoutUsesNegotiatedReadyMany(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	for _, test := range []struct {
		name       string
		capability bool
	}{
		{name: "capability-on", capability: true},
		{name: "capability-off", capability: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			testSyncRevisionReadyFanout(t, databaseURL, test.capability)
		})
	}
}

func testSyncRevisionReadyFanout(
	t *testing.T,
	databaseURL string,
	capability bool,
) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	changedTable := fmt.Sprintf("wp3_changed_%d", suffix)
	unchangedTable := fmt.Sprintf("wp3_unchanged_%d", suffix)
	project := fmt.Sprintf("wp3-project-%d", suffix)
	const subscriptionCount = 5

	schema := manifest.Schema{Tables: map[string]manifest.Table{
		changedTable: {Columns: map[string]manifest.Column{
			"id": {Type: "id", PrimaryKey: true}, "value": {Type: "string"},
		}},
		unchangedTable: {Columns: map[string]manifest.Column{
			"id": {Type: "id", PrimaryKey: true}, "value": {Type: "string"},
		}},
	}}
	changedDefinition := manifest.SyncDefinition{Table: changedTable, Key: "id", Columns: []string{"id", "value"}, Mode: "eager"}
	unchangedDefinition := manifest.SyncDefinition{Table: unchangedTable, Key: "id", Columns: []string{"id", "value"}, Mode: "eager"}
	if _, err := schemasync.ApplyWithSync(ctx, databaseURL, schema, map[string]manifest.SyncDefinition{
		changedTable: changedDefinition, unchangedTable: unchangedDefinition,
	}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(changedTable))
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(unchangedTable))
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+changedTable+"_stage") + `()`)
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+unchangedTable+"_stage") + `()`)
		_ = db.Close()
	})

	app := gonvex.NewApp()
	emptySnapshot := func(*gonvex.QueryCtx, struct{}) ([]map[string]any, error) {
		return []map[string]any{}, nil
	}
	functions := make(map[string]manifest.FunctionEntry, subscriptionCount)
	paths := make([]string, 0, subscriptionCount)
	for index := 0; index < subscriptionCount; index++ {
		path := fmt.Sprintf("sync.wp3%d", index)
		definition := unchangedDefinition
		if index == 0 {
			definition = changedDefinition
		}
		app.Sync(path, emptySnapshot, gonvex.SyncTable(definition.Table).Columns("id", "value"))
		definitionCopy := definition
		functions[path] = manifest.FunctionEntry{Kind: manifest.FunctionKindSync, Sync: &definitionCopy}
		paths = append(paths, path)
	}
	runtime := NewWithApp(config.Config{
		ProjectDatabases:          map[string]string{project: databaseURL},
		TenantListenerLimit:       1,
		TenantListenerIdleTimeout: time.Hour,
	}, app)
	if err := runtime.runtime.SyncManifest(manifest.Manifest{Project: project, Functions: functions, Schema: schema}); err != nil {
		t.Fatal(err)
	}
	connection, peer := newSyncReadyTestConnection(t, capability)
	connection.server = runtime
	connection.project = project
	connection.tenant = project
	ready := make(chan struct{})
	close(ready)
	listenerKey := tenantListenerKey{project: project, tenant: project}
	runtime.subscriptions.listeners.active[listenerKey] = &tenantListener{
		key: listenerKey, databaseURL: databaseURL, ready: ready, connected: true, cancel: func() {},
	}
	t.Cleanup(func() {
		manager := runtime.subscriptions.listeners
		manager.mu.Lock()
		listener := manager.active[listenerKey]
		delete(manager.active, listenerKey)
		if listener != nil && listener.idle != nil {
			listener.idle.Stop()
		}
		manager.mu.Unlock()
		runtime.tenantStores.Close()
	})

	clock, err := currentSyncClock(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	for index, path := range paths {
		connection.openSyncWithClock(ctx, clientMessage{
			ID: fmt.Sprintf("sync-%d", index), Path: path,
		}, databaseURL, clock)
	}
	// Each open produces a snapshot and a ready. Draining all of them makes the
	// revision below the only source of frames in the fan-out assertion.
	readSyncTestFrames(t, peer, subscriptionCount*2)

	if _, err := db.Exec(`INSERT INTO ` + quoteIdent(changedTable) + ` ("id", "value") VALUES ('row-a', 'changed')`); err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	subscriptions := make([]*syncSubscription, 0, subscriptionCount)
	for index := 0; index < subscriptionCount; index++ {
		subscriptions = append(subscriptions, connection.syncs[fmt.Sprintf("sync-%d", index)])
	}
	connection.mu.Unlock()
	// Deliver the changed subscription first, matching the causal delta-before-
	// ready path; the remaining unchanged subscriptions then share its ready
	// flush window.
	for _, subscription := range subscriptions {
		if err := runtime.deliverSync(subscription); err != nil {
			t.Fatal(err)
		}
	}

	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	deltas := 0
	plainReadies := 0
	readyManyFrames := 0
	readyCoverage := map[string]bool{}
	deltaFrameIndex := -1
	changedReadyFrameIndex := -1
	for frameIndex := 0; deltas < 1 || len(readyCoverage) < subscriptionCount; frameIndex++ {
		var frame serverMessage
		if err := peer.ReadJSON(&frame); err != nil {
			t.Fatal(err)
		}
		switch frame.Type {
		case "sync.delta":
			deltas++
			if frame.ID == "sync-0" {
				deltaFrameIndex = frameIndex
			}
		case "sync.ready":
			plainReadies++
			readyCoverage[frame.ID] = true
			if frame.ID == "sync-0" {
				changedReadyFrameIndex = frameIndex
			}
		case "sync.readyMany":
			readyManyFrames++
			for _, ready := range frame.Ready {
				readyCoverage[ready.ID] = true
				if ready.ID == "sync-0" {
					changedReadyFrameIndex = frameIndex
				}
			}
		default:
			t.Fatalf("unexpected post-commit frame: %#v", frame)
		}
	}
	if deltas != 1 || len(readyCoverage) != subscriptionCount {
		t.Fatalf(
			"post-commit frames: deltas=%d ready=%d readyMany=%d coverage=%d; want one delta and %d covered subscriptions",
			deltas, plainReadies, readyManyFrames, len(readyCoverage), subscriptionCount,
		)
	}
	if deltaFrameIndex < 0 || changedReadyFrameIndex <= deltaFrameIndex {
		t.Fatalf("changed subscription ordering: delta frame=%d ready frame=%d", deltaFrameIndex, changedReadyFrameIndex)
	}
	if !capability && readyManyFrames != 0 {
		t.Fatalf("legacy client received %d sync.readyMany frames", readyManyFrames)
	}
}
