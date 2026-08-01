//go:build integration

package server

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gonvex/gonvex/pkg/manifest"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
)

func TestOfflineVisibilityChangeCannotResumeReadyWithoutReconciliation(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	suffix := time.Now().UnixNano()
	tasksTable := fmt.Sprintf("sync_tasks_%d", suffix)
	readsTable := fmt.Sprintf("sync_reads_%d", suffix)
	schema := manifest.Schema{Tables: map[string]manifest.Table{
		tasksTable: {Columns: map[string]manifest.Column{
			"id":    {Type: "id", PrimaryKey: true},
			"title": {Type: "string"},
		}},
		readsTable: {Columns: map[string]manifest.Column{
			"id":     {Type: "id", PrimaryKey: true},
			"taskId": {Type: "id"},
		}},
	}}
	definitions, err := syncDefinitionsForSchema(map[string]manifest.SyncDefinition{
		tasksTable: {
			Table:            tasksTable,
			Key:              "id",
			Columns:          []string{"id", "title"},
			VisibilityTables: []string{readsTable},
		},
	}, schema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := schemasync.ApplyWithSync(ctx, databaseURL, schema, definitions); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tasksTable))
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(readsTable))
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+tasksTable+"_stage") + `()`)
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_sync_"+readsTable+"_stage") + `()`)
	})

	if _, err := db.Exec(`INSERT INTO ` + quoteIdent(tasksTable) + ` ("id", "title") VALUES ('task-a', 'before')`); err != nil {
		t.Fatal(err)
	}
	before, err := currentSyncClock(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ` + quoteIdent(readsTable) + ` ("id", "taskId") VALUES ('read-a', 'task-a')`); err != nil {
		t.Fatal(err)
	}
	after, err := currentSyncClock(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision <= before.Revision {
		t.Fatalf("dependency write did not advance the durable cursor: before=%d after=%d", before.Revision, after.Revision)
	}

	sourceOnly, err := readSyncChanges(ctx, databaseURL, before.Revision, after.Revision, tasksTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceOnly) != 0 {
		t.Fatalf("test requires an offline dependency-only revision, got %#v", sourceOnly)
	}
	allRelevant, err := readSyncChanges(ctx, databaseURL, before.Revision, after.Revision, tasksTable, readsTable)
	if err != nil {
		t.Fatal(err)
	}
	if !syncVisibilityChanged(allRelevant, tasksTable) {
		t.Fatal("dependency-only reconnect must reconcile the authoritative computed collection")
	}
}
