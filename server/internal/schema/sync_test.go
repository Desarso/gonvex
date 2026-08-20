package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSyncTriggerProjectsOnlyDeclaredColumnsAndFinalizesAtCommit(t *testing.T) {
	table := manifest.Table{Columns: map[string]manifest.Column{
		"id":          {Type: "id", PrimaryKey: true},
		"workspaceId": {Type: "id"},
		"title":       {Type: "text"},
		"secret":      {Type: "text"},
		"updatedAt":   {Type: "number"},
	}}
	definition := manifest.ReplicaCollectionDefinition{
		Table:        "tasks",
		Key:          "id",
		Columns:      []string{"id", "workspaceId", "title"},
		EqualFilters: map[string]string{"workspaceId": "workspaceId"},
		OrderBy:      "updatedAt",
	}

	columns, err := syncColumns("tasks", table, definition)
	if err != nil {
		t.Fatal(err)
	}
	sql, err := syncTriggerSQL("tasks", definition.Key, columns)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		`jsonb_build_object('id', NEW."id"`,
		`'workspaceId', NEW."workspaceId"`,
		`'updatedAt', NEW."updatedAt"`,
		`CREATE CONSTRAINT TRIGGER "gonvex_sync_tasks_finalize_trigger"`,
		`DEFERRABLE INITIALLY DEFERRED`,
		`gonvex_sync_finalize_transaction()`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected sync SQL to contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, `'secret'`) {
		t.Fatalf("sync SQL must not capture undeclared columns:\n%s", sql)
	}
}

func TestSyncColumnsRejectMissingFilterColumn(t *testing.T) {
	_, err := syncColumns("tasks", manifest.Table{Columns: map[string]manifest.Column{
		"id": {Type: "id", PrimaryKey: true},
	}}, manifest.ReplicaCollectionDefinition{
		Table:        "tasks",
		Key:          "id",
		Columns:      []string{"id"},
		EqualFilters: map[string]string{"workspaceId": "workspaceId"},
	})
	if err == nil || !strings.Contains(err.Error(), "tasks.workspaceId") {
		t.Fatalf("expected missing filter column error, got %v", err)
	}
}

func TestSyncInfrastructureAssignsOneRevisionPerTransaction(t *testing.T) {
	for _, want := range []string{
		`UPDATE _gonvex_sync_clock`,
		`WHERE transaction_id = txid_current()::bigint AND revision IS NULL`,
		`row_number() OVER (ORDER BY event_id)`,
		`ALTER TABLE _gonvex_sync_changes RENAME COLUMN mutation_id TO command_id`,
		`ALTER TABLE _gonvex_sync_changes DROP COLUMN mutation_id`,
		`current_setting('gonvex.command_id', true)`,
		`pg_notify(`,
		`'gonvex_change_feed'`,
		`array_agg(DISTINCT table_name ORDER BY table_name)`,
		`'tables', changed_tables`,
		`octet_length(notify_payload::text) > 7000`,
	} {
		if !strings.Contains(syncInfrastructureSQL, want) {
			t.Fatalf("expected sync infrastructure SQL to contain %q", want)
		}
	}
}

func TestSyncTriggerNotificationIncludesAllTransactionTables(t *testing.T) {
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
	tableX := fmt.Sprintf("sync_payload_x_%d", suffix)
	tableY := fmt.Sprintf("sync_payload_y_%d", suffix)
	tables := map[string]manifest.Table{
		tableX: {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}},
		tableY: {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}},
	}
	definitions := map[string]manifest.ReplicaCollectionDefinition{
		tableX: {Table: tableX, Key: "id", Columns: []string{"id"}},
		tableY: {Table: tableY, Key: "id", Columns: []string{"id"}},
	}
	if _, err := ApplyWithSync(ctx, databaseURL, manifest.Schema{Tables: tables}, definitions); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableX))
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableY))
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent(syncArtifactName(tableX, "stage")) + `()`)
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent(syncArtifactName(tableY, "stage")) + `()`)
	})

	listener, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(context.Background()) })
	if _, err := listener.Exec(ctx, `LISTEN `+ChangeFeedNotifyChannel); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+quoteIdent(tableX)+` ("id") VALUES ('x-1'), ('x-2')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+quoteIdent(tableY)+` ("id") VALUES ('y-1')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		notification, err := listener.WaitForNotification(waitCtx)
		if err != nil {
			t.Fatalf("wait for sync notification: %v", err)
		}
		var payload struct {
			Tables []string `json:"tables"`
		}
		if json.Unmarshal([]byte(notification.Payload), &payload) != nil {
			continue
		}
		seen := map[string]bool{}
		for _, table := range payload.Tables {
			seen[table] = true
		}
		if seen[tableX] && seen[tableY] {
			if len(payload.Tables) != 2 {
				t.Fatalf("transaction notification tables = %#v, want only %q and %q", payload.Tables, tableX, tableY)
			}
			return
		}
	}
}
