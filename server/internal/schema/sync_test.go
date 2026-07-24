package schema

import (
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestSyncTriggerProjectsOnlyDeclaredColumnsAndFinalizesAtCommit(t *testing.T) {
	table := manifest.Table{Columns: map[string]manifest.Column{
		"id":          {Type: "id", PrimaryKey: true},
		"workspaceId": {Type: "id"},
		"title":       {Type: "text"},
		"secret":      {Type: "text"},
		"updatedAt":   {Type: "number"},
	}}
	definition := manifest.SyncDefinition{
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
	}}, manifest.SyncDefinition{
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
		`current_setting('gonvex.mutation_id', true)`,
		`pg_notify(`,
		`'gonvex_sync_change'`,
	} {
		if !strings.Contains(syncInfrastructureSQL, want) {
			t.Fatalf("expected sync infrastructure SQL to contain %q", want)
		}
	}
}
