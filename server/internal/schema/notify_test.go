package schema

import (
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestNotifySQLForTableUsesTableNameAndChannel(t *testing.T) {
	sql, err := NotifySQLForTable("messages", manifest.Table{Columns: map[string]manifest.Column{
		"id":   {Type: "id"},
		"body": {Type: "text"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"gonvex_notify_messages_insert",
		"gonvex_messages_notify_update",
		"AFTER DELETE ON \"messages\"",
		"pg_notify('gonvex_table_change'",
		"'table', 'messages'",
		"'operation', 'update'",
		"'changedColumns', CASE WHEN cardinality(changed_columns) <= 100",
		"FULL OUTER JOIN new_rows new_row USING (\"id\")",
		"jsonb_object_keys(",
		"LIMIT 101",
		"CASE WHEN row_count < 500 THEN ids ELSE ARRAY[]::text[] END",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected notify SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLForTableWithoutIDUsesBroadInvalidation(t *testing.T) {
	sql, err := NotifySQLForTable("events", manifest.Table{Columns: map[string]manifest.Column{
		"name": {Type: "text"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "SELECT id FROM") {
		t.Fatalf("table without id should not read ids:\n%s", sql)
	}
	if !strings.Contains(sql, "'broad', true") {
		t.Fatalf("table without id should use broad invalidation:\n%s", sql)
	}
}

func TestCreateTableSQLRejectsInvalidTableName(t *testing.T) {
	_, err := createTableSQL("bad-name", manifest.Table{Columns: map[string]manifest.Column{
		"id": {Type: "id", PrimaryKey: true},
	}})
	if err == nil {
		t.Fatal("expected invalid table name error")
	}
}

func TestColumnDefinitionCanDeferNotNullForExistingRows(t *testing.T) {
	column := manifest.Column{Type: "text"}

	enforced, err := columnDefinition("title", column, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(enforced, "NOT NULL") {
		t.Fatalf("expected enforced column to contain NOT NULL: %s", enforced)
	}

	deferred, err := columnDefinition("title", column, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(deferred, "NOT NULL") {
		t.Fatalf("expected deferred column to omit NOT NULL: %s", deferred)
	}
}

func TestTrigramIndexSQLUsesGinTrigramOps(t *testing.T) {
	sql := trigramIndexSQL("tasks_search_text_trgm", "tasks", []string{"name", "title", "description"})

	for _, want := range []string{
		`CREATE INDEX IF NOT EXISTS "tasks_search_text_trgm" ON "tasks" USING gin`,
		`COALESCE("name"::text, '')`,
		`COALESCE("title"::text, '')`,
		`COALESCE("description"::text, '')`,
		`gin_trgm_ops`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected trigram SQL to contain %q:\n%s", want, sql)
		}
	}
}
