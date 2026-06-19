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
		NotifyChannel,
		"'table', 'messages'",
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
