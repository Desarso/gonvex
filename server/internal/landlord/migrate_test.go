package landlord

import (
	"context"
	"strings"
	"testing"
)

func TestApplyWithEmptyDatabaseURLIsNoop(t *testing.T) {
	result, err := Apply(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Applied) != 0 {
		t.Fatalf("expected no applied migrations, got %#v", result.Applied)
	}
}

func TestMigrationStatementsIncludeLandlordTables(t *testing.T) {
	joined := ""
	for _, statement := range migrationStatements() {
		joined += "\n" + statement.sql
	}
	for _, table := range []string{"users", "tenants", "memberships", "sessions"} {
		if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS "+table) {
			t.Fatalf("expected migration for %s in:\n%s", table, joined)
		}
	}
}
