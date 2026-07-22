package schema

import (
	"context"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestRememberExistingColumnPreservesPrimaryKeyAcrossConstraintRows(t *testing.T) {
	for _, rows := range [][]existingColumn{
		{{Type: "id", PrimaryKey: true}, {Type: "id", PrimaryKey: false}},
		{{Type: "id", PrimaryKey: false}, {Type: "id", PrimaryKey: true}},
	} {
		columns := map[string]existingColumn{}
		for _, row := range rows {
			rememberExistingColumn(columns, "id", row)
		}
		if !columns["id"].PrimaryKey {
			t.Fatal("primary-key metadata was lost when another key constraint produced a duplicate row")
		}
	}
}

func TestNeedsIndexUniquenessRebuild(t *testing.T) {
	tests := []struct {
		name                           string
		exists, current, desired, want bool
	}{
		{name: "missing", exists: false, current: false, desired: true, want: false},
		{name: "matching ordinary", exists: true, current: false, desired: false, want: false},
		{name: "matching unique", exists: true, current: true, desired: true, want: false},
		{name: "strengthen", exists: true, current: false, desired: true, want: true},
		{name: "weaken", exists: true, current: true, desired: false, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := needsIndexUniquenessRebuild(test.exists, test.current, test.desired); got != test.want {
				t.Fatalf("needsIndexUniquenessRebuild() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestBtreeIndexSQLIncludesDeclaredUniqueness(t *testing.T) {
	columns := []string{quoteIdent("customer_id")}
	if got := btreeIndexSQL("accounts_by_customer", "accounts", columns, true); got != `CREATE UNIQUE INDEX IF NOT EXISTS "accounts_by_customer" ON "accounts" ("customer_id")` {
		t.Fatalf("unexpected unique index SQL: %s", got)
	}
	if got := btreeIndexSQL("accounts_by_customer", "accounts", columns, false); got != `CREATE INDEX IF NOT EXISTS "accounts_by_customer" ON "accounts" ("customer_id")` {
		t.Fatalf("unexpected ordinary index SQL: %s", got)
	}
}

func TestCreateIndexesFromExistingSkipsMatchingIndexWithoutDatabaseCall(t *testing.T) {
	table := manifest.Table{Indexes: map[string]manifest.Index{
		"by_customer": {Columns: []string{"customer_id"}},
	}}
	applied, err := createIndexesFromExisting(
		context.Background(),
		nil,
		"accounts",
		table,
		map[string]bool{"accounts_by_customer": false},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Fatalf("matching index caused database work: %#v", applied)
	}
}
