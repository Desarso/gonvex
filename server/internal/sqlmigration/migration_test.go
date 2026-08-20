package sqlmigration

import (
	"strings"
	"testing"
)

func TestParseOrdersAndScopesMigrations(t *testing.T) {
	migrations, err := Parse(map[string][]byte{
		"0002_tenant.sql":        []byte("-- gonvex:scope tenant\nSELECT 2;"),
		"0001_control_plane.sql": []byte("-- gonvex:scope control-plane\n-- gonvex:no-transaction\nSELECT 1;"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if migrations[0].Name != "0001_control_plane.sql" || migrations[1].Name != "0002_tenant.sql" {
		t.Fatalf("not lexically ordered: %#v", migrations)
	}
	if migrations[0].Scope != ScopeControlPlane || !migrations[0].NoTransaction {
		t.Fatalf("directives not parsed: %#v", migrations[0])
	}
	if got := Filter(migrations, ScopeControlPlane); len(got) != 1 || got[0].Name != "0001_control_plane.sql" {
		t.Fatalf("Control Plane filtering failed: %#v", got)
	}
	if got := Filter(migrations, ScopeTenant); len(got) != 1 || got[0].Name != "0002_tenant.sql" {
		t.Fatalf("tenant filtering failed: %#v", got)
	}
}

func TestParseNormalizesLegacyControlPlaneScope(t *testing.T) {
	migrations, err := Parse(map[string][]byte{
		"0001_legacy_scope.sql": []byte("-- gonvex:scope landlord\nSELECT 1;"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 1 || migrations[0].Scope != ScopeControlPlane {
		t.Fatalf("legacy scope was not normalized: %#v", migrations)
	}
}

func TestParseRequiresExplicitScope(t *testing.T) {
	_, err := Parse(map[string][]byte{"0001_missing.sql": []byte("SELECT 1;")})
	if err == nil || !strings.Contains(err.Error(), "missing required") {
		t.Fatalf("expected explicit-scope error, got %v", err)
	}
}

func TestSplitStatementsPreservesPostgresBodies(t *testing.T) {
	source := "-- gonvex:scope tenant\nCREATE FUNCTION x() RETURNS void AS $$ BEGIN PERFORM 1; END $$ LANGUAGE plpgsql; SELECT ';';"
	statements, err := splitStatements(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 2 {
		t.Fatalf("got %d statements: %#v", len(statements), statements)
	}
}
