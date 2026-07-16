package server

import (
	"database/sql"
	"testing"
)

func TestTenantStoreDatabaseStatsAggregatesOnlyRequestedProject(t *testing.T) {
	first, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	first.SetMaxOpenConns(40)

	second, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.SetMaxOpenConns(60)

	other, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	other.SetMaxOpenConns(900)

	resolver := &tenantStoreResolver{stores: map[string]*tenantStore{
		"project-a:tenant-a": {DB: first},
		"project-a:tenant-b": {DB: second},
		"project-b:tenant-a": {DB: other},
	}}
	stats := resolver.DatabaseStats("project-a")
	if stats.Pools != 2 || stats.MaxOpenConnections != 100 {
		t.Fatalf("DatabaseStats(project-a) = %+v", stats)
	}
}
