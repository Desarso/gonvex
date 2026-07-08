package datafiles

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestIngestLargeCSVIfPresent is an opportunistic smoke test against a real
// exported dataset (set GONVEX_LARGE_CSV or keep the default dev path). It
// skips when the file is absent so CI stays hermetic.
func TestIngestLargeCSVIfPresent(t *testing.T) {
	path := os.Getenv("GONVEX_LARGE_CSV")
	if path == "" {
		path = "/Users/whagons/Desktop/coding/whagons/whagons5-client/tmp/big_tasks_1m.csv"
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("large CSV not present at %s", path)
	}

	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()

	start := time.Now()
	meta, err := manager.Ingest(ctx, scope, "bigcsv", "big_tasks.csv", fetchFile(path, "big_tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ingest: %s in %s", meta.Summary(), time.Since(start))
	if len(meta.Tables) != 1 || meta.Tables[0].RowCount < 100_000 {
		t.Fatalf("unexpected tables: %+v", meta.Tables)
	}

	start = time.Now()
	_, rows, _, err := manager.Query(ctx, scope, meta.FileKey,
		`SELECT status, count(*) AS n, round(avg(hours_to_complete), 1) AS avg_hours FROM data GROUP BY status ORDER BY n DESC`, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("group-by query: %d rows in %s, first=%v", len(rows), time.Since(start), rows[0])
	if len(rows) == 0 {
		t.Fatal("no aggregation rows")
	}

	start = time.Now()
	_, profiles, err := manager.Profile(ctx, scope, meta.FileKey, "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("profile: %d tables in %s", len(profiles), time.Since(start))
}
