package datafiles

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func fetchString(content, filename string) FetchFunc {
	return func(context.Context) (io.ReadCloser, string, error) {
		return io.NopCloser(strings.NewReader(content)), filename, nil
	}
}

func fetchFile(path, filename string) FetchFunc {
	return func(context.Context) (io.ReadCloser, string, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, "", err
		}
		return file, filename, nil
	}
}

const sampleCSV = `task_id,name,status,hours
1,Clean lobby,open,1.5
2,Fix pump,done,4
3,Inspect pool,open,2.25
4,Restock towels,done,0.5
`

func TestIngestCSVAndQuery(t *testing.T) {
	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()

	meta, err := manager.Ingest(ctx, scope, "file123", "tasks.csv", fetchString(sampleCSV, "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if meta.FileKey != "data_csv_file123" {
		t.Fatalf("fileKey = %q", meta.FileKey)
	}
	if len(meta.Tables) != 1 || meta.Tables[0].RowCount != 4 {
		t.Fatalf("tables = %+v", meta.Tables)
	}
	if !strings.Contains(meta.Summary(), "4 rows") {
		t.Fatalf("summary = %q", meta.Summary())
	}

	columns, rows, truncated, err := manager.Query(ctx, scope, meta.FileKey,
		`SELECT status, count(*) AS n, sum(hours) AS total FROM data GROUP BY status ORDER BY status`, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if truncated || len(rows) != 2 || len(columns) != 3 {
		t.Fatalf("rows=%v truncated=%v columns=%v", rows, truncated, columns)
	}
	if rows[0]["status"] != "done" {
		t.Fatalf("rows[0] = %v", rows[0])
	}

	// Idempotent re-ingest returns the same artifact.
	again, err := manager.Ingest(ctx, scope, "file123", "tasks.csv", fetchString(sampleCSV, "tasks.csv"))
	if err != nil || again.FileKey != meta.FileKey {
		t.Fatalf("re-ingest: %v %v", again, err)
	}
}

func TestQueryLimitAndTruncation(t *testing.T) {
	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()
	meta, err := manager.Ingest(ctx, scope, "f2", "tasks.csv", fetchString(sampleCSV, "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	_, rows, truncated, err := manager.Query(ctx, scope, meta.FileKey, `SELECT * FROM data`, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || !truncated {
		t.Fatalf("rows=%d truncated=%v", len(rows), truncated)
	}
}

func TestValidateSelectRejectsWrites(t *testing.T) {
	bad := []string{
		`DROP TABLE data`,
		`INSERT INTO data VALUES (1)`,
		`SELECT 1; DROP TABLE data`,
		`UPDATE data SET name='x'`,
		`SELECT * FROM read_csv_auto('/etc/passwd')`,
		`COPY data TO '/tmp/out.csv'`,
		`ATTACH '/tmp/other.db'`,
		`PRAGMA database_list`,
		`CREATE TABLE evil AS SELECT 1`,
	}
	for _, query := range bad {
		if err := ValidateSelect(query); err == nil {
			t.Fatalf("ValidateSelect(%q) should fail", query)
		}
	}
	good := []string{
		`SELECT * FROM data LIMIT 5`,
		`  select status, count(*) from data group by 1`,
		`WITH open AS (SELECT * FROM data WHERE status='open') SELECT count(*) FROM open;`,
		"-- comment\nSELECT 1",
	}
	for _, query := range good {
		if err := ValidateSelect(query); err != nil {
			t.Fatalf("ValidateSelect(%q) = %v", query, err)
		}
	}
}

func TestReadOnlyArtifactBlocksWrites(t *testing.T) {
	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()
	meta, err := manager.Ingest(ctx, scope, "f3", "tasks.csv", fetchString(sampleCSV, "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	path := manager.artifactPath(scope, meta.FileKey)
	db, err := openDuckDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TABLE evil (x INT)`); err == nil {
		t.Fatal("read-only artifact accepted a write")
	}
	if _, err := db.QueryContext(ctx, `SELECT * FROM read_text('/etc/hosts')`); err == nil {
		t.Fatal("read-only artifact allowed external file access")
	}
}

func TestIngestXLSXMultiSheet(t *testing.T) {
	workbook := excelize.NewFile()
	sheet1 := workbook.GetSheetName(0)
	_ = workbook.SetSheetName(sheet1, "Tasks")
	_ = workbook.SetSheetRow("Tasks", "A1", &[]any{"name", "status"})
	_ = workbook.SetSheetRow("Tasks", "A2", &[]any{"Clean lobby", "open"})
	_ = workbook.SetSheetRow("Tasks", "A3", &[]any{"Fix pump", "done"})
	if _, err := workbook.NewSheet("Spots"); err != nil {
		t.Fatal(err)
	}
	_ = workbook.SetSheetRow("Spots", "A1", &[]any{"spot", "floor"})
	_ = workbook.SetSheetRow("Spots", "A2", &[]any{"Pool", "1"})

	path := t.TempDir() + "/book.xlsx"
	if err := workbook.SaveAs(path); err != nil {
		t.Fatal(err)
	}

	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()
	meta, err := manager.Ingest(ctx, scope, "book1", "book.xlsx", fetchFile(path, "book.xlsx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Tables) != 2 {
		t.Fatalf("tables = %+v", meta.Tables)
	}
	byName := map[string]TableMeta{}
	for _, table := range meta.Tables {
		byName[table.TableName] = table
	}
	if byName["Tasks"].RowCount != 2 || byName["Spots"].RowCount != 1 {
		t.Fatalf("row counts wrong: %+v", meta.Tables)
	}

	_, rows, _, err := manager.Query(ctx, scope, meta.FileKey, `SELECT count(*) AS n FROM Tasks WHERE status='open'`, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %v", rows)
	}
}

func TestProfile(t *testing.T) {
	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()
	meta, err := manager.Ingest(ctx, scope, "f4", "tasks.csv", fetchString(sampleCSV, "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	tables, profiles, err := manager.Profile(ctx, scope, meta.FileKey, "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || len(profiles) != 1 {
		t.Fatalf("tables=%d profiles=%d", len(tables), len(profiles))
	}
	columns, _ := profiles[0]["columns"].([]map[string]any)
	if len(columns) != 4 {
		t.Fatalf("profiled columns = %v", profiles[0]["columns"])
	}
	var statusProfile map[string]any
	for _, column := range columns {
		if column["name"] == "status" {
			statusProfile = column
		}
	}
	if statusProfile == nil {
		t.Fatalf("no status profile: %v", columns)
	}
	examples, _ := statusProfile["examples"].([]string)
	if len(examples) != 2 {
		t.Fatalf("status examples = %v", statusProfile["examples"])
	}
}

func TestEnsureReingestsMissingArtifact(t *testing.T) {
	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()
	meta, err := manager.Ingest(ctx, scope, "f5", "tasks.csv", fetchString(sampleCSV, "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(manager.artifactPath(scope, meta.FileKey)); err != nil {
		t.Fatal(err)
	}
	_, rows, _, err := manager.Query(ctx, scope, meta.FileKey, `SELECT count(*) AS n FROM data`, 5, fetchString(sampleCSV, "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %v", rows)
	}
}

func TestUnsupportedExtension(t *testing.T) {
	if _, err := FileKeyFor("id", "notes.pdf"); err == nil {
		t.Fatal("pdf should be rejected")
	}
}

// Regression: DuckDB's parallel scanner rejects null_padding when quoted
// fields contain newlines. Small files scan single-threaded and never hit it,
// so build a file large enough to trigger the parallel path.
func TestIngestCSVQuotedNewlines(t *testing.T) {
	// ~45MB: the parallel scanner only engages on large files, and only then
	// rejects the null_padding + quoted-newline combination.
	var b strings.Builder
	b.WriteString("task_id,name,description,status\n")
	filler := strings.Repeat("padding ", 15)
	for i := 0; i < 300000; i++ {
		fmt.Fprintf(&b, "%d,Task %d,\"line one\nline two for row %d\n%s\",open\n", i, i, i, filler)
	}

	manager := NewManager(t.TempDir())
	scope := Scope{ProjectID: "p", TenantID: "t"}
	ctx := context.Background()

	meta, err := manager.Ingest(ctx, scope, "multiline1", "tasks.csv", fetchString(b.String(), "tasks.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Tables) != 1 || meta.Tables[0].RowCount != 300000 {
		t.Fatalf("tables = %+v", meta.Tables)
	}

	_, rows, _, err := manager.Query(ctx, scope, meta.FileKey,
		`SELECT count(*) AS n FROM data WHERE description LIKE '%line two%'`, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
}
