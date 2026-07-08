// Package datafiles ingests uploaded CSV/XLSX/XLS attachments into per-file
// DuckDB artifacts and answers inspect/query/profile requests against them.
//
// One uploaded data file becomes one DuckDB database on local disk under
// <Root>/<project>/<tenant>/<fileKey>.duckdb with a JSON sidecar describing
// its tables. Artifacts are disposable: when one is missing (fresh deploy,
// cleaned temp dir) it is re-ingested from the original stored upload via the
// fetch callback, so no runtime Postgres state is needed.
package datafiles

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

const (
	// DefaultRowLimit bounds rows returned directly to the model.
	DefaultRowLimit = 100
	// MaxRowLimit is the hard cap on rows returned by Query/Inspect.
	MaxRowLimit = 500
	// MaxColumns caps columns per ingested table.
	MaxColumns = 200
	// MaxSheetRows caps rows ingested per workbook sheet (CSV is uncapped —
	// DuckDB streams it natively).
	MaxSheetRows = 2_000_000
)

type Scope struct {
	ProjectID string
	TenantID  string
}

// FetchFunc re-opens the original uploaded bytes for (re-)ingestion. It
// returns the content reader and the original filename (for format detection).
type FetchFunc func(ctx context.Context) (io.ReadCloser, string, error)

type TableMeta struct {
	TableName   string   `json:"tableName"`
	RowCount    int64    `json:"rowCount"`
	Columns     []string `json:"columns"`
	ColumnTypes []string `json:"columnTypes,omitempty"`
}

// Meta is the sidecar record written next to every artifact.
type Meta struct {
	FileKey    string      `json:"fileKey"`
	FileID     string      `json:"fileId"`
	Filename   string      `json:"filename"`
	Format     string      `json:"format"`
	Tables     []TableMeta `json:"tables"`
	Warnings   []string    `json:"warnings,omitempty"`
	IngestedAt time.Time   `json:"ingestedAt"`
}

type Manager struct {
	Root string
}

func NewManager(root string) *Manager {
	root = strings.TrimSpace(root)
	if root == "" {
		root = filepath.Join(os.TempDir(), "gonvex-data")
	}
	return &Manager{Root: root}
}

var fileIDSanitizer = regexp.MustCompile(`[^A-Za-z0-9_-]`)

// FileKeyFor derives the stable artifact key for an upload. The format tag is
// embedded so a missing artifact can be re-ingested without extra metadata.
func FileKeyFor(fileID, filename string) (string, error) {
	format, err := detectFormat(filename)
	if err != nil {
		return "", err
	}
	sanitized := fileIDSanitizer.ReplaceAllString(strings.TrimSpace(fileID), "_")
	if sanitized == "" {
		return "", fmt.Errorf("data file id is required")
	}
	return "data_" + format + "_" + sanitized, nil
}

// fileIDFromKey reverses FileKeyFor for artifact re-ingestion. Sanitized ids
// round-trip only when the original id was already storage-safe, which gonvex
// storage ids are.
func fileIDFromKey(fileKey string) (fileID, format string, err error) {
	rest, ok := strings.CutPrefix(fileKey, "data_")
	if !ok {
		return "", "", fmt.Errorf("unknown data file key %q", fileKey)
	}
	format, fileID, ok = strings.Cut(rest, "_")
	if !ok || fileID == "" {
		return "", "", fmt.Errorf("unknown data file key %q", fileKey)
	}
	switch format {
	case "csv", "xlsx", "xls":
		return fileID, format, nil
	}
	return "", "", fmt.Errorf("unknown data file key %q", fileKey)
}

// FileIDFromKey extracts the storage file id embedded in a data file key.
func FileIDFromKey(fileKey string) (string, bool) {
	fileID, _, err := fileIDFromKey(fileKey)
	return fileID, err == nil
}

func detectFormat(filename string) (string, error) {
	switch strings.ToLower(strings.TrimPrefix(filepath.Ext(strings.TrimSpace(filename)), ".")) {
	case "csv", "tsv", "txt":
		return "csv", nil
	case "xlsx", "xlsm":
		return "xlsx", nil
	case "xls":
		return "xls", nil
	}
	return "", fmt.Errorf("unsupported data file type %q (supported: .csv, .tsv, .xlsx, .xls)", filepath.Ext(filename))
}

func (m *Manager) scopeDir(scope Scope) string {
	project := fileIDSanitizer.ReplaceAllString(scope.ProjectID, "_")
	tenant := fileIDSanitizer.ReplaceAllString(scope.TenantID, "_")
	if project == "" {
		project = "_default"
	}
	if tenant == "" {
		tenant = "_default"
	}
	return filepath.Join(m.Root, project, tenant)
}

func (m *Manager) artifactPath(scope Scope, fileKey string) string {
	return filepath.Join(m.scopeDir(scope), fileKey+".duckdb")
}

func (m *Manager) metaPath(scope Scope, fileKey string) string {
	return filepath.Join(m.scopeDir(scope), fileKey+".json")
}

func (m *Manager) readMeta(scope Scope, fileKey string) (*Meta, error) {
	raw, err := os.ReadFile(m.metaPath(scope, fileKey))
	if err != nil {
		return nil, err
	}
	var meta Meta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (m *Manager) writeMeta(scope Scope, meta *Meta) error {
	raw, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(m.metaPath(scope, meta.FileKey), raw, 0o644)
}

// Ingest builds the DuckDB artifact for an upload (idempotent per fileKey)
// and returns its metadata.
func (m *Manager) Ingest(ctx context.Context, scope Scope, fileID, filename string, fetch FetchFunc) (*Meta, error) {
	fileKey, err := FileKeyFor(fileID, filename)
	if err != nil {
		return nil, err
	}
	if meta, err := m.readMeta(scope, fileKey); err == nil {
		if _, statErr := os.Stat(m.artifactPath(scope, fileKey)); statErr == nil {
			return meta, nil
		}
	}
	return m.ingest(ctx, scope, fileKey, fileID, filename, fetch)
}

// Ensure returns the artifact path for a fileKey, re-ingesting from the
// original upload when the artifact is missing.
func (m *Manager) Ensure(ctx context.Context, scope Scope, fileKey string, fetch FetchFunc) (string, *Meta, error) {
	path := m.artifactPath(scope, fileKey)
	if meta, err := m.readMeta(scope, fileKey); err == nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return path, meta, nil
		}
	}
	if fetch == nil {
		return "", nil, fmt.Errorf("data file %s is not ingested and no source is available", fileKey)
	}
	fileID, _, err := fileIDFromKey(fileKey)
	if err != nil {
		return "", nil, err
	}
	meta, err := m.ingest(ctx, scope, fileKey, fileID, "", fetch)
	if err != nil {
		return "", nil, err
	}
	return path, meta, nil
}

func (m *Manager) ingest(ctx context.Context, scope Scope, fileKey, fileID, filename string, fetch FetchFunc) (*Meta, error) {
	if fetch == nil {
		return nil, fmt.Errorf("data ingestion requires a source fetcher")
	}
	source, fetchedName, err := fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch uploaded file: %w", err)
	}
	defer source.Close()
	if strings.TrimSpace(filename) == "" {
		filename = fetchedName
	}
	if strings.TrimSpace(filename) == "" {
		filename = "upload." + strings.SplitN(strings.TrimPrefix(fileKey, "data_"), "_", 2)[0]
	}
	format, err := detectFormat(filename)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(m.scopeDir(scope), 0o755); err != nil {
		return nil, err
	}

	// Spool the upload to disk: DuckDB and the workbook readers need a real file.
	spool, err := os.CreateTemp(m.scopeDir(scope), "ingest-*."+format)
	if err != nil {
		return nil, err
	}
	defer os.Remove(spool.Name())
	if _, err := io.Copy(spool, source); err != nil {
		spool.Close()
		return nil, fmt.Errorf("spool uploaded file: %w", err)
	}
	if err := spool.Close(); err != nil {
		return nil, err
	}

	// Build into a temp artifact, then rename into place so concurrent readers
	// never see a half-built database.
	tempArtifact := m.artifactPath(scope, fileKey) + ".building"
	os.Remove(tempArtifact)
	defer os.Remove(tempArtifact)

	db, err := openDuckDB(tempArtifact, false)
	if err != nil {
		return nil, err
	}
	var warnings []string
	switch format {
	case "csv":
		warnings, err = ingestCSV(ctx, db, spool.Name())
	case "xlsx":
		warnings, err = ingestXLSX(ctx, db, spool.Name())
	case "xls":
		warnings, err = ingestXLS(ctx, db, spool.Name())
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	tables, err := describeTables(ctx, db)
	closeErr := db.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tabular data found in %s", filename)
	}
	if err := os.Rename(tempArtifact, m.artifactPath(scope, fileKey)); err != nil {
		return nil, err
	}

	meta := &Meta{
		FileKey:    fileKey,
		FileID:     fileID,
		Filename:   filename,
		Format:     format,
		Tables:     tables,
		Warnings:   warnings,
		IngestedAt: time.Now().UTC(),
	}
	if err := m.writeMeta(scope, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Summary renders the one-line ingest summary shown in chat and stored on the
// app's data-file row.
func (meta *Meta) Summary() string {
	if meta == nil {
		return ""
	}
	parts := make([]string, 0, len(meta.Tables))
	var rows int64
	for _, table := range meta.Tables {
		rows += table.RowCount
		parts = append(parts, fmt.Sprintf("%s (%s rows, %d columns)", table.TableName, formatCount(table.RowCount), len(table.Columns)))
	}
	label := "table"
	if len(meta.Tables) != 1 {
		label = "tables"
	}
	return fmt.Sprintf("Ingested %s into DuckDB: %d %s, %s rows total — %s.",
		meta.Filename, len(meta.Tables), label, formatCount(rows), strings.Join(parts, "; "))
}

func formatCount(n int64) string {
	raw := fmt.Sprintf("%d", n)
	if len(raw) <= 3 {
		return raw
	}
	var b strings.Builder
	lead := len(raw) % 3
	if lead > 0 {
		b.WriteString(raw[:lead])
	}
	for i := lead; i < len(raw); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(raw[i : i+3])
	}
	return b.String()
}

// openDuckDB opens an artifact. Read-only opens also disable external access
// so model-authored SQL cannot touch the local filesystem or network.
func openDuckDB(path string, readOnly bool) (*sql.DB, error) {
	dsn := path
	if readOnly {
		dsn += "?access_mode=read_only&enable_external_access=false"
	}
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func describeTables(ctx context.Context, db *sql.DB) ([]TableMeta, error) {
	rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	tables := make([]TableMeta, 0, len(names))
	for _, name := range names {
		table := TableMeta{TableName: name}
		colRows, err := db.QueryContext(ctx, `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position`, name)
		if err != nil {
			return nil, err
		}
		for colRows.Next() {
			var colName, colType string
			if err := colRows.Scan(&colName, &colType); err != nil {
				colRows.Close()
				return nil, err
			}
			table.Columns = append(table.Columns, colName)
			table.ColumnTypes = append(table.ColumnTypes, colType)
		}
		colRows.Close()
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+quoteIdent(name)).Scan(&table.RowCount); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
