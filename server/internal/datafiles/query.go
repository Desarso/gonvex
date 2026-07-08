package datafiles

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// query.go — read-only access to ingested artifacts: bounded SELECT queries,
// SUMMARIZE-based profiling, and schema/sample inspection.

var (
	lineCommentRe  = regexp.MustCompile(`--[^\n]*`)
	blockCommentRe = regexp.MustCompile(`(?s)/\*.*?\*/`)
	// Statements and functions that reach outside the artifact. The artifact is
	// also opened read-only with external access disabled, so this blocklist is
	// defense-in-depth, not the primary guard.
	blockedSQLRe = regexp.MustCompile(`(?i)\b(attach|detach|copy|export|import|install|load|pragma|create|insert|update|delete|drop|alter|truncate|vacuum|checkpoint|read_csv|read_csv_auto|read_text|read_blob|read_json|read_json_auto|read_parquet|read_xlsx|glob|getenv)\b`)
)

// ValidateSelect enforces the SELECT-only contract for model-authored SQL.
func ValidateSelect(query string) error {
	stripped := blockCommentRe.ReplaceAllString(query, " ")
	stripped = lineCommentRe.ReplaceAllString(stripped, " ")
	stripped = strings.TrimSpace(stripped)
	stripped = strings.TrimSuffix(stripped, ";")
	if strings.Contains(stripped, ";") {
		return fmt.Errorf("data queries must be a single SELECT statement")
	}
	lower := strings.ToLower(strings.TrimSpace(stripped))
	if !strings.HasPrefix(lower, "select") && !strings.HasPrefix(lower, "with") {
		return fmt.Errorf("data queries must be read-only SELECT statements")
	}
	if match := blockedSQLRe.FindString(stripped); match != "" {
		return fmt.Errorf("data queries may not use %q", strings.ToLower(match))
	}
	return nil
}

func clampLimit(limit, fallback int) int {
	if limit <= 0 {
		return fallback
	}
	if limit > MaxRowLimit {
		return MaxRowLimit
	}
	return limit
}

// Query runs a validated SELECT against the artifact and returns up to limit
// rows plus a truncation flag.
func (m *Manager) Query(ctx context.Context, scope Scope, fileKey, query string, limit int, fetch FetchFunc) (columns []string, rows []map[string]any, truncated bool, err error) {
	if err := ValidateSelect(query); err != nil {
		return nil, nil, false, err
	}
	limit = clampLimit(limit, DefaultRowLimit)
	path, _, err := m.Ensure(ctx, scope, fileKey, fetch)
	if err != nil {
		return nil, nil, false, err
	}
	db, err := openDuckDB(path, true)
	if err != nil {
		return nil, nil, false, err
	}
	defer db.Close()

	result, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, false, err
	}
	defer result.Close()
	columns, err = result.Columns()
	if err != nil {
		return nil, nil, false, err
	}
	for result.Next() {
		if len(rows) >= limit {
			truncated = true
			break
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := result.Scan(pointers...); err != nil {
			return nil, nil, false, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = normalizeValue(values[i])
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, false, err
	}
	return columns, rows, truncated, nil
}

// Profile computes per-column statistics with DuckDB's SUMMARIZE, adding
// example values for low-cardinality text columns.
func (m *Manager) Profile(ctx context.Context, scope Scope, fileKey, tableName string, maxColumns int, fetch FetchFunc) ([]TableMeta, []map[string]any, error) {
	path, meta, err := m.Ensure(ctx, scope, fileKey, fetch)
	if err != nil {
		return nil, nil, err
	}
	db, err := openDuckDB(path, true)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	if maxColumns <= 0 || maxColumns > MaxColumns {
		maxColumns = MaxColumns
	}

	var tables []TableMeta
	for _, table := range meta.Tables {
		if tableName != "" && !strings.EqualFold(table.TableName, tableName) {
			continue
		}
		tables = append(tables, table)
	}
	if len(tables) == 0 {
		return nil, nil, fmt.Errorf("table %q not found in %s", tableName, fileKey)
	}

	profiles := make([]map[string]any, 0, len(tables))
	for _, table := range tables {
		summary, err := summarizeTable(ctx, db, table, maxColumns)
		if err != nil {
			return nil, nil, err
		}
		profiles = append(profiles, summary)
	}
	return tables, profiles, nil
}

func summarizeTable(ctx context.Context, db *sql.DB, table TableMeta, maxColumns int) (map[string]any, error) {
	rows, err := db.QueryContext(ctx, `SUMMARIZE SELECT * FROM `+quoteIdent(table.TableName))
	if err != nil {
		return nil, fmt.Errorf("profile %s: %w", table.TableName, err)
	}
	columnNames, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	var columnProfiles []map[string]any
	for rows.Next() {
		values := make([]any, len(columnNames))
		pointers := make([]any, len(columnNames))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			rows.Close()
			return nil, err
		}
		raw := map[string]any{}
		for i, name := range columnNames {
			raw[name] = normalizeValue(values[i])
		}
		profile := map[string]any{
			"name":          raw["column_name"],
			"type":          raw["column_type"],
			"min":           raw["min"],
			"max":           raw["max"],
			"distinctCount": raw["approx_unique"],
		}
		if mean, ok := parseFloat(raw["avg"]); ok {
			profile["mean"] = mean
		}
		if pct, ok := parseFloat(raw["null_percentage"]); ok {
			if count, ok := parseFloat(raw["count"]); ok {
				profile["nullCount"] = int64(pct * count / 100.0)
			}
		}
		columnProfiles = append(columnProfiles, profile)
		if len(columnProfiles) >= maxColumns {
			break
		}
	}
	closeErr := rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}

	// Example values make categorical columns legible to the model. Only
	// low-cardinality text columns get them, so id-like columns stay compact.
	for _, profile := range columnProfiles {
		colType, _ := profile["type"].(string)
		distinct, ok := parseFloat(profile["distinctCount"])
		if !ok || distinct <= 0 || distinct > 50 || !strings.Contains(strings.ToUpper(colType), "VARCHAR") {
			continue
		}
		name, _ := profile["name"].(string)
		if name == "" {
			continue
		}
		examples, err := columnExamples(ctx, db, table.TableName, name, 5)
		if err == nil && len(examples) > 0 {
			profile["examples"] = examples
		}
	}
	return map[string]any{
		"tableName": table.TableName,
		"rowCount":  table.RowCount,
		"columns":   columnProfiles,
	}, nil
}

func columnExamples(ctx context.Context, db *sql.DB, tableName, columnName string, limit int) ([]string, error) {
	query := fmt.Sprintf(`SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL LIMIT %d`,
		quoteIdent(columnName), quoteIdent(tableName), quoteIdent(columnName), limit)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var examples []string
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		examples = append(examples, fmt.Sprint(normalizeValue(value)))
	}
	return examples, rows.Err()
}

// Sample returns the first rows of a table for inspection.
func (m *Manager) Sample(ctx context.Context, scope Scope, fileKey, tableName string, limit int, fetch FetchFunc) ([]map[string]any, error) {
	_, meta, err := m.Ensure(ctx, scope, fileKey, fetch)
	if err != nil {
		return nil, err
	}
	resolved := ""
	for _, table := range meta.Tables {
		if tableName == "" || strings.EqualFold(table.TableName, tableName) {
			resolved = table.TableName
			break
		}
	}
	if resolved == "" {
		return nil, fmt.Errorf("table %q not found in %s", tableName, fileKey)
	}
	limit = clampLimit(limit, 10)
	_, rows, _, err := m.Query(ctx, scope, fileKey,
		fmt.Sprintf(`SELECT * FROM %s LIMIT %d`, quoteIdent(resolved), limit), limit, fetch)
	return rows, err
}

// Metadata returns the sidecar metadata, re-ingesting if needed.
func (m *Manager) Metadata(ctx context.Context, scope Scope, fileKey string, fetch FetchFunc) (*Meta, error) {
	_, meta, err := m.Ensure(ctx, scope, fileKey, fetch)
	return meta, err
}

// normalizeValue converts DuckDB driver values into JSON-friendly ones.
func normalizeValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	case *big.Int:
		return v.String()
	case big.Int:
		return v.String()
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v
	default:
		if _, err := json.Marshal(v); err == nil {
			return v
		}
		return fmt.Sprint(v)
	}
}

func parseFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		text := fmt.Sprint(value)
		parsed, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	}
}
