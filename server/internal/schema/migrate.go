package schema

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gonvex/gonvex/pkg/manifest"
)

type Result struct {
	Applied  []string `json:"applied"`
	Warnings []string `json:"warnings"`
}

type existingColumn struct {
	Type       string
	Nullable   bool
	PrimaryKey bool
}

func Apply(ctx context.Context, databaseURL string, desired manifest.Schema) (Result, error) {
	if databaseURL == "" || len(desired.Tables) == 0 {
		return Result{}, nil
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return Result{}, err
	}

	result := Result{}
	tableNames := sortedTableNames(desired.Tables)
	for _, tableName := range tableNames {
		table := desired.Tables[tableName]
		exists, err := tableExists(ctx, db, tableName)
		if err != nil {
			return result, err
		}

		if !exists {
			statement, err := createTableSQL(tableName, table)
			if err != nil {
				return result, err
			}
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return result, err
			}
			result.Applied = append(result.Applied, fmt.Sprintf("created table %s", tableName))
		} else {
			applied, warnings, err := reconcileColumns(ctx, db, tableName, table)
			if err != nil {
				return result, err
			}
			result.Applied = append(result.Applied, applied...)
			result.Warnings = append(result.Warnings, warnings...)
		}

		applied, err := createIndexes(ctx, db, tableName, table)
		if err != nil {
			return result, err
		}
		result.Applied = append(result.Applied, applied...)
	}

	applied, err := InstallNotifyTriggers(ctx, db, desired.Tables)
	if err != nil {
		return result, err
	}
	result.Applied = append(result.Applied, applied...)

	return result, nil
}

func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`, table).Scan(&exists)
	return exists, err
}

func existingColumns(ctx context.Context, db *sql.DB, table string) (map[string]existingColumn, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.column_name,
			c.udt_name,
			c.is_nullable = 'YES',
			COALESCE(tc.constraint_type = 'PRIMARY KEY', false)
		FROM information_schema.columns c
		LEFT JOIN information_schema.key_column_usage kcu
			ON kcu.table_schema = c.table_schema
			AND kcu.table_name = c.table_name
			AND kcu.column_name = c.column_name
		LEFT JOIN information_schema.table_constraints tc
			ON tc.constraint_schema = kcu.constraint_schema
			AND tc.constraint_name = kcu.constraint_name
			AND tc.constraint_type = 'PRIMARY KEY'
		WHERE c.table_schema = 'public' AND c.table_name = $1
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := map[string]existingColumn{}
	for rows.Next() {
		var column string
		var udtName string
		var nullable bool
		var primaryKey bool
		if err := rows.Scan(&column, &udtName, &nullable, &primaryKey); err != nil {
			return nil, err
		}
		columns[column] = existingColumn{Type: manifestType(udtName), Nullable: nullable, PrimaryKey: primaryKey}
	}
	return columns, rows.Err()
}

func createTableSQL(name string, table manifest.Table) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("invalid table name %q", name)
	}
	columnNames := sortedColumnNames(table.Columns)
	if len(columnNames) == 0 {
		return "", fmt.Errorf("table %s has no columns", name)
	}

	definitions := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		column := table.Columns[columnName]
		definition, err := columnDefinition(columnName, column, true)
		if err != nil {
			return "", err
		}
		definitions = append(definitions, definition)
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(name), strings.Join(definitions, ", ")), nil
}

func reconcileColumns(ctx context.Context, db *sql.DB, tableName string, table manifest.Table) ([]string, []string, error) {
	existing, err := existingColumns(ctx, db, tableName)
	if err != nil {
		return nil, nil, err
	}

	var applied []string
	var warnings []string
	for columnName := range existing {
		if _, ok := table.Columns[columnName]; !ok {
			return applied, warnings, fmt.Errorf("unsafe schema change for %s.%s: dropping columns is not automatic", tableName, columnName)
		}
	}
	for _, columnName := range sortedColumnNames(table.Columns) {
		column := table.Columns[columnName]
		current, ok := existing[columnName]
		if ok {
			if current.Type != "" && !compatibleColumnType(current.Type, column.Type) {
				return applied, warnings, fmt.Errorf("unsafe schema change for %s.%s: existing type %s does not match desired type %s", tableName, columnName, current.Type, column.Type)
			}
			if current.PrimaryKey != column.PrimaryKey {
				return applied, warnings, fmt.Errorf("unsafe schema change for %s.%s: primary key changes are not automatic", tableName, columnName)
			}
			if current.Nullable && !column.Nullable && !column.PrimaryKey {
				nullCount, err := nullRowCount(ctx, db, tableName, columnName)
				if err != nil {
					return applied, warnings, err
				}
				if nullCount > 0 {
					return applied, warnings, fmt.Errorf("unsafe schema change for %s.%s: cannot make column non-null while %d existing row(s) contain null", tableName, columnName, nullCount)
				}
				statement := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", quoteIdent(tableName), quoteIdent(columnName))
				if _, err := db.ExecContext(ctx, statement); err != nil {
					return applied, warnings, err
				}
				applied = append(applied, fmt.Sprintf("set %s.%s not null", tableName, columnName))
			}
			if !current.Nullable && column.Nullable && !column.PrimaryKey {
				statement := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", quoteIdent(tableName), quoteIdent(columnName))
				if _, err := db.ExecContext(ctx, statement); err != nil {
					return applied, warnings, err
				}
				applied = append(applied, fmt.Sprintf("dropped not-null from %s.%s", tableName, columnName))
			}
			continue
		}

		empty, err := tableEmpty(ctx, db, tableName)
		if err != nil {
			return applied, warnings, err
		}
		if column.PrimaryKey && !empty {
			return applied, warnings, fmt.Errorf("unsafe schema change for %s.%s: cannot add primary key column to table with existing rows", tableName, columnName)
		}
		if !column.Nullable && !column.PrimaryKey && !empty {
			return applied, warnings, fmt.Errorf("unsafe schema change for %s.%s: cannot add required column to %s because existing rows need a value", tableName, columnName, tableName)
		}
		definition, err := columnDefinition(columnName, column, empty)
		if err != nil {
			return applied, warnings, err
		}

		statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", quoteIdent(tableName), definition)
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return applied, warnings, err
		}

		applied = append(applied, fmt.Sprintf("added column %s.%s", tableName, columnName))
	}

	return applied, warnings, nil
}

func tableEmpty(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	var exists bool
	statement := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)", quoteIdent(tableName))
	if err := db.QueryRowContext(ctx, statement).Scan(&exists); err != nil {
		return false, err
	}
	return !exists, nil
}

func nullRowCount(ctx context.Context, db *sql.DB, tableName string, columnName string) (int64, error) {
	var count int64
	statement := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", quoteIdent(tableName), quoteIdent(columnName))
	if err := db.QueryRowContext(ctx, statement).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func manifestType(udtName string) string {
	switch udtName {
	case "text", "varchar", "bpchar":
		return "string"
	case "int4":
		return "int"
	case "int8":
		return "int64"
	case "float8":
		return "float64"
	case "bool":
		return "bool"
	case "timestamptz":
		return "time"
	case "jsonb":
		return "json"
	default:
		return udtName
	}
}

func compatibleColumnType(current string, desired string) bool {
	if current == desired {
		return true
	}
	if current == "string" && (desired == "id" || desired == "text") {
		return true
	}
	return false
}

func createIndexes(ctx context.Context, db *sql.DB, tableName string, table manifest.Table) ([]string, error) {
	var applied []string
	for _, indexName := range sortedIndexNames(table.Indexes) {
		index := table.Indexes[indexName]
		if len(index.Columns) == 0 {
			continue
		}

		columns := make([]string, 0, len(index.Columns))
		for _, column := range index.Columns {
			if !validIdent(column) {
				return applied, fmt.Errorf("invalid index column %q", column)
			}
			columns = append(columns, quoteIdent(column))
		}

		unique := ""
		if index.Unique {
			unique = "UNIQUE "
		}
		physicalName := tableName + "_" + indexName
		if !validIdent(physicalName) {
			return applied, fmt.Errorf("invalid index name %q", physicalName)
		}

		statement := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)", unique, quoteIdent(physicalName), quoteIdent(tableName), strings.Join(columns, ", "))
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return applied, err
		}
		applied = append(applied, fmt.Sprintf("ensured index %s", physicalName))
	}

	return applied, nil
}

func columnDefinition(name string, column manifest.Column, creatingTable bool) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("invalid column name %q", name)
	}

	sqlType, err := columnSQLType(column.Type)
	if err != nil {
		return "", err
	}

	parts := []string{quoteIdent(name), sqlType}
	if column.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	} else if creatingTable && !column.Nullable {
		parts = append(parts, "NOT NULL")
	}
	return strings.Join(parts, " "), nil
}

func columnSQLType(kind string) (string, error) {
	switch kind {
	case "id", "string", "text":
		return "TEXT", nil
	case "int":
		return "INTEGER", nil
	case "int64":
		return "BIGINT", nil
	case "float64":
		return "DOUBLE PRECISION", nil
	case "bool":
		return "BOOLEAN", nil
	case "time":
		return "TIMESTAMPTZ", nil
	case "json":
		return "JSONB", nil
	default:
		return "", fmt.Errorf("unsupported column type %q", kind)
	}
}

func sortedTableNames(tables map[string]manifest.Table) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedColumnNames(columns map[string]manifest.Column) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedIndexNames(indexes map[string]manifest.Index) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validIdent(value string) bool {
	return identPattern.MatchString(value)
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
