package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

// ErrUnsafeChange marks migrations Apply refuses to perform automatically
// (type changes, primary-key changes). Callers that apply a schema to
// databases they merely discovered — rather than own — can errors.Is on it
// and skip instead of failing.
var ErrUnsafeChange = errors.New("unsafe schema change")

type Result struct {
	Applied  []string `json:"applied"`
	Warnings []string `json:"warnings"`
}

type existingColumn struct {
	Type       string
	Nullable   bool
	PrimaryKey bool
}

type databaseSchemaSnapshot struct {
	tables  map[string]bool
	columns map[string]map[string]existingColumn
	indexes map[string]bool
}

func Apply(ctx context.Context, databaseURL string, desired manifest.Schema) (Result, error) {
	if databaseURL == "" || len(desired.Tables) == 0 {
		return Result{}, nil
	}

	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return Result{}, err
	}

	// Serialize schema application per database with a Postgres advisory lock.
	// CREATE TABLE/INDEX IF NOT EXISTS is NOT concurrency-safe: the existence
	// check and the pg_class insert are not atomic, so two sessions applying the
	// same schema at once (e.g. a runtime sync racing a tenant clone/normalize
	// pass, or overlapping syncs) both pass the check and one loses with
	// "duplicate key ... pg_class_relname_nsp_index" (23505). The lock makes any
	// second applier wait until the first finishes, after which IF NOT EXISTS
	// correctly no-ops. The key is derived from the database name so applies to
	// different databases still run in parallel.
	unlock, err := acquireSchemaLock(ctx, db)
	if err != nil {
		return Result{}, err
	}
	defer unlock()
	snapshot, err := inspectDatabaseSchema(ctx, db)
	if err != nil {
		return Result{}, err
	}

	result := Result{}
	tableNames := sortedTableNames(desired.Tables)
	for _, tableName := range tableNames {
		table := desired.Tables[tableName]
		exists := snapshot.tables[tableName]

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
			applied, warnings, err := reconcileColumnsFromExisting(ctx, db, tableName, table, snapshot.columns[tableName])
			if err != nil {
				return result, err
			}
			result.Applied = append(result.Applied, applied...)
			result.Warnings = append(result.Warnings, warnings...)
		}

		applied, err := createIndexesFromExisting(ctx, db, tableName, table, snapshot.indexes)
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

func inspectDatabaseSchema(ctx context.Context, db *sql.DB) (databaseSchemaSnapshot, error) {
	snapshot := databaseSchemaSnapshot{
		tables:  map[string]bool{},
		columns: map[string]map[string]existingColumn{},
		indexes: map[string]bool{},
	}

	columnRows, err := db.QueryContext(ctx, `
		SELECT
			relation.relname,
			attribute.attname,
			type.typname,
			NOT attribute.attnotnull,
			EXISTS (
				SELECT 1
				FROM pg_catalog.pg_index primary_index
				WHERE primary_index.indrelid = relation.oid
					AND primary_index.indisprimary
					AND attribute.attnum = ANY(primary_index.indkey)
			)
		FROM pg_catalog.pg_attribute attribute
		JOIN pg_catalog.pg_class relation ON relation.oid = attribute.attrelid
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
		JOIN pg_catalog.pg_type type ON type.oid = attribute.atttypid
		WHERE namespace.nspname = current_schema()
			AND relation.relkind IN ('r', 'p')
			AND attribute.attnum > 0
			AND NOT attribute.attisdropped
	`)
	if err != nil {
		return snapshot, err
	}
	for columnRows.Next() {
		var tableName string
		var columnName string
		var udtName string
		var nullable bool
		var primaryKey bool
		if err := columnRows.Scan(&tableName, &columnName, &udtName, &nullable, &primaryKey); err != nil {
			columnRows.Close()
			return snapshot, err
		}
		if snapshot.columns[tableName] == nil {
			snapshot.columns[tableName] = map[string]existingColumn{}
		}
		snapshot.tables[tableName] = true
		rememberExistingColumn(snapshot.columns[tableName], columnName, existingColumn{
			Type:       manifestType(udtName),
			Nullable:   nullable,
			PrimaryKey: primaryKey,
		})
	}
	if err := columnRows.Close(); err != nil {
		return snapshot, err
	}
	if err := columnRows.Err(); err != nil {
		return snapshot, err
	}

	indexRows, err := db.QueryContext(ctx, `
		SELECT relation.relname, idx.indisunique
		FROM pg_catalog.pg_index AS idx
		JOIN pg_catalog.pg_class AS relation ON relation.oid = idx.indexrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = current_schema()
	`)
	if err != nil {
		return snapshot, err
	}
	for indexRows.Next() {
		var indexName string
		var unique bool
		if err := indexRows.Scan(&indexName, &unique); err != nil {
			indexRows.Close()
			return snapshot, err
		}
		snapshot.indexes[indexName] = unique
	}
	if err := indexRows.Close(); err != nil {
		return snapshot, err
	}
	if err := indexRows.Err(); err != nil {
		return snapshot, err
	}

	return snapshot, nil
}

// acquireSchemaLock takes a session-level Postgres advisory lock on a dedicated
// connection, keyed by the current database, and returns a function that
// releases the lock and returns the connection. Holding the lock on one
// connection is enough to exclude any other applier for this database; the DDL
// itself may run on other pool connections. The returned unlock is safe to call
// once via defer.
func acquireSchemaLock(ctx context.Context, db *sql.DB) (func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	// hashtext yields a stable int4 from the database name; two sessions on the
	// same database contend on the same key, different databases do not.
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext('gonvex_schema_apply:' || current_database()))`); err != nil {
		conn.Close()
		return nil, err
	}
	return func() {
		// Release with a fresh context so unlock still runs if ctx is cancelled.
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext('gonvex_schema_apply:' || current_database()))`)
		conn.Close()
	}, nil
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
			attribute.attname,
			type.typname,
			NOT attribute.attnotnull,
			EXISTS (
				SELECT 1
				FROM pg_catalog.pg_index primary_index
				WHERE primary_index.indrelid = relation.oid
					AND primary_index.indisprimary
					AND attribute.attnum = ANY(primary_index.indkey)
			)
		FROM pg_catalog.pg_attribute attribute
		JOIN pg_catalog.pg_class relation ON relation.oid = attribute.attrelid
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
		JOIN pg_catalog.pg_type type ON type.oid = attribute.atttypid
		WHERE namespace.nspname = current_schema()
			AND relation.relname = $1
			AND relation.relkind IN ('r', 'p')
			AND attribute.attnum > 0
			AND NOT attribute.attisdropped
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
		rememberExistingColumn(columns, column, existingColumn{Type: manifestType(udtName), Nullable: nullable, PrimaryKey: primaryKey})
	}
	return columns, rows.Err()
}

// A column can appear more than once when it participates in both a primary key
// and another key constraint. Preserve the primary-key fact regardless of row
// order so reconciliation never mistakes an unchanged key for an unsafe change.
func rememberExistingColumn(columns map[string]existingColumn, name string, candidate existingColumn) {
	if current, ok := columns[name]; ok {
		candidate.PrimaryKey = candidate.PrimaryKey || current.PrimaryKey
	}
	columns[name] = candidate
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
	return reconcileColumnsFromExisting(ctx, db, tableName, table, existing)
}

func reconcileColumnsFromExisting(ctx context.Context, db *sql.DB, tableName string, table manifest.Table, existing map[string]existingColumn) ([]string, []string, error) {
	var applied []string
	var warnings []string
	for columnName := range existing {
		if _, ok := table.Columns[columnName]; !ok {
			warnings = append(warnings, fmt.Sprintf("kept existing column %s.%s not declared in schema", tableName, columnName))
		}
	}
	for _, columnName := range sortedColumnNames(table.Columns) {
		column := table.Columns[columnName]
		current, ok := existing[columnName]
		if ok {
			if current.Type != "" && !compatibleColumnType(current.Type, column.Type) {
				return applied, warnings, fmt.Errorf("%w for %s.%s: existing type %s does not match desired type %s", ErrUnsafeChange, tableName, columnName, current.Type, column.Type)
			}
			if current.PrimaryKey != column.PrimaryKey {
				return applied, warnings, fmt.Errorf("%w for %s.%s: primary key changes are not automatic", ErrUnsafeChange, tableName, columnName)
			}
			if current.Nullable && !column.Nullable && !column.PrimaryKey {
				nullCount, err := nullRowCount(ctx, db, tableName, columnName)
				if err != nil {
					return applied, warnings, err
				}
				if nullCount > 0 {
					warnings = append(warnings, fmt.Sprintf("kept %s.%s nullable because %d existing row(s) contain null", tableName, columnName, nullCount))
					continue
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
			return applied, warnings, fmt.Errorf("%w for %s.%s: cannot add primary key column to table with existing rows", ErrUnsafeChange, tableName, columnName)
		}
		enforceNotNull := empty || column.Nullable || column.PrimaryKey
		definition, err := columnDefinition(columnName, column, enforceNotNull)
		if err != nil {
			return applied, warnings, err
		}

		statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", quoteIdent(tableName), definition)
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return applied, warnings, err
		}

		applied = append(applied, fmt.Sprintf("added column %s.%s", tableName, columnName))
		if !column.Nullable && !column.PrimaryKey && !empty {
			warnings = append(warnings, fmt.Sprintf("added %s.%s as nullable because %s has existing rows", tableName, columnName, tableName))
		}
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
	// A wider existing integer column (BIGINT) already holds every value of a
	// narrower desired INTEGER, so keep it as-is rather than forcing an unsafe
	// narrowing. Cloned tenants infer int64/BIGINT from JSON numbers where the
	// declared schema uses int/INTEGER.
	if current == "int64" && desired == "int" {
		return true
	}
	return false
}

func createIndexes(ctx context.Context, db *sql.DB, tableName string, table manifest.Table) ([]string, error) {
	existing, err := existingIndexes(ctx, db)
	if err != nil {
		return nil, err
	}
	return createIndexesFromExisting(ctx, db, tableName, table, existing)
}

func createIndexesFromExisting(ctx context.Context, db *sql.DB, tableName string, table manifest.Table, existing map[string]bool) ([]string, error) {
	var applied []string
	installedTrigram := false
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

		physicalName := tableName + "_" + indexName
		if !validIdent(physicalName) {
			return applied, fmt.Errorf("invalid index name %q", physicalName)
		}

		// CREATE INDEX IF NOT EXISTS does not reconcile a pre-existing index
		// whose uniqueness changed. That can leave a declared UniqueIndex backed
		// by an ordinary index indefinitely, weakening tenant data integrity. Drop
		// only the same physical index when its uniqueness differs; the normal
		// creation path below then recreates the declared contract and surfaces
		// duplicate data as a migration error instead of silently accepting it.
		reconcileUniqueness := false
		currentUnique, exists := existing[physicalName]
		if index.Kind == "" || index.Kind == "btree" {
			reconcileUniqueness = needsIndexUniquenessRebuild(exists, currentUnique, index.Unique)
		}
		if exists && !reconcileUniqueness {
			continue
		}

		statement := ""
		switch index.Kind {
		case "", "btree":
			statement = btreeIndexSQL(physicalName, tableName, columns, index.Unique)
		case "trigram":
			if index.Unique {
				return applied, fmt.Errorf("trigram index %s.%s cannot be unique", tableName, indexName)
			}
			if !installedTrigram {
				if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err != nil {
					return applied, err
				}
				installedTrigram = true
			}
			statement = trigramIndexSQL(physicalName, tableName, index.Columns)
		default:
			return applied, fmt.Errorf("unsupported index kind %q for %s.%s", index.Kind, tableName, indexName)
		}
		if reconcileUniqueness {
			if _, err := db.ExecContext(ctx, "DROP INDEX "+quoteIdent(physicalName)); err != nil {
				return applied, err
			}
			if _, err := db.ExecContext(ctx, statement); err != nil {
				// Restore the prior index contract if strengthening uniqueness finds
				// duplicate data. The migration still fails, but it does not leave the
				// table without the index it had before the attempt.
				restore := btreeIndexSQL(physicalName, tableName, columns, currentUnique)
				if _, restoreErr := db.ExecContext(ctx, restore); restoreErr != nil {
					return applied, fmt.Errorf("reconcile index %s: %w (restore failed: %v)", physicalName, err, restoreErr)
				}
				return applied, err
			}
			applied = append(applied, fmt.Sprintf("reconciled index uniqueness %s", physicalName))
		} else if _, err := db.ExecContext(ctx, statement); err != nil {
			return applied, err
		}
		existing[physicalName] = index.Unique
		applied = append(applied, fmt.Sprintf("created index %s", physicalName))
	}

	return applied, nil
}

func existingIndexes(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT relation.relname, idx.indisunique
		FROM pg_catalog.pg_index AS idx
		JOIN pg_catalog.pg_class AS relation ON relation.oid = idx.indexrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = current_schema()
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	indexes := map[string]bool{}
	for rows.Next() {
		var name string
		var unique bool
		if err := rows.Scan(&name, &unique); err != nil {
			return nil, err
		}
		indexes[name] = unique
	}
	return indexes, rows.Err()
}

func needsIndexUniquenessRebuild(exists bool, currentUnique bool, desiredUnique bool) bool {
	return exists && currentUnique != desiredUnique
}

func btreeIndexSQL(physicalName string, tableName string, columns []string, unique bool) string {
	modifier := ""
	if unique {
		modifier = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)", modifier, quoteIdent(physicalName), quoteIdent(tableName), strings.Join(columns, ", "))
}

func existingIndexUniqueness(ctx context.Context, db *sql.DB, physicalName string) (bool, bool, error) {
	var unique bool
	err := db.QueryRowContext(ctx, `
		SELECT idx.indisunique
		FROM pg_catalog.pg_index AS idx
		JOIN pg_catalog.pg_class AS relation ON relation.oid = idx.indexrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = current_schema() AND relation.relname = $1
	`, physicalName).Scan(&unique)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, unique, nil
}

func trigramIndexSQL(indexName string, tableName string, columns []string) string {
	if len(columns) == 1 {
		return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING gin (%s gin_trgm_ops)", quoteIdent(indexName), quoteIdent(tableName), quoteIdent(columns[0]))
	}

	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, fmt.Sprintf("COALESCE(%s::text, '')", quoteIdent(column)))
	}
	expression := strings.Join(parts, " || ' ' || ")
	return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING gin ((%s) gin_trgm_ops)", quoteIdent(indexName), quoteIdent(tableName), expression)
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
