package data

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type TableInfo struct {
	Name     string   `json:"name"`
	Columns  []string `json:"columns"`
	RowCount int64    `json:"rowCount"`
}

type RowsResult struct {
	Table   string           `json:"table"`
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Total   int64            `json:"total"`
	Offset  int              `json:"offset"`
	Limit   int              `json:"limit"`
}

type RowsOptions struct {
	Limit         int
	Offset        int
	Search        string
	SortColumn    string
	SortDirection string
	Filters       []RowsFilter
	Columns       []string
	ExactTotal    bool
	EstimateTotal bool
}

type RowsFilter struct {
	Column   string `json:"column"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type InsertResult struct {
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`
}

func ListTables(ctx context.Context, databaseURL string) ([]TableInfo, error) {
	if databaseURL == "" {
		return []TableInfo{}, nil
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		columns, err := tableColumns(ctx, db, name)
		if err != nil {
			return nil, err
		}

		count, err := rowCount(ctx, db, name)
		if err != nil {
			return nil, err
		}

		tables = append(tables, TableInfo{Name: name, Columns: columns, RowCount: count})
	}

	return tables, rows.Err()
}

func ReadRows(ctx context.Context, databaseURL string, table string, options RowsOptions) (RowsResult, error) {
	if !validIdent(table) {
		return RowsResult{}, fmt.Errorf("invalid table name %q", table)
	}
	if databaseURL == "" {
		return RowsResult{Table: table, Columns: []string{}, Rows: []map[string]any{}, Limit: normalizedLimit(options.Limit)}, nil
	}
	limit := normalizedLimit(options.Limit)
	if options.Offset < 0 {
		options.Offset = 0
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return RowsResult{}, err
	}
	defer db.Close()

	columns, err := tableColumns(ctx, db, table)
	if err != nil {
		return RowsResult{}, err
	}
	if len(columns) == 0 {
		return RowsResult{}, fmt.Errorf("table %q does not exist", table)
	}
	allowedColumns := map[string]bool{}
	for _, column := range columns {
		allowedColumns[column] = true
	}

	selectedColumns, err := selectedRowsColumns(columns, allowedColumns, options.Columns)
	if err != nil {
		return RowsResult{}, err
	}

	where, args, err := rowsWhereClause(table, columns, allowedColumns, options.Search, options.Filters)
	if err != nil {
		return RowsResult{}, err
	}

	var total int64
	if options.ExactTotal {
		countQuery := fmt.Sprintf("SELECT count(*) FROM %s%s", quoteIdent(table), where)
		if err := db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
			return RowsResult{}, err
		}
	} else if options.EstimateTotal && where == "" {
		total, err = estimatedRowCount(ctx, db, table)
		if err != nil {
			return RowsResult{}, err
		}
	}

	orderBy, err := rowsOrderBy(columns, allowedColumns, options.SortColumn, options.SortDirection)
	if err != nil {
		return RowsResult{}, err
	}
	args = append(args, limit, options.Offset)
	selectList := quoteIdents(selectedColumns)
	query := fmt.Sprintf("SELECT %s FROM %s%s%s LIMIT $%d OFFSET $%d", strings.Join(selectList, ", "), quoteIdent(table), where, orderBy, len(args)-1, len(args))
	if useDefaultTaskPageQuery(table, allowedColumns, options, where) {
		selectList = quoteQualifiedIdents("t", selectedColumns)
		query = fmt.Sprintf(
			"WITH page AS (SELECT %s, %s FROM %s ORDER BY %s DESC, %s DESC LIMIT $%d OFFSET $%d) SELECT %s FROM page JOIN %s t ON t.%s = page.%s ORDER BY page.%s DESC, page.%s DESC",
			quoteIdent("id"),
			quoteIdent("created_at"),
			quoteIdent(table),
			quoteIdent("created_at"),
			quoteIdent("id"),
			len(args)-1,
			len(args),
			strings.Join(selectList, ", "),
			quoteIdent(table),
			quoteIdent("id"),
			quoteIdent("id"),
			quoteIdent("created_at"),
			quoteIdent("id"),
		)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return RowsResult{}, err
	}
	defer rows.Close()

	result := RowsResult{Table: table, Columns: selectedColumns, Rows: []map[string]any{}, Total: total, Offset: options.Offset, Limit: limit}
	for rows.Next() {
		values := make([]any, len(selectedColumns))
		pointers := make([]any, len(selectedColumns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}

		row := map[string]any{}
		for index, column := range selectedColumns {
			row[column] = normalizeValue(values[index])
		}
		result.Rows = append(result.Rows, row)
	}
	if !options.ExactTotal && total == 0 {
		result.Total = int64(options.Offset + len(result.Rows))
		if len(result.Rows) == limit {
			result.Total += int64(limit)
		}
	}

	return result, rows.Err()
}

func estimatedRowCount(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var total int64
	if err := db.QueryRowContext(ctx, "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE oid = $1::regclass", table).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func selectedRowsColumns(allColumns []string, allowedColumns map[string]bool, requested []string) ([]string, error) {
	if len(requested) == 0 {
		return allColumns, nil
	}
	columns := make([]string, 0, len(requested))
	seen := map[string]bool{}
	for _, column := range requested {
		column = strings.TrimSpace(column)
		if column == "" || seen[column] {
			continue
		}
		if !allowedColumns[column] || !validIdent(column) {
			return nil, fmt.Errorf("invalid selected column %q", column)
		}
		seen[column] = true
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return allColumns, nil
	}
	return columns, nil
}

func quoteIdents(values []string) []string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quoteIdent(value))
	}
	return quoted
}

func quoteQualifiedIdents(prefix string, values []string) []string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quoteIdent(prefix)+"."+quoteIdent(value))
	}
	return quoted
}

func useDefaultTaskPageQuery(table string, allowedColumns map[string]bool, options RowsOptions, where string) bool {
	return table == "tasks" && where == "" && options.SortColumn == "" && allowedColumns["id"] && allowedColumns["created_at"]
}

func normalizedLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func rowsWhereClause(table string, columns []string, allowedColumns map[string]bool, search string, filters []RowsFilter) (string, []any, error) {
	var clauses []string
	var args []any
	search = strings.TrimSpace(search)
	if search != "" {
		if table == "tasks" {
			args = append(args, "%"+search+"%")
			searchArg := len(args)
			exactSearch := taskExactSearchValue(search)
			parts := []string{}
			for _, column := range []string{"name", "title", "description"} {
				if allowedColumns[column] {
					parts = append(parts, fmt.Sprintf("%s ILIKE $%d", quoteIdent(column), searchArg))
				}
			}
			for _, column := range []string{"status", "priority", "flag_color"} {
				if allowedColumns[column] {
					args = append(args, exactSearch)
					parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(column), len(args)))
				}
			}
			if allowedColumns["pg_id"] {
				if id, err := strconv.ParseInt(search, 10, 64); err == nil {
					args = append(args, id)
					parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent("pg_id"), len(args)))
				}
			}
			if len(parts) > 0 {
				clauses = append(clauses, "("+strings.Join(parts, " OR ")+")")
			}
		} else {
			parts := make([]string, 0, len(columns))
			args = append(args, "%"+search+"%")
			for _, column := range columns {
				parts = append(parts, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d", quoteIdent(column), len(args)))
			}
			clauses = append(clauses, "("+strings.Join(parts, " OR ")+")")
		}
	}

	for _, filter := range filters {
		if filter.Column == "" {
			continue
		}
		if !allowedColumns[filter.Column] || !validIdent(filter.Column) {
			return "", nil, fmt.Errorf("invalid filter column %q", filter.Column)
		}
		column := quoteIdent(filter.Column)
		switch filter.Operator {
		case "equals":
			args = append(args, filter.Value)
			clauses = append(clauses, fmt.Sprintf("%s::text = $%d", column, len(args)))
		case "notEquals":
			args = append(args, filter.Value)
			clauses = append(clauses, fmt.Sprintf("(%s IS NULL OR %s::text <> $%d)", column, column, len(args)))
		case "startsWith":
			args = append(args, filter.Value+"%")
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d", column, len(args)))
		case "endsWith":
			args = append(args, "%"+filter.Value)
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d", column, len(args)))
		case "empty":
			clauses = append(clauses, fmt.Sprintf("(%s IS NULL OR %s::text = '')", column, column))
		case "notEmpty":
			clauses = append(clauses, fmt.Sprintf("(%s IS NOT NULL AND %s::text <> '')", column, column))
		default:
			args = append(args, "%"+filter.Value+"%")
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d", column, len(args)))
		}
	}

	if len(clauses) == 0 {
		return "", args, nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args, nil
}

func taskExactSearchValue(search string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(search), " ", "_"))
}

func taskSearchExpression() string {
	columns := []string{"name", "title", "description", "status", "priority", "assignee", "project", "label", "flag_color"}
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, fmt.Sprintf("COALESCE(%s, '')", quoteIdent(column)))
	}
	return strings.Join(parts, " || ' ' || ")
}

func rowsOrderBy(columns []string, allowedColumns map[string]bool, sortColumn string, sortDirection string) (string, error) {
	direction := "ASC"
	if strings.EqualFold(sortDirection, "desc") {
		direction = "DESC"
	}
	if sortColumn != "" {
		if !allowedColumns[sortColumn] || !validIdent(sortColumn) {
			return "", fmt.Errorf("invalid sort column %q", sortColumn)
		}
		return fmt.Sprintf(" ORDER BY %s %s", quoteIdent(sortColumn), direction), nil
	}
	for _, candidate := range []string{"created_at", "id"} {
		if allowedColumns[candidate] {
			defaultDirection := "ASC"
			if candidate == "created_at" {
				defaultDirection = "DESC"
			}
			return fmt.Sprintf(" ORDER BY %s %s", quoteIdent(candidate), defaultDirection), nil
		}
	}
	if len(columns) > 0 {
		return fmt.Sprintf(" ORDER BY %s ASC", quoteIdent(columns[0])), nil
	}
	return "", nil
}

func InsertRow(ctx context.Context, databaseURL string, table string, values map[string]any) (InsertResult, error) {
	if !validIdent(table) {
		return InsertResult{}, fmt.Errorf("invalid table name %q", table)
	}
	if databaseURL == "" {
		return InsertResult{}, fmt.Errorf("database URL is not configured")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return InsertResult{}, err
	}
	defer db.Close()

	columns, err := tableColumns(ctx, db, table)
	if err != nil {
		return InsertResult{}, err
	}
	if len(columns) == 0 {
		return InsertResult{}, fmt.Errorf("table %q does not exist", table)
	}

	allowed := map[string]bool{}
	for _, column := range columns {
		allowed[column] = true
	}
	if allowed["id"] && blankValue(values["id"]) {
		nextID, err := nextNumericID(ctx, db, table)
		if err != nil {
			return InsertResult{}, err
		}
		values["id"] = nextID
	}

	insertColumns := make([]string, 0, len(values))
	args := make([]any, 0, len(values))
	placeholders := make([]string, 0, len(values))
	for _, column := range columns {
		value, ok := values[column]
		if !ok || value == "" {
			continue
		}
		if !allowed[column] || !validIdent(column) {
			return InsertResult{}, fmt.Errorf("invalid column name %q", column)
		}
		insertColumns = append(insertColumns, quoteIdent(column))
		args = append(args, value)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	if len(insertColumns) == 0 {
		return InsertResult{}, fmt.Errorf("no values provided")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING *",
		quoteIdent(table),
		strings.Join(insertColumns, ", "),
		strings.Join(placeholders, ", "),
	)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return InsertResult{}, err
	}
	defer rows.Close()

	if !rows.Next() {
		return InsertResult{}, fmt.Errorf("insert did not return a row")
	}

	returnedColumns, err := rows.Columns()
	if err != nil {
		return InsertResult{}, err
	}
	returnedValues := make([]any, len(returnedColumns))
	pointers := make([]any, len(returnedColumns))
	for index := range returnedValues {
		pointers[index] = &returnedValues[index]
	}
	if err := rows.Scan(pointers...); err != nil {
		return InsertResult{}, err
	}

	row := map[string]any{}
	for index, column := range returnedColumns {
		row[column] = normalizeValue(returnedValues[index])
	}
	return InsertResult{Table: table, Row: row}, rows.Err()
}

func blankValue(value any) bool {
	return value == nil || value == ""
}

func nextNumericID(ctx context.Context, db *sql.DB, table string) (string, error) {
	if !validIdent(table) {
		return "", fmt.Errorf("invalid table name %q", table)
	}

	var next int64
	query := fmt.Sprintf("SELECT COALESCE(MAX(CASE WHEN id ~ '^[0-9]+$' THEN id::bigint END), 0) + 1 FROM %s", quoteIdent(table))
	if err := db.QueryRowContext(ctx, query).Scan(&next); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", next), nil
}

func tableColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

func rowCount(ctx context.Context, db *sql.DB, table string) (int64, error) {
	if !validIdent(table) {
		return 0, fmt.Errorf("invalid table name %q", table)
	}

	var count int64
	query := fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(table))
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func normalizeValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return typed
	}
}

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validIdent(value string) bool {
	return identPattern.MatchString(value)
}

func quoteIdent(value string) string {
	parts := strings.Split(value, ".")
	for index, part := range parts {
		parts[index] = `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

func SortTables(tables []TableInfo) {
	sort.Slice(tables, func(i int, j int) bool {
		return tables[i].Name < tables[j].Name
	})
}
