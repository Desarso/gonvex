package data

import (
	"context"
	"database/sql"
	"encoding/json"
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
	Limit           int
	Offset          int
	Search          string
	SortColumn      string
	SortDirection   string
	Filters         []RowsFilter
	Columns         []string
	ExactTotal      bool
	EstimateTotal   bool
	CursorCreatedAt string
	CursorID        string
}

type RowsFilter struct {
	Column   string `json:"column"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
	ValueTo  string `json:"valueTo,omitempty"`
}

type InsertResult struct {
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`
}

type ReferenceReplacementColumn struct {
	Table    string `json:"table"`
	Column   string `json:"column"`
	DataType string `json:"dataType"`
	Rows     int64  `json:"rows"`
}

type ReferenceReplacementResult struct {
	TextRows int64                        `json:"textRows"`
	JSONRows int64                        `json:"jsonRows"`
	Columns  []ReferenceReplacementColumn `json:"columns"`
	DryRun   bool                         `json:"dryRun"`
}

func ListTables(ctx context.Context, databaseURL string) ([]TableInfo, error) {
	if databaseURL == "" {
		return []TableInfo{}, nil
	}

	db, err := openDB(databaseURL)
	if err != nil {
		return nil, err
	}

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

	db, err := openDB(databaseURL)
	if err != nil {
		return RowsResult{}, err
	}

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
	if useKeysetTaskPageQuery(table, allowedColumns, options, where) {
		args = args[:len(args)-2]
		args = append(args, options.CursorCreatedAt, options.CursorID, limit)
		selectList = quoteQualifiedIdents("t", selectedColumns)
		query = fmt.Sprintf(
			"SELECT %s FROM %s t WHERE (t.%s, t.%s) < ($%d::timestamptz, $%d) ORDER BY t.%s DESC, t.%s DESC LIMIT $%d",
			strings.Join(selectList, ", "),
			quoteIdent(table),
			quoteIdent("created_at"),
			quoteIdent("id"),
			len(args)-2,
			len(args)-1,
			quoteIdent("created_at"),
			quoteIdent("id"),
			len(args),
		)
	} else if useDefaultTaskPageQuery(table, allowedColumns, options, where) {
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

func useKeysetTaskPageQuery(table string, allowedColumns map[string]bool, options RowsOptions, where string) bool {
	return table == "tasks" && where == "" && options.SortColumn == "" &&
		strings.TrimSpace(options.CursorID) != "" && strings.TrimSpace(options.CursorCreatedAt) != "" &&
		allowedColumns["id"] && allowedColumns["created_at"]
}

func useDefaultTaskPageQuery(table string, allowedColumns map[string]bool, options RowsOptions, where string) bool {
	return table == "tasks" && where == "" && options.SortColumn == "" &&
		strings.TrimSpace(options.CursorID) == "" &&
		allowedColumns["id"] && allowedColumns["created_at"]
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
			exactSearch := taskExactSearchValue(search)
			searchExpression := taskSearchExpression(allowedColumns)
			parts := []string{}
			if searchExpression != "" {
				args = append(args, likeEscapedValue(search))
				parts = append(parts, fmt.Sprintf("%s ILIKE '%%' || $%d || '%%' ESCAPE '\\'", searchExpression, len(args)))
			}
			for _, column := range []string{"status", "priority", "flag_color"} {
				if allowedColumns[column] {
					args = append(args, exactSearch)
					parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(column), len(args)))
				}
			}
			if allowedColumns["pg_id"] {
				args = append(args, likeContainsPattern(search))
				parts = append(parts, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d ESCAPE '\\'", quoteIdent("pg_id"), len(args)))
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
			if table == "tasks" && taskIntegerColumn(filter.Column) {
				value, ok := parseIntegerFilterValue(filter.Value)
				if !ok {
					clauses = append(clauses, "false")
					continue
				}
				args = append(args, value)
				clauses = append(clauses, fmt.Sprintf("%s = $%d", column, len(args)))
				continue
			}
			args = append(args, filter.Value)
			clauses = append(clauses, fmt.Sprintf("%s::text = $%d", column, len(args)))
		case "notEquals":
			if table == "tasks" && taskIntegerColumn(filter.Column) {
				value, ok := parseIntegerFilterValue(filter.Value)
				if !ok {
					continue
				}
				args = append(args, value)
				clauses = append(clauses, fmt.Sprintf("(%s IS NULL OR %s <> $%d)", column, column, len(args)))
				continue
			}
			args = append(args, filter.Value)
			clauses = append(clauses, fmt.Sprintf("(%s IS NULL OR %s::text <> $%d)", column, column, len(args)))
		case "notContains":
			args = append(args, "%"+filter.Value+"%")
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') NOT ILIKE $%d", column, len(args)))
		case "startsWith":
			args = append(args, filter.Value+"%")
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d", column, len(args)))
		case "endsWith":
			args = append(args, "%"+filter.Value)
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') ILIKE $%d", column, len(args)))
		case "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual":
			if strings.TrimSpace(filter.Value) == "" {
				continue
			}
			operator := map[string]string{
				"lessThan":           "<",
				"lessThanOrEqual":    "<=",
				"greaterThan":        ">",
				"greaterThanOrEqual": ">=",
			}[filter.Operator]
			args = append(args, filter.Value)
			clauses = append(clauses, fmt.Sprintf("(%s IS NOT NULL AND %s %s $%d)", column, column, operator, len(args)))
		case "inRange":
			from := strings.TrimSpace(filter.Value)
			to := strings.TrimSpace(filter.ValueTo)
			if from == "" && to == "" {
				continue
			}
			parts := []string{fmt.Sprintf("%s IS NOT NULL", column)}
			if from != "" {
				args = append(args, from)
				parts = append(parts, fmt.Sprintf("%s >= $%d", column, len(args)))
			}
			if to != "" {
				args = append(args, to)
				parts = append(parts, fmt.Sprintf("%s <= $%d", column, len(args)))
			}
			clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
		case "empty":
			clauses = append(clauses, fmt.Sprintf("(%s IS NULL OR %s::text = '')", column, column))
		case "notEmpty":
			clauses = append(clauses, fmt.Sprintf("(%s IS NOT NULL AND %s::text <> '')", column, column))
		case "oneOf":
			var values []string
			if err := json.Unmarshal([]byte(filter.Value), &values); err != nil {
				return "", nil, fmt.Errorf("invalid oneOf filter value for column %q", filter.Column)
			}
			if len(values) == 0 {
				continue
			}
			placeholders := make([]string, 0, len(values))
			for _, value := range values {
				args = append(args, value)
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
			}
			clauses = append(clauses, fmt.Sprintf("COALESCE(%s::text, '') IN (%s)", column, strings.Join(placeholders, ", ")))
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

func taskIntegerColumn(column string) bool {
	switch column {
	case "pg_id", "notes_count", "attachment_count", "view_count", "estimate_minutes", "progress":
		return true
	default:
		return false
	}
}

func parseIntegerFilterValue(value string) (int64, bool) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func taskExactSearchValue(search string) string {
	return strings.ToLower(strings.ReplaceAll(normalizeSearch(search), " ", "_"))
}

func normalizeSearch(search string) string {
	return strings.Join(strings.Fields(search), " ")
}

func likeContainsPattern(value string) string {
	return "%" + likeEscapedValue(value) + "%"
}

func likeEscapedValue(value string) string {
	value = normalizeSearch(value)
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	value = strings.ReplaceAll(value, `_`, `\_`)
	return value
}

func taskSearchExpression(allowedColumns map[string]bool) string {
	if allowedColumns["search_text"] {
		return quoteIdent("search_text")
	}
	columns := []string{"name", "title", "description", "status", "priority", "assignee", "project", "label", "flag_color"}
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		if allowedColumns[column] {
			parts = append(parts, fmt.Sprintf("COALESCE(%s, '')", quoteIdent(column)))
		}
	}
	if len(parts) == 0 {
		return ""
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

	db, err := openDB(databaseURL)
	if err != nil {
		return InsertResult{}, err
	}

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

func UpdateRow(
	ctx context.Context,
	databaseURL string,
	table string,
	rowID string,
	values map[string]any,
) (InsertResult, error) {
	if !validIdent(table) {
		return InsertResult{}, fmt.Errorf("invalid table name %q", table)
	}
	if databaseURL == "" {
		return InsertResult{}, fmt.Errorf("database URL is not configured")
	}
	rowID = strings.TrimSpace(rowID)
	if rowID == "" {
		return InsertResult{}, fmt.Errorf("row id is required")
	}
	if len(values) == 0 {
		return InsertResult{}, fmt.Errorf("at least one value is required")
	}
	for _, column := range []string{"_id", "id", "tenantId", "tenant_id"} {
		if _, ok := values[column]; ok {
			return InsertResult{}, fmt.Errorf("column %q cannot be changed", column)
		}
	}

	db, err := openDB(databaseURL)
	if err != nil {
		return InsertResult{}, err
	}
	columns, err := tableColumns(ctx, db, table)
	if err != nil {
		return InsertResult{}, err
	}
	if len(columns) == 0 {
		return InsertResult{}, fmt.Errorf("table %q does not exist", table)
	}
	allowed := make(map[string]bool, len(columns))
	for _, column := range columns {
		allowed[column] = true
	}
	for column := range values {
		if !allowed[column] || !validIdent(column) {
			return InsertResult{}, fmt.Errorf("invalid column name %q", column)
		}
	}
	idColumn := ""
	if allowed["_id"] {
		idColumn = "_id"
	} else if allowed["id"] {
		idColumn = "id"
	}
	if idColumn == "" {
		return InsertResult{}, fmt.Errorf("table %q has no supported identity column", table)
	}

	assignments := make([]string, 0, len(values))
	args := make([]any, 0, len(values)+1)
	for _, column := range columns {
		value, ok := values[column]
		if !ok {
			continue
		}
		args = append(args, value)
		assignments = append(
			assignments,
			fmt.Sprintf("%s = $%d", quoteIdent(column), len(args)),
		)
	}
	if len(assignments) == 0 {
		return InsertResult{}, fmt.Errorf("at least one value is required")
	}
	args = append(args, rowID)
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s::text = $%d RETURNING *",
		quoteIdent(table),
		strings.Join(assignments, ", "),
		quoteIdent(idColumn),
		len(args),
	)
	return scanChangedRow(ctx, db, query, args, table, "update")
}

func DeleteRow(
	ctx context.Context,
	databaseURL string,
	table string,
	rowID string,
) (InsertResult, error) {
	if !validIdent(table) {
		return InsertResult{}, fmt.Errorf("invalid table name %q", table)
	}
	if databaseURL == "" {
		return InsertResult{}, fmt.Errorf("database URL is not configured")
	}
	rowID = strings.TrimSpace(rowID)
	if rowID == "" {
		return InsertResult{}, fmt.Errorf("row id is required")
	}
	db, err := openDB(databaseURL)
	if err != nil {
		return InsertResult{}, err
	}
	columns, err := tableColumns(ctx, db, table)
	if err != nil {
		return InsertResult{}, err
	}
	allowed := make(map[string]bool, len(columns))
	for _, column := range columns {
		allowed[column] = true
	}
	idColumn := ""
	if allowed["_id"] {
		idColumn = "_id"
	} else if allowed["id"] {
		idColumn = "id"
	}
	if idColumn == "" {
		return InsertResult{}, fmt.Errorf("table %q has no supported identity column", table)
	}
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s::text = $1 RETURNING *",
		quoteIdent(table),
		quoteIdent(idColumn),
	)
	return scanChangedRow(ctx, db, query, []any{rowID}, table, "delete")
}

func scanChangedRow(
	ctx context.Context,
	db *sql.DB,
	query string,
	args []any,
	table string,
	action string,
) (InsertResult, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return InsertResult{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return InsertResult{}, fmt.Errorf("%s did not find a row", action)
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

// ReplaceReferences repairs exact string references in project-owned text and
// JSON columns. Identity and tenant-routing columns are deliberately excluded:
// callers may repair foreign references, but cannot rewrite row identities or
// move data between tenants through this maintenance operation.
func ReplaceReferences(
	ctx context.Context,
	databaseURL string,
	replacements map[string]string,
	dryRun bool,
) (ReferenceReplacementResult, error) {
	result := ReferenceReplacementResult{
		Columns: []ReferenceReplacementColumn{},
		DryRun:  dryRun,
	}
	if databaseURL == "" {
		return result, fmt.Errorf("database URL is not configured")
	}
	cleaned := map[string]string{}
	for source, replacement := range replacements {
		source = strings.TrimSpace(source)
		replacement = strings.TrimSpace(replacement)
		if source == "" || replacement == "" {
			return result, fmt.Errorf("replacement ids cannot be empty")
		}
		if source != replacement {
			cleaned[source] = replacement
		}
	}
	if len(cleaned) == 0 {
		return result, fmt.Errorf("at least one replacement is required")
	}

	db, err := openDB(databaseURL)
	if err != nil {
		return result, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE "_gonvex_reference_replacements" (
			source_id text PRIMARY KEY,
			replacement_id text NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return result, err
	}
	sources := make([]string, 0, len(cleaned))
	for source := range cleaned {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for _, source := range sources {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO "_gonvex_reference_replacements" (source_id, replacement_id) VALUES ($1, $2)`,
			source,
			cleaned[source],
		); err != nil {
			return result, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION pg_temp.gonvex_replace_references(value jsonb)
		RETURNS jsonb
		LANGUAGE plpgsql
		STABLE
		AS $$
		DECLARE
			kind text;
			scalar text;
			mapped text;
			next_value jsonb;
		BEGIN
			IF value IS NULL THEN RETURN NULL; END IF;
			kind := jsonb_typeof(value);
			IF kind = 'string' THEN
				scalar := value #>> '{}';
				SELECT replacement_id INTO mapped
				  FROM "_gonvex_reference_replacements"
				 WHERE source_id = scalar;
				RETURN CASE WHEN mapped IS NULL THEN value ELSE to_jsonb(mapped) END;
			ELSIF kind = 'array' THEN
				SELECT COALESCE(
					jsonb_agg(pg_temp.gonvex_replace_references(item.value) ORDER BY item.ordinality),
					'[]'::jsonb
				)
				  INTO next_value
				  FROM jsonb_array_elements(value) WITH ORDINALITY AS item(value, ordinality);
				RETURN next_value;
			ELSIF kind = 'object' THEN
				SELECT COALESCE(
					jsonb_object_agg(item.key, pg_temp.gonvex_replace_references(item.value)),
					'{}'::jsonb
				)
				  INTO next_value
				  FROM jsonb_each(value) AS item(key, value);
				RETURN next_value;
			END IF;
			RETURN value;
		END
		$$
	`); err != nil {
		return result, err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT table_name, column_name, data_type
		  FROM information_schema.columns
		 WHERE table_schema = 'public'
		   AND table_name NOT LIKE '\_gonvex\_%' ESCAPE '\'
		   AND table_name NOT LIKE 'gonvex\_%' ESCAPE '\'
		   AND table_name <> '_gonvex_reference_replacements'
		   AND is_generated = 'NEVER'
		   AND data_type IN (
			 'text',
			 'character varying',
			 'character',
			 'json',
			 'jsonb'
		   )
		 ORDER BY table_name, ordinal_position
	`)
	if err != nil {
		return result, err
	}
	type referenceColumn struct {
		table    string
		column   string
		dataType string
	}
	var columns []referenceColumn
	for rows.Next() {
		var column referenceColumn
		if err := rows.Scan(&column.table, &column.column, &column.dataType); err != nil {
			rows.Close()
			return result, err
		}
		if column.column == "_id" || column.column == "id" ||
			column.column == "tenantId" || column.column == "tenant_id" {
			continue
		}
		columns = append(columns, column)
	}
	if err := rows.Close(); err != nil {
		return result, err
	}

	for _, column := range columns {
		tableName := quoteIdent(column.table)
		columnName := quoteIdent(column.column)
		var affected int64
		switch column.dataType {
		case "json", "jsonb":
			condition := fmt.Sprintf(`
				%s IS NOT NULL
				AND EXISTS (
					SELECT 1
					  FROM "_gonvex_reference_replacements" replacements
					 WHERE %s::text LIKE '%%' || to_jsonb(replacements.source_id)::text || '%%'
				)
			`, columnName, columnName)
			if dryRun {
				query := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", tableName, condition)
				if err := tx.QueryRowContext(ctx, query).Scan(&affected); err != nil {
					return result, err
				}
			} else {
				value := fmt.Sprintf("pg_temp.gonvex_replace_references(%s::jsonb)", columnName)
				if column.dataType == "json" {
					value += "::json"
				}
				query := fmt.Sprintf(
					"UPDATE %s SET %s = %s WHERE %s",
					tableName,
					columnName,
					value,
					condition,
				)
				update, err := tx.ExecContext(ctx, query)
				if err != nil {
					return result, err
				}
				affected, err = update.RowsAffected()
				if err != nil {
					return result, err
				}
			}
			result.JSONRows += affected
		default:
			if dryRun {
				query := fmt.Sprintf(`
					SELECT count(*)
					  FROM %s target
					  JOIN "_gonvex_reference_replacements" replacements
					    ON target.%s = replacements.source_id
				`, tableName, columnName)
				if err := tx.QueryRowContext(ctx, query).Scan(&affected); err != nil {
					return result, err
				}
			} else {
				query := fmt.Sprintf(`
					UPDATE %s target
					   SET %s = replacements.replacement_id
					  FROM "_gonvex_reference_replacements" replacements
					 WHERE target.%s = replacements.source_id
				`, tableName, columnName, columnName)
				update, err := tx.ExecContext(ctx, query)
				if err != nil {
					return result, err
				}
				affected, err = update.RowsAffected()
				if err != nil {
					return result, err
				}
			}
			result.TextRows += affected
		}
		if affected > 0 {
			result.Columns = append(result.Columns, ReferenceReplacementColumn{
				Table:    column.table,
				Column:   column.column,
				DataType: column.dataType,
				Rows:     affected,
			})
		}
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

func blankValue(value any) bool {
	return value == nil || value == ""
}

func cleanDeleteIDs(ids []string) []string {
	cleaned := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		cleaned = append(cleaned, id)
	}
	return cleaned
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
