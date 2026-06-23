package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// normalizedTaskColumn marks the normalized Whagons tenant schema (camelCase
// foreign keys such as statusId); flatTaskColumn marks the legacy denormalized
// mirror the dashboard project database stores directly.
const (
	normalizedTaskColumn = "statusId"
	flatTaskColumn       = "status_name"
)

// taskGridColumn maps a denormalized grid column to the SQL expression that
// produces it from the normalized task schema.
type taskGridColumn struct {
	name string
	expr string
}

// taskGridProjectionColumns lists every column the dashboard "tasks.grid" surface can
// request. Keeping the set complete means column selection always succeeds; the
// grid renderer simply sees empty values where a tenant has no source data.
func taskGridProjectionColumns(taskColumns map[string]bool, joins map[string]resolvedTaskGridJoin) []taskGridColumn {
	task := func(fallback string, candidates ...string) string {
		return taskColumnExpr(taskColumns, fallback, candidates...)
	}
	taskText := func(fallback string, candidates ...string) string {
		return textExpr(task("", candidates...), fallback)
	}
	ref := func(alias string, fallback string, candidates ...string) string {
		return joinColumnExpr(joins[alias], fallback, candidates...)
	}

	return []taskGridColumn{
		{name: "id", expr: task(`''`, "_id", "id")},
		{name: "pg_id", expr: task(`NULL`, "pgId", "pg_id", "id")},
		{name: "name", expr: task(`''`, "name", "title")},
		{name: "title", expr: task(`''`, "title", "name")},
		{name: "description", expr: task(`NULL`, "description")},
		{name: "form_id", expr: task(`NULL`, "formId", "form_id")},
		{name: "sla_id", expr: task(`NULL`, "slaId", "sla_id")},
		{name: "approval_id", expr: task(`NULL`, "approvalId", "approval_id")},
		{name: "notes_count", expr: task(`0`, "notesCount", "notes_count")},
		{name: "category_icon", expr: ref("c", task(`NULL`, "category_icon"), "icon")},
		{name: "category_color", expr: ref("c", task(`NULL`, "category_color"), "color")},
		{name: "category_name", expr: ref("c", task(`NULL`, "category_name"), "name")},
		{name: "tag_names", expr: taskText(`''`, "tag_names", "tagIds")},
		{name: "tag_colors", expr: task(`''`, "tag_colors")},
		{name: "attachment_count", expr: task(`0`, "attachment_count")},
		{name: "view_count", expr: task(`0`, "viewCount", "view_count")},
		{name: "status", expr: ref("s", task(`''`, "status_name", "status"), "name")},
		{name: "status_name", expr: ref("s", task(`NULL`, "status_name"), "name")},
		{name: "status_color", expr: ref("s", task(`NULL`, "status_color"), "color")},
		{name: "status_action", expr: ref("s", task(`NULL`, "status_action"), "action")},
		{name: "status_icon", expr: ref("s", task(`NULL`, "status_icon"), "icon")},
		{name: "status_working_animation", expr: task(`''`, "status_working_animation")},
		{name: "status_initial", expr: ref("s", task(`NULL`, "status_initial"), "initial")},
		{name: "priority", expr: ref("p", task(`''`, "priority_name", "priority"), "name")},
		{name: "priority_name", expr: ref("p", task(`NULL`, "priority_name"), "name")},
		{name: "priority_color", expr: ref("p", task(`NULL`, "priority_color"), "color")},
		{name: "assignee", expr: task(`''`, "assignee")},
		{name: "assignee_names", expr: task(`''`, "assignee_names")},
		{name: "assignee_ids", expr: taskText(`''`, "assignee_ids", "userIds")},
		{name: "assignee_avatar_urls", expr: task(`''`, "assignee_avatar_urls")},
		{name: "all_user_names", expr: task(`''`, "all_user_names")},
		{name: "all_user_avatar_urls", expr: task(`''`, "all_user_avatar_urls")},
		{name: "due_date", expr: task(`NULL`, "due_date", "dueDate")},
		{name: "due_at", expr: task(`NULL`, "due_at", "dueDate")},
		{name: "start_date", expr: task(`NULL`, "start_date", "startDate")},
		{name: "spot_id", expr: task(`NULL`, "spot_id", "spotId")},
		{name: "spot_name", expr: ref("sp", task(`NULL`, "spot_name"), "name")},
		{name: "workspace_name", expr: ref("w", task(`NULL`, "workspace_name"), "name")},
		{name: "progress", expr: task(`0`, "progress")},
		{name: "flag_color", expr: task(`NULL`, "flag_color", "flagColor")},
		{name: "created_at", expr: task(`NULL`, "created_at", "createdAt", "_creationTime")},
		{name: "updated_at", expr: task(`NULL`, "updated_at", "updatedAt")},
	}
}

// taskGridJoin is a reference table LEFT JOINed onto the tasks table; joins are
// emitted only when the table exists.
type taskGridJoin struct {
	alias      string
	table      string
	taskColumn string
}

type resolvedTaskGridJoin struct {
	taskGridJoin
	columns      map[string]bool
	targetColumn string
	available    bool
}

var taskGridJoins = []taskGridJoin{
	{alias: "s", table: "statuses", taskColumn: "statusId"},
	{alias: "p", table: "priorities", taskColumn: "priorityId"},
	{alias: "c", table: "categories", taskColumn: "categoryId"},
	{alias: "sp", table: "spots", taskColumn: "spotId"},
	{alias: "w", table: "workspaces", taskColumn: "workspaceId"},
}

// taskGridSearchColumns are the human-meaningful columns free-text search spans;
// status/priority colours and ids are deliberately excluded.
var taskGridSearchColumns = []string{
	"name", "title", "description",
	"status_name", "priority_name", "category_name", "spot_name", "workspace_name",
}

// ReadTaskGrid reads rows for the dashboard "tasks.grid" surface. A flat
// denormalized mirror is read directly; a normalized Whagons tenant schema is
// projected into the same shape via reference-table joins so the grid renderer
// works unchanged across both.
func ReadTaskGrid(ctx context.Context, databaseURL string, options RowsOptions) (RowsResult, error) {
	if databaseURL == "" {
		return RowsResult{Table: "tasks", Columns: []string{}, Rows: []map[string]any{}, Limit: normalizedLimit(options.Limit)}, nil
	}
	db, err := openDB(databaseURL)
	if err != nil {
		return RowsResult{}, err
	}
	columns, err := tableColumns(ctx, db, "tasks")
	if err != nil {
		return RowsResult{}, err
	}
	taskColumns := map[string]bool{}
	for _, column := range columns {
		taskColumns[column] = true
	}
	if readTaskGridFlat(taskColumns) {
		return ReadRows(ctx, databaseURL, "tasks", options)
	}
	return readNormalizedTaskGrid(ctx, db, taskColumns, options)
}

func readTaskGridFlat(taskColumns map[string]bool) bool {
	return taskColumns[flatTaskColumn] && !taskColumns[normalizedTaskColumn]
}

func readNormalizedTaskGrid(ctx context.Context, db *sql.DB, taskColumns map[string]bool, options RowsOptions) (RowsResult, error) {
	limit := normalizedLimit(options.Limit)
	if options.Offset < 0 {
		options.Offset = 0
	}

	tables, err := publicTableSet(ctx, db)
	if err != nil {
		return RowsResult{}, err
	}
	tableColumnSets, err := taskGridTableColumnSets(ctx, db, tables)
	if err != nil {
		return RowsResult{}, err
	}
	tableColumnSets["tasks"] = taskColumns
	joins := resolveTaskGridJoins(tables, tableColumnSets, taskColumns)
	projection := taskGridProjectionColumns(taskColumns, joins)
	source := taskGridSource(projection, joins, taskColumns["deletedAt"])

	columns := make([]string, len(projection))
	allowed := map[string]bool{}
	for index, column := range projection {
		columns[index] = column.name
		allowed[column.name] = true
	}

	selected, err := selectedRowsColumns(columns, allowed, options.Columns)
	if err != nil {
		return RowsResult{}, err
	}

	where, args, err := rowsWhereClause("tasks_grid", taskGridSearchColumns, allowed, options.Search, options.Filters)
	if err != nil {
		return RowsResult{}, err
	}

	var total int64
	if options.ExactTotal || (options.EstimateTotal && where == "") {
		countQuery := fmt.Sprintf("SELECT count(*) FROM %s%s", source, where)
		if err := db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
			return RowsResult{}, err
		}
	}

	orderBy, err := taskGridOrderBy(allowed, options.SortColumn, options.SortDirection)
	if err != nil {
		return RowsResult{}, err
	}

	args = append(args, limit, options.Offset)
	query := fmt.Sprintf("SELECT %s FROM %s%s%s LIMIT $%d OFFSET $%d", strings.Join(quoteIdents(selected), ", "), source, where, orderBy, len(args)-1, len(args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return RowsResult{}, err
	}
	defer rows.Close()

	result := RowsResult{Table: "tasks", Columns: selected, Rows: []map[string]any{}, Total: total, Offset: options.Offset, Limit: limit}
	for rows.Next() {
		values := make([]any, len(selected))
		pointers := make([]any, len(selected))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}
		row := map[string]any{}
		for index, column := range selected {
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

// taskGridOrderBy orders the denormalized rows with a unique id tiebreaker so
// offset pagination stays stable even when the sort column (or created_at) is
// null across rows.
func taskGridOrderBy(allowed map[string]bool, sortColumn string, sortDirection string) (string, error) {
	if sortColumn != "" {
		if !allowed[sortColumn] || !validIdent(sortColumn) {
			return "", fmt.Errorf("invalid sort column %q", sortColumn)
		}
		direction := "ASC"
		if strings.EqualFold(sortDirection, "desc") {
			direction = "DESC"
		}
		return fmt.Sprintf(" ORDER BY %s %s NULLS LAST, %s DESC", quoteIdent(sortColumn), direction, quoteIdent("id")), nil
	}
	return fmt.Sprintf(" ORDER BY %s DESC NULLS LAST, %s DESC", quoteIdent("created_at"), quoteIdent("id")), nil
}

// taskGridSource builds the denormalized projection of the tasks table as a
// subquery. Joins and join-dependent columns are dropped when their reference
// table is missing, and soft-deleted rows are excluded when the column exists.
func taskGridSource(columns []taskGridColumn, joins map[string]resolvedTaskGridJoin, hasDeletedAt bool) string {
	projection := make([]string, len(columns))
	for index, column := range columns {
		projection[index] = fmt.Sprintf("%s AS %s", column.expr, quoteIdent(column.name))
	}

	var joinSQL strings.Builder
	for _, join := range taskGridJoins {
		resolved := joins[join.alias]
		if resolved.available {
			joinSQL.WriteString(fmt.Sprintf(
				" LEFT JOIN %s %s ON %s.%s = t.%s",
				quoteIdent(join.table),
				quoteIdent(join.alias),
				quoteIdent(join.alias),
				quoteIdent(resolved.targetColumn),
				quoteIdent(join.taskColumn),
			))
		}
	}

	where := ""
	if hasDeletedAt {
		where = ` WHERE t."deletedAt" IS NULL`
	}

	return fmt.Sprintf("(SELECT %s FROM %s t%s%s) g", strings.Join(projection, ", "), quoteIdent("tasks"), joinSQL.String(), where)
}

func taskGridTableColumnSets(ctx context.Context, db *sql.DB, tables map[string]bool) (map[string]map[string]bool, error) {
	sets := map[string]map[string]bool{}
	for _, join := range taskGridJoins {
		if !tables[join.table] {
			continue
		}
		columns, err := tableColumns(ctx, db, join.table)
		if err != nil {
			return nil, err
		}
		set := map[string]bool{}
		for _, column := range columns {
			set[column] = true
		}
		sets[join.table] = set
	}
	return sets, nil
}

func resolveTaskGridJoins(tables map[string]bool, tableColumnSets map[string]map[string]bool, taskColumns map[string]bool) map[string]resolvedTaskGridJoin {
	joins := map[string]resolvedTaskGridJoin{}
	for _, join := range taskGridJoins {
		columns := tableColumnSets[join.table]
		targetColumn := relationIDColumn(columns)
		joins[join.alias] = resolvedTaskGridJoin{
			taskGridJoin: join,
			columns:      columns,
			targetColumn: targetColumn,
			available:    tables[join.table] && taskColumns[join.taskColumn] && targetColumn != "",
		}
	}
	return joins
}

func relationIDColumn(columns map[string]bool) string {
	if columns["id"] {
		return "id"
	}
	if columns["_id"] {
		return "_id"
	}
	return ""
}

func taskColumnExpr(columns map[string]bool, fallback string, candidates ...string) string {
	for _, candidate := range candidates {
		if columns[candidate] {
			return "t." + quoteIdent(candidate)
		}
	}
	return fallback
}

func joinColumnExpr(join resolvedTaskGridJoin, fallback string, candidates ...string) string {
	if !join.available {
		return fallback
	}
	for _, candidate := range candidates {
		if join.columns[candidate] {
			return quoteIdent(join.alias) + "." + quoteIdent(candidate)
		}
	}
	return fallback
}

func textExpr(expr string, fallback string) string {
	if expr == "" {
		return fallback
	}
	return fmt.Sprintf("COALESCE(%s::text, %s)", expr, fallback)
}

func publicTableSet(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	set := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		set[name] = true
	}
	return set, rows.Err()
}
