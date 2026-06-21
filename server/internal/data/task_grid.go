package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// normalizedTaskColumn marks the normalized Whagons tenant schema (camelCase
// foreign keys such as statusId); flatTaskColumn marks the denormalized mirror
// the dashboard project database stores directly.
const (
	normalizedTaskColumn = "statusId"
	flatTaskColumn       = "status_name"
)

// taskGridColumn maps a denormalized grid column to the SQL expression that
// produces it from the normalized task schema. dep names the reference-table
// alias the expression needs; when that table is absent the column resolves to
// NULL so selection never fails on partially seeded tenants.
type taskGridColumn struct {
	name string
	expr string
	dep  string
}

// taskGridProjection lists every column the dashboard "tasks.grid" surface can
// request. Keeping the set complete means column selection always succeeds; the
// grid renderer simply sees empty values where a tenant has no source data.
var taskGridProjection = []taskGridColumn{
	{name: "id", expr: `t."id"`},
	{name: "pg_id", expr: `t."pgId"`},
	{name: "name", expr: `t."name"`},
	{name: "title", expr: `COALESCE(t."name", '')`},
	{name: "description", expr: `t."description"`},
	{name: "form_id", expr: `t."formId"`},
	{name: "sla_id", expr: `t."slaId"`},
	{name: "approval_id", expr: `t."approvalId"`},
	{name: "notes_count", expr: `0`},
	{name: "category_icon", expr: `c."icon"`, dep: "c"},
	{name: "category_color", expr: `c."color"`, dep: "c"},
	{name: "category_name", expr: `c."name"`, dep: "c"},
	{name: "tag_names", expr: `''`},
	{name: "tag_colors", expr: `''`},
	{name: "attachment_count", expr: `0`},
	{name: "view_count", expr: `0`},
	{name: "status", expr: `COALESCE(s."name", '')`, dep: "s"},
	{name: "status_name", expr: `s."name"`, dep: "s"},
	{name: "status_color", expr: `s."color"`, dep: "s"},
	{name: "status_action", expr: `s."action"`, dep: "s"},
	{name: "status_icon", expr: `s."icon"`, dep: "s"},
	{name: "status_working_animation", expr: `''`},
	{name: "status_initial", expr: `s."initial"`, dep: "s"},
	{name: "priority", expr: `COALESCE(p."name", '')`, dep: "p"},
	{name: "priority_name", expr: `p."name"`, dep: "p"},
	{name: "priority_color", expr: `p."color"`, dep: "p"},
	{name: "assignee", expr: `''`},
	{name: "assignee_names", expr: `''`},
	{name: "assignee_ids", expr: `COALESCE(t."userIds"::text, '')`},
	{name: "assignee_avatar_urls", expr: `''`},
	{name: "all_user_names", expr: `''`},
	{name: "all_user_avatar_urls", expr: `''`},
	{name: "due_date", expr: `t."dueDate"`},
	{name: "due_at", expr: `t."dueDate"`},
	{name: "start_date", expr: `t."startDate"`},
	{name: "spot_id", expr: `t."spotId"`},
	{name: "spot_name", expr: `sp."name"`, dep: "sp"},
	{name: "workspace_name", expr: `w."name"`, dep: "w"},
	{name: "progress", expr: `0`},
	{name: "flag_color", expr: `t."flagColor"`},
	{name: "created_at", expr: `t."createdAt"`},
	{name: "updated_at", expr: `t."updatedAt"`},
}

// taskGridJoin is a reference table LEFT JOINed onto the tasks table; joins are
// emitted only when the table exists.
type taskGridJoin struct {
	alias string
	table string
	on    string
}

var taskGridJoins = []taskGridJoin{
	{alias: "s", table: "statuses", on: `s."id" = t."statusId"`},
	{alias: "p", table: "priorities", on: `p."id" = t."priorityId"`},
	{alias: "c", table: "categories", on: `c."id" = t."categoryId"`},
	{alias: "sp", table: "spots", on: `sp."id" = t."spotId"`},
	{alias: "w", table: "workspaces", on: `w."id" = t."workspaceId"`},
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
	if taskColumns[flatTaskColumn] || !taskColumns[normalizedTaskColumn] {
		return ReadRows(ctx, databaseURL, "tasks", options)
	}
	return readNormalizedTaskGrid(ctx, db, taskColumns, options)
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
	source := taskGridSource(tables, taskColumns["deletedAt"])

	columns := make([]string, len(taskGridProjection))
	allowed := map[string]bool{}
	for index, column := range taskGridProjection {
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
func taskGridSource(tables map[string]bool, hasDeletedAt bool) string {
	available := map[string]bool{}
	for _, join := range taskGridJoins {
		available[join.alias] = tables[join.table]
	}

	projection := make([]string, len(taskGridProjection))
	for index, column := range taskGridProjection {
		if column.dep != "" && !available[column.dep] {
			projection[index] = fmt.Sprintf("NULL AS %s", quoteIdent(column.name))
			continue
		}
		projection[index] = fmt.Sprintf("%s AS %s", column.expr, quoteIdent(column.name))
	}

	var joins strings.Builder
	for _, join := range taskGridJoins {
		if available[join.alias] {
			joins.WriteString(fmt.Sprintf(" LEFT JOIN %s %s ON %s", quoteIdent(join.table), join.alias, join.on))
		}
	}

	where := ""
	if hasDeletedAt {
		where = ` WHERE t."deletedAt" IS NULL`
	}

	return fmt.Sprintf("(SELECT %s FROM %s t%s%s) g", strings.Join(projection, ", "), quoteIdent("tasks"), joins.String(), where)
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
