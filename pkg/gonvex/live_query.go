package gonvex

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// LiveQueryPlan is the structured, inspectable query surface used by Live
// Queries. Gonvex compiles the same plan to PostgreSQL online and clients can
// evaluate its portable operators against cached replica rows offline.
type LiveQueryPlan struct {
	Table      string          `json:"table"`
	Key        string          `json:"key"`
	Columns    []string        `json:"columns,omitempty"`
	ResultPath []string        `json:"resultPath,omitempty"`
	Where      *LiveExpression `json:"where,omitempty"`
	Search     *LiveSearch     `json:"search,omitempty"`
	Sort       *LiveSort       `json:"sort,omitempty"`
	Window     *LiveWindow     `json:"window,omitempty"`
	ServerOnly bool            `json:"serverOnly,omitempty"`
}

type LiveExpression struct {
	Operator string            `json:"operator"`
	Column   string            `json:"column,omitempty"`
	Value    *LiveValue        `json:"value,omitempty"`
	ValueTo  *LiveValue        `json:"valueTo,omitempty"`
	Children []*LiveExpression `json:"children,omitempty"`
}

type LiveValue struct {
	Argument string `json:"argument,omitempty"`
	Literal  any    `json:"literal,omitempty"`
}

type LiveSearch struct {
	Argument string   `json:"argument"`
	Columns  []string `json:"columns"`
}

type LiveSort struct {
	ColumnArgument    string   `json:"columnArgument,omitempty"`
	DirectionArgument string   `json:"directionArgument,omitempty"`
	AllowedColumns    []string `json:"allowedColumns"`
	DefaultColumn     string   `json:"defaultColumn"`
	DefaultDirection  string   `json:"defaultDirection"`
}

type LiveWindow struct {
	OffsetArgument string `json:"offsetArgument"`
	LimitArgument  string `json:"limitArgument"`
	DefaultLimit   int    `json:"defaultLimit"`
	MaxLimit       int    `json:"maxLimit"`
}

type CompiledLiveQuery struct {
	SQL       string
	Arguments []any
	Portable  bool
	Limit     int
	Offset    int
}

func LiveTable(table string) *LiveQueryPlan {
	return &LiveQueryPlan{Table: strings.TrimSpace(table), Key: "id"}
}

func (p *LiveQueryPlan) EntityKey(column string) *LiveQueryPlan {
	p.Key = strings.TrimSpace(column)
	return p
}

func (p *LiveQueryPlan) Select(columns ...string) *LiveQueryPlan {
	p.Columns = cleanDependencyNames(columns)
	return p
}

// ResultRowsAt identifies the array in the Query result that owns membership
// and ordering. Omit it when the result itself is the row array.
func (p *LiveQueryPlan) ResultRowsAt(path ...string) *LiveQueryPlan {
	p.ResultPath = cleanDependencyNames(path)
	return p
}

func (p *LiveQueryPlan) Filter(expression *LiveExpression) *LiveQueryPlan {
	p.Where = expression
	return p
}

func (p *LiveQueryPlan) SearchArg(argument string, columns ...string) *LiveQueryPlan {
	p.Search = &LiveSearch{Argument: strings.TrimSpace(argument), Columns: cleanDependencyNames(columns)}
	return p
}

func (p *LiveQueryPlan) SortArgs(columnArgument, directionArgument, defaultColumn, defaultDirection string, allowed ...string) *LiveQueryPlan {
	p.Sort = &LiveSort{
		ColumnArgument: strings.TrimSpace(columnArgument), DirectionArgument: strings.TrimSpace(directionArgument),
		AllowedColumns: cleanDependencyNames(allowed), DefaultColumn: strings.TrimSpace(defaultColumn),
		DefaultDirection: normalizeSortDirection(defaultDirection),
	}
	return p
}

func (p *LiveQueryPlan) WindowArgs(offsetArgument, limitArgument string, defaultLimit, maxLimit int) *LiveQueryPlan {
	if defaultLimit <= 0 {
		defaultLimit = 100
	}
	if maxLimit < defaultLimit {
		maxLimit = defaultLimit
	}
	p.Window = &LiveWindow{OffsetArgument: strings.TrimSpace(offsetArgument), LimitArgument: strings.TrimSpace(limitArgument), DefaultLimit: defaultLimit, MaxLimit: maxLimit}
	return p
}

func (p *LiveQueryPlan) OnlineOnly() *LiveQueryPlan {
	p.ServerOnly = true
	return p
}

func Arg(name string) *LiveValue   { return &LiveValue{Argument: strings.TrimSpace(name)} }
func Literal(value any) *LiveValue { return &LiveValue{Literal: value} }
func Eq(column string, value *LiveValue) *LiveExpression {
	return liveComparison("eq", column, value, nil)
}
func Neq(column string, value *LiveValue) *LiveExpression {
	return liveComparison("neq", column, value, nil)
}
func GreaterThan(column string, value *LiveValue) *LiveExpression {
	return liveComparison("gt", column, value, nil)
}
func GreaterOrEqual(column string, value *LiveValue) *LiveExpression {
	return liveComparison("gte", column, value, nil)
}
func LessThan(column string, value *LiveValue) *LiveExpression {
	return liveComparison("lt", column, value, nil)
}
func LessOrEqual(column string, value *LiveValue) *LiveExpression {
	return liveComparison("lte", column, value, nil)
}
func Contains(column string, value *LiveValue) *LiveExpression {
	return liveComparison("contains", column, value, nil)
}
func ContainsInsensitive(column string, value *LiveValue) *LiveExpression {
	return liveComparison("containsInsensitive", column, value, nil)
}
func In(column string, value *LiveValue) *LiveExpression {
	return liveComparison("in", column, value, nil)
}
func Range(column string, from, through *LiveValue) *LiveExpression {
	return liveComparison("range", column, from, through)
}
func All(expressions ...*LiveExpression) *LiveExpression { return liveBoolean("and", expressions) }
func Any(expressions ...*LiveExpression) *LiveExpression { return liveBoolean("or", expressions) }
func Not(expression *LiveExpression) *LiveExpression {
	return liveBoolean("not", []*LiveExpression{expression})
}

func ServerExpression(sql string) *LiveExpression {
	return &LiveExpression{Operator: "serverOnly", Value: Literal(strings.TrimSpace(sql))}
}

func liveComparison(operator, column string, value, valueTo *LiveValue) *LiveExpression {
	return &LiveExpression{Operator: operator, Column: strings.TrimSpace(column), Value: value, ValueTo: valueTo}
}

func liveBoolean(operator string, expressions []*LiveExpression) *LiveExpression {
	children := make([]*LiveExpression, 0, len(expressions))
	for _, expression := range expressions {
		if expression != nil {
			children = append(children, expression)
		}
	}
	return &LiveExpression{Operator: operator, Children: children}
}

// Compile produces parameterized PostgreSQL. Identifiers must be declared in
// the plan and values always travel as bind parameters.
func (p *LiveQueryPlan) Compile(arguments map[string]any) (CompiledLiveQuery, error) {
	if p == nil || !validPlanIdentifier(p.Table) || !validPlanIdentifier(p.Key) {
		return CompiledLiveQuery{}, fmt.Errorf("gonvex: Live Query requires a valid table and entity key")
	}
	columns := append([]string(nil), p.Columns...)
	if len(columns) == 0 {
		columns = []string{"*"}
	} else {
		for _, column := range columns {
			if !validPlanIdentifier(column) {
				return CompiledLiveQuery{}, fmt.Errorf("gonvex: invalid Live Query column %q", column)
			}
		}
	}
	state := liveCompiler{arguments: arguments, portable: !p.ServerOnly}
	where := make([]string, 0, 2)
	if p.Where != nil {
		expression, err := state.expression(p.Where)
		if err != nil {
			return CompiledLiveQuery{}, err
		}
		where = append(where, expression)
	}
	if p.Search != nil {
		value := strings.TrimSpace(fmt.Sprint(arguments[p.Search.Argument]))
		if value != "" {
			parts := make([]string, 0, len(p.Search.Columns))
			for _, column := range p.Search.Columns {
				if !validPlanIdentifier(column) {
					return CompiledLiveQuery{}, fmt.Errorf("gonvex: invalid search column %q", column)
				}
				parts = append(parts, quotePlanIdentifier(column)+" ILIKE "+state.bind("%"+value+"%"))
			}
			if len(parts) > 0 {
				where = append(where, "("+strings.Join(parts, " OR ")+")")
			}
		}
	}
	selected := "*"
	if columns[0] != "*" {
		quoted := make([]string, len(columns))
		for index, column := range columns {
			quoted[index] = quotePlanIdentifier(column)
		}
		selected = strings.Join(quoted, ", ")
	}
	sql := "SELECT " + selected + " FROM " + quotePlanIdentifier(p.Table)
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	if p.Sort != nil {
		column := strings.TrimSpace(fmt.Sprint(arguments[p.Sort.ColumnArgument]))
		if !containsPlanString(p.Sort.AllowedColumns, column) {
			column = p.Sort.DefaultColumn
		}
		if !validPlanIdentifier(column) {
			return CompiledLiveQuery{}, fmt.Errorf("gonvex: invalid sort column %q", column)
		}
		direction := normalizeSortDirection(fmt.Sprint(arguments[p.Sort.DirectionArgument]))
		if strings.TrimSpace(fmt.Sprint(arguments[p.Sort.DirectionArgument])) == "" {
			direction = normalizeSortDirection(p.Sort.DefaultDirection)
		}
		sql += " ORDER BY " + quotePlanIdentifier(column) + " " + strings.ToUpper(direction) + ", " + quotePlanIdentifier(p.Key) + " " + strings.ToUpper(direction)
	}
	limit, offset := 0, 0
	if p.Window != nil {
		limit = planInt(arguments[p.Window.LimitArgument], p.Window.DefaultLimit)
		if limit <= 0 {
			limit = p.Window.DefaultLimit
		}
		if limit > p.Window.MaxLimit {
			limit = p.Window.MaxLimit
		}
		offset = planInt(arguments[p.Window.OffsetArgument], 0)
		if offset < 0 {
			offset = 0
		}
		sql += " LIMIT " + state.bind(limit) + " OFFSET " + state.bind(offset)
	}
	return CompiledLiveQuery{SQL: sql, Arguments: state.values, Portable: state.portable, Limit: limit, Offset: offset}, nil
}

type liveCompiler struct {
	arguments map[string]any
	values    []any
	portable  bool
}

func (c *liveCompiler) bind(value any) string {
	c.values = append(c.values, value)
	return fmt.Sprintf("$%d", len(c.values))
}
func (c *liveCompiler) value(value *LiveValue) any {
	if value == nil {
		return nil
	}
	if value.Argument != "" {
		return c.arguments[value.Argument]
	}
	return value.Literal
}
func (c *liveCompiler) expression(expression *LiveExpression) (string, error) {
	if expression == nil {
		return "TRUE", nil
	}
	if expression.Operator == "serverOnly" {
		c.portable = false
		raw, _ := expression.Value.Literal.(string)
		if strings.TrimSpace(raw) == "" {
			return "", fmt.Errorf("gonvex: empty server-only expression")
		}
		return "(" + raw + ")", nil
	}
	if expression.Operator == "and" || expression.Operator == "or" || expression.Operator == "not" {
		parts := make([]string, 0, len(expression.Children))
		for _, child := range expression.Children {
			part, err := c.expression(child)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		if expression.Operator == "not" {
			if len(parts) != 1 {
				return "", fmt.Errorf("gonvex: not requires one expression")
			}
			return "NOT (" + parts[0] + ")", nil
		}
		if len(parts) == 0 {
			return "TRUE", nil
		}
		return "(" + strings.Join(parts, " "+strings.ToUpper(expression.Operator)+" ") + ")", nil
	}
	if !validPlanIdentifier(expression.Column) {
		return "", fmt.Errorf("gonvex: invalid filter column %q", expression.Column)
	}
	column := quotePlanIdentifier(expression.Column)
	value := c.value(expression.Value)
	switch expression.Operator {
	case "eq":
		return column + " = " + c.bind(value), nil
	case "neq":
		return column + " <> " + c.bind(value), nil
	case "gt", "gte", "lt", "lte":
		operators := map[string]string{"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
		return column + " " + operators[expression.Operator] + " " + c.bind(value), nil
	case "contains":
		return column + " LIKE " + c.bind("%"+fmt.Sprint(value)+"%"), nil
	case "containsInsensitive":
		return column + " ILIKE " + c.bind("%"+fmt.Sprint(value)+"%"), nil
	case "in":
		return column + " = ANY(" + c.bind(value) + ")", nil
	case "range":
		return column + " BETWEEN " + c.bind(value) + " AND " + c.bind(c.value(expression.ValueTo)), nil
	default:
		return "", fmt.Errorf("gonvex: unsupported Live Query operator %q", expression.Operator)
	}
}

func normalizeSortDirection(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "asc") {
		return "asc"
	}
	return "desc"
}
func containsPlanString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
func validPlanIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || index > 0 && char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
func quotePlanIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
func planInt(value any, fallback int) int {
	switch current := value.(type) {
	case int:
		return current
	case float64:
		return int(current)
	case json.Number:
		parsed, err := current.Int64()
		if err == nil {
			return int(parsed)
		}
	}
	return fallback
}

func (p *LiveQueryPlan) dependency() ReadDependency {
	columns := append([]string(nil), p.Columns...)
	filters := planExpressionColumns(p.Where)
	orders := []string{}
	if p.Search != nil {
		filters = append(filters, p.Search.Columns...)
	}
	if p.Sort != nil {
		orders = append(orders, p.Sort.AllowedColumns...)
	}
	return ReadDependency{Table: p.Table, Columns: cleanDependencyNames(columns), Filters: cleanDependencyNames(filters), OrdersBy: cleanDependencyNames(orders), Windowed: p.Window != nil}
}

func planExpressionColumns(expression *LiveExpression) []string {
	if expression == nil {
		return nil
	}
	values := []string{}
	if expression.Column != "" {
		values = append(values, expression.Column)
	}
	for _, child := range expression.Children {
		values = append(values, planExpressionColumns(child)...)
	}
	sort.Strings(values)
	return values
}

type livePlanOption struct{ plan *LiveQueryPlan }

func LivePlan(plan *LiveQueryPlan) FunctionOption { return livePlanOption{plan: plan} }
func (option livePlanOption) applyFunctionOption(target *FunctionDependencies) {
	if option.plan == nil {
		return
	}
	target.LiveQueryPlan = option.plan
	target.Reads = append(target.Reads, option.plan.dependency())
}
