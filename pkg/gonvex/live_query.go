package gonvex

// LiveQueryPlan is the structured, inspectable query contract emitted by a
// TypeScript module artifact. The host uses it to compile PostgreSQL windows
// and to route changes without executing application code in Go.
type LiveQueryPlan struct {
	Table      string          `json:"table"`
	Key        string          `json:"key"`
	Columns    []string        `json:"columns,omitempty"`
	ResultPath []string        `json:"resultPath,omitempty"`
	Where      *LiveExpression `json:"where,omitempty"`
	Search     *LiveSearch     `json:"search,omitempty"`
	Filters    *LiveFilters    `json:"filters,omitempty"`
	Sort       *LiveSort       `json:"sort,omitempty"`
	Window     *LiveWindow     `json:"window,omitempty"`
	ServerOnly bool            `json:"serverOnly,omitempty"`
}

type FilterOperator string

type LiveFilters struct {
	Argument         string           `json:"argument"`
	AllowedColumns   []string         `json:"allowedColumns"`
	AllowedOperators []FilterOperator `json:"allowedOperators"`
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
	Count          string `json:"count,omitempty"`
}
