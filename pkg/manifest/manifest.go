package manifest

type FunctionKind string

const (
	FunctionKindQuery   FunctionKind = "query"
	FunctionKindReducer FunctionKind = "reducer"
	FunctionKindAction  FunctionKind = "action"
	FunctionKindHTTP    FunctionKind = "http"
)

type DeliveryMode string

const (
	DeliveryOneShot DeliveryMode = "oneShot"
	DeliveryLive    DeliveryMode = "live"
	DeliveryReplica DeliveryMode = "replica"
)

const NotifySchemaVersion = "15"

type FunctionEntry struct {
	Kind         FunctionKind                 `json:"kind"`
	Handler      string                       `json:"handler"`
	File         string                       `json:"file"`
	Internal     bool                         `json:"internal,omitempty"`
	Delivery     DeliveryMode                 `json:"delivery,omitempty"`
	Dependencies FunctionDependencies         `json:"dependencies,omitempty"`
	Replica      *ReplicaCollectionDefinition `json:"replica,omitempty"`
}

// ReplicaCollectionDefinition describes an entity-shaped, locally materialized collection.
// V1 intentionally supports a single source table and equality filters. More
// complex joins and aggregates remain ordinary live queries.
type ReplicaCollectionDefinition struct {
	Table                 string            `json:"table"`
	Key                   string            `json:"key"`
	Columns               []string          `json:"columns"`
	EqualFilters          map[string]string `json:"equalFilters,omitempty"`
	ExcludeWhenSet        []string          `json:"excludeWhenSet,omitempty"`
	VisibilityTables      []string          `json:"visibilityTables,omitempty"`
	OrderBy               string            `json:"orderBy,omitempty"`
	OrderDirection        string            `json:"orderDirection,omitempty"`
	Mode                  string            `json:"mode,omitempty"`
	MaxRows               int               `json:"maxRows,omitempty"`
	MaxBytes              int64             `json:"maxBytes,omitempty"`
	RetentionMilliseconds int64             `json:"retentionMs,omitempty"`
}

// FunctionDependencies contain generated, inspectable delivery contracts.
// Live Queries without a structured plan are rejected rather than broadly
// invalidated.
type FunctionDependencies struct {
	Reads                []ReadDependency                `json:"reads,omitempty"`
	ShareByPermissions   bool                            `json:"shareByPermissions,omitempty"`
	ShareByVisibility    string                          `json:"shareByVisibility,omitempty"`
	ShareResultFrom      string                          `json:"shareResultFrom,omitempty"`
	ShareResultField     string                          `json:"shareResultField,omitempty"`
	OptimisticReducer    *OptimisticReducerDefinition    `json:"optimisticReducer,omitempty"`
	OptimisticProjection *OptimisticProjectionDefinition `json:"optimisticProjection,omitempty"`
	LiveQueryPlan        *LiveQueryPlan                  `json:"liveQueryPlan,omitempty"`
	NonOptimisticReason  string                          `json:"nonOptimisticReason,omitempty"`
}

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

type OptimisticReducerDefinition struct {
	Entity     string   `json:"entity"`
	RowIDPath  []string `json:"rowIdPath"`
	FieldsPath []string `json:"fieldsPath"`
}

type OptimisticProjectionDefinition struct {
	Entity     string   `json:"entity"`
	Key        string   `json:"key"`
	ResultPath []string `json:"resultPath"`
}

type ReadDependency struct {
	Table    string   `json:"table"`
	Columns  []string `json:"columns,omitempty"`
	Filters  []string `json:"filters,omitempty"`
	OrdersBy []string `json:"ordersBy,omitempty"`
	Windowed bool     `json:"windowed,omitempty"`
}

type Schema struct {
	Tables         map[string]Table `json:"tables"`
	LandlordTables map[string]Table `json:"landlordTables,omitempty"`
	TenantTables   map[string]Table `json:"tenantTables,omitempty"`
}

type Table struct {
	Columns map[string]Column `json:"columns"`
	Indexes map[string]Index  `json:"indexes"`
}

type Column struct {
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primaryKey"`
}

type Index struct {
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
	Kind    string   `json:"kind,omitempty"`
}

type Manifest struct {
	Project             string                   `json:"project"`
	GeneratedAt         string                   `json:"generatedAt"`
	Functions           map[string]FunctionEntry `json:"functions"`
	Schema              Schema                   `json:"schema"`
	Bundle              *SourceBundle            `json:"bundle,omitempty"`
	NotifySchemaVersion string                   `json:"notifySchemaVersion,omitempty"`
}

func EmptySchema() Schema {
	return Schema{
		Tables:         map[string]Table{},
		LandlordTables: map[string]Table{},
		TenantTables:   map[string]Table{},
	}
}

func (s Schema) Normalize() Schema {
	if s.Tables == nil {
		s.Tables = map[string]Table{}
	}
	if s.LandlordTables == nil && s.TenantTables == nil {
		return s
	}
	if s.LandlordTables == nil {
		s.LandlordTables = map[string]Table{}
	}
	if s.TenantTables == nil {
		s.TenantTables = s.Tables
	}
	s.Tables = s.TenantTables
	return s
}

func (s Schema) LandlordSchema() Schema {
	s = s.Normalize()
	if s.LandlordTables == nil {
		return Schema{Tables: s.Tables}
	}
	return Schema{Tables: s.LandlordTables}
}

func (s Schema) TenantSchema() Schema {
	s = s.Normalize()
	if s.TenantTables == nil {
		return Schema{Tables: s.Tables}
	}
	return Schema{Tables: s.TenantTables}
}
