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
	Args         any                          `json:"args,omitempty"`
	Result       any                          `json:"result,omitempty"`
	Internal     bool                         `json:"internal,omitempty"`
	Delivery     DeliveryMode                 `json:"delivery,omitempty"`
	Dependencies FunctionDependencies         `json:"dependencies,omitempty"`
	Replica      *ReplicaCollectionDefinition `json:"replica,omitempty"`
	Offline      any                          `json:"offline,omitempty"`
	Optimistic   any                          `json:"optimistic,omitempty"`
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
	Tables            map[string]Table `json:"tables"`
	ControlPlaneTables map[string]Table `json:"controlPlaneTables,omitempty"`
	// LandlordTables is the legacy wire/source name for ControlPlaneTables.
	// Normalize aliases both fields to the same map so callers cannot observe
	// two divergent control-plane schemas during the terminology migration.
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

// ModuleArtifact is the language-neutral module payload emitted by the
// TypeScript CLI. The fields describe the artifact rather than a runtime
// implementation so other module languages can use the same wire shape.
type ModuleArtifact struct {
	Language     string                    `json:"language"`
	Generation   int                       `json:"generation"`
	Hash         string                    `json:"hash,omitempty"`
	// ArtifactHash is a compatibility alias accepted from older deployment
	// payloads. Hash is the canonical wire name.
	ArtifactHash string                    `json:"artifactHash,omitempty"`
	Entrypoint   string                    `json:"entrypoint"`
	Functions    map[string]ModuleFunction `json:"functions"`
	Files        map[string]string         `json:"files"`
	JavaScript   *ModuleJavaScript         `json:"javascript,omitempty"`
}

// ModuleFunction carries generated function metadata and opaque schema and
// policy values. The Go CLI retains these values without interpreting them.
type ModuleFunction struct {
	Kind         FunctionKind                 `json:"kind"`
	Handler      string                       `json:"handler"`
	File         string                       `json:"file"`
	Export       string                       `json:"export,omitempty"`
	Args         any                          `json:"args,omitempty"`
	Result       any                          `json:"result,omitempty"`
	Dependencies FunctionDependencies         `json:"dependencies,omitempty"`
	Internal     bool                         `json:"internal,omitempty"`
	Delivery     DeliveryMode                 `json:"delivery,omitempty"`
	Replica      *ReplicaCollectionDefinition `json:"replica,omitempty"`
	Offline      any                          `json:"offline,omitempty"`
	Optimistic   any                          `json:"optimistic,omitempty"`
}

type ModuleJavaScript struct {
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	Code      string `json:"code"`
	SourceMap string `json:"sourceMap,omitempty"`
}

type Manifest struct {
	Project             string                   `json:"project"`
	GeneratedAt         string                   `json:"generatedAt"`
	Functions           map[string]FunctionEntry `json:"functions"`
	Schema              Schema                   `json:"schema"`
	Bundle              *SourceBundle            `json:"bundle,omitempty"`
	Module              *ModuleArtifact          `json:"module,omitempty"`
	NotifySchemaVersion string                   `json:"notifySchemaVersion,omitempty"`
}

func EmptySchema() Schema {
	controlPlaneTables := map[string]Table{}
	tenantTables := map[string]Table{}
	return Schema{
		Tables:            tenantTables,
		ControlPlaneTables: controlPlaneTables,
		LandlordTables:    controlPlaneTables,
		TenantTables:      tenantTables,
	}
}

func (s Schema) Normalize() Schema {
	// ControlPlaneTables is canonical. During the wire migration, accept the
	// old landlordTables spelling and merge both spellings into one map when a
	// payload contains both. Preferred terminology wins on key collisions.
	controlPlaneTables := s.ControlPlaneTables
	if controlPlaneTables == nil {
		controlPlaneTables = s.LandlordTables
	}
	if controlPlaneTables == nil {
		controlPlaneTables = map[string]Table{}
	}
	if s.ControlPlaneTables != nil && s.LandlordTables != nil && !sameTableMap(s.ControlPlaneTables, s.LandlordTables) {
		merged := make(map[string]Table, len(s.LandlordTables)+len(s.ControlPlaneTables))
		for name, table := range s.LandlordTables {
			merged[name] = table
		}
		for name, table := range s.ControlPlaneTables {
			merged[name] = table
		}
		controlPlaneTables = merged
	}
	s.ControlPlaneTables = controlPlaneTables
	s.LandlordTables = controlPlaneTables

	if s.TenantTables == nil {
		if s.Tables == nil {
			s.TenantTables = map[string]Table{}
		} else {
			s.TenantTables = s.Tables
		}
	}
	s.Tables = s.TenantTables
	return s
}

// Normalize applies compatibility aliases to a module artifact while
// retaining both spellings for callers that still consume artifactHash.
func (a ModuleArtifact) Normalize() ModuleArtifact {
	if a.Hash == "" {
		a.Hash = a.ArtifactHash
	}
	if a.ArtifactHash == "" {
		a.ArtifactHash = a.Hash
	}
	return a
}

// Normalize resolves schema and module compatibility aliases. It is safe to
// call after JSON unmarshalling or before emitting a generated binding.
func (m Manifest) Normalize() Manifest {
	m.Schema = m.Schema.Normalize()
	if m.Module != nil {
		normalized := m.Module.Normalize()
		m.Module = &normalized
	}
	return m
}

// ControlPlaneSchema returns the control-plane portion of the schema.
func (s Schema) ControlPlaneSchema() Schema {
	s = s.Normalize()
	return Schema{Tables: s.ControlPlaneTables}
}

// LandlordSchema is retained as a source-compatible alias for
// ControlPlaneSchema. New code should use ControlPlaneSchema.
func (s Schema) LandlordSchema() Schema {
	return s.ControlPlaneSchema()
}

func (s Schema) TenantSchema() Schema {
	s = s.Normalize()
	if s.TenantTables == nil {
		return Schema{Tables: s.Tables}
	}
	return Schema{Tables: s.TenantTables}
}

func sameTableMap(left, right map[string]Table) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftTable := range left {
		rightTable, ok := right[name]
		if !ok || !sameTable(leftTable, rightTable) {
			return false
		}
	}
	return true
}

func sameTable(left, right Table) bool {
	if len(left.Columns) != len(right.Columns) || len(left.Indexes) != len(right.Indexes) {
		return false
	}
	for name, leftColumn := range left.Columns {
		if right.Columns[name] != leftColumn {
			return false
		}
	}
	for name, leftIndex := range left.Indexes {
		rightIndex, ok := right.Indexes[name]
		if !ok || leftIndex.Unique != rightIndex.Unique || leftIndex.Kind != rightIndex.Kind || !sameStrings(leftIndex.Columns, rightIndex.Columns) {
			return false
		}
	}
	return true
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
