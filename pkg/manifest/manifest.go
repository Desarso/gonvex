package manifest

type FunctionKind string

const (
	FunctionKindQuery            FunctionKind = "query"
	FunctionKindMutation         FunctionKind = "mutation"
	FunctionKindAction           FunctionKind = "action"
	FunctionKindHTTP             FunctionKind = "http"
	FunctionKindInternalMutation FunctionKind = "internalMutation"
	FunctionKindLiveGrid         FunctionKind = "liveGrid"
)

type FunctionEntry struct {
	Kind    FunctionKind `json:"kind"`
	Handler string       `json:"handler"`
	File    string       `json:"file"`
}

type Schema struct {
	Tables map[string]Table `json:"tables"`
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
}

type Manifest struct {
	Project     string                   `json:"project"`
	GeneratedAt string                   `json:"generatedAt"`
	Functions   map[string]FunctionEntry `json:"functions"`
	Schema      Schema                   `json:"schema"`
}

func EmptySchema() Schema {
	return Schema{Tables: map[string]Table{}}
}
