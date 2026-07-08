package sandboxes

import "context"

type Language string

const (
	LanguageGo         Language = "go"
	LanguageTypeScript Language = "typescript"
)

type Mode string

const (
	ModeAnalysis Mode = "analysis"
	ModePreview  Mode = "preview"
	ModeApply    Mode = "apply"
)

type DatasetRef struct {
	FileKey   string `json:"fileKey"`
	TableName string `json:"tableName,omitempty"`
}

type Limits struct {
	TimeoutMs       int64 `json:"timeoutMs,omitempty"`
	MemoryBytes     int64 `json:"memoryBytes,omitempty"`
	MaxOutputBytes  int64 `json:"maxOutputBytes,omitempty"`
	MaxRowsReturned int   `json:"maxRowsReturned,omitempty"`
}

type RunRequest struct {
	SessionID string         `json:"sessionId,omitempty"`
	Purpose   string         `json:"purpose"`
	Language  Language       `json:"language"`
	Mode      Mode           `json:"mode,omitempty"`
	Code      string         `json:"code"`
	Files     []DatasetRef   `json:"files,omitempty"`
	Env       map[string]any `json:"env,omitempty"`
	Limits    Limits         `json:"limits,omitempty"`
}

type LogLine struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message"`
}

type RunResult struct {
	OK         bool           `json:"ok"`
	Summary    string         `json:"summary,omitempty"`
	Result     map[string]any `json:"result,omitempty"`
	Warnings   []string       `json:"warnings,omitempty"`
	Logs       []LogLine      `json:"logs,omitempty"`
	Error      string         `json:"error,omitempty"`
	DurationMs int64          `json:"durationMs,omitempty"`
}

type InspectRequest struct {
	FileKey   string `json:"fileKey,omitempty"`
	Filename  string `json:"filename,omitempty"`
	Operation string `json:"operation,omitempty"`
	TableName string `json:"tableName,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type InspectResult struct {
	OK       bool             `json:"ok"`
	FileKey  string           `json:"fileKey,omitempty"`
	Summary  string           `json:"summary,omitempty"`
	Tables   []map[string]any `json:"tables,omitempty"`
	Warnings []string         `json:"warnings,omitempty"`
	Error    string           `json:"error,omitempty"`
}

type QueryRequest struct {
	FileKey string `json:"fileKey"`
	SQL     string `json:"sql"`
	Limit   int    `json:"limit,omitempty"`
}

type QueryResult struct {
	OK        bool             `json:"ok"`
	Columns   []string         `json:"columns,omitempty"`
	Rows      []map[string]any `json:"rows,omitempty"`
	RowCount  int              `json:"rowCount,omitempty"`
	Truncated bool             `json:"truncated,omitempty"`
	Warnings  []string         `json:"warnings,omitempty"`
	Error     string           `json:"error,omitempty"`
}

type ProfileRequest struct {
	FileKey    string `json:"fileKey"`
	TableName  string `json:"tableName,omitempty"`
	MaxColumns int    `json:"maxColumns,omitempty"`
}

// ColumnProfile is the per-column statistical summary returned by Profile.
// Numeric stats are only set when the column parses as numeric.
type ColumnProfile struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	NullCount     int64    `json:"nullCount"`
	DistinctCount int64    `json:"distinctCount"`
	Min           any      `json:"min,omitempty"`
	Max           any      `json:"max,omitempty"`
	Mean          *float64 `json:"mean,omitempty"`
	Examples      []string `json:"examples,omitempty"`
}

type TableProfile struct {
	TableName string          `json:"tableName"`
	RowCount  int64           `json:"rowCount"`
	Columns   []ColumnProfile `json:"columns"`
}

type ProfileResult struct {
	OK       bool           `json:"ok"`
	FileKey  string         `json:"fileKey,omitempty"`
	Tables   []TableProfile `json:"tables,omitempty"`
	Warnings []string       `json:"warnings,omitempty"`
	Error    string         `json:"error,omitempty"`
}

type Runner interface {
	Run(ctx context.Context, req RunRequest) (RunResult, error)
}

type DatasetRegistry interface {
	Inspect(ctx context.Context, req InspectRequest) (InspectResult, error)
	Query(ctx context.Context, req QueryRequest) (QueryResult, error)
	Profile(ctx context.Context, req ProfileRequest) (ProfileResult, error)
}
