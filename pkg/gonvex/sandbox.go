package gonvex

import "context"

type SandboxMode string

const (
	SandboxModeAnalysis SandboxMode = "analysis"
	SandboxModePreview  SandboxMode = "preview"
	SandboxModeApply    SandboxMode = "apply"
)

type SandboxFileRef struct {
	FileKey   string `json:"fileKey"`
	TableName string `json:"tableName,omitempty"`
}

type SandboxLimits struct {
	TimeoutMs       int64 `json:"timeoutMs,omitempty"`
	MemoryBytes     int64 `json:"memoryBytes,omitempty"`
	MaxOutputBytes  int64 `json:"maxOutputBytes,omitempty"`
	MaxRowsReturned int   `json:"maxRowsReturned,omitempty"`
}

type GoSandboxRequest struct {
	SessionID string           `json:"sessionId,omitempty"`
	Purpose   string           `json:"purpose"`
	Mode      SandboxMode      `json:"mode,omitempty"`
	Code      string           `json:"code"`
	Files     []SandboxFileRef `json:"files,omitempty"`
	Env       map[string]any   `json:"env,omitempty"`
	Limits    SandboxLimits    `json:"limits,omitempty"`
}

type SandboxLogLine struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message"`
}

type GoSandboxResult struct {
	OK         bool             `json:"ok"`
	Summary    string           `json:"summary,omitempty"`
	Result     map[string]any   `json:"result,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
	Logs       []SandboxLogLine `json:"logs,omitempty"`
	Error      string           `json:"error,omitempty"`
	DurationMs int64            `json:"durationMs,omitempty"`
}

type SandboxAPI interface {
	RunGo(ctx context.Context, req GoSandboxRequest) (GoSandboxResult, error)
}

type sandboxUnavailable struct{}

func (sandboxUnavailable) RunGo(context.Context, GoSandboxRequest) (GoSandboxResult, error) {
	return GoSandboxResult{OK: false, Error: "gonvex: sandbox runtime is not configured"}, nil
}

func UnavailableSandbox() SandboxAPI {
	return sandboxUnavailable{}
}
