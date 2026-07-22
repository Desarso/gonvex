package gonvex

import "context"

type SandboxMode string

// SandboxProgressEvent is emitted around each host RPC during RunGo so apps can
// show live process progress for long-running sandboxes.
type SandboxProgressEvent struct {
	At         int64  `json:"at"`
	Phase      string `json:"phase"` // "start" | "end"
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	OK         *bool  `json:"ok,omitempty"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"durationMs,omitempty"`
}

type SandboxProgressFunc func(SandboxProgressEvent)

type sandboxProgressKey struct{}

// ContextWithSandboxProgress attaches a progress reporter for host RPCs.
func ContextWithSandboxProgress(ctx context.Context, fn SandboxProgressFunc) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, sandboxProgressKey{}, fn)
}

// SandboxProgressFromContext returns the progress reporter, if any.
func SandboxProgressFromContext(ctx context.Context) (SandboxProgressFunc, bool) {
	if ctx == nil {
		return nil, false
	}
	fn, ok := ctx.Value(sandboxProgressKey{}).(SandboxProgressFunc)
	return fn, ok && fn != nil
}

const (
	SandboxModeAnalysis SandboxMode = "analysis"
	SandboxModePreview  SandboxMode = "preview"
	SandboxModeApply    SandboxMode = "apply"
)

// sandboxIdentityKey carries the authenticated user, optional permissions, and
// optional tenant that should back host RPCs from a Go sandbox process.
// Scheduled assistant loops build RuntimeContext with an empty caller, then
// bind the thread owner onto ctx.User — the sandbox host was created earlier
// with that empty caller closed over. Injecting identity into the RunGo
// context lets host RPCs (whagonsAction/Mutation/Query) run as the bound user
// and tenant instead of failing with "Not authenticated" / "tenantId is
// required" / "Tenant membership required".
type sandboxIdentityKey struct{}

type sandboxIdentity struct {
	User        *User
	Permissions map[string]any
	TenantID    string
}

// ContextWithSandboxIdentity returns a child context that carries user identity
// for sandbox host RPCs. No-op when user is nil or has an empty ID.
// Prefer ContextWithSandboxSession when the active tenant is known so host
// RPCs get the same automatic tenantId injection as the frontend sandbox.
func ContextWithSandboxIdentity(ctx context.Context, user *User, permissions map[string]any) context.Context {
	return ContextWithSandboxSession(ctx, "", user, permissions)
}

// ContextWithSandboxSession returns a child context that carries user identity
// and the active tenant for sandbox host RPCs. TenantID may be empty when only
// the user binding is needed; host code falls back to the closed-over tenant
// from RuntimeContext construction in that case.
// No-op when user is nil or has an empty ID (tenant alone is not enough to
// authenticate host RPCs).
func ContextWithSandboxSession(ctx context.Context, tenantID string, user *User, permissions map[string]any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if user == nil || user.ID == "" {
		return ctx
	}
	return context.WithValue(ctx, sandboxIdentityKey{}, sandboxIdentity{
		User:        user,
		Permissions: permissions,
		TenantID:    tenantID,
	})
}

// SandboxIdentityFromContext returns the identity attached by
// ContextWithSandboxIdentity / ContextWithSandboxSession, if any.
func SandboxIdentityFromContext(ctx context.Context) (user *User, permissions map[string]any, ok bool) {
	if ctx == nil {
		return nil, nil, false
	}
	identity, present := ctx.Value(sandboxIdentityKey{}).(sandboxIdentity)
	if !present || identity.User == nil || identity.User.ID == "" {
		return nil, nil, false
	}
	return identity.User, identity.Permissions, true
}

// SandboxTenantFromContext returns the tenant attached by
// ContextWithSandboxSession, if any. Empty string when unset.
func SandboxTenantFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	identity, present := ctx.Value(sandboxIdentityKey{}).(sandboxIdentity)
	if !present {
		return ""
	}
	return identity.TenantID
}

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
