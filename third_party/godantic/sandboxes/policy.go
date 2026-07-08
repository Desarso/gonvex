package sandboxes

// policy.go — provider-agnostic approval metadata for sandbox/data tools.
// Godantic classifies what a tool call is allowed to do; the HOST decides how
// to obtain approval (consent card, allowlist, hard block). Godantic knows
// nothing about the host's tables or permission system.

type ApprovalCategory string

const (
	// ApprovalNone marks calls that are safe to auto-run: read-only data access
	// or sandbox runs that cannot produce side effects outside the sandbox.
	ApprovalNone ApprovalCategory = "none"
	// ApprovalHost marks calls the host must explicitly approve before running
	// (anything that applies writes or has side effects outside the sandbox).
	ApprovalHost ApprovalCategory = "host"
)

// ToolPolicy describes one tool call's expected behavior so the host can gate
// it consistently across providers.
type ToolPolicy struct {
	Tool        string           `json:"tool"`
	Language    Language         `json:"language,omitempty"`
	Mode        Mode             `json:"mode,omitempty"`
	ReadOnly    bool             `json:"readOnly"`
	SideEffects string           `json:"sideEffects,omitempty"`
	Approval    ApprovalCategory `json:"approval"`
	Limits      Limits           `json:"limits"`
}

// Default limit profiles per mode, from the sandbox design doc.
var (
	analysisLimits = Limits{TimeoutMs: 60_000, MemoryBytes: 1 << 30, MaxOutputBytes: 1 << 20, MaxRowsReturned: 500}
	previewLimits  = Limits{TimeoutMs: 300_000, MemoryBytes: 1 << 30, MaxOutputBytes: 1 << 20, MaxRowsReturned: 500}
)

// DataToolPolicy classifies the read-only data tools (inspect/query/profile).
// They never mutate anything, so they auto-run.
func DataToolPolicy(tool string) ToolPolicy {
	return ToolPolicy{
		Tool:        tool,
		ReadOnly:    true,
		SideEffects: "none",
		Approval:    ApprovalNone,
		Limits:      analysisLimits,
	}
}

// RunPolicy classifies a sandbox run request. Analysis and preview runs
// auto-run; apply runs always require host approval. A host that detects
// write calls inside submitted code should escalate to ApprovalHost even for
// analysis/preview — pass codeMayWrite for that.
func RunPolicy(req RunRequest, codeMayWrite bool) ToolPolicy {
	mode := req.Mode
	if mode == "" {
		mode = ModeAnalysis
	}
	policy := ToolPolicy{
		Tool:     "sandbox_run_go",
		Language: req.Language,
		Mode:     mode,
		Limits:   analysisLimits,
	}
	switch mode {
	case ModePreview:
		policy.Limits = previewLimits
	case ModeApply:
		policy.Limits = previewLimits
	}
	if mode == ModeApply || codeMayWrite {
		policy.ReadOnly = false
		policy.SideEffects = "writes"
		policy.Approval = ApprovalHost
		return policy
	}
	policy.ReadOnly = true
	policy.SideEffects = "sandbox-only"
	policy.Approval = ApprovalNone
	return policy
}
