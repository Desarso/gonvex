package gonvex

import (
	"context"
	"encoding/json"
	"fmt"
)

// SandboxAPI is the host-owned bridge used by Actions that explicitly declare
// the sandbox capability. JavaScript receives typed methods, not this generic
// dispatcher. The generic form keeps process and storage details out of the
// language-neutral module contract.
type SandboxAPI interface {
	Call(ctx context.Context, operation string, payload json.RawMessage, duckdb bool) (any, error)
}

type sandboxUnavailable struct{}

func (sandboxUnavailable) Call(context.Context, string, json.RawMessage, bool) (any, error) {
	return nil, fmt.Errorf("gonvex: sandbox is not configured")
}

func UnavailableSandbox() SandboxAPI { return sandboxUnavailable{} }
