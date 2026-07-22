package server

import (
	"encoding/json"
	"strings"
)

// injectSandboxHostTenantArgs mirrors the frontend sandbox host: every allowed
// whagonsQuery/Mutation/Action call gets the active tenantId injected so agent
// code never has to pass it (and never accidentally omits it).
// Existing tenantId values are overwritten to the active host tenant so a
// stale/wrong arg cannot cross-tenant a sandbox RPC.
func injectSandboxHostTenantArgs(args json.RawMessage, tenantID string) json.RawMessage {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		if len(args) == 0 {
			return json.RawMessage(`{}`)
		}
		return args
	}
	var payload map[string]any
	if len(args) == 0 {
		payload = map[string]any{}
	} else if err := json.Unmarshal(args, &payload); err != nil || payload == nil {
		payload = map[string]any{}
	}
	payload["tenantId"] = tenantID
	encoded, err := json.Marshal(payload)
	if err != nil {
		return args
	}
	return encoded
}

// sandboxHostUnknownCuratedAction reports whether assistant.sandboxAction failed
// because the name is not on the curated surface (so the host may fall back to
// a registered runtime Action with the same path).
func sandboxHostUnknownCuratedAction(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Unknown app action:")
}
