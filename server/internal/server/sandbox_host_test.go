package server

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestInjectSandboxHostTenantArgsAddsTenant(t *testing.T) {
	got := injectSandboxHostTenantArgs(json.RawMessage(`{"tasks":[]}`), "testing3")
	var payload map[string]any
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["tenantId"] != "testing3" {
		t.Fatalf("tenantId = %#v", payload["tenantId"])
	}
	tasks, ok := payload["tasks"].([]any)
	if !ok || len(tasks) != 0 {
		t.Fatalf("tasks = %#v", payload["tasks"])
	}
}

func TestInjectSandboxHostTenantArgsOverwritesStaleTenant(t *testing.T) {
	got := injectSandboxHostTenantArgs(json.RawMessage(`{"tenantId":"other","confirm":true}`), "testing3")
	var payload map[string]any
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["tenantId"] != "testing3" {
		t.Fatalf("tenantId = %#v", payload["tenantId"])
	}
	if payload["confirm"] != true {
		t.Fatalf("confirm = %#v", payload["confirm"])
	}
}

func TestInjectSandboxHostTenantArgsEmptyArgs(t *testing.T) {
	got := injectSandboxHostTenantArgs(nil, "testing3")
	var payload map[string]any
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["tenantId"] != "testing3" {
		t.Fatalf("tenantId = %#v", payload["tenantId"])
	}
}

func TestSandboxHostUnknownCuratedAction(t *testing.T) {
	if !sandboxHostUnknownCuratedAction(errors.New(`Unknown app action: tasks.bulkRestore. Use a curated action documented in the skills.`)) {
		t.Fatal("expected unknown curated action")
	}
	if sandboxHostUnknownCuratedAction(errors.New("Tenant membership required")) {
		t.Fatal("membership errors must not fall back")
	}
	if sandboxHostUnknownCuratedAction(nil) {
		t.Fatal("nil error is not unknown")
	}
}
