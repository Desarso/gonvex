package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestLoadProfileExpandsRuntimeVariablesWithoutMutatingSource(t *testing.T) {
	raw := `{
		"version": 1,
		"name": "whagons-workspace",
		"variables": {"workspaceId": "workspace-a"},
		"subscriptions": [
			{"path": "bulk.tasksByWorkspace", "args": {
				"tenantId": "${tenant}",
				"workspaceIds": ["${workspaceId}"],
				"viewer": "${userId}",
				"literal": "before-${tenant}"
			}}
		]
	}`

	profile, err := loadProfileReader(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("loadProfileReader returned error: %v", err)
	}
	if profile.Name != "whagons-workspace" || len(profile.Subscriptions) != 1 {
		t.Fatalf("unexpected profile: %#v", profile)
	}

	args, err := profile.Subscriptions[0].expandedArgs(map[string]string{
		"tenant":      "loadtest",
		"userId":      "user-42",
		"workspaceId": "workspace-b",
	})
	if err != nil {
		t.Fatalf("expandedArgs returned error: %v", err)
	}
	got := args.(map[string]any)
	if got["tenantId"] != "loadtest" || got["viewer"] != "user-42" {
		t.Fatalf("runtime placeholders were not expanded: %#v", got)
	}
	workspaceIDs := got["workspaceIds"].([]any)
	if len(workspaceIDs) != 1 || workspaceIDs[0] != "workspace-b" {
		t.Fatalf("workspace placeholder was not overridden: %#v", workspaceIDs)
	}
	if got["literal"] != "before-${tenant}" {
		t.Fatalf("partial placeholders must stay literal, got %#v", got["literal"])
	}

	source := profile.Subscriptions[0].Args.(map[string]any)
	if source["tenantId"] != "${tenant}" {
		t.Fatalf("expansion mutated profile source: %#v", source)
	}
}

func TestLoadProfileRejectsInvalidSubscription(t *testing.T) {
	for name, raw := range map[string]string{
		"unsupported version": `{"version":2,"subscriptions":[{"path":"users.me","args":{}}]}`,
		"missing path":        `{"version":1,"subscriptions":[{"args":{}}]}`,
		"invalid path":        `{"version":1,"subscriptions":[{"path":"users/me","args":{}}]}`,
		"missing args":        `{"version":1,"subscriptions":[{"path":"users.me"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadProfileReader(strings.NewReader(raw)); err == nil {
				t.Fatal("expected profile validation error")
			}
		})
	}
}

func TestSyntheticJWTUsesDistinctSubjects(t *testing.T) {
	first := syntheticJWT("load-user-1")
	second := syntheticJWT("load-user-2")
	if first == second {
		t.Fatal("synthetic tokens must differ per user")
	}
	parts := strings.Split(first, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts, want 3", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	if claims["sub"] != "load-user-1" {
		t.Fatalf("unexpected subject: %#v", claims)
	}
}
