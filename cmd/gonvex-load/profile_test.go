package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
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

	args, err := profile.Subscriptions[0].expandedArgs(map[string]any{
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
		"unsupported version": `{"version":3,"subscriptions":[{"path":"users.me","args":{}}]}`,
		"missing path":        `{"version":1,"subscriptions":[{"args":{}}]}`,
		"invalid path":        `{"version":1,"subscriptions":[{"path":"users/me","args":{}}]}`,
		"non-object args":     `{"version":1,"subscriptions":[{"path":"users.me","args":[]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadProfileReader(strings.NewReader(raw)); err == nil {
				t.Fatal("expected profile validation error")
			}
		})
	}
}

func TestBundledWhagonsProfilesValidate(t *testing.T) {
	for _, path := range []string{
		"profiles/whagons-prod-2026-08-11.json",
		"profiles/whagons-1000-users.json",
	} {
		t.Run(path, func(t *testing.T) {
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			profile, err := loadProfileReader(file)
			if err != nil {
				t.Fatal(err)
			}
			if len(profile.expandedSubscriptions()) < 99 {
				t.Fatalf("profile has only %d subscription slots", len(profile.expandedSubscriptions()))
			}
			if profile.Name == "whagons-1000-users" && (profile.Users != 1000 || profile.connectionCount(0) != 1700) {
				t.Fatalf("unexpected scaled profile: users=%d connections=%d", profile.Users, profile.connectionCount(0))
			}
		})
	}
}

func TestVersionTwoProfilePlansUsersPoolsAndMutationTemplates(t *testing.T) {
	profile, err := loadProfileReader(strings.NewReader(`{
		"version":2,
		"name":"sessions",
		"users":10,
		"connectionsPerUser":1.7,
		"pools":{"workspace":["w-a","w-b"],"limit":[10,20]},
		"subscriptionsPerConnection":[1,2],
		"subscriptions":[
			{"path":"tasks.list","args":{"workspaceId":"$workspace","limit":"${limit}"}},
			{"path":"users.me","args":{}}
		],
		"mutations":[{"path":"tasks.create","args":{"workspaceId":"$workspace","owner":"$userId"},"ratePerUserPerMinute":0.2,"activeUsers":0.2}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := profile.connectionCount(0); got != 17 {
		t.Fatalf("connectionCount = %d, want 17", got)
	}
	variables := profile.sessionVariables(3, map[string]string{"userId": "user-3"})
	args, err := profile.Subscriptions[0].expandedArgs(variables)
	if err != nil {
		t.Fatal(err)
	}
	expanded := args.(map[string]any)
	if expanded["workspaceId"] == nil || expanded["limit"] == nil {
		t.Fatalf("pool placeholders were not expanded: %#v", expanded)
	}
	if _, ok := expanded["limit"].(json.Number); !ok {
		t.Fatalf("numeric pool value lost its JSON type: %#v", expanded["limit"])
	}
	second := profile.sessionVariables(3, map[string]string{"userId": "user-3"})
	if variables["workspace"] != second["workspace"] {
		t.Fatalf("the same user must get stable session pool choices: %#v vs %#v", variables, second)
	}
	mutationArgs, err := profile.Mutations[0].expandedArgs(variables)
	if err != nil || mutationArgs["owner"] != "user-3" {
		t.Fatalf("mutation template expansion failed: args=%#v err=%v", mutationArgs, err)
	}
}

func TestProfileTemplateReportsMissingPool(t *testing.T) {
	_, err := expandProfileValue(map[string]any{"workspaceId": "$workspace"}, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "$workspace") {
		t.Fatalf("expected a useful missing-pool error, got %v", err)
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
