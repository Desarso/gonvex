package main

import (
	"encoding/json"
	"strings"
	"testing"

	identity "github.com/gonvex/gonvex/server/pkg/controlplane/identity"
)

func TestIdentityPlanMappingPreservesReviewedLegacyIDs(t *testing.T) {
	plan := testIdentityMigrationPlan(t, `{"items":[
		{"legacy":{"legacyUserId":"legacy-a"},"account":{"id":"acct-a"}},
		{"legacy":{"legacyUserId":"legacy-b"},"account":{"id":"acct-b"}}
	]}`)
	mapping, err := identityPlanMapping(plan)
	if err != nil {
		t.Fatal(err)
	}
	if mapping["legacy-a"] != "acct-a" || mapping["legacy-b"] != "acct-b" {
		t.Fatalf("unexpected identity mapping: %#v", mapping)
	}
}

func TestIdentityPlanMappingRejectsAmbiguousLegacyID(t *testing.T) {
	plan := testIdentityMigrationPlan(t, `{"items":[
		{"legacy":{"legacyUserId":"legacy"},"account":{"id":"acct-a"}},
		{"legacy":{"legacyUserId":"legacy"},"account":{"id":"acct-b"}}
	]}`)
	if _, err := identityPlanMapping(plan); err == nil || !strings.Contains(err.Error(), "maps to multiple accounts") {
		t.Fatalf("ambiguous legacy mapping was accepted: %v", err)
	}
}

func testIdentityMigrationPlan(t *testing.T, raw string) identity.MigrationPlan {
	t.Helper()
	var plan identity.MigrationPlan
	if err := json.Unmarshal([]byte(raw), &plan); err != nil {
		t.Fatal(err)
	}
	return plan
}
