package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func workspaceVisibilityPlan() manifest.VisibilityPlan {
	return manifest.VisibilityPlan{
		Table: "tasks",
		Key:   "id",
		Sets: map[string]manifest.VisibilitySet{
			"workspaces": {
				Table:  "workspaceMembers",
				Select: "workspaceId",
				Where: []manifest.VisibilityConstraint{{
					Table: "workspaceMembers", Column: "memberId", Context: "member.id",
				}},
			},
		},
		Where: &manifest.VisibilityExpression{
			Operator: "or",
			Children: []*manifest.VisibilityExpression{
				{Operator: "permission", Value: "tasks.viewAll"},
				{Operator: "inSet", Column: "workspaceId", Set: "workspaces"},
			},
		},
	}
}

func TestVisibilityOldAndNewRowsFailClosed(t *testing.T) {
	plan := workspaceVisibilityPlan()
	resolved := &resolvedVisibilityContext{
		Permissions: map[string]any{"tasks.viewAll": false},
		Sets: map[string]map[string]struct{}{
			"workspaces": {"workspace-a": {}},
		},
	}
	if !visibilityRawRowMatches(json.RawMessage(`{"id":"task-1","workspaceId":"workspace-a"}`), plan, resolved) {
		t.Fatal("a row in the member's workspace must be visible")
	}
	if visibilityRawRowMatches(json.RawMessage(`{"id":"task-1","workspaceId":"workspace-b"}`), plan, resolved) {
		t.Fatal("a row outside the member's workspace must fail closed")
	}
	resolved.Permissions["tasks.viewAll"] = true
	if !visibilityRawRowMatches(json.RawMessage(`{"id":"task-1","workspaceId":"workspace-b"}`), plan, resolved) {
		t.Fatal("the explicit view-all permission must admit the row")
	}
}

func TestVisibilityTransitionOperationCoversAllOldAndNewStates(t *testing.T) {
	tests := []struct {
		oldVisible bool
		newVisible bool
		operation  string
		emit       bool
	}{
		{oldVisible: true, newVisible: true, operation: "update", emit: true},
		{oldVisible: false, newVisible: true, operation: "insert", emit: true},
		{oldVisible: true, newVisible: false, operation: "delete", emit: true},
		{oldVisible: false, newVisible: false, operation: "", emit: false},
	}
	for _, test := range tests {
		operation, emit := visibilityTransitionOperation(test.oldVisible, test.newVisible)
		if operation != test.operation || emit != test.emit {
			t.Fatalf("transition old=%v new=%v = (%q, %v), want (%q, %v)",
				test.oldVisible, test.newVisible, operation, emit, test.operation, test.emit)
		}
	}
}

func TestMemberChangeIdentitiesIncludesOldAndNewAuthorityKeys(t *testing.T) {
	identities := memberChangeIdentities(syncChangeBatch{changes: []syncLogChange{
		{
			table:    "members",
			oldValue: json.RawMessage(`{"id":"member-old","account_id":"account-old","user_id":"legacy-old"}`),
			newValue: json.RawMessage(`{"id":"member-new","accountId":"account-new"}`),
		},
		{table: "tasks", oldValue: json.RawMessage(`{"account_id":"must-not-match"}`)},
	}})
	for _, identity := range []string{"member-old", "account-old", "legacy-old", "member-new", "account-new"} {
		if _, ok := identities[identity]; !ok {
			t.Fatalf("missing member identity %q in %#v", identity, identities)
		}
	}
	if _, ok := identities["must-not-match"]; ok {
		t.Fatal("a non-member row was treated as membership authority")
	}
}

func TestVisibilityFingerprintSharesOnlyEquivalentInputs(t *testing.T) {
	plan := workspaceVisibilityPlan()
	first := &resolvedVisibilityContext{
		Direct:      map[string]string{"member.id": "member-a", "account.id": "account-a"},
		Permissions: map[string]any{"tasks.viewAll": false, "unrelated": true},
		Sets:        map[string]map[string]struct{}{"workspaces": {"workspace-a": {}}},
	}
	second := &resolvedVisibilityContext{
		Direct:      map[string]string{"member.id": "member-b", "account.id": "account-b"},
		Permissions: map[string]any{"tasks.viewAll": false, "unrelated": false},
		Sets:        map[string]map[string]struct{}{"workspaces": {"workspace-a": {}}},
	}
	if visibilityFingerprint(plan, first) != visibilityFingerprint(plan, second) {
		t.Fatal("different identities with identical effective inputs should share one visibility fingerprint")
	}
	second.Sets["workspaces"] = map[string]struct{}{"workspace-b": {}}
	if visibilityFingerprint(plan, first) == visibilityFingerprint(plan, second) {
		t.Fatal("different effective workspace sets must not share execution")
	}

	directPlan := manifest.VisibilityPlan{
		Table: "notes", Key: "id", Sets: map[string]manifest.VisibilitySet{},
		Where: &manifest.VisibilityExpression{Operator: "eqContext", Column: "ownerId", Context: "member.id"},
	}
	if visibilityFingerprint(directPlan, first) == visibilityFingerprint(directPlan, second) {
		t.Fatal("direct member comparisons must retain member identity in the fingerprint")
	}
}

func TestVisibilitySQLUsesOneSafePlaceholderSequence(t *testing.T) {
	plan := workspaceVisibilityPlan()
	builder := &visibilitySQLBuilder{}
	for index := 0; index < 9; index++ {
		builder.argument(index)
	}
	predicate, err := compileVisibilitySQL(
		plan.Where, plan,
		map[string]string{"member.id": "member-a"},
		map[string]any{"tasks.viewAll": false}, "", builder, "r",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(predicate, `$10`) {
		t.Fatalf("visibility subquery did not continue the outer placeholder sequence: %s", predicate)
	}
	if strings.Contains(predicate, `$100`) {
		t.Fatalf("visibility placeholder replacement corrupted a double-digit parameter: %s", predicate)
	}
	if !strings.Contains(predicate, "FROM members AS _gonvex_member") ||
		!strings.Contains(predicate, "_gonvex_member.status = 'active'") ||
		!strings.Contains(predicate, "_gonvex_member.permissions ->>") {
		t.Fatalf("permission SQL did not recheck the active authoritative tenant member: %s", predicate)
	}
	foundMember := false
	for _, argument := range builder.args[9:] {
		if argument == "member-a" {
			foundMember = true
		}
	}
	if !foundMember {
		t.Fatalf("visibility SQL lost the member context argument: %#v", builder.args)
	}
}

func TestManifestUpdateRequiresVisibilityForEveryLiveDelivery(t *testing.T) {
	current := manifest.Manifest{
		Functions: map[string]manifest.FunctionEntry{
			"tasks.grid": {
				Kind: manifest.FunctionKindQuery, Delivery: manifest.DeliveryLive,
				Dependencies: manifest.FunctionDependencies{LiveQueryPlan: &manifest.LiveQueryPlan{Table: "tasks", Key: "id", Columns: []string{"id"}}},
			},
		},
	}
	if err := (&Server{}).requireVisibilityPlans(current); err == nil || !strings.Contains(err.Error(), "explicit visibility plan") {
		t.Fatalf("missing visibility plan error = %v", err)
	}
	current.Visibility = map[string]manifest.VisibilityPlan{
		"tasks": {Table: "tasks", Key: "id", Sets: map[string]manifest.VisibilitySet{}, Where: &manifest.VisibilityExpression{Operator: "public"}},
	}
	if err := (&Server{}).requireVisibilityPlans(current); err != nil {
		t.Fatalf("explicit public visibility plan was rejected: %v", err)
	}
}
