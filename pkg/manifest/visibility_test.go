package manifest

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestVisibilityPlansRoundTripThroughManifestAndModuleArtifact(t *testing.T) {
	plan := VisibilityPlan{
		Table: "tasks",
		Key:   "id",
		Sets: map[string]VisibilitySet{
			"teams": {
				Table:  "memberTeams",
				Select: "teamId",
				Joins: []VisibilityJoin{{
					Table: "members", LeftColumn: "memberId", RightColumn: "id",
				}},
				Where: []VisibilityConstraint{{
					Table: "members", Column: "accountId", Context: "account.id",
				}},
			},
		},
		Where: &VisibilityExpression{
			Operator: "or",
			Children: []*VisibilityExpression{
				{Operator: "permission", Value: "tasks:read"},
				{Operator: "inSet", Column: "teamId", Set: "teams"},
			},
		},
	}
	want := map[string]VisibilityPlan{"tasks": plan}
	original := Manifest{
		Project:     "whagons",
		GeneratedAt: "now",
		Functions:   map[string]FunctionEntry{},
		Schema:      EmptySchema(),
		Visibility:  want,
		Module: &ModuleArtifact{
			Language: "typescript", Generation: 1,
			Functions: map[string]ModuleFunction{}, Files: map[string]string{}, Visibility: want,
		},
	}

	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal visibility manifest: %v", err)
	}
	var decoded Manifest
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal visibility manifest: %v", err)
	}
	decoded = decoded.Normalize()
	if !reflect.DeepEqual(decoded.Visibility, want) {
		t.Fatalf("manifest visibility changed during normalization: %#v", decoded.Visibility)
	}
	if decoded.Module == nil || !reflect.DeepEqual(decoded.Module.Visibility, want) {
		t.Fatalf("module visibility changed during normalization: %#v", decoded.Module)
	}
}

func TestManifestNormalizeCopiesVisibilityAcrossArtifactBoundary(t *testing.T) {
	plan := VisibilityPlan{Table: "tasks", Key: "id", Sets: map[string]VisibilitySet{}}
	plans := map[string]VisibilityPlan{"tasks": plan}

	fromArtifact := Manifest{Schema: EmptySchema(), Module: &ModuleArtifact{Visibility: plans}}.Normalize()
	if !reflect.DeepEqual(fromArtifact.Visibility, plans) {
		t.Fatalf("artifact visibility was not projected to the manifest: %#v", fromArtifact.Visibility)
	}

	fromManifest := Manifest{Schema: EmptySchema(), Visibility: plans, Module: &ModuleArtifact{}}.Normalize()
	if fromManifest.Module == nil || !reflect.DeepEqual(fromManifest.Module.Visibility, plans) {
		t.Fatalf("manifest visibility was not retained in the module artifact: %#v", fromManifest.Module)
	}
}
