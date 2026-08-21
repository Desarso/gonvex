package manifest

import "testing"

func TestModuleArtifactValidatesAgentToolTargets(t *testing.T) {
	artifact := validAgentArtifact(t)
	if err := artifact.Validate(); err != nil {
		t.Fatalf("valid agent artifact: %v", err)
	}

	broken := validAgentArtifact(t)
	tool := broken.Functions["agents.run"].ActionCapabilities.Tools["searchTasks"]
	tool.Function = "tasks.public"
	broken.Functions["agents.run"].ActionCapabilities.Tools["searchTasks"] = tool
	public := broken.Functions["agents.searchTasks"]
	public.Internal = false
	broken.Functions["tasks.public"] = public
	broken.Hash, _ = broken.ComputedHash()
	if err := broken.Validate(); err == nil {
		t.Fatal("public Query tool target unexpectedly validated")
	}
}

func TestModuleArtifactRejectsUndeclaredActionAuthority(t *testing.T) {
	artifact := validAgentArtifact(t)
	action := artifact.Functions["agents.run"]
	action.ActionProfile = "standard"
	artifact.Functions["agents.run"] = action
	artifact.Hash, _ = artifact.ComputedHash()
	if err := artifact.Validate(); err == nil {
		t.Fatal("standard Action with agent tools unexpectedly validated")
	}
}

func validAgentArtifact(t *testing.T) ModuleArtifact {
	t.Helper()
	artifact := ModuleArtifact{
		Language: LanguageTypeScript, Generation: ModuleArtifactGeneration, Entrypoint: "gonvex/index.ts",
		Functions: map[string]ModuleFunction{
			"agents.searchTasks": {
				Kind: FunctionKindQuery, Handler: "searchTasks", File: "gonvex/index.ts", Internal: true,
				Args: ModuleSchema{"kind": "object", "fields": map[string]any{}}, Result: ModuleSchema{"kind": "array", "items": map[string]any{"kind": "any"}},
				Dependencies: FunctionDependencies{LiveQueryPlan: &LiveQueryPlan{Table: "tasks", Key: "id", Columns: []string{"id"}}},
			},
			"agents.run": {
				Kind: FunctionKindAction, Handler: "run", File: "gonvex/index.ts", ActionProfile: "agent",
				Args: ModuleSchema{"kind": "object", "fields": map[string]any{}}, Result: ModuleSchema{"kind": "any"},
				ActionCapabilities: &ActionCapabilities{Sandbox: &SandboxCapability{DuckDB: true}, Tools: map[string]ActionToolBinding{
					"searchTasks": {Kind: FunctionKindQuery, Function: "agents.searchTasks"},
				}},
			},
		},
		Files:      map[string]string{},
		JavaScript: &ModuleJavaScript{Path: "gonvex/_build/module.js", Hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Code: ""},
	}
	// DecodeJavaScript rejects empty code, so use a harmless byte and matching hash.
	artifact.JavaScript.Code = "IA=="
	artifact.JavaScript.Hash = "36a9e7f1c95b82ffb99743e0c5c4ce95d83c9a430aac59f84ef3cbfab6145068"
	artifact.Hash, _ = artifact.ComputedHash()
	return artifact
}
