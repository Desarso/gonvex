package manifest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func validCronArtifact() ModuleArtifact {
	code := []byte(`export const run = () => null;`)
	digest := sha256.Sum256(code)
	artifact := ModuleArtifact{
		Language:   LanguageTypeScript,
		Generation: ModuleArtifactGeneration,
		Entrypoint: "gonvex/index.ts",
		Functions: map[string]ModuleFunction{
			"jobs.run": {
				Kind: FunctionKindAction, Handler: "run", File: "gonvex/index.ts",
				Args: ModuleSchema{"kind": "any"}, Result: ModuleSchema{"kind": "any"},
			},
		},
		JavaScript: &ModuleJavaScript{
			Path: "gonvex/index.js",
			Hash: hex.EncodeToString(digest[:]),
			Code: base64.StdEncoding.EncodeToString(code),
		},
	}
	artifact.Hash, _ = artifact.ComputedHash()
	return artifact
}

func TestModuleArtifactValidatesRecurringCronDeclarations(t *testing.T) {
	artifact := validCronArtifact()
	artifact.Crons = []ModuleCron{{
		Name: "daily", Function: "jobs.run", Scope: "tenant", Expression: "0 1 * * *", Args: json.RawMessage(`{"kind":"daily"}`),
	}}
	artifact.Hash, _ = artifact.ComputedHash()
	if err := artifact.Validate(); err != nil {
		t.Fatalf("valid cron artifact: %v", err)
	}
}

func TestModuleArtifactRejectsInvalidRecurringCronDeclarations(t *testing.T) {
	tests := []struct {
		name string
		cron ModuleCron
		want string
	}{
		{name: "ambiguous", cron: ModuleCron{Name: "daily", Function: "jobs.run", Scope: "project", IntervalMS: 1000, Expression: "* * * * *"}, want: "exactly one"},
		{name: "query target", cron: ModuleCron{Name: "daily", Function: "jobs.read", Scope: "project", IntervalMS: 1000}, want: "unknown function"},
		{name: "scope", cron: ModuleCron{Name: "daily", Function: "jobs.run", Scope: "workspace", IntervalMS: 1000}, want: "unknown scope"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := validCronArtifact()
			artifact.Crons = []ModuleCron{test.cron}
			artifact.Hash, _ = artifact.ComputedHash()
			if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestModuleArtifactRejectsOneShotQueryWithoutStructuredPlan(t *testing.T) {
	artifact := validCronArtifact()
	artifact.Functions["jobs.read"] = ModuleFunction{
		Kind: FunctionKindQuery, Handler: "read", File: "gonvex/index.ts", Delivery: DeliveryOneShot,
		Args: ModuleSchema{"kind": "any"}, Result: ModuleSchema{"kind": "any"},
	}
	artifact.Hash, _ = artifact.ComputedHash()
	if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "one-shot query \"jobs.read\" requires a structured live query plan") {
		t.Fatalf("Validate() error = %v, want one-shot query plan rejection", err)
	}
}
