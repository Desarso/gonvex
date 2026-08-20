package moduleengine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestCronSpecsFromArtifact(t *testing.T) {
	crons := cronSpecsFromArtifact([]manifest.ModuleCron{
		{Name: " reports ", Function: " reports.generate ", Scope: "project", IntervalMS: 2500},
		{Name: "cleanup", Function: "tasks.cleanup", Scope: "tenant", Expression: "0 1 * * *", Args: json.RawMessage(`{"expired":true}`)},
	})
	if len(crons) != 2 {
		t.Fatalf("cron count = %d", len(crons))
	}
	if crons[0].Name != "reports" || crons[0].FunctionPath != "reports.generate" || crons[0].Interval != 2500*time.Millisecond {
		t.Fatalf("interval cron = %#v", crons[0])
	}
	if !crons[1].PerTenant || crons[1].Expression != "0 1 * * *" || string(crons[1].Args) != `{"expired":true}` {
		t.Fatalf("tenant cron = %#v", crons[1])
	}

	engine := &RemoteEngine{crons: crons}
	copy := engine.Crons()
	copy[1].Args[0] = '['
	if string(engine.Crons()[1].Args) != `{"expired":true}` {
		t.Fatal("RemoteEngine.Crons returned mutable argument storage")
	}
}
