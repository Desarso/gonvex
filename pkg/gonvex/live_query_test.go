package gonvex

import (
	"reflect"
	"testing"
)

func TestLiveQueryPlanCompilesParameterizedPostgres(t *testing.T) {
	plan := LiveTable("tasks").Select("id", "title", "deadline").ResultRowsAt("rows").
		Filter(All(Eq("workspace_id", Arg("workspace")), Neq("status", Literal("done")))).
		SearchArg("search", "title").
		SortArgs("sort", "direction", "deadline", "asc", "deadline", "created_at").
		WindowArgs("offset", "limit", 100, 150)
	compiled, err := plan.Compile(map[string]any{"workspace": "w1", "search": "freezer", "sort": "DROP TABLE", "direction": "asc", "offset": 50, "limit": 500})
	if err != nil {
		t.Fatal(err)
	}
	wantSQL := `SELECT "id", "title", "deadline" FROM "tasks" WHERE ("workspace_id" = $1 AND "status" <> $2) AND ("title" ILIKE $3) ORDER BY "deadline" ASC, "id" ASC LIMIT $4 OFFSET $5`
	if compiled.SQL != wantSQL {
		t.Fatalf("SQL = %s\nwant  %s", compiled.SQL, wantSQL)
	}
	if !reflect.DeepEqual(compiled.Arguments, []any{"w1", "done", "%freezer%", 150, 50}) {
		t.Fatalf("args = %#v", compiled.Arguments)
	}
	if !compiled.Portable {
		t.Fatal("portable plan marked server-only")
	}
	if !reflect.DeepEqual(plan.ResultPath, []string{"rows"}) {
		t.Fatalf("result path = %#v", plan.ResultPath)
	}
}

func TestLiveQueryRegistrationRequiresStructuredPlan(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("LiveQuery accepted arbitrary reactive handler")
		}
	}()
	NewApp().LiveQuery("tasks.grid", func(*QueryCtx, struct{}) ([]any, error) { return nil, nil })
}
