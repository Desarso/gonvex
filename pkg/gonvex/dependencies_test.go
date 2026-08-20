package gonvex

import (
	"context"
	"testing"
)

func TestLiveQueryDerivesDependenciesFromPlan(t *testing.T) {
	app := NewApp()
	app.LiveQuery("tasks.list", func(*QueryCtx, struct{}) ([]string, error) { return nil, nil },
		LivePlan(LiveTable("tasks").Select("id", "title").Filter(Eq("status", Arg("status"))).SortArgs("sort", "direction", "updated_at", "desc", "updated_at").WindowArgs("offset", "limit", 100, 200)),
		ShareByPermissions(),
	)
	app.Reducer("leases.beat", func(*ReducerCtx, struct{}) (any, error) { return nil, nil }, OnlineOnlyNonOptimistic("test fixture"))
	function, ok := app.Lookup("tasks.list")
	if !ok || len(function.Dependencies.Reads) != 1 {
		t.Fatalf("dependencies were not registered: %#v", function.Dependencies)
	}
	read := function.Dependencies.Reads[0]
	if read.Table != "tasks" || len(read.Columns) != 2 || !read.Windowed || len(read.Filters) != 1 || !function.Dependencies.ShareByPermissions {
		t.Fatalf("unexpected dependencies: %#v", function.Dependencies)
	}
	reducer, ok := app.Lookup("leases.beat")
	if !ok || reducer.Kind != FunctionKindReducer {
		t.Fatalf("unexpected reducer registration: %#v", reducer)
	}
}

func TestQueryChangeRoundTrip(t *testing.T) {
	ctx := WithQueryChange(context.Background(), "invalidate", 1234.5)
	info := QueryChange(ctx)
	if info.Reason != "invalidate" || info.ChangedAtMS != 1234.5 {
		t.Fatalf("query change = %#v", info)
	}
	if empty := QueryChange(nil); empty != (QueryChangeInfo{}) {
		t.Fatalf("nil query change = %#v", empty)
	}
}
