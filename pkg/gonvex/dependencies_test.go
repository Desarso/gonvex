package gonvex

import (
	"context"
	"testing"
)

func TestFunctionDependencyOptions(t *testing.T) {
	app := NewApp()
	app.Query("tasks.list", func(*QueryCtx, struct{}) ([]string, error) { return nil, nil },
		Reads("tasks").Columns("id", "title").Filters("status").OrdersBy("updated_at").Windowed(),
		ReadsEphemeral(),
		ShareByPermissions(),
	)
	app.Mutation("leases.beat", func(*MutationCtx, struct{}) (any, error) { return nil, nil }, WritesEphemeral())
	function, ok := app.Lookup("tasks.list")
	if !ok || len(function.Dependencies.Reads) != 1 {
		t.Fatalf("dependencies were not registered: %#v", function.Dependencies)
	}
	read := function.Dependencies.Reads[0]
	if read.Table != "tasks" || len(read.Columns) != 2 || !read.Windowed || !function.Dependencies.ReadsEphemeral || !function.Dependencies.ShareByPermissions {
		t.Fatalf("unexpected dependencies: %#v", function.Dependencies)
	}
	mutation, ok := app.Lookup("leases.beat")
	if !ok || !mutation.Dependencies.WritesEphemeral || len(mutation.Dependencies.Writes) != 0 {
		t.Fatalf("unexpected ephemeral write dependencies: %#v", mutation.Dependencies)
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
