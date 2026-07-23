package gonvex

import (
	"context"
	"testing"
)

func TestFunctionDependencyOptions(t *testing.T) {
	app := NewApp()
	app.Query("tasks.list", func(*QueryCtx, struct{}) ([]string, error) { return nil, nil },
		Reads("tasks").Columns("id", "title").Filters("status").OrdersBy("updated_at").Windowed(),
		ShareByPermissions(),
	)
	function, ok := app.Lookup("tasks.list")
	if !ok || len(function.Dependencies.Reads) != 1 {
		t.Fatalf("dependencies were not registered: %#v", function.Dependencies)
	}
	read := function.Dependencies.Reads[0]
	if read.Table != "tasks" || len(read.Columns) != 2 || !read.Windowed || !function.Dependencies.ShareByPermissions {
		t.Fatalf("unexpected dependencies: %#v", function.Dependencies)
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
