package gonvex

import "testing"

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
