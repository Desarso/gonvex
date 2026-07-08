package sandbox

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

func TestRunnerRunGoReturnsJSONResult(t *testing.T) {
	runner := NewRunner(t.TempDir())
	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Verify the Go sandbox can return structured output.",
		Code: `return map[string]any{
	"message": "ok",
	"count": 3,
}, nil`,
	})
	if err != nil {
		t.Fatalf("RunGo returned error: %v", err)
	}
	if !result.OK {
		t.Fatalf("result.OK = false, error = %q", result.Error)
	}
	if got := result.Result["message"]; got != "ok" {
		t.Fatalf("message = %v, want ok", got)
	}
}

func TestRunnerRunGoCanCallHostAPI(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Host = HostFunc(func(_ context.Context, req HostCallRequest) (any, error) {
		if req.Kind != "query" {
			t.Fatalf("kind = %q, want query", req.Kind)
		}
		if req.Path != "tasks.list" {
			t.Fatalf("path = %q, want tasks.list", req.Path)
		}
		var args map[string]any
		if err := json.Unmarshal(req.Args, &args); err != nil {
			t.Fatalf("args unmarshal failed: %v", err)
		}
		if args["limit"] != float64(2) {
			t.Fatalf("limit = %v, want 2", args["limit"])
		}
		return map[string]any{"count": 2}, nil
	})

	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Verify Go sandbox host API calls.",
		Code: `tasks, err := whagonsQuery("tasks.list", map[string]any{"limit": 2})
if err != nil {
	return nil, err
}
return map[string]any{"tasks": tasks}, nil`,
	})
	if err != nil {
		t.Fatalf("RunGo returned error: %v", err)
	}
	if !result.OK {
		t.Fatalf("result.OK = false, error = %q", result.Error)
	}
	tasks, ok := result.Result["tasks"].(map[string]any)
	if !ok {
		t.Fatalf("tasks = %#v, want object", result.Result["tasks"])
	}
	if tasks["count"] != float64(2) {
		t.Fatalf("tasks.count = %v, want 2", tasks["count"])
	}
}

func TestRunnerRunGoCanCallWhdataHelpers(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.Host = HostFunc(func(_ context.Context, req HostCallRequest) (any, error) {
		if req.Kind != "data.query" {
			t.Fatalf("kind = %q, want data.query", req.Kind)
		}
		var args map[string]any
		if err := json.Unmarshal(req.Args, &args); err != nil {
			t.Fatalf("args unmarshal failed: %v", err)
		}
		if args["fileKey"] != "data_csv_abc" {
			t.Fatalf("fileKey = %v", args["fileKey"])
		}
		if args["sql"] != "SELECT count(*) AS n FROM data" {
			t.Fatalf("sql = %v", args["sql"])
		}
		return map[string]any{"ok": true, "rows": []any{map[string]any{"n": 4}}}, nil
	})

	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Verify Go sandbox whdata helpers.",
		Code: `data, err := whdataQuery("data_csv_abc", "SELECT count(*) AS n FROM data", 10)
if err != nil {
	return nil, err
}
return data, nil`,
	})
	if err != nil {
		t.Fatalf("RunGo returned error: %v", err)
	}
	if !result.OK {
		t.Fatalf("result.OK = false, error = %q", result.Error)
	}
	rows, ok := result.Result["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %#v", result.Result["rows"])
	}
}

func TestRunnerRunGoBlocksImports(t *testing.T) {
	runner := NewRunner(t.TempDir())
	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Verify unsafe imports are blocked before execution.",
		Code: `import "os/exec"
return nil, nil`,
	})
	if err != nil {
		t.Fatalf("RunGo returned error: %v", err)
	}
	if result.OK {
		t.Fatalf("result.OK = true, want blocked")
	}
	if !strings.Contains(result.Error, "blocked unsafe Go code pattern") {
		t.Fatalf("error = %q, want blocked pattern error", result.Error)
	}
}
