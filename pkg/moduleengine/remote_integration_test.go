package moduleengine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestRemoteEngineExecutesV8AndSwapsGenerationWithoutReconnect(t *testing.T) {
	binary := os.Getenv("GONVEX_TEST_MODULE_HOST_BINARY")
	if binary == "" {
		candidate, err := filepath.Abs("../../rust/target/debug/gonvex-module-host")
		if err == nil {
			if info, statErr := os.Stat(candidate); statErr == nil && !info.IsDir() {
				binary = candidate
			}
		}
	}
	if binary == "" {
		t.Skip("build gonvex-module-host or set GONVEX_TEST_MODULE_HOST_BINARY")
	}

	host := NewRemoteHost(HostOptions{
		Binary:           binary,
		StartTimeout:     20 * time.Second,
		RequestTimeout:   10 * time.Second,
		ShutdownTimeout:  5 * time.Second,
		DrainTimeout:     2 * time.Second,
		IsolatePoolSize:  2,
		ExecutionTimeout: 2 * time.Second,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close module host: %v", err)
		}
	})

	first := newRemoteTestEngine(t, host, 1)
	if err := first.Activate(context.Background()); err != nil {
		t.Fatalf("activate first generation: %v", err)
	}
	firstGeneration := first.Generation()
	if firstGeneration == 0 {
		t.Fatal("first module generation was not assigned")
	}

	runtimeContext := gonvex.RuntimeContext{
		Context: context.Background(),
		Auth:    gonvex.AuthContext{Account: &gonvex.User{ID: "acct-gabriel", Email: "gabriel@example.test"}},
		Tenant:  &gonvex.TenantIdentity{ID: "tenant-el-rey", ProjectID: "runtime-bridge-test"},
		Member:  &gonvex.Member{ID: "member-gabriel", AccountID: "acct-gabriel", Status: "active"},
	}
	queryResult, err := first.InvokeQuery(
		&gonvex.QueryCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.echo", Args: json.RawMessage(`{"value":"hello"}`)},
	)
	if err != nil {
		t.Fatalf("invoke first generation: %v", err)
	}
	assertRemoteResult(t, queryResult.Value, map[string]any{
		"version": float64(1), "value": "hello", "account": "acct-gabriel",
		"member": "member-gabriel", "tenant": "tenant-el-rey", "hasDb": false,
	})

	actionResult, err := first.InvokeAction(
		&gonvex.ActionCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.capabilities", Args: json.RawMessage(`{}`)},
	)
	if err != nil {
		t.Fatalf("invoke action capability probe: %v", err)
	}
	assertRemoteResult(t, actionResult.Value, map[string]any{
		"hasDb": false, "hasFetch": true, "hasStorage": false,
	})

	outbox := &recordingActionOutbox{}
	reducerResult, err := first.InvokeReducer(
		&gonvex.ReducerCtx{RuntimeContext: gonvex.RuntimeContext{
			Context: runtimeContext.Context,
			Auth:    runtimeContext.Auth,
			Tenant:  runtimeContext.Tenant,
			Member:  runtimeContext.Member,
			Tx:      &sql.Tx{},
			Outbox:  outbox,
		}},
		Invocation{Path: "system.enqueue", Args: json.RawMessage(`{"notificationId":"notification-1"}`)},
	)
	if err != nil {
		t.Fatalf("invoke reducer Action outbox: %v", err)
	}
	assertRemoteResult(t, reducerResult.Value, map[string]any{"outboxId": "outbox-1"})
	if outbox.path != "notifications.deliver" {
		t.Fatalf("V8 Action outbox path = %q", outbox.path)
	}
	if args, ok := outbox.args.(map[string]any); !ok || args["notificationId"] != "notification-1" {
		t.Fatalf("V8 Action outbox args = %#v", outbox.args)
	}

	second := newRemoteTestEngine(t, host, 2)
	if err := second.Activate(context.Background()); err != nil {
		t.Fatalf("activate second generation: %v", err)
	}
	if second.Generation() <= firstGeneration {
		t.Fatalf("second generation %d did not advance past %d", second.Generation(), firstGeneration)
	}

	// The old Go engine handle remains connected. Its next invocation is routed
	// to the atomically active Rust generation, proving a dev swap needs no
	// worker or WebSocket recycle.
	swapped, err := first.InvokeQuery(
		&gonvex.QueryCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.echo", Args: json.RawMessage(`{"value":"after-swap"}`)},
	)
	if err != nil {
		t.Fatalf("invoke through old handle after swap: %v", err)
	}
	assertRemoteResult(t, swapped.Value, map[string]any{
		"version": float64(2), "value": "after-swap", "account": "acct-gabriel",
		"member": "member-gabriel", "tenant": "tenant-el-rey", "hasDb": false,
	})
}

func newRemoteTestEngine(t *testing.T, host *RemoteHost, version int) *RemoteEngine {
	t.Helper()
	code := []byte(`
export async function echo(ctx, args) {
  return {
    version: ` + jsonNumber(version) + `,
    value: args.value,
    account: ctx.account?.id ?? null,
    member: ctx.member?.id ?? null,
    tenant: ctx.tenant?.id ?? null,
    hasDb: Object.prototype.hasOwnProperty.call(ctx, "db"),
  };
}
export async function capabilities(ctx) {
  return {
    hasDb: Object.prototype.hasOwnProperty.call(ctx, "db"),
    hasFetch: typeof ctx.fetch === "function",
    hasStorage: Object.prototype.hasOwnProperty.call(ctx, "storage"),
  };
}
export async function enqueue(ctx, args) {
  return { outboxId: await ctx.actions.enqueue("notifications.deliver", args) };
}
`)
	digest := sha256.Sum256(code)
	hash := hex.EncodeToString(digest[:])
	artifact := manifest.ModuleArtifact{
		Language:   manifest.LanguageTypeScript,
		Generation: 1,
		Entrypoint: "gonvex/index.ts",
		Functions: map[string]manifest.ModuleFunction{
			"system.echo": {
				Kind: manifest.FunctionKindQuery, Handler: "echo", Export: "echo", File: "gonvex/index.ts",
			},
			"system.capabilities": {
				Kind: manifest.FunctionKindAction, Handler: "capabilities", Export: "capabilities", File: "gonvex/index.ts",
			},
			"system.enqueue": {
				Kind: manifest.FunctionKindReducer, Handler: "enqueue", Export: "enqueue", File: "gonvex/index.ts",
			},
		},
		Files: map[string]string{},
		JavaScript: &manifest.ModuleJavaScript{
			Path: "gonvex/_build/module.js", Hash: hash, Code: base64.StdEncoding.EncodeToString(code),
		},
	}
	engine, err := NewRemoteEngine(host, "runtime-bridge-test", artifact)
	if err != nil {
		t.Fatalf("create remote engine: %v", err)
	}
	return engine
}

func assertRemoteResult(t *testing.T, value any, expected map[string]any) {
	t.Helper()
	actual, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("remote result is %T, want object: %#v", value, value)
	}
	for key, want := range expected {
		if got := actual[key]; got != want {
			t.Fatalf("remote result %s = %#v, want %#v (full result %#v)", key, got, want, actual)
		}
	}
}

func jsonNumber(value int) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
