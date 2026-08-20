package moduleengine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	if status := host.Status(); !status.Ready || !status.Running || !status.Connected || status.Epoch == 0 {
		t.Fatalf("module host is not healthy after activation: %+v", status)
	}

	runtimeContext := gonvex.RuntimeContext{
		Context: context.Background(),
		Env:     map[string]string{"MAIL_API_KEY": "test-secret"},
		Auth:    gonvex.AuthContext{Account: &gonvex.Account{ID: "acct-gabriel", Email: "gabriel@example.test"}},
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
		"member": "member-gabriel", "tenant": "tenant-el-rey", "hasDb": false, "hasEnv": false,
	})
	_, err = first.InvokeQuery(
		&gonvex.QueryCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.echo", Args: json.RawMessage(`{"value":42}`)},
	)
	var invalidArgs *gonvex.DispatchError
	if !errors.As(err, &invalidArgs) || invalidArgs.Code != "invalid_args" {
		t.Fatalf("invalid arguments error = %#v, want invalid_args DispatchError", err)
	}
	_, err = first.InvokeQuery(
		&gonvex.QueryCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.badResult", Args: json.RawMessage(`{}`)},
	)
	var invalidResult *gonvex.DispatchError
	if !errors.As(err, &invalidResult) || invalidResult.Code != "invalid_result" {
		t.Fatalf("invalid result error = %#v, want invalid_result DispatchError", err)
	}

	actionResult, err := first.InvokeAction(
		&gonvex.ActionCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.capabilities", Args: json.RawMessage(`{}`)},
	)
	if err != nil {
		t.Fatalf("invoke action capability probe: %v", err)
	}
	assertRemoteResult(t, actionResult.Value, map[string]any{
		"hasDb": false, "hasFetch": true, "hasStorage": false, "mailKey": "test-secret",
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

	host.mu.Lock()
	process := host.process
	previousEpoch := host.epoch
	host.mu.Unlock()
	if process == nil || process.cmd == nil || process.cmd.Process == nil {
		t.Fatal("managed module host process is missing")
	}
	if err := process.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill module host: %v", err)
	}
	<-process.exited
	if status := host.Status(); status.Ready || status.Running || status.Error == "" {
		t.Fatalf("dead module host reported healthy: %+v", status)
	}

	recovered, err := second.InvokeQuery(
		&gonvex.QueryCtx{RuntimeContext: runtimeContext},
		Invocation{Path: "system.echo", Args: json.RawMessage(`{"value":"after-restart"}`)},
	)
	if err != nil {
		t.Fatalf("invoke after module host restart: %v", err)
	}
	assertRemoteResult(t, recovered.Value, map[string]any{
		"version": float64(2), "value": "after-restart", "account": "acct-gabriel",
		"member": "member-gabriel", "tenant": "tenant-el-rey", "hasDb": false,
	})
	if status := host.Status(); !status.Ready || status.Epoch <= previousEpoch {
		t.Fatalf("module host did not recover on the next invocation: %+v", status)
	}
}

func newRemoteTestEngine(t *testing.T, host *RemoteHost, version int) *RemoteEngine {
	t.Helper()
	code := []byte(`
export async function echo(ctx, args) {
  return {
    version: ` + jsonNumber(version) + `,
    value: args.value,
    account: ctx.auth.account?.id ?? null,
    member: ctx.member?.id ?? null,
    tenant: ctx.tenant?.id ?? null,
    hasDb: Object.prototype.hasOwnProperty.call(ctx, "db"),
    hasEnv: Object.prototype.hasOwnProperty.call(ctx, "env"),
  };
}
export async function capabilities(ctx) {
  return {
    hasDb: Object.prototype.hasOwnProperty.call(ctx, "db"),
    hasFetch: typeof ctx.fetch === "function",
    hasStorage: Object.prototype.hasOwnProperty.call(ctx, "storage"),
    mailKey: ctx.env.MAIL_API_KEY,
  };
}
export async function badResult() { return "not-a-boolean"; }
export async function enqueue(ctx, args) {
  return { outboxId: await ctx.actions.enqueue("notifications.deliver", args) };
}
`)
	digest := sha256.Sum256(code)
	hash := hex.EncodeToString(digest[:])
	artifact := manifest.ModuleArtifact{
		Language:   manifest.LanguageTypeScript,
		Generation: manifest.ModuleArtifactGeneration,
		Entrypoint: "gonvex/index.ts",
		Functions: map[string]manifest.ModuleFunction{
			"system.badResult": {
				Kind: manifest.FunctionKindQuery, Handler: "badResult", Export: "badResult", File: "gonvex/index.ts",
				Args: moduleObjectSchema(nil), Result: manifest.ModuleSchema{"kind": "boolean"},
				Dependencies: manifest.FunctionDependencies{LiveQueryPlan: &manifest.LiveQueryPlan{Table: "system_results", Key: "id", Columns: []string{"id"}}},
			},
			"system.echo": {
				Kind: manifest.FunctionKindQuery, Handler: "echo", Export: "echo", File: "gonvex/index.ts",
				Args: moduleObjectSchema(map[string]manifest.ModuleSchema{"value": {"kind": "string"}}),
				Result: moduleObjectSchema(map[string]manifest.ModuleSchema{
					"version": {"kind": "number"}, "value": {"kind": "string"},
					"account": {"kind": "any"}, "member": {"kind": "any"}, "tenant": {"kind": "any"},
					"hasDb": {"kind": "boolean"}, "hasEnv": {"kind": "boolean"},
				}),
				Dependencies: manifest.FunctionDependencies{LiveQueryPlan: &manifest.LiveQueryPlan{Table: "system_results", Key: "id", Columns: []string{"id"}}},
			},
			"system.capabilities": {
				Kind: manifest.FunctionKindAction, Handler: "capabilities", Export: "capabilities", File: "gonvex/index.ts",
				Args: moduleObjectSchema(nil),
				Result: moduleObjectSchema(map[string]manifest.ModuleSchema{
					"hasDb": {"kind": "boolean"}, "hasFetch": {"kind": "boolean"}, "hasStorage": {"kind": "boolean"},
					"mailKey": {"kind": "string"},
				}),
			},
			"system.enqueue": {
				Kind: manifest.FunctionKindReducer, Handler: "enqueue", Export: "enqueue", File: "gonvex/index.ts",
				Args:         moduleObjectSchema(map[string]manifest.ModuleSchema{"notificationId": {"kind": "string"}}),
				Result:       moduleObjectSchema(map[string]manifest.ModuleSchema{"outboxId": {"kind": "string"}}),
				Offline:      map[string]any{"mode": "onlineOnly", "reason": "runtime bridge test"},
				Dependencies: manifest.FunctionDependencies{NonOptimisticReason: "runtime bridge test"},
			},
		},
		Files: map[string]string{},
		JavaScript: &manifest.ModuleJavaScript{
			Path: "gonvex/_build/module.js", Hash: hash, Code: base64.StdEncoding.EncodeToString(code),
		},
	}
	artifact.Hash, _ = artifact.ComputedHash()
	engine, err := NewRemoteEngine(host, "runtime-bridge-test", artifact)
	if err != nil {
		t.Fatalf("create remote engine: %v", err)
	}
	return engine
}

func moduleObjectSchema(fields map[string]manifest.ModuleSchema) manifest.ModuleSchema {
	if fields == nil {
		fields = map[string]manifest.ModuleSchema{}
	}
	return manifest.ModuleSchema{"kind": "object", "fields": fields}
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

// This crosses the actual CLI artifact boundary: TypeScript source is bundled
// by @gonvex/cli, then the resulting default ModuleBuilder export is loaded and
// invoked through the production Rust/V8 module host.
func TestRemoteEngineExecutesBundledModuleBuilderArtifact(t *testing.T) {
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
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cliArtifact := filepath.Join(root, "packages", "gonvex", "dist", "module-artifact.js")
	sdk := filepath.Join(root, "packages", "module-sdk", "dist", "index.js")
	if _, err := os.Stat(cliArtifact); err != nil {
		t.Skip("build @gonvex/cli before running bundled artifact integration")
	}
	if _, err := os.Stat(sdk); err != nil {
		t.Skip("build @gonvex/module-sdk before running bundled artifact integration")
	}
	project, err := os.MkdirTemp("", "gonvex-builder-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(project) })
	backend := filepath.Join(project, "gonvex")
	if err := os.Mkdir(backend, 0o755); err != nil {
		t.Fatal(err)
	}
	sdkURL := "file://" + filepath.ToSlash(sdk)
	source := fmt.Sprintf(`import { createModule, schema } from %q;
const app = createModule({ name: "builder-runtime", version: "1" });
app.action("reports.daily", {
  args: schema.object({}),
  result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});
export default app;
`, sdkURL)
	entrypoint := filepath.Join(backend, "index.ts")
	if err := os.WriteFile(entrypoint, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	buildScript := fmt.Sprintf(`import { buildModuleArtifact } from %q;
const root = process.argv[1];
const artifact = await buildModuleArtifact({
  root,
  backendDir: root + "/gonvex",
  files: [root + "/gonvex/index.ts"],
  migrations: [],
});
process.stdout.write(JSON.stringify(artifact));
`, "file://"+filepath.ToSlash(cliArtifact))
	cmd := exec.Command("node", "--input-type=module", "-e", buildScript, project)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build bundled TypeScript artifact: %v\n%s", err, output)
	}
	var artifact manifest.ModuleArtifact
	if err := json.Unmarshal(output, &artifact); err != nil {
		t.Fatalf("decode bundled artifact: %v\n%s", err, output)
	}

	host := NewRemoteHost(HostOptions{
		Binary: binary, StartTimeout: 20 * time.Second, RequestTimeout: 10 * time.Second,
		ShutdownTimeout: 5 * time.Second, DrainTimeout: 2 * time.Second,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := host.Close(ctx); closeErr != nil {
			t.Errorf("close module host: %v", closeErr)
		}
	})
	engine, err := NewRemoteEngine(host, "builder-runtime-test", artifact)
	if err != nil {
		t.Fatalf("create remote engine: %v", err)
	}
	if err := engine.Activate(context.Background()); err != nil {
		t.Fatalf("activate bundled ModuleBuilder artifact: %v", err)
	}
	result, err := engine.InvokeAction(&gonvex.ActionCtx{RuntimeContext: gonvex.RuntimeContext{Context: context.Background()}}, Invocation{
		Path: "reports.daily", Args: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("invoke bundled ModuleBuilder action: %v", err)
	}
	assertRemoteResult(t, result.Value, map[string]any{"ok": true})
}
