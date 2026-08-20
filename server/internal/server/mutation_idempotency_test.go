package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

func TestMutationIdempotencyContextRoundTrip(t *testing.T) {
	ctx := withMutationIdempotency(context.Background(), "  key-1  ", "user-1")
	claim, ok := mutationIdempotencyFromContext(ctx)
	if !ok {
		t.Fatal("expected an idempotency claim in the context")
	}
	if claim.Key != "key-1" || claim.Subject != "user-1" {
		t.Fatalf("unexpected claim %+v", claim)
	}

	if _, ok := mutationIdempotencyFromContext(context.Background()); ok {
		t.Fatal("empty context must not carry a claim")
	}
	if _, ok := mutationIdempotencyFromContext(withMutationIdempotency(context.Background(), "   ", "user-1")); ok {
		t.Fatal("a blank key must not create a claim")
	}
}

func TestCallerContextSubject(t *testing.T) {
	if subject := (callerContext{}).subject(); subject != "" {
		t.Fatalf("expected empty subject for anonymous caller, got %q", subject)
	}
	caller := callerContext{user: &gonvex.User{ID: "user-42"}}
	if subject := caller.subject(); subject != "user-42" {
		t.Fatalf("expected user id as subject, got %q", subject)
	}
}

func mutationIdempotencyTestContext(t *testing.T) (*Server, *gonvex.ReducerCtx) {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv("GONVEX_TEST_POSTGRES_URL"))
	if baseURL == "" {
		t.Skip("set GONVEX_TEST_POSTGRES_URL to run PostgreSQL mutation-idempotency integration tests")
	}
	name := "gonvex_idem_" + tenantRegistryTestSuffix(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, name)
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	server := &Server{}
	mutationCtx := &gonvex.ReducerCtx{RuntimeContext: gonvex.RuntimeContext{
		Context:     context.Background(),
		DatabaseURL: databaseURL,
		DB:          db,
		Logger:      slog.Default(),
	}}
	return server, mutationCtx
}

func TestMutationIdempotencyExecutesOncePerKey(t *testing.T) {
	server, mutationCtx := mutationIdempotencyTestContext(t)
	var executions atomic.Int64
	exec := func(*gonvex.ReducerCtx, string, json.RawMessage) (any, error) {
		return map[string]any{"value": executions.Add(1)}, nil
	}

	mutationCtx.Context = withMutationIdempotency(context.Background(), "key-1", "user-1")
	first, err := server.runMutationInTx(mutationCtx, "tasks.create", nil, exec)
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	duplicate, err := server.runMutationInTx(mutationCtx, "tasks.create", nil, exec)
	if err != nil {
		t.Fatalf("duplicate delivery: %v", err)
	}
	if executions.Load() != 1 {
		t.Fatalf("expected exactly one execution, got %d", executions.Load())
	}
	if fmt.Sprint(first) != fmt.Sprint(duplicate) {
		t.Fatalf("duplicate result %v does not match first result %v", duplicate, first)
	}

	// A different authenticated subject must never observe another user's
	// stored result, even under the same key.
	mutationCtx.Context = withMutationIdempotency(context.Background(), "key-1", "user-2")
	if _, err := server.runMutationInTx(mutationCtx, "tasks.create", nil, exec); err != nil {
		t.Fatalf("other subject: %v", err)
	}
	if executions.Load() != 2 {
		t.Fatalf("expected a fresh execution for the other subject, got %d", executions.Load())
	}

	// Reusing a key for a different mutation is a client bug; refuse to
	// replay a result that belongs to another path.
	mutationCtx.Context = withMutationIdempotency(context.Background(), "key-1", "user-1")
	if _, err := server.runMutationInTx(mutationCtx, "tasks.delete", nil, exec); err == nil {
		t.Fatal("expected an error for a key reused across paths")
	}
}

func TestMutationIdempotencyFailedMutationDoesNotClaim(t *testing.T) {
	server, mutationCtx := mutationIdempotencyTestContext(t)
	var executions atomic.Int64
	failing := errors.New("handler failed")
	exec := func(*gonvex.ReducerCtx, string, json.RawMessage) (any, error) {
		if executions.Add(1) == 1 {
			return nil, failing
		}
		return "recovered", nil
	}

	mutationCtx.Context = withMutationIdempotency(context.Background(), "key-retry", "user-1")
	if _, err := server.runMutationInTx(mutationCtx, "tasks.create", nil, exec); !errors.Is(err, failing) {
		t.Fatalf("expected the handler error, got %v", err)
	}
	result, err := server.runMutationInTx(mutationCtx, "tasks.create", nil, exec)
	if err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if result != "recovered" {
		t.Fatalf("unexpected retry result %v", result)
	}
	if executions.Load() != 2 {
		t.Fatalf("expected the failed attempt to release the key, got %d executions", executions.Load())
	}
}

func TestMutationIdempotencyConcurrentDuplicatesExecuteOnce(t *testing.T) {
	server, mutationCtx := mutationIdempotencyTestContext(t)
	var executions atomic.Int64
	exec := func(*gonvex.ReducerCtx, string, json.RawMessage) (any, error) {
		return executions.Add(1), nil
	}
	mutationCtx.Context = withMutationIdempotency(context.Background(), "key-race", "user-1")

	const duplicates = 8
	results := make([]any, duplicates)
	failures := make([]error, duplicates)
	var wg sync.WaitGroup
	for index := range duplicates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine needs its own ReducerCtx: runMutationInTx
			// mutates the shared Tx/Scheduler fields.
			ctx := &gonvex.ReducerCtx{RuntimeContext: mutationCtx.RuntimeContext}
			results[index], failures[index] = server.runMutationInTx(ctx, "tasks.create", nil, exec)
		}()
	}
	wg.Wait()

	if executions.Load() != 1 {
		t.Fatalf("expected exactly one execution across %d concurrent duplicates, got %d", duplicates, executions.Load())
	}
	for index := range duplicates {
		if failures[index] != nil {
			t.Fatalf("duplicate %d failed: %v", index, failures[index])
		}
		if fmt.Sprint(results[index]) != "1" {
			t.Fatalf("duplicate %d observed result %v instead of the committed result", index, results[index])
		}
	}
}
