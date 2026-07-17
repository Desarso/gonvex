package dbpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectionBudgetBlocksAtLimitAndResumesAfterRelease(t *testing.T) {
	budget := &connectionBudget{limit: func() int { return 2 }}
	if err := budget.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := budget.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := budget.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire at limit returned %v, want context deadline exceeded", err)
	}

	budget.release()
	if err := budget.acquire(context.Background()); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	budget.release()
	budget.release()
}

func TestTotalConnectionBudgetDoesNotAllowUnlimitedConfiguration(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_TOTAL_CONNS", "0")
	if got := runtimeBudget.limit(); got != defaultMaxTotal {
		t.Fatalf("total connection limit = %d, want %d", got, defaultMaxTotal)
	}
}
