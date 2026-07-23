package dbpool

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	stats := budget.stats()
	if stats.Limit != 2 || stats.Active != 2 || stats.Waiters != 0 || stats.WaitCount != 1 {
		t.Fatalf("budget stats after timeout = %+v", stats)
	}
	if stats.WaitDuration < 15*time.Millisecond {
		t.Fatalf("budget wait duration = %s, want at least 15ms", stats.WaitDuration)
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

func TestTotalConnectionBudgetHonorsBoundedDeploymentConfiguration(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_TOTAL_CONNS", "48")
	if got := runtimeBudget.limit(); got != 48 {
		t.Fatalf("total connection limit = %d, want configured limit 48", got)
	}
}

func TestTotalConnectionBudgetCannotExceedRuntimeSafetyCeiling(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_TOTAL_CONNS", "200")
	if got := runtimeBudget.limit(); got != maxTotalSafetyCeiling {
		t.Fatalf("total connection limit = %d, want safety ceiling %d", got, maxTotalSafetyCeiling)
	}
}

func TestBudgetedPoolRetainsAtMostOneWarmConnection(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "2")
	budget := &connectionBudget{limit: func() int { return 1 }}
	db := sql.OpenDB(&limitedConnector{connector: testConnector{}, budget: budget})
	configureBudgeted(db)
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	budget.mu.Lock()
	active := budget.active
	budget.mu.Unlock()
	if active != 1 {
		t.Fatalf("retained connections after request = %d, want 1", active)
	}

	stats := db.Stats()
	if stats.Idle != 1 {
		t.Fatalf("idle connections after request = %d, want 1", stats.Idle)
	}
}

type testConnector struct{}

func (testConnector) Connect(context.Context) (driver.Conn, error) { return testConn{}, nil }
func (testConnector) Driver() driver.Driver                        { return testDriver{} }

type testDriver struct{}

func (testDriver) Open(string) (driver.Conn, error) { return testConn{}, nil }

type testConn struct{}

func (testConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (testConn) Close() error                        { return nil }
func (testConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (testConn) Ping(context.Context) error          { return nil }
