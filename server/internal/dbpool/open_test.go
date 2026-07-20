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

func TestBudgetedPoolReleasesConnectionInsteadOfRetainingIdleSlot(t *testing.T) {
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
	if active != 0 {
		t.Fatalf("active connections after request = %d, want 0", active)
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
