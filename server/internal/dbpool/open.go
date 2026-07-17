package dbpool

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const defaultMaxTotal = 20

var runtimeBudget = &connectionBudget{limit: func() int {
	return positiveEnvironmentInt("GONVEX_DB_MAX_TOTAL_CONNS", defaultMaxTotal)
}}

// Open creates a PostgreSQL pool whose physical connections count against the
// process-wide budget shared by every runtime database pool.
func Open(databaseURL string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(&limitedConnector{
		connector: stdlib.GetConnector(*config),
		budget:    runtimeBudget,
	})
	Configure(db)
	return db, nil
}

// PGXConn unwraps a connection obtained through sql.Conn.Raw.
func PGXConn(raw any) (*pgx.Conn, bool) {
	limited, ok := raw.(*limitedConn)
	if !ok {
		return nil, false
	}
	connection, ok := limited.Conn.(*stdlib.Conn)
	if !ok {
		return nil, false
	}
	return connection.Conn(), true
}

type connectionBudget struct {
	mu      sync.Mutex
	active  int
	changed chan struct{}
	limit   func() int
}

func (b *connectionBudget) acquire(ctx context.Context) error {
	for {
		b.mu.Lock()
		if b.changed == nil {
			b.changed = make(chan struct{})
		}
		if b.active < b.limit() {
			b.active++
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (b *connectionBudget) release() {
	b.mu.Lock()
	if b.active > 0 {
		b.active--
	}
	if b.changed != nil {
		close(b.changed)
		b.changed = make(chan struct{})
	}
	b.mu.Unlock()
}

type limitedConnector struct {
	connector driver.Connector
	budget    *connectionBudget
}

func (c *limitedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.budget.acquire(ctx); err != nil {
		return nil, err
	}
	connection, err := c.connector.Connect(ctx)
	if err != nil {
		c.budget.release()
		return nil, err
	}
	return &limitedConn{Conn: connection, budget: c.budget}, nil
}

func (c *limitedConnector) Driver() driver.Driver {
	return c.connector.Driver()
}

type limitedConn struct {
	driver.Conn
	budget *connectionBudget
	once   sync.Once
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.budget.release)
	return err
}

func (c *limitedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connection, ok := c.Conn.(driver.ConnBeginTx); ok {
		return connection.BeginTx(ctx, opts)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if connection, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return connection.PrepareContext(ctx, query)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if connection, ok := c.Conn.(driver.ExecerContext); ok {
		return connection.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if connection, ok := c.Conn.(driver.QueryerContext); ok {
		return connection.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *limitedConn) Ping(ctx context.Context) error {
	if connection, ok := c.Conn.(driver.Pinger); ok {
		return connection.Ping(ctx)
	}
	return nil
}

func (c *limitedConn) ResetSession(ctx context.Context) error {
	if connection, ok := c.Conn.(driver.SessionResetter); ok {
		return connection.ResetSession(ctx)
	}
	return nil
}

func (c *limitedConn) IsValid() bool {
	if connection, ok := c.Conn.(driver.Validator); ok {
		return connection.IsValid()
	}
	return true
}

func (c *limitedConn) CheckNamedValue(value *driver.NamedValue) error {
	if connection, ok := c.Conn.(driver.NamedValueChecker); ok {
		return connection.CheckNamedValue(value)
	}
	return driver.ErrSkip
}
