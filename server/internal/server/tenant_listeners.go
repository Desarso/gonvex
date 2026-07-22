package server

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/server/internal/schema"
	"github.com/jackc/pgx/v5"
)

type tenantListenerKey struct {
	project string
	tenant  string
}

type tenantListener struct {
	key           tenantListenerKey
	refs          int
	cancel        context.CancelFunc
	idle          *time.Timer
	ready         chan struct{}
	readyOnce     sync.Once
	needsRecovery bool
}

type tenantListenerManager struct {
	server *Server
	mu     sync.Mutex
	active map[tenantListenerKey]*tenantListener
}

func newTenantListenerManager(server *Server) *tenantListenerManager {
	return &tenantListenerManager{server: server, active: map[tenantListenerKey]*tenantListener{}}
}

func (m *tenantListenerManager) acquire(project, tenant string) <-chan struct{} {
	databaseURL := strings.TrimSpace(m.server.databaseURLForTenant(project, tenant))
	if databaseURL == "" || m.server.config.TenantListenerLimit == 0 {
		return nil
	}
	key := tenantListenerKey{project: project, tenant: tenant}
	m.mu.Lock()
	if listener := m.active[key]; listener != nil {
		listener.refs++
		if listener.idle != nil {
			listener.idle.Stop()
			listener.idle = nil
		}
		m.mu.Unlock()
		return listener.ready
	}
	if len(m.active) >= m.server.config.TenantListenerLimit {
		m.mu.Unlock()
		m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerLimitRefusals++ })
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	listener := &tenantListener{key: key, refs: 1, cancel: cancel, ready: make(chan struct{})}
	m.active[key] = listener
	active := len(m.active)
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ActiveTenantListeners = active })
	go m.run(ctx, listener, databaseURL)
	return listener.ready
}

func (m *tenantListenerManager) markNeedsRecovery(project, tenant string) {
	m.mu.Lock()
	if listener := m.active[tenantListenerKey{project: project, tenant: tenant}]; listener != nil {
		listener.needsRecovery = true
	}
	m.mu.Unlock()
}

func (m *tenantListenerManager) markReady(listener *tenantListener) bool {
	m.mu.Lock()
	needsRecovery := listener.needsRecovery
	listener.needsRecovery = false
	m.mu.Unlock()
	listener.readyOnce.Do(func() { close(listener.ready) })
	return needsRecovery
}

func (m *tenantListenerManager) release(project, tenant string) {
	key := tenantListenerKey{project: project, tenant: tenant}
	m.mu.Lock()
	listener := m.active[key]
	if listener == nil {
		m.mu.Unlock()
		return
	}
	if listener.refs > 0 {
		listener.refs--
	}
	if listener.refs == 0 && listener.idle == nil {
		listener.idle = time.AfterFunc(m.server.config.TenantListenerIdleTimeout, func() { m.expire(key, listener) })
	}
	m.mu.Unlock()
}

func (m *tenantListenerManager) expire(key tenantListenerKey, expected *tenantListener) {
	m.mu.Lock()
	listener := m.active[key]
	if listener != expected || listener.refs != 0 {
		m.mu.Unlock()
		return
	}
	delete(m.active, key)
	listener.cancel()
	active := len(m.active)
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ActiveTenantListeners = active })
}

func (m *tenantListenerManager) run(ctx context.Context, listener *tenantListener, databaseURL string) {
	backoff := 250 * time.Millisecond
	connectedBefore := false
	for ctx.Err() == nil {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		connection, err := pgx.Connect(connectCtx, databaseURL)
		cancel()
		if err == nil {
			_, err = connection.Exec(ctx, "LISTEN "+schema.NotifyChannel)
		}
		if err == nil {
			needsRecovery := m.markReady(listener)
			if connectedBefore {
				m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerReconnects++ })
			}
			if connectedBefore || needsRecovery {
				m.server.subscriptions.refreshTenant(listener.key.project, listener.key.tenant)
			}
			connectedBefore = true
			backoff = 250 * time.Millisecond
			err = m.wait(ctx, connection, listener.key)
		}
		if connection != nil {
			_ = connection.Close(context.Background())
		}
		if ctx.Err() != nil {
			return
		}
		m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerFailures++ })
		jitterWindow := backoff / 4
		jitter := time.Duration(0)
		if jitterWindow > 0 {
			jitter = time.Duration(time.Now().UnixNano() % int64(jitterWindow))
		}
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 10*time.Second {
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
}

func (m *tenantListenerManager) wait(ctx context.Context, connection *pgx.Conn, key tenantListenerKey) error {
	for {
		notification, err := connection.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		payload := tableNotifyPayload{}
		if notification.Payload != "" && notification.Payload[0] != '{' {
			payload.Table = notification.Payload
			payload.Broad = true
		} else if json.Unmarshal([]byte(notification.Payload), &payload) != nil || payload.Table == "" {
			continue
		}
		rowIDs := map[string]bool{}
		for _, id := range payload.IDs {
			rowIDs[id] = true
		}
		m.server.scheduleTableChange(tableChange{
			project: key.project, tenant: key.tenant, table: payload.Table,
			broad: payload.Broad, rowIDs: rowIDs, operation: payload.Operation,
			changedColumns: normalizedColumns(payload.ChangedColumns), changedAtMS: epochMillis(time.Now().UTC()),
		})
	}
}
