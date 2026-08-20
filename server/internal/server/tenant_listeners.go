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

type syncNotifyPayload struct {
	Epoch    string   `json:"epoch"`
	Revision uint64   `json:"revision"`
	Tables   []string `json:"tables"`
}

type tenantListener struct {
	key           tenantListenerKey
	databaseURL   string
	refs          int
	cancel        context.CancelFunc
	idle          *time.Timer
	ready         chan struct{}
	connected     bool
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
		if listener.databaseURL != databaseURL {
			// Tenant routing can be hydrated after the first subscription opens.
			// A listener connected before that hydration points at the project
			// database forever, so sync/query invalidations from the real
			// tenant database are never observed. Carry the existing references to
			// a replacement listener; their eventual release calls are key-based.
			if listener.idle != nil {
				listener.idle.Stop()
				listener.idle = nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			replacement := &tenantListener{
				key: key, databaseURL: databaseURL, refs: listener.refs + 1,
				cancel: cancel, ready: make(chan struct{}), needsRecovery: true,
			}
			m.active[key] = replacement
			m.mu.Unlock()
			listener.cancel()
			m.server.markTenantReplicasOutOfDate(project, tenant, "listener-route-changed")
			go m.run(ctx, replacement, databaseURL)
			return replacement.ready
		}
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
	listener := &tenantListener{
		key: key, databaseURL: databaseURL, refs: 1, cancel: cancel, ready: make(chan struct{}),
	}
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
	if current := m.active[listener.key]; current != listener {
		m.mu.Unlock()
		return false
	}
	needsRecovery := listener.needsRecovery
	listener.needsRecovery = false
	if !listener.connected {
		listener.connected = true
		close(listener.ready)
	}
	m.mu.Unlock()
	return needsRecovery
}

func (m *tenantListenerManager) markDisconnected(listener *tenantListener) {
	m.mu.Lock()
	if current := m.active[listener.key]; current != listener || !listener.connected {
		m.mu.Unlock()
		return
	}
	listener.connected = false
	listener.needsRecovery = true
	listener.ready = make(chan struct{})
	m.mu.Unlock()
	m.server.markTenantReplicasOutOfDate(listener.key.project, listener.key.tenant, "listener-reconnecting")
}

// whileConnected serializes a freshness-sensitive action with listener
// disconnect detection. In particular, a sync subscription must be published
// before a concurrent disconnect enumerates subscriptions, and replica.ready must
// be written before the corresponding replica.syncing frame. Returning the current
// readiness barrier lets callers retry safely when they observed an older,
// already-closed barrier just as the listener disconnected.
func (m *tenantListenerManager) whileConnected(
	project string,
	tenant string,
	action func(),
) (<-chan struct{}, bool) {
	key := tenantListenerKey{project: project, tenant: tenant}
	m.mu.Lock()
	defer m.mu.Unlock()
	listener := m.active[key]
	if listener == nil {
		return nil, false
	}
	if !listener.connected {
		return listener.ready, false
	}
	action()
	return listener.ready, true
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
			_, err = connection.Exec(ctx, "LISTEN "+schema.ChangeFeedNotifyChannel)
		}
		if err == nil {
			needsRecovery := m.markReady(listener)
			// A committed outbox row may predate this process (for example a
			// crash immediately after Reducer commit). Drain whenever the tenant
			// becomes active, independent of a new application-table revision.
			go m.server.drainActionOutbox(listener.key.project, listener.key.tenant)
			m.server.startMembershipProjection(func() {
				m.server.drainControlPlaneMembershipOutbox(listener.key.project, listener.key.tenant)
			})
			if connectedBefore {
				m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerReconnects++ })
			}
			if connectedBefore || needsRecovery {
				m.server.subscriptions.refreshTenant(listener.key.project, listener.key.tenant)
				// PostgreSQL notifications are edge-triggered and are lost while
				// LISTEN is disconnected. Sync cursors are durable, so force one
				// coalesced delivery pass after reconnect to replay every missed
				// revision from the change log.
				m.server.notifySyncRevision(listener.key.project, listener.key.tenant, nil, "", 0)
			}
			connectedBefore = true
			backoff = 250 * time.Millisecond
			err = m.wait(ctx, connection, listener)
			if err != nil && ctx.Err() == nil {
				m.markDisconnected(listener)
			}
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

func (m *tenantListenerManager) wait(ctx context.Context, connection *pgx.Conn, listener *tenantListener) error {
	key := listener.key
	for {
		notification, err := connection.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		payload := parseSyncNotifyPayload(notification.Payload)
		if payload.Revision == 0 {
			m.markNeedsRecovery(key.project, key.tenant)
			continue
		}
		m.server.notifySyncRevision(key.project, key.tenant, payload.Tables, payload.Epoch, payload.Revision)
		go m.server.drainActionOutbox(key.project, key.tenant)
		m.server.startMembershipProjection(func() {
			m.server.drainControlPlaneMembershipOutbox(key.project, key.tenant)
		})
		m.dispatchCommittedRevision(ctx, listener, payload)
	}
}

func (m *tenantListenerManager) dispatchCommittedRevision(ctx context.Context, listener *tenantListener, payload syncNotifyPayload) {
	changes, err := readReplicaChanges(ctx, listener.databaseURL, payload.Revision-1, payload.Revision)
	if err != nil {
		m.markNeedsRecovery(listener.key.project, listener.key.tenant)
		m.server.subscriptions.refreshTenant(listener.key.project, listener.key.tenant)
		return
	}
	for _, batch := range groupReplicaChanges(changes) {
		m.server.projectCommittedMemberChanges(listener.key.project, listener.key.tenant, batch)
		m.server.refreshCommittedMemberConnections(listener.key.project, listener.key.tenant, batch)
		changedTables := replicaBatchTables(batch)
		m.server.invalidateVisibilityContexts(listener.key.project, listener.key.tenant, changedTables)
		visibilityChange := tableChange{
			project: listener.key.project,
			tenant:  listener.key.tenant,
			tables:  map[string]bool{},
		}
		for _, table := range changedTables {
			visibilityChange.tables[table] = true
		}
		m.server.subscriptions.rebindVisibilityForChange(visibilityChange)
		m.server.resetReplicasForVisibilityChange(visibilityChange)
		m.server.routeReplicaTransaction(listener.key.project, listener.key.tenant, payload.Epoch, batch)
		byTable := map[string]*tableChange{}
		for _, committed := range batch.changes {
			change := byTable[committed.table]
			if change == nil {
				change = &tableChange{
					project: listener.key.project, tenant: listener.key.tenant, table: committed.table,
					requiredRevision: batch.revision,
					rowIDs:           map[string]bool{},
					changedAtMS:      epochMillis(time.Now().UTC()), originCommandID: strings.TrimSpace(committed.originCommandID), triggerObserved: true,
				}
				byTable[committed.table] = change
			}
			change.rowIDs[committed.rowID] = true
			if len(committed.oldValue) > 0 && string(committed.oldValue) != "null" {
				change.oldValues = append(change.oldValues, append(json.RawMessage(nil), committed.oldValue...))
			}
			if len(committed.newValue) > 0 && string(committed.newValue) != "null" {
				change.newValues = append(change.newValues, append(json.RawMessage(nil), committed.newValue...))
			}
			change.changedColumns = appendUniqueStrings(change.changedColumns, committed.changedColumns...)
			if change.operation == "" {
				change.operation = committed.operation
			} else if change.operation != committed.operation {
				change.operation = "mixed"
			}
		}
		for _, change := range byTable {
			m.server.scheduleTableChange(*change)
		}
	}
}

func (m *tenantListenerManager) healthy(project, tenant string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	listener := m.active[tenantListenerKey{project: project, tenant: tenant}]
	return listener != nil && listener.connected && !listener.needsRecovery
}

func parseSyncNotifyPayload(raw string) syncNotifyPayload {
	payload := syncNotifyPayload{}
	if json.Unmarshal([]byte(raw), &payload) != nil || strings.TrimSpace(payload.Epoch) == "" || payload.Revision == 0 {
		return syncNotifyPayload{}
	}
	payload.Tables = appendUniqueStrings(nil, payload.Tables...)
	if len(payload.Tables) == 0 {
		// Legacy notifications and the trigger's oversized-payload fallback omit
		// tables. An explicit empty list is equally non-actionable, so all three
		// cases retain the full-delivery correctness backstop.
		payload.Tables = nil
	}
	return payload
}
