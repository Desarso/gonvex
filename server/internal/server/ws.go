package server

import (
	"compress/flate"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/data"
	"github.com/gonvex/gonvex/server/internal/sandbox"
	"github.com/gorilla/websocket"
)

type clientMessage struct {
	Type               string          `json:"type"`
	ID                 string          `json:"id"`
	Path               string          `json:"path,omitempty"`
	Args               json.RawMessage `json:"args,omitempty"`
	Token              string          `json:"token,omitempty"`
	Project            string          `json:"project,omitempty"`
	Tenant             string          `json:"tenant,omitempty"`
	Trace              *messageTrace   `json:"trace,omitempty"`
	Kind               string          `json:"kind,omitempty"`
	Reason             string          `json:"reason,omitempty"`
	Outcome            string          `json:"outcome,omitempty"`
	Error              string          `json:"error,omitempty"`
	ClientSentAtMS     float64         `json:"clientSentAtMs,omitempty"`
	ClientReceivedAtMS float64         `json:"clientReceivedAtMs,omitempty"`
	ClientDurationMS   float64         `json:"clientDurationMs,omitempty"`
	Device             json.RawMessage `json:"device,omitempty"`
}

type serverMessage struct {
	Type          string               `json:"type"`
	ID            string               `json:"id,omitempty"`
	Path          string               `json:"path,omitempty"`
	Project       string               `json:"project,omitempty"`
	Tenant        string               `json:"tenant,omitempty"`
	Result        any                  `json:"result,omitempty"`
	Error         string               `json:"error,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	Trace         any                  `json:"trace,omitempty"`
	QueryCache    *queryCacheDirective `json:"queryCache,omitempty"`
	CacheScope    string               `json:"cacheScope,omitempty"`
	CacheRevision string               `json:"cacheRevision,omitempty"`
}

// explicitNull makes a nil handler result serialize as an explicit JSON null
// on *.result messages. Convex resolves null-returning functions to null;
// omitting the field (omitempty) would leave clients reading `undefined`,
// which useQuery treats as "still loading".
func explicitNull(result any) any {
	if result == nil {
		return json.RawMessage("null")
	}
	return result
}

type messageTrace struct {
	ClientSentAtMS                float64 `json:"clientSentAtMs,omitempty"`
	ServerReceivedAtMS            float64 `json:"serverReceivedAtMs,omitempty"`
	ServerMutationStartedAtMS     float64 `json:"serverMutationStartedAtMs,omitempty"`
	ServerMutationCommittedAtMS   float64 `json:"serverMutationCommittedAtMs,omitempty"`
	ServerCompletedAtMS           float64 `json:"serverCompletedAtMs,omitempty"`
	ServerBroadcastScheduledAtMS  float64 `json:"serverBroadcastScheduledAtMs,omitempty"`
	ServerChangeCommittedAtMS     float64 `json:"serverChangeCommittedAtMs,omitempty"`
	ServerSubscriptionStartedAtMS float64 `json:"serverSubscriptionStartedAtMs,omitempty"`
	ServerSubscriptionSentAtMS    float64 `json:"serverSubscriptionSentAtMs,omitempty"`
	ServerDurationMS              float64 `json:"serverDurationMs,omitempty"`
}

type clientDeviceInfo struct {
	UserAgent               string  `json:"userAgent,omitempty"`
	BrowserName             string  `json:"browserName,omitempty"`
	BrowserVersion          string  `json:"browserVersion,omitempty"`
	DeviceType              string  `json:"deviceType,omitempty"`
	Platform                string  `json:"platform,omitempty"`
	Language                string  `json:"language,omitempty"`
	Timezone                string  `json:"timezone,omitempty"`
	ViewportWidth           int     `json:"viewportWidth,omitempty"`
	ViewportHeight          int     `json:"viewportHeight,omitempty"`
	HardwareConcurrency     int     `json:"hardwareConcurrency,omitempty"`
	DeviceMemory            float64 `json:"deviceMemory,omitempty"`
	TouchPoints             int     `json:"touchPoints,omitempty"`
	ConnectionType          string  `json:"connectionType,omitempty"`
	EffectiveConnectionType string  `json:"effectiveConnectionType,omitempty"`
}

type taskGridArgs struct {
	Offset          int               `json:"offset"`
	Limit           int               `json:"limit"`
	Columns         []string          `json:"columns"`
	Search          string            `json:"search,omitempty"`
	Sort            string            `json:"sort,omitempty"`
	Direction       string            `json:"direction,omitempty"`
	Count           string            `json:"count,omitempty"`
	Filters         []data.RowsFilter `json:"filters,omitempty"`
	CursorCreatedAt string            `json:"cursorCreatedAt,omitempty"`
	CursorID        string            `json:"cursorId,omitempty"`
}

type randomizeStatusPriorityArgs struct {
	Count int `json:"count"`
}

const (
	tableChangeDebounce    = 75 * time.Millisecond
	tableChangeFanoutLimit = 16
)

type querySubscription struct {
	conn       *wsConn
	id         string
	project    string
	tenant     string
	path       string
	args       json.RawMessage
	rowIDs     map[string]bool
	caller     callerContext
	ctx        context.Context
	cancel     context.CancelFunc
	token      *struct{}
	cacheScope string
}

type tableChange struct {
	project     string
	tenant      string
	table       string
	tables      map[string]bool
	broad       bool
	rowIDs      map[string]bool
	changedAtMS float64
}

type wsConn struct {
	server        *Server
	conn          *websocket.Conn
	project       string
	tenant        string
	user          *gonvex.User
	perms         map[string]any
	auth          bool
	authToken     string
	authCheckedAt time.Time
	cacheScope    string
	mu            sync.Mutex
	subs          map[string]querySubscription
}

type callerContext struct {
	user        *gonvex.User
	permissions map[string]any
}

var wsUpgrader = websocket.Upgrader{
	EnableCompression: true,
	CheckOrigin:       func(_ *http.Request) bool { return true },
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.EnableWriteCompression(true)
	_ = conn.SetCompressionLevel(flate.BestSpeed)
	project := projectID(r)
	client := &wsConn{server: s, conn: conn, project: project, tenant: tenantIDFromRequest(project, tenantID(r)), subs: map[string]querySubscription{}}
	s.addWSConn(client)
	defer func() {
		client.cancelSubscriptions()
		s.removeWSConn(client)
		_ = conn.Close()
	}()
	var initialCache *queryCacheDirective
	if !s.projectRequiresAuthentication(r.Context(), client.project) {
		initialCache = s.queryCacheDirective(client.project, client.tenant, callerContext{})
		if initialCache != nil {
			client.cacheScope = initialCache.Scope
		}
	}
	client.write(serverMessage{
		Type:       "session.ready",
		Project:    client.project,
		Tenant:     client.tenant,
		QueryCache: initialCache,
	})

	for {
		var message clientMessage
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		client.handle(r.Context(), message)
	}
}

func (c *wsConn) handle(ctx context.Context, message clientMessage) {
	receivedAt := time.Now()
	switch message.Type {
	case "auth":
		requestedProject := strings.TrimSpace(message.Project)
		if requestedProject == "" {
			requestedProject = c.project
		}
		user, permissions, project, tenant, err := c.server.authenticateSocket(ctx, requestedProject, c.tenant, message.Token, message.Tenant)
		if err != nil {
			c.clearAuthentication()
			c.write(serverMessage{Type: "auth.error", ID: message.ID, Error: err.Error()})
			return
		}
		caller := callerContext{user: user, permissions: permissions}
		directive := c.server.queryCacheDirective(project, tenant, caller)
		cacheScope := ""
		if directive != nil {
			cacheScope = directive.Scope
		}
		c.mu.Lock()
		c.user = user
		c.perms = permissions
		c.project = project
		c.tenant = tenant
		c.auth = true
		c.authToken = message.Token
		c.authCheckedAt = time.Now()
		c.cacheScope = cacheScope
		subs := make([]querySubscription, 0, len(c.subs))
		for id, sub := range c.subs {
			if sub.cancel != nil {
				sub.cancel()
			}
			subCtx, cancel := context.WithCancel(ctx)
			sub.ctx = subCtx
			sub.cancel = cancel
			sub.project = project
			sub.tenant = tenant
			sub.caller = caller
			sub.token = &struct{}{}
			sub.cacheScope = cacheScope
			c.subs[id] = sub
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		authResult := map[string]any{"userId": user.ID, "projectId": project, "tenantId": tenant}
		if directive != nil {
			authResult["queryCache"] = directive
		}
		c.write(serverMessage{Type: "auth.result", ID: message.ID, Result: authResult})
		c.server.rerunSubscriptions(subs, "initial", 0)
	case "query.subscribe":
		if !c.requireAuth(ctx, "query.error", message.ID) {
			return
		}
		if message.ID == "" || message.Path == "" {
			c.write(serverMessage{Type: "query.error", ID: message.ID, Error: "query id and path are required"})
			return
		}
		subCtx, cancel := context.WithCancel(ctx)
		sub := querySubscription{conn: c, id: message.ID, project: c.project, tenant: c.tenant, path: message.Path, args: message.Args, caller: c.caller(), ctx: subCtx, cancel: cancel, token: &struct{}{}, cacheScope: c.currentCacheScope()}
		c.mu.Lock()
		previous, hadPrevious := c.subs[message.ID]
		c.subs[message.ID] = sub
		c.mu.Unlock()
		if hadPrevious && previous.cancel != nil {
			previous.cancel()
		}
		go c.server.executeSubscription(subCtx, sub, "initial", 0)
	case "query.unsubscribe":
		c.mu.Lock()
		sub, ok := c.subs[message.ID]
		if ok {
			delete(c.subs, message.ID)
		}
		c.mu.Unlock()
		if ok && sub.cancel != nil {
			sub.cancel()
		}
	case "mutation.call":
		if !c.requireAuth(ctx, "mutation.error", message.ID) {
			return
		}
		trace := traceFromClient(message.Trace)
		trace.ServerReceivedAtMS = epochMillis(receivedAt)
		trace.ServerMutationStartedAtMS = epochMillis(time.Now())
		result, err := c.server.executeTenantMutationForCaller(ctx, c.project, c.tenant, c.caller(), message.Path, message.Args)
		committedAt := time.Now().UTC()
		trace.ServerMutationCommittedAtMS = epochMillis(committedAt)
		trace.ServerCompletedAtMS = epochMillis(committedAt)
		trace.ServerDurationMS = float64(committedAt.Sub(receivedAt).Microseconds()) / 1000
		if err != nil {
			c.write(serverMessage{Type: "mutation.error", ID: message.ID, Path: message.Path, Error: err.Error(), Trace: trace})
			c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, message.ID, "mutation", message.Path, "server", "", "error", err.Error(), trace))
			return
		}
		trace.ServerBroadcastScheduledAtMS = epochMillis(time.Now())
		c.write(serverMessage{Type: "mutation.result", ID: message.ID, Path: message.Path, Result: explicitNull(result), Trace: trace})
		c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, message.ID, "mutation", message.Path, "server", "", "ok", "", trace))
		c.server.broadcastMutationInvalidationsAt(c.project, c.tenant, message.Path, committedAt)
	case "action.call":
		if !c.requireAuth(ctx, "action.error", message.ID) {
			return
		}
		trace := traceFromClient(message.Trace)
		trace.ServerReceivedAtMS = epochMillis(receivedAt)
		result, err := c.server.executeTenantActionForCaller(ctx, c.project, c.tenant, c.caller(), message.Path, message.Args)
		completedAt := time.Now().UTC()
		trace.ServerCompletedAtMS = epochMillis(completedAt)
		trace.ServerDurationMS = float64(completedAt.Sub(receivedAt).Microseconds()) / 1000
		if err != nil {
			c.write(serverMessage{Type: "action.error", ID: message.ID, Path: message.Path, Error: err.Error(), Trace: trace})
			c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, message.ID, "action", message.Path, "server", "", "error", err.Error(), trace))
			return
		}
		c.write(serverMessage{Type: "action.result", ID: message.ID, Path: message.Path, Result: explicitNull(result), Trace: trace})
		c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, message.ID, "action", message.Path, "server", "", "ok", "", trace))
		// Actions write rows too (assistant.processThread appends the reply,
		// tasks.bulkDelete soft-deletes, ...). Without a broadcast their writes
		// never invalidate live queries, so clients sit on stale results until a
		// reload. Mirror the mutation path's completion broadcast.
		c.server.broadcastMutationInvalidationsAt(c.project, c.tenant, message.Path, completedAt)
	case "telemetry.event":
		c.server.recordTransactionTelemetry(transactionEntryFromClientTelemetry(c.project, c.tenant, message))
	default:
		c.write(serverMessage{Type: "query.error", ID: message.ID, Error: "unknown websocket message type"})
	}
}

func traceFromClient(in *messageTrace) *messageTrace {
	if in == nil {
		return &messageTrace{}
	}
	copy := *in
	return &copy
}

func epochMillis(t time.Time) float64 {
	return float64(t.UTC().UnixNano()) / float64(time.Millisecond)
}

func transactionEntryFromTrace(project string, tenant string, operationID string, kind string, path string, phase string, reason string, outcome string, errorMessage string, trace *messageTrace) transactionTelemetryEntry {
	now := time.Now().UTC()
	entry := transactionTelemetryEntry{
		Time:        now.Format(time.RFC3339Nano),
		Project:     project,
		Tenant:      tenant,
		OperationID: operationID,
		Kind:        kind,
		Path:        path,
		Phase:       phase,
		Reason:      reason,
		Outcome:     outcome,
		Error:       errorMessage,
	}
	if trace == nil {
		return entry
	}
	entry.ClientSentAtMS = trace.ClientSentAtMS
	entry.ServerReceivedAtMS = trace.ServerReceivedAtMS
	entry.ServerCommittedAtMS = trace.ServerMutationCommittedAtMS
	entry.ServerCompletedAtMS = trace.ServerCompletedAtMS
	entry.ServerSentAtMS = trace.ServerSubscriptionSentAtMS
	entry.ChangeCommittedAtMS = trace.ServerChangeCommittedAtMS
	entry.ServerDurationMS = trace.ServerDurationMS
	if trace.ServerMutationStartedAtMS > 0 && trace.ServerMutationCommittedAtMS > 0 {
		entry.ServerCommitMS = float64(trace.ServerMutationCommittedAtMS - trace.ServerMutationStartedAtMS)
	} else if trace.ServerReceivedAtMS > 0 && trace.ServerMutationCommittedAtMS > 0 {
		entry.ServerCommitMS = float64(trace.ServerMutationCommittedAtMS - trace.ServerReceivedAtMS)
	}
	if trace.ClientSentAtMS > 0 && trace.ServerMutationCommittedAtMS > 0 {
		entry.ClientToCommitMS = float64(trace.ServerMutationCommittedAtMS - trace.ClientSentAtMS)
	}
	if trace.ServerSubscriptionStartedAtMS > 0 && trace.ServerSubscriptionSentAtMS > 0 {
		entry.SubscriptionDurationMS = float64(trace.ServerSubscriptionSentAtMS - trace.ServerSubscriptionStartedAtMS)
	}
	return entry
}

func transactionEntryFromClientTelemetry(project string, tenant string, message clientMessage) transactionTelemetryEntry {
	trace := traceFromClient(message.Trace)
	entry := transactionEntryFromTrace(project, tenant, message.ID, message.Kind, message.Path, "browser", message.Reason, message.Outcome, message.Error, trace)
	entry.ClientReceivedAtMS = message.ClientReceivedAtMS
	entry.ClientDurationMS = message.ClientDurationMS
	if len(message.Device) > 0 {
		entry.DeviceJSON = string(message.Device)
		var device clientDeviceInfo
		if err := json.Unmarshal(message.Device, &device); err == nil {
			entry.UserAgent = device.UserAgent
			entry.BrowserName = device.BrowserName
			entry.BrowserVersion = device.BrowserVersion
			entry.DeviceType = device.DeviceType
			entry.Platform = device.Platform
			entry.Language = device.Language
			entry.Timezone = device.Timezone
			entry.ViewportWidth = device.ViewportWidth
			entry.ViewportHeight = device.ViewportHeight
		}
	}
	if entry.Time == "" {
		entry.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if message.ClientReceivedAtMS > 0 {
		if message.ClientSentAtMS > 0 {
			entry.ClientSentAtMS = message.ClientSentAtMS
		}
		if message.ClientDurationMS > 0 {
			entry.ClientRoundTripMS = message.ClientDurationMS
		} else if entry.ClientSentAtMS > 0 {
			entry.ClientRoundTripMS = float64(message.ClientReceivedAtMS - entry.ClientSentAtMS)
		}
		if trace.ServerCompletedAtMS > 0 {
			entry.ServerToBrowserMS = float64(message.ClientReceivedAtMS - trace.ServerCompletedAtMS)
		} else if trace.ServerSubscriptionSentAtMS > 0 {
			entry.ServerToBrowserMS = float64(message.ClientReceivedAtMS - trace.ServerSubscriptionSentAtMS)
		}
		if trace.ServerChangeCommittedAtMS > 0 {
			entry.ChangeToBrowserMS = float64(message.ClientReceivedAtMS - trace.ServerChangeCommittedAtMS)
		}
	}
	if entry.Outcome == "" {
		entry.Outcome = "ok"
	}
	return entry
}

func (c *wsConn) requireAuth(ctx context.Context, errorType string, id string) bool {
	if !c.server.projectRequiresAuthentication(ctx, c.project) {
		return true
	}
	c.mu.Lock()
	authenticated := c.auth
	c.mu.Unlock()
	if authenticated && c.revalidateAppAuth(ctx) == nil {
		return true
	}
	if authenticated {
		c.clearAuthentication()
	}
	c.write(serverMessage{Type: errorType, ID: id, Error: "authentication is required"})
	return false
}

func (c *wsConn) revalidateAppAuth(ctx context.Context) error {
	c.mu.Lock()
	token := c.authToken
	project := c.project
	tenant := c.tenant
	checkedAt := c.authCheckedAt
	authenticated := c.auth
	c.mu.Unlock()
	nativeEnabled := false
	if strings.TrimSpace(c.server.projectRegistryURL()) != "" {
		var err error
		nativeEnabled, err = c.server.nativeAppAuthEnabled(ctx, project)
		if err != nil {
			return fmt.Errorf("project authentication configuration is unavailable")
		}
	}
	if !c.server.config.RequireAuth && !nativeEnabled {
		return nil
	}
	if !authenticated {
		return fmt.Errorf("authentication is required")
	}
	nativeToken := strings.HasPrefix(strings.TrimSpace(token), "gvx_session_")
	if nativeEnabled && !nativeToken {
		return fmt.Errorf("a Gonvex app session is required")
	}
	if !nativeToken || time.Since(checkedAt) < 5*time.Second {
		return nil
	}
	session, _, err := c.server.validateAppSession(ctx, project, token, tenant)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.authToken == token {
		c.user = &gonvex.User{ID: session.User.ID, Email: session.User.Email}
		c.perms = session.Permissions
		c.authCheckedAt = time.Now()
	}
	c.mu.Unlock()
	return nil
}

func (c *wsConn) caller() callerContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return callerContext{user: c.user, permissions: c.perms}
}

func (c *wsConn) clearAuthentication() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.user = nil
	c.perms = nil
	c.auth = false
	c.authToken = ""
	c.authCheckedAt = time.Time{}
	c.cacheScope = ""
	for id, sub := range c.subs {
		if sub.cancel != nil {
			sub.cancel()
		}
		sub.caller = callerContext{}
		sub.cacheScope = ""
		sub.token = &struct{}{}
		c.subs[id] = sub
	}
}

func (c *wsConn) currentCacheScope() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cacheScope
}

func (c *wsConn) cancelSubscriptions() {
	c.mu.Lock()
	subs := make([]querySubscription, 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.subs = map[string]querySubscription{}
	c.mu.Unlock()
	for _, sub := range subs {
		if sub.cancel != nil {
			sub.cancel()
		}
	}
}

func (c *wsConn) write(message serverMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.WriteJSON(message)
}

func (s *Server) addWSConn(conn *wsConn) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	s.wsConns[conn] = true
}

func (s *Server) removeWSConn(conn *wsConn) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	delete(s.wsConns, conn)
}

func (s *Server) revokeAppAuthConnections(projectID string, userID string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := connection.project == projectID && connection.user != nil && connection.user.ID == userID && strings.HasPrefix(connection.authToken, "gvx_session_")
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "session-revoked", Error: "authentication session was revoked"})
		}
	}
}

func (s *Server) revokeAppAuthTokenConnection(token string) {
	if token == "" {
		return
	}
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := constantTimeString(connection.authToken, token)
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "session-revoked", Error: "authentication session was revoked"})
		}
	}
}

// enforceNativeAppAuthConnections immediately cancels anonymous and legacy
// subscriptions when a project transitions from public/legacy auth to native
// app auth. Existing native sessions can remain and are revalidated normally.
func (s *Server) enforceNativeAppAuthConnections(projectID string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := connection.project == projectID && !strings.HasPrefix(strings.TrimSpace(connection.authToken), "gvx_session_")
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "auth-required", Error: "this project now requires a Gonvex app session"})
		}
	}
}

func (s *Server) websocketStats() (int, int) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		connections = append(connections, conn)
	}
	s.wsMu.RUnlock()

	subscriptions := 0
	for _, conn := range connections {
		conn.mu.Lock()
		subscriptions += len(conn.subs)
		conn.mu.Unlock()
	}
	return len(connections), subscriptions
}

// rerunProjectSubscriptions refreshes every live query after a project bundle
// is installed. A client can connect while /dev/sync is still compiling the
// bundle and receive an initial "not implemented" error; table-specific
// invalidation is insufficient because reference-data queries do not depend on
// the tasks table. The new bundle can also change any query's implementation,
// so all subscriptions for that project must be evaluated again.
func (s *Server) projectSubscriptions(projectID string) []querySubscription {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		connections = append(connections, conn)
	}
	s.wsMu.RUnlock()

	subs := make([]querySubscription, 0)
	for _, conn := range connections {
		conn.mu.Lock()
		for _, sub := range conn.subs {
			if sub.project == projectID {
				subs = append(subs, sub)
			}
		}
		conn.mu.Unlock()
	}
	return subs
}

func (s *Server) rerunProjectSubscriptions(projectID string) {
	s.rerunSubscriptions(s.projectSubscriptions(projectID), "invalidate", epochMillis(time.Now().UTC()))
}

func (s *Server) broadcastTableChange(projectID string, table string) {
	s.broadcastTenantTableChange(projectID, tenantIDFromRequest(projectID, ""), table)
}

func (s *Server) broadcastTenantTableChange(projectID string, tenantID string, table string) {
	s.broadcastTenantTableChangeAt(projectID, tenantID, table, time.Now().UTC())
}

func (s *Server) broadcastTenantTableChangeAt(projectID string, tenantID string, table string, changedAt time.Time) {
	s.scheduleTableChange(tableChange{project: projectID, tenant: tenantIDFromRequest(projectID, tenantID), table: table, broad: true, changedAtMS: epochMillis(changedAt)})
}

// broadcastMutationInvalidationsAt translates a public function path into the
// physical tables its handler can change. App modules do not always share a
// name with their tables (for example chat.sendWorkspaceChat writes
// workspaceChat), so invalidating only the path prefix leaves both the row
// cache and live queries stale after a successful mutation.
func (s *Server) broadcastMutationInvalidationsAt(projectID string, tenantID string, path string, changedAt time.Time) {
	tables := mutationInvalidationTables(path)
	if len(tables) == 0 {
		s.cache.invalidateQueries(context.Background(), projectID, tenantIDFromRequest(projectID, tenantID), nil)
		return
	}
	changedTables := make(map[string]bool, len(tables))
	for _, table := range tables {
		changedTables[table] = true
	}
	s.scheduleTableChange(tableChange{
		project:     projectID,
		tenant:      tenantIDFromRequest(projectID, tenantID),
		tables:      changedTables,
		broad:       true,
		changedAtMS: epochMillis(changedAt),
	})
}

func (s *Server) broadcastMutationInvalidations(projectID string, tenantID string, path string) {
	s.broadcastMutationInvalidationsAt(projectID, tenantID, path, time.Now().UTC())
}

func (s *Server) broadcastRowIDChange(projectID string, table string, rowIDs []string) {
	s.broadcastTenantRowIDChange(projectID, tenantIDFromRequest(projectID, ""), table, rowIDs)
}

func (s *Server) broadcastTenantRowIDChange(projectID string, tenantID string, table string, rowIDs []string) {
	ids := map[string]bool{}
	for _, id := range rowIDs {
		ids[id] = true
	}
	s.scheduleTableChange(tableChange{project: projectID, tenant: tenantIDFromRequest(projectID, tenantID), table: table, rowIDs: ids, changedAtMS: epochMillis(time.Now())})
}

func (s *Server) scheduleTableChange(change tableChange) {
	changedTables := tableChangeTables(change)
	s.cache.invalidateQueries(context.Background(), change.project, change.tenant, changedTables)
	for _, table := range changedTables {
		s.cache.invalidateRows(context.Background(), change.project, change.tenant, table)
	}
	s.tableChangeMu.Lock()
	tableKey := strings.Join(changedTables, "\x1f")
	key := strings.Join([]string{change.project, change.tenant, tableKey}, ":")
	pending := s.tableChanges[key]
	pending.project = change.project
	pending.tenant = change.tenant
	pending.table = change.table
	if pending.tables == nil && len(change.tables) > 0 {
		pending.tables = map[string]bool{}
	}
	for table := range change.tables {
		pending.tables[table] = true
	}
	pending.broad = pending.broad || change.broad
	if change.changedAtMS > pending.changedAtMS {
		pending.changedAtMS = change.changedAtMS
	}
	if pending.rowIDs == nil {
		pending.rowIDs = map[string]bool{}
	}
	for id := range change.rowIDs {
		pending.rowIDs[id] = true
	}
	s.tableChanges[key] = pending
	if timer := s.tableChangeWait[key]; timer != nil {
		timer.Stop()
	}
	s.tableChangeWait[key] = time.AfterFunc(tableChangeDebounce, func() {
		s.flushTableChange(key)
	})
	s.tableChangeMu.Unlock()
}

func (s *Server) flushTableChange(key string) {
	s.tableChangeMu.Lock()
	change := s.tableChanges[key]
	delete(s.tableChangeWait, key)
	delete(s.tableChanges, key)
	s.tableChangeMu.Unlock()

	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		connections = append(connections, conn)
	}
	s.wsMu.RUnlock()
	subs := []querySubscription{}
	for _, conn := range connections {
		conn.mu.Lock()
		for _, sub := range conn.subs {
			if sub.project == change.project && sub.tenant == change.tenant && tableChangeMatchesSubscription(sub, change) && subscriptionIntersectsChange(sub, change) {
				subs = append(subs, sub)
			}
		}
		conn.mu.Unlock()
	}
	s.rerunSubscriptions(subs, "invalidate", change.changedAtMS)
}

func tableChangeMatchesSubscription(sub querySubscription, change tableChange) bool {
	if len(change.tables) == 0 {
		return change.table == "" || subscriptionDependsOnTable(sub.path, change.table)
	}
	for table := range change.tables {
		if table == "" || subscriptionDependsOnTable(sub.path, table) {
			return true
		}
	}
	return false
}

func tableChangeTables(change tableChange) []string {
	if len(change.tables) == 0 {
		return []string{change.table}
	}
	tables := make([]string, 0, len(change.tables))
	for table := range change.tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func (s *Server) rerunSubscriptions(subs []querySubscription, reason string, changeCommittedAtMS float64) {
	if len(subs) == 0 {
		return
	}
	limit := tableChangeFanoutLimit
	if len(subs) < limit {
		limit = len(subs)
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for _, sub := range subs {
		sub := sub
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.executeSubscription(sub.ctx, sub, reason, changeCommittedAtMS)
		}()
	}
	wg.Wait()
}

func subscriptionIntersectsChange(sub querySubscription, change tableChange) bool {
	if change.broad || subscriptionCanChangeMembership(sub) || len(change.rowIDs) == 0 || len(sub.rowIDs) == 0 {
		return true
	}
	for id := range change.rowIDs {
		if sub.rowIDs[id] {
			return true
		}
	}
	return false
}

func subscriptionCanChangeMembership(sub querySubscription) bool {
	if sub.path != "tasks.grid" {
		return true
	}
	var args taskGridArgs
	if len(sub.args) > 0 {
		if err := json.Unmarshal(sub.args, &args); err != nil {
			return true
		}
	}
	return strings.TrimSpace(args.Search) != "" || args.Sort != "" || len(args.Filters) > 0
}

func (s *Server) executeSubscription(ctx context.Context, sub querySubscription, reason string, changeCommittedAtMS float64) {
	// A subscription can be cancelled while a table-change fan-out is already
	// queued. That stale rerun belongs to the subscription, not the connection,
	// and must never clear authentication for every other live subscription.
	if ctx.Err() != nil {
		return
	}
	if err := sub.conn.revalidateAppAuth(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		sub.conn.clearAuthentication()
		sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Error: "authentication is required"})
		return
	}
	startedAt := time.Now().UTC()
	cacheRevision := s.nextQueryCacheRevision()
	result, err := s.executeTenantQueryForCallerCached(ctx, sub.project, sub.tenant, sub.caller, sub.path, sub.args, sub.cacheScope, reason)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Error: err.Error()})
		s.recordTransactionTelemetry(transactionTelemetryEntry{
			Time:        time.Now().UTC().Format(time.RFC3339Nano),
			Project:     sub.project,
			Tenant:      sub.tenant,
			OperationID: sub.id,
			Kind:        "query",
			Path:        sub.path,
			Phase:       "server",
			Reason:      reason,
			Outcome:     "error",
			Error:       err.Error(),
		})
		return
	}
	sub.rowIDs = resultRowIDs(result)
	sub.conn.mu.Lock()
	current, ok := sub.conn.subs[sub.id]
	if ok && current.token == sub.token {
		current.rowIDs = sub.rowIDs
		sub.conn.subs[sub.id] = current
	}
	sub.conn.mu.Unlock()
	if !ok || current.token != sub.token {
		return
	}
	sentAt := time.Now().UTC()
	trace := &messageTrace{
		ServerChangeCommittedAtMS:     changeCommittedAtMS,
		ServerSubscriptionStartedAtMS: epochMillis(startedAt),
		ServerSubscriptionSentAtMS:    epochMillis(sentAt),
		ServerDurationMS:              float64(sentAt.Sub(startedAt).Microseconds()) / 1000,
	}
	sub.conn.write(serverMessage{Type: "query.result", ID: sub.id, Path: sub.path, Result: explicitNull(result), Reason: reason, Trace: trace, CacheScope: sub.cacheScope, CacheRevision: cacheRevision})
	s.recordTransactionTelemetry(transactionEntryFromTrace(sub.project, sub.tenant, sub.id, "query", sub.path, "server", reason, "ok", "", trace))
}

func resultRowIDs(result any) map[string]bool {
	rowsResult, ok := result.(data.RowsResult)
	if !ok {
		return nil
	}
	ids := map[string]bool{}
	for _, row := range rowsResult.Rows {
		if value, ok := row["id"].(string); ok && value != "" {
			ids[value] = true
		}
	}
	return ids
}

func (s *Server) executeQuery(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantQuery(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantQuery(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantQueryForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantQueryForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantQueryForCallerCached(ctx, projectID, tenantID, caller, path, rawArgs, "", "internal")
}

func (s *Server) executeTenantQueryForCallerCached(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage, cacheScope string, reason string) (result any, err error) {
	kind := s.functionKind(projectID, path, "query")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	execution.entry.Cache = "bypass"
	execution.entry.Source = "database"
	execution.entry.Reason = reason
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	cacheKey := ""
	cacheGeneration := ""
	queryTables := subscriptionTables(path)
	if strings.TrimSpace(cacheScope) != "" && s.cache.enabled() {
		if generation, ok := s.cache.queryGeneration(ctx, projectID, tenantID, queryTables); ok {
			cacheGeneration = generation
			cacheKey = s.cache.queryKey(projectID, tenantID, generation, cacheScope, path, rawArgs)
		} else {
			execution.entry.Cache = "error"
		}
		if cacheKey != "" {
			payload, outcome := s.cache.read(ctx, cacheKey)
			if outcome == "hit" {
				if decodeErr := json.Unmarshal(payload, &result); decodeErr == nil {
					execution.entry.Cache = "hit"
					execution.entry.Source = "redis"
					s.metrics.recordCache(projectID, "hit")
					return result, nil
				}
				execution.entry.Cache = "error"
				s.metrics.recordCache(projectID, "bypass")
			} else {
				execution.entry.Cache = outcome
				if outcome == "miss" {
					s.metrics.recordCache(projectID, "miss")
				} else {
					s.metrics.recordCache(projectID, "bypass")
				}
			}
		} else {
			s.metrics.recordCache(projectID, "bypass")
		}
	} else if strings.TrimSpace(cacheScope) != "" {
		s.metrics.recordCache(projectID, "bypass")
	}

	result, err = s.executeTenantQueryForCallerUncached(ctx, projectID, tenantID, caller, path, rawArgs)
	if err == nil && cacheKey != "" {
		currentGeneration, generationOK := s.cache.queryGeneration(ctx, projectID, tenantID, queryTables)
		if payload, encodeErr := json.Marshal(result); encodeErr == nil && generationOK && currentGeneration == cacheGeneration {
			s.cache.set(ctx, cacheKey, payload)
		}
	}
	return result, err
}

func (s *Server) executeTenantQueryForCallerUncached(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (any, error) {
	if isLegacyTaskQuery(path) {
		return s.executeLegacyQuery(ctx, projectID, tenantID, path, rawArgs)
	}
	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup(path); ok {
		queryCtx, err := s.queryContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		return app.ExecuteQuery(queryCtx, path, rawArgs)
	}
	return nil, fmt.Errorf("query %q is not implemented by the runtime", path)
}

func (s *Server) executeLegacyQuery(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (any, error) {
	// Resolve the project/tenant database before reading. The registered-function
	// path hydrates via appForProject + runtimeContext, but the legacy grid path
	// skips both, so without this the first query after a (re)start hits the
	// fallback control DB and fails with relation "tasks" does not exist.
	s.hydrateRuntimeStateForProject(ctx, projectID)
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, tenantID)
	var err error
	databaseURL, err = s.ensureRuntimeTenantDatabase(ctx, projectID, tenantIDFromRequest(projectID, tenantID), databaseURL)
	if err != nil {
		return nil, err
	}
	switch path {
	case "tasks.grid":
		var args taskGridArgs
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return nil, err
			}
		}
		return data.ReadTaskGrid(ctx, databaseURL, data.RowsOptions{
			Limit:           args.Limit,
			Offset:          args.Offset,
			Search:          args.Search,
			SortColumn:      args.Sort,
			SortDirection:   args.Direction,
			Columns:         args.Columns,
			Filters:         args.Filters,
			ExactTotal:      args.Count != "false" && args.Count != "estimate",
			EstimateTotal:   args.Count == "estimate",
			CursorCreatedAt: args.CursorCreatedAt,
			CursorID:        args.CursorID,
		})
	default:
		return nil, fmt.Errorf("query %q is not implemented by the runtime", path)
	}
}

func (s *Server) executeMutation(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantMutation(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantMutation(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantMutationForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantMutationForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	kind := s.functionKind(projectID, path, "mutation")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	if isLegacyTaskMutation(path) {
		return s.executeLegacyMutation(ctx, projectID, tenantID, path, rawArgs)
	}
	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup(path); ok {
		mutationCtx, err := s.mutationContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		result, err := s.executeRegisteredMutation(app, mutationCtx, path, rawArgs)
		if err != nil {
			return nil, err
		}
		if path == "tenants.create" {
			if err := s.provisionCreatedTenant(ctx, projectID, result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("mutation %q is not implemented by the runtime", path)
}

func (s *Server) executeRegisteredMutation(app *gonvex.App, mutationCtx *gonvex.MutationCtx, path string, rawArgs json.RawMessage) (any, error) {
	return s.runMutationInTx(mutationCtx, path, rawArgs, app.ExecuteMutation)
}

// runMutationInTx runs a mutation-style handler inside a database transaction
// when a database is configured, committing on success and rolling back on
// error. It is shared by client-triggered mutations and scheduled internal
// mutations so both get the same transactional guarantees.
func (s *Server) runMutationInTx(mutationCtx *gonvex.MutationCtx, path string, rawArgs json.RawMessage, exec func(*gonvex.MutationCtx, string, json.RawMessage) (any, error)) (any, error) {
	if mutationCtx.DB == nil {
		return exec(mutationCtx, path, rawArgs)
	}
	if mutationCtx.Context == nil {
		mutationCtx.Context = context.Background()
	}
	tx, err := mutationCtx.DB.BeginTx(mutationCtx.Context, nil)
	if err != nil {
		return nil, err
	}
	mutationCtx.Tx = tx
	originalScheduler := mutationCtx.Scheduler
	deferred := newDeferredScheduler(originalScheduler)
	mutationCtx.Scheduler = deferred
	defer func() {
		mutationCtx.Scheduler = originalScheduler
	}()
	result, err := exec(mutationCtx, path, rawArgs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	mutationCtx.Tx = nil
	if err := deferred.flush(); err != nil {
		mutationCtx.Logger.Error("failed to publish committed scheduled work", "path", path, "error", err)
	}
	return result, nil
}

func (s *Server) executeLegacyMutation(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (any, error) {
	s.hydrateRuntimeStateForProject(ctx, projectID)
	s.hydrateProjectTenantDatabases(ctx, projectID)
	switch path {
	case "tasks.randomizeStatusPriority":
		var args randomizeStatusPriorityArgs
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return nil, err
			}
		}
		databaseURL := s.databaseURLForTenant(projectID, tenantID)
		databaseURL, err := s.ensureRuntimeTenantDatabase(ctx, projectID, tenantIDFromRequest(projectID, tenantID), databaseURL)
		if err != nil {
			return nil, err
		}
		result, err := data.RandomizeTaskStatusPriority(ctx, databaseURL, args.Count)
		if err != nil {
			return nil, err
		}
		go s.broadcastTenantTableChange(projectID, tenantID, "tasks")
		return result, nil
	default:
		return nil, fmt.Errorf("mutation %q is not implemented by the runtime", path)
	}
}

func (s *Server) executeAction(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantAction(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantAction(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantActionForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantActionForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	kind := s.functionKind(projectID, path, "action")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup(path); ok {
		actionCtx, err := s.actionContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		return app.ExecuteAction(actionCtx, path, rawArgs)
	}
	return nil, fmt.Errorf("action %q is not implemented by the runtime", path)
}

func (s *Server) functionKind(projectID string, path string, fallback string) string {
	if function, ok := s.app.Lookup(path); ok && function.Kind != "" {
		return string(function.Kind)
	}
	if entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]; ok && entry.Kind != "" {
		return string(entry.Kind)
	}
	return fallback
}

func (s *Server) queryContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.QueryCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.QueryCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) mutationContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.MutationCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.MutationCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) actionContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.ActionCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.ActionCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) runtimeContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (gonvex.RuntimeContext, error) {
	if err := s.requireProjectDatabase(projectID); err != nil {
		return gonvex.RuntimeContext{}, err
	}
	activeTenant := tenantIDFromRequest(projectID, tenantID)
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, activeTenant)
	var err error
	databaseURL, err = s.ensureRuntimeTenantDatabase(ctx, projectID, activeTenant, databaseURL)
	if err != nil {
		return gonvex.RuntimeContext{}, err
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, activeTenant), databaseURL)
	if err != nil {
		return gonvex.RuntimeContext{}, err
	}
	landlordURL := s.databaseURLForProject(projectID)
	landlordStore, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, "__landlord__"), landlordURL)
	if err != nil {
		return gonvex.RuntimeContext{}, err
	}
	logger := slog.Default().With("project", projectID, "tenant", activeTenant)
	storageAPI := s.storageForTenant(ctx, projectID, activeTenant, store.DB, caller, logger)
	dataAPI := s.dataForTenant(projectID, activeTenant, storageAPI)
	return gonvex.RuntimeContext{
		Context:     ctx,
		ProjectID:   projectID,
		TenantID:    activeTenant,
		DatabaseURL: store.DatabaseURL,
		DB:          store.DB,
		LandlordDB:  landlordStore.DB,
		TenantDB:    store.DB,
		Storage:     storageAPI,
		Sandbox:     s.sandboxForCaller(projectID, activeTenant, caller, dataAPI),
		Data:        dataAPI,
		Scheduler:   s.scheduler.For(projectID, activeTenant),
		User:        caller.user,
		Permissions: caller.permissions,
		Logger:      logger,
		NotifyTableChange: func(table string) {
			s.broadcastTenantTableChange(projectID, activeTenant, table)
		},
		Env: s.projectEnvValues(ctx, projectID),
	}, nil
}

func (s *Server) sandboxForCaller(projectID string, tenantID string, caller callerContext, dataAPI gonvex.DataAPI) gonvex.SandboxAPI {
	if dataAPI == nil {
		dataAPI = gonvex.UnavailableData()
	}
	runner := sandbox.NewRunner("")
	runner.Host = sandbox.HostFunc(func(ctx context.Context, req sandbox.HostCallRequest) (any, error) {
		args := req.Args
		if len(args) == 0 {
			args = json.RawMessage(`{}`)
		}
		switch strings.TrimSpace(req.Kind) {
		case "query":
			path, resolvedArgs, err := s.resolveSandboxFunction(ctx, projectID, tenantID, caller, "query", strings.TrimSpace(req.Path), args)
			if err != nil {
				return nil, err
			}
			return s.executeTenantQueryForCaller(ctx, projectID, tenantID, caller, path, resolvedArgs)
		case "action":
			path := strings.TrimSpace(req.Path)
			// Curated-action parity with the browser sandbox: names that are not
			// registered runtime actions (e.g. "spots.bulkCreate") route through
			// the app's assistant.sandboxAction dispatcher when one is registered,
			// so whagonsAction(name, args) supports the same curated surface as
			// api.whagons.action in the TS sandbox.
			app := s.appForProject(ctx, projectID)
			if fn, ok := app.Lookup(path); !ok || fn.Kind != gonvex.FunctionKindAction {
				if _, ok := app.Lookup("assistant.sandboxAction"); ok {
					wrapped, wrapErr := json.Marshal(map[string]any{"name": path, "args": json.RawMessage(args)})
					if wrapErr != nil {
						return nil, wrapErr
					}
					result, err := s.executeTenantActionForCaller(ctx, projectID, tenantID, caller, "assistant.sandboxAction", wrapped)
					if err == nil {
						s.broadcastMutationInvalidations(projectID, tenantID, path)
					}
					return result, err
				}
			}
			result, err := s.executeTenantActionForCaller(ctx, projectID, tenantID, caller, path, args)
			if err == nil {
				s.broadcastMutationInvalidations(projectID, tenantID, req.Path)
			}
			return result, err
		case "mutation":
			path, resolvedArgs, err := s.resolveSandboxFunction(ctx, projectID, tenantID, caller, "mutation", strings.TrimSpace(req.Path), args)
			if err != nil {
				return nil, err
			}
			result, err := s.executeTenantMutationForCaller(ctx, projectID, tenantID, caller, path, resolvedArgs)
			if err == nil {
				s.broadcastMutationInvalidations(projectID, tenantID, path)
			}
			return result, err
		case "data.inspect":
			var inspectReq gonvex.DataInspectRequest
			if err := json.Unmarshal(args, &inspectReq); err != nil {
				return nil, fmt.Errorf("invalid data.inspect args: %w", err)
			}
			return dataAPI.Inspect(ctx, inspectReq)
		case "data.query":
			var queryReq gonvex.DataQueryRequest
			if err := json.Unmarshal(args, &queryReq); err != nil {
				return nil, fmt.Errorf("invalid data.query args: %w", err)
			}
			return dataAPI.Query(ctx, queryReq)
		case "data.profile":
			var profileReq gonvex.DataProfileRequest
			if err := json.Unmarshal(args, &profileReq); err != nil {
				return nil, fmt.Errorf("invalid data.profile args: %w", err)
			}
			return dataAPI.Profile(ctx, profileReq)
		default:
			return nil, fmt.Errorf("unsupported sandbox host call kind %q", req.Kind)
		}
	})
	return runner
}

// resolveSandboxFunction gates sandbox-originated query/mutation calls through
// the app's assistant.sandboxResolve query when one is registered. The app
// owns the policy (blocked modules, name aliases like tasks.list); the runtime
// just executes whatever {name, args} the resolver returns. Apps without a
// resolver keep the raw path — same behavior as before this hook existed.
func (s *Server) resolveSandboxFunction(ctx context.Context, projectID string, tenantID string, caller callerContext, kind string, path string, args json.RawMessage) (string, json.RawMessage, error) {
	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup("assistant.sandboxResolve"); !ok {
		return path, args, nil
	}
	wrapped, err := json.Marshal(map[string]any{"kind": kind, "name": path, "args": args})
	if err != nil {
		return "", nil, err
	}
	resolved, err := s.executeTenantQueryForCaller(ctx, projectID, tenantID, caller, "assistant.sandboxResolve", wrapped)
	if err != nil {
		return "", nil, err
	}
	resolvedMap, ok := resolved.(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("assistant.sandboxResolve returned an unexpected result for %q", path)
	}
	name, _ := resolvedMap["name"].(string)
	if strings.TrimSpace(name) == "" {
		return "", nil, fmt.Errorf("assistant.sandboxResolve returned no function name for %q", path)
	}
	resolvedArgs, err := json.Marshal(resolvedMap["args"])
	if err != nil {
		return "", nil, err
	}
	return name, resolvedArgs, nil
}

// storageForTenant builds the per-request storage handle bound to the active
// tenant database. It returns nil (leaving the not-configured fallback in
// place) when storage is unconfigured or the metadata table cannot be ensured,
// so storage problems never break functions that don't use storage.
func (s *Server) storageForTenant(ctx context.Context, projectID, tenantID string, db *sql.DB, caller callerContext, logger *slog.Logger) gonvex.StorageAPI {
	if s.storage == nil || db == nil {
		return nil
	}
	ownerID := ""
	if caller.user != nil {
		ownerID = caller.user.ID
	}
	tenant, err := s.storage.Tenant(ctx, db, projectID, tenantID, ownerID)
	if err != nil {
		logger.Warn("storage unavailable for tenant", "error", err)
		return nil
	}
	return tenant
}

func isLegacyTaskQuery(path string) bool {
	return path == "tasks.grid"
}

func isLegacyTaskMutation(path string) bool {
	return path == "tasks.randomizeStatusPriority"
}

func subscriptionTable(path string) string {
	prefix, _, ok := strings.Cut(path, ".")
	if !ok || prefix == "" {
		return ""
	}
	return prefix
}

func subscriptionDependsOnTable(path string, table string) bool {
	table = strings.TrimSpace(table)
	if table == "" {
		return true
	}
	for _, dep := range subscriptionTables(path) {
		if dep == table {
			return true
		}
	}
	return false
}

func subscriptionTables(path string) []string {
	switch path {
	case "calls.listForUser":
		return []string{"callSignals"}
	case "chat.listConversations":
		return []string{"conversations"}
	case "chat.listAllParticipants":
		return []string{"conversationParticipants"}
	case "chat.listAllMessages":
		return []string{"directMessages"}
	case "chat.listAllReactions":
		return []string{"messageReactions"}
	case "chat.listAllLinkPreviews":
		return []string{"linkPreviews"}
	case "chat.listWorkspaceChat":
		return []string{"workspaceChat"}
	case "chat.workspaceChatUnreadSummary":
		return []string{"notifications"}
	case "bulk.allReferenceData":
		return []string{
			"approvalApprovers",
			"approvals",
			"audienceTeams",
			"audienceUserTeams",
			"categories",
			"categoryCustomFields",
			"categoryPriorities",
			"cleaningStatuses",
			"customFields",
			"employeeProfiles",
			"fieldUpdatePermissions",
			"formFields",
			"formVersions",
			"forms",
			"invitations",
			"jobPositions",
			"notificationSoundRules",
			"priorities",
			"properties",
			"roles",
			"slaPolicies",
			"slas",
			"spotTypes",
			"spots",
			"statuses",
			"statusTransitionGroups",
			"statusTransitions",
			"tags",
			"taskApprovalInstances",
			"taskForms",
			"teamSettingsWorkspaces",
			"teams",
			"templates",
			"userTeams",
			"users",
			"workspaceGroups",
			"workspaces",
		}
	case "bulk.tasksByWorkspace", "bulk.taskSummaryCounts", "bulk.workspaceTaskCounts", "bulk.cachedWorkspaceTaskCounts":
		return []string{"tasks", "taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances"}
	case "bulk.taskPivotData":
		return []string{"taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances", "tasks"}
	case "roles.effectivePermissions":
		return []string{"roles", "rolePermissions", "userTeams", "users"}
	case "users.myPermissions":
		return []string{"roles", "rolePermissions", "permissions", "userTeams", "users"}
	case "users.myTenants":
		return []string{"tenants", "userTenantMap", "users"}
	case "taskResources.listTaskNotes", "taskResources.listTaskNotesPaged", "taskResources.noteSummariesByTenant", "taskResources.noteCountsByTenant", "taskResources.attachmentCountsByTenant", "taskResources.listTaskIdsWithAttachments", "taskResources.listWorkspaceAttachments":
		return []string{"taskNotes", "tasks", "users"}
	case "taskResources.listTaskViewsByTaskId", "taskResources.listTaskViewsByTaskPgId":
		return []string{"taskViews", "tasks", "users"}
	case "taskResources.listTaskHistory", "taskResources.listTaskLogs":
		return []string{"taskLogs", "tasks", "users"}
	case "taskResources.listTaskSignatures", "taskResources.listTaskIdsWithSignatures":
		return []string{"taskSignatures", "tasks", "users"}
	case "taskResources.listSharedToMe", "taskResources.listTaskShares", "taskResources.sharedWithMeCount":
		return []string{"taskShares", "taskAcks", "tasks", "users"}
	case "taskResources.getTaskAcknowledgmentProgressForTask":
		return []string{"taskAcks", "taskAckReads", "taskShares", "tasks"}
	}
	if strings.HasPrefix(path, "tasks.") {
		return []string{"tasks", "taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances"}
	}
	if strings.HasPrefix(path, "taskFindings.") {
		return []string{"taskFindings", "findingNotes", "taskLogs", "taskWorkspaceContexts", "tasks"}
	}
	if strings.HasPrefix(path, "settings.") {
		return []string{"settings", "plugins", "envVars"}
	}
	if table := subscriptionTable(path); table != "" {
		return []string{table}
	}
	return nil
}

func mutationInvalidationTable(path string) string {
	tables := mutationInvalidationTables(path)
	if len(tables) > 0 {
		return tables[0]
	}
	return ""
}

func mutationInvalidationTables(path string) []string {
	switch path {
	case "calls.sendSignal", "calls.acknowledgeSignals", "calls.acknowledgeRoomSignals":
		return []string{"callSignals"}
	case "chat.createConversation":
		return []string{"conversations", "conversationParticipants"}
	case "chat.sendMessage":
		return []string{"directMessages", "conversations", "notifications", "linkPreviews"}
	case "chat.updateMessage", "chat.deleteMessage", "chat.markMessagesDelivered":
		return []string{"directMessages"}
	case "chat.markAsRead":
		return []string{"conversationParticipants", "directMessages"}
	case "chat.sendWorkspaceChat":
		return []string{"workspaceChat", "notifications", "linkPreviews"}
	case "chat.updateWorkspaceChatMessage", "chat.deleteWorkspaceChatMessage":
		return []string{"workspaceChat"}
	case "chat.addReaction", "chat.removeReaction":
		return []string{"messageReactions"}
	case "chat.markWorkspaceChatRead":
		return []string{"notifications"}
	case "chat.generateUploadUrl":
		return []string{"files"}
	case "taskResources.assignUser", "taskResources.unassignUser":
		return []string{"taskUsers", "tasks", "taskLogs", "notifications"}
	case "taskResources.createNote", "taskResources.updateNote", "taskResources.removeNote", "taskResources.markVoiceMemoListened", "taskResources.removeNoteAttachment":
		return []string{"taskNotes", "tasks", "taskLogs"}
	case "taskResources.createSignature":
		return []string{"taskSignatures", "tasks", "taskLogs"}
	case "taskResources.shareTask", "taskResources.revokeShare":
		return []string{"taskShares", "taskAcks", "tasks", "notifications"}
	case "taskResources.acknowledgeTaskShare":
		return []string{"taskShares", "taskAcks", "taskAckReads", "tasks", "notifications"}
	case "taskResources.recordTaskViewByTaskId", "taskResources.recordTaskViewByTaskPgId":
		return []string{"taskViews", "tasks"}
	case "taskResources.addTagByTaskId", "taskResources.addTagByPgId", "taskResources.addTagByTaskPgId", "taskResources.removeTagByTaskId", "taskResources.removeTagByPgId", "taskResources.removeTagByTaskPgId":
		return []string{"taskTags", "tasks", "taskLogs"}
	case "taskResources.generateUploadUrl":
		return []string{"files"}
	case "scheduling.processWorkspaceChatMessage":
		return []string{"scheduleAvailabilitySubmissions", "scheduleAvailabilityItems", "scheduleRosterEntries"}
	}
	if strings.HasPrefix(path, "techSupport.") {
		return []string{"supportSessions"}
	}
	if strings.HasPrefix(path, "taskFindings.") {
		return []string{"taskFindings", "findingNotes", "taskLogs", "taskWorkspaceContexts", "tasks"}
	}
	if strings.HasPrefix(path, "tasks.") {
		return []string{"tasks", "taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances"}
	}
	if table := subscriptionTable(path); table != "" {
		return []string{table}
	}
	return []string{""}
}
