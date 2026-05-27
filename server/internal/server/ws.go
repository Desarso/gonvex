package server

import (
	"compress/flate"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/server/internal/data"
	"github.com/gorilla/websocket"
)

type clientMessage struct {
	Type string          `json:"type"`
	ID   string          `json:"id"`
	Path string          `json:"path,omitempty"`
	Args json.RawMessage `json:"args,omitempty"`
}

type serverMessage struct {
	Type   string `json:"type"`
	ID     string `json:"id,omitempty"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
	Reason string `json:"reason,omitempty"`
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

const tableChangeDebounce = 75 * time.Millisecond

type querySubscription struct {
	conn    *wsConn
	id      string
	project string
	path    string
	args    json.RawMessage
	rowIDs  map[string]bool
	ctx     context.Context
	cancel  context.CancelFunc
	token   *struct{}
}

type tableChange struct {
	project string
	table   string
	broad   bool
	rowIDs  map[string]bool
}

type wsConn struct {
	server  *Server
	conn    *websocket.Conn
	project string
	mu      sync.Mutex
	subs    map[string]querySubscription
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
	client := &wsConn{server: s, conn: conn, project: projectID(r), subs: map[string]querySubscription{}}
	s.addWSConn(client)
	defer func() {
		client.cancelSubscriptions()
		s.removeWSConn(client)
		_ = conn.Close()
	}()

	for {
		var message clientMessage
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		client.handle(r.Context(), message)
	}
}

func (c *wsConn) handle(ctx context.Context, message clientMessage) {
	switch message.Type {
	case "query.subscribe":
		if message.ID == "" || message.Path == "" {
			c.write(serverMessage{Type: "query.error", ID: message.ID, Error: "query id and path are required"})
			return
		}
		subCtx, cancel := context.WithCancel(ctx)
		sub := querySubscription{conn: c, id: message.ID, project: c.project, path: message.Path, args: message.Args, ctx: subCtx, cancel: cancel, token: &struct{}{}}
		c.mu.Lock()
		previous, hadPrevious := c.subs[message.ID]
		c.subs[message.ID] = sub
		c.mu.Unlock()
		if hadPrevious && previous.cancel != nil {
			previous.cancel()
		}
		go c.server.executeSubscription(subCtx, sub, "initial")
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
		result, err := c.server.executeMutation(ctx, c.project, message.Path, message.Args)
		if err != nil {
			c.write(serverMessage{Type: "mutation.error", ID: message.ID, Error: err.Error()})
			return
		}
		c.write(serverMessage{Type: "mutation.result", ID: message.ID, Result: result})
	case "action.call":
		result, err := c.server.executeAction(ctx, c.project, message.Path, message.Args)
		if err != nil {
			c.write(serverMessage{Type: "action.error", ID: message.ID, Error: err.Error()})
			return
		}
		c.write(serverMessage{Type: "action.result", ID: message.ID, Result: result})
	default:
		c.write(serverMessage{Type: "query.error", ID: message.ID, Error: "unknown websocket message type"})
	}
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

func (s *Server) broadcastTableChange(projectID string, table string) {
	s.scheduleTableChange(tableChange{project: projectID, table: table, broad: true})
}

func (s *Server) broadcastRowIDChange(projectID string, table string, rowIDs []string) {
	ids := map[string]bool{}
	for _, id := range rowIDs {
		ids[id] = true
	}
	s.scheduleTableChange(tableChange{project: projectID, table: table, rowIDs: ids})
}

func (s *Server) scheduleTableChange(change tableChange) {
	s.cache.invalidateRows(context.Background(), change.project, change.table)
	s.tableChangeMu.Lock()
	key := change.project + ":" + change.table
	pending := s.tableChanges[key]
	pending.project = change.project
	pending.table = change.table
	pending.broad = pending.broad || change.broad
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
	for _, conn := range connections {
		conn.mu.Lock()
		subs := make([]querySubscription, 0, len(conn.subs))
		for _, sub := range conn.subs {
			if sub.project == change.project && subscriptionTable(sub.path) == change.table && subscriptionIntersectsChange(sub, change) {
				subs = append(subs, sub)
			}
		}
		conn.mu.Unlock()
		for _, sub := range subs {
			s.executeSubscription(sub.ctx, sub, "invalidate")
		}
	}
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

func (s *Server) executeSubscription(ctx context.Context, sub querySubscription, reason string) {
	result, err := s.executeQuery(ctx, sub.project, sub.path, sub.args)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Error: err.Error()})
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
	sub.conn.write(serverMessage{Type: "query.result", ID: sub.id, Result: result, Reason: reason})
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
	started := time.Now()
	defer func() {
		s.metrics.recordFunction(path, s.functionKind(path, "query"), time.Since(started), err)
	}()

	switch path {
	case "tasks.grid":
		var args taskGridArgs
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return nil, err
			}
		}
		return data.ReadRows(ctx, s.databaseURLForProject(projectID), "tasks", data.RowsOptions{
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
	started := time.Now()
	defer func() {
		s.metrics.recordFunction(path, s.functionKind(path, "mutation"), time.Since(started), err)
	}()

	switch path {
	case "tasks.randomizeStatusPriority":
		var args randomizeStatusPriorityArgs
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return nil, err
			}
		}
		result, err := data.RandomizeTaskStatusPriority(ctx, s.databaseURLForProject(projectID), args.Count)
		if err != nil {
			return nil, err
		}
		go s.broadcastTableChange(projectID, "tasks")
		return result, nil
	default:
		return nil, fmt.Errorf("mutation %q is not implemented by the runtime", path)
	}
}

func (s *Server) executeAction(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	started := time.Now()
	defer func() {
		s.metrics.recordFunction(path, s.functionKind(path, "action"), time.Since(started), err)
	}()

	return nil, fmt.Errorf("action %q is not implemented by the runtime", path)
}

func (s *Server) functionKind(path string, fallback string) string {
	if entry, ok := s.runtime.Manifest().Functions[path]; ok && entry.Kind != "" {
		return string(entry.Kind)
	}
	return fallback
}

func subscriptionTable(path string) string {
	if strings.HasPrefix(path, "tasks.") {
		return "tasks"
	}
	return ""
}
