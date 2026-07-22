package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
)

const minimumPatchResultBytes = 4 << 10

type subscriptionRevision struct {
	Epoch    string `json:"epoch"`
	Sequence uint64 `json:"sequence"`
}

type dependencyKey struct {
	project string
	tenant  string
	table   string
}

type subscriptionScope struct {
	project string
	tenant  string
}

type subscriptionManager struct {
	server *Server
	epoch  string

	mu        sync.Mutex
	groups    map[string]*sharedSubscription
	byTable   map[dependencyKey]map[*sharedSubscription]struct{}
	broad     map[subscriptionScope]map[*sharedSubscription]struct{}
	listeners *tenantListenerManager
	sequence  atomic.Uint64
	execute   func(context.Context, *sharedSubscription, querySubscription, string) (any, error)
}

type sharedSubscription struct {
	manager             *subscriptionManager
	key                 string
	project             string
	tenant              string
	path                string
	args                json.RawMessage
	caller              callerContext
	cacheScope          string
	reads               []manifest.ReadDependency
	unknownDependencies bool

	ctx    context.Context
	cancel context.CancelFunc

	mu                 sync.Mutex
	listeners          map[*subscriptionToken]querySubscription
	running            bool
	awaitingListener   bool
	dirty              bool
	requested          uint64
	completed          uint64
	revision           uint64
	pendingReason      string
	pendingChangedAtMS float64
	lastResult         json.RawMessage
	lastError          string
	lastHash           [sha256.Size]byte
	hasHash            bool
	rowIDs             map[string]bool
	idleTimer          *time.Timer
}

func newSubscriptionManager(server *Server) *subscriptionManager {
	epochBytes := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	manager := &subscriptionManager{
		server:  server,
		epoch:   hex.EncodeToString(epochBytes[:8]),
		groups:  map[string]*sharedSubscription{},
		byTable: map[dependencyKey]map[*sharedSubscription]struct{}{},
		broad:   map[subscriptionScope]map[*sharedSubscription]struct{}{},
	}
	manager.listeners = newTenantListenerManager(server)
	manager.execute = func(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string) (any, error) {
		return server.executeTenantQueryForCallerCached(ctx, group.project, group.tenant, listener.caller, group.path, group.args, group.cacheScope, reason)
	}
	return manager
}

func (m *subscriptionManager) attach(sub querySubscription) {
	key, reads, unknown := m.groupKeyAndDependencies(sub)
	m.mu.Lock()
	baseKey := key
	group := m.groups[key]
	for shard := 1; group != nil && m.server.config.SharedSubscriptionMaxFanout > 0; shard++ {
		group.mu.Lock()
		full := len(group.listeners) >= m.server.config.SharedSubscriptionMaxFanout
		group.mu.Unlock()
		if !full {
			break
		}
		key = baseKey + ":" + strconv.Itoa(shard)
		group = m.groups[key]
	}
	created := false
	if group == nil {
		ctx, cancel := context.WithCancel(context.Background())
		group = &sharedSubscription{
			manager: m, key: key, project: sub.project, tenant: sub.tenant,
			path: sub.path, args: append(json.RawMessage(nil), sub.args...), caller: sub.caller,
			cacheScope: sub.cacheScope, reads: reads, unknownDependencies: unknown,
			ctx: ctx, cancel: cancel, listeners: map[*subscriptionToken]querySubscription{}, awaitingListener: true,
		}
		m.groups[key] = group
		m.indexGroupLocked(group)
		created = true
	}
	group.mu.Lock()
	if group.idleTimer != nil {
		group.idleTimer.Stop()
		group.idleTimer = nil
	}
	group.listeners[sub.token] = sub
	hasSnapshot := len(group.lastResult) > 0
	lastError := group.lastError
	running := group.running
	awaitingListener := group.awaitingListener
	revision := group.revision
	snapshot := append(json.RawMessage(nil), group.lastResult...)
	group.mu.Unlock()
	groups, listenerCount := m.countsLocked()
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedSubscriptions = groups
		metric.SubscriptionListeners = listenerCount
	})
	var listenerReady <-chan struct{}
	if created {
		listenerReady = m.listeners.acquire(sub.project, sub.tenant)
	}
	if hasSnapshot {
		// Preserve per-connection delivery order and avoid one goroutine per
		// late-listener snapshot. Each WebSocket already has its own reader
		// goroutine, so this only applies backpressure to that connection.
		group.sendFullTo(sub, snapshot, revision, "initial", 0)
		return
	}
	if lastError != "" {
		if listenerCurrent(sub) {
			sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Path: sub.path, Error: lastError})
		}
		return
	}
	if !created && (running || awaitingListener) {
		// The active execution broadcasts its first authoritative snapshot to
		// every listener, including this one. A newly created group may also be
		// waiting for its tenant LISTEN connection before that execution starts.
		return
	}
	if listenerReady == nil {
		group.markListenerReady()
		group.request("initial", 0)
		return
	}
	go func() {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-listenerReady:
			group.markListenerReady()
			group.request("initial", 0)
		case <-timer.C:
			m.listeners.markNeedsRecovery(sub.project, sub.tenant)
			group.markListenerReady()
			group.request("initial", 0)
		case <-group.ctx.Done():
		}
	}()
}

func (group *sharedSubscription) markListenerReady() {
	group.mu.Lock()
	group.awaitingListener = false
	group.mu.Unlock()
}

func (m *subscriptionManager) detach(sub querySubscription) {
	if sub.token == nil {
		return
	}
	key, _, _ := m.groupKeyAndDependencies(sub)
	m.mu.Lock()
	group := m.groups[key]
	if group == nil {
		// A bundle/auth change can alter the computed key; find the listener by
		// its stable token instead of leaking the old group.
		for _, candidate := range m.groups {
			candidate.mu.Lock()
			_, found := candidate.listeners[sub.token]
			candidate.mu.Unlock()
			if found {
				group = candidate
				break
			}
		}
	}
	if group == nil {
		m.mu.Unlock()
		return
	}
	group.mu.Lock()
	delete(group.listeners, sub.token)
	empty := len(group.listeners) == 0
	if empty && group.idleTimer == nil {
		grace := m.server.config.SharedSubscriptionGrace
		group.idleTimer = time.AfterFunc(grace, func() { m.expire(group) })
	}
	group.mu.Unlock()
	groups, listenerCount := m.countsLocked()
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedSubscriptions = groups
		metric.SubscriptionListeners = listenerCount
	})
}

func (m *subscriptionManager) expire(group *sharedSubscription) {
	m.mu.Lock()
	current := m.groups[group.key]
	if current != group {
		m.mu.Unlock()
		return
	}
	group.mu.Lock()
	if len(group.listeners) != 0 {
		group.idleTimer = nil
		group.mu.Unlock()
		m.mu.Unlock()
		return
	}
	delete(m.groups, group.key)
	m.unindexGroupLocked(group)
	group.idleTimer = nil
	group.cancel()
	group.mu.Unlock()
	groups, listenerCount := m.countsLocked()
	m.mu.Unlock()
	m.listeners.release(group.project, group.tenant)
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedSubscriptions = groups
		metric.SubscriptionListeners = listenerCount
	})
}

func (m *subscriptionManager) countsLocked() (int, int) {
	listeners := 0
	for _, group := range m.groups {
		group.mu.Lock()
		listeners += len(group.listeners)
		group.mu.Unlock()
	}
	return len(m.groups), listeners
}

func (m *subscriptionManager) request(sub querySubscription, reason string, changedAtMS float64) {
	key, _, _ := m.groupKeyAndDependencies(sub)
	m.mu.Lock()
	group := m.groups[key]
	if group == nil {
		for _, candidate := range m.groups {
			candidate.mu.Lock()
			_, found := candidate.listeners[sub.token]
			candidate.mu.Unlock()
			if found {
				group = candidate
				break
			}
		}
	}
	m.mu.Unlock()
	if group != nil {
		group.request(reason, changedAtMS)
	}
}

func (m *subscriptionManager) requestChange(change tableChange) {
	m.mu.Lock()
	candidates := map[*sharedSubscription]struct{}{}
	scope := subscriptionScope{project: change.project, tenant: change.tenant}
	for group := range m.broad[scope] {
		candidates[group] = struct{}{}
	}
	for _, table := range tableChangeTables(change) {
		for group := range m.byTable[dependencyKey{project: change.project, tenant: change.tenant, table: table}] {
			candidates[group] = struct{}{}
		}
	}
	inspected := len(candidates)
	selected := make([]*sharedSubscription, 0, inspected)
	for group := range candidates {
		if group.matches(change) {
			selected = append(selected, group)
		}
	}
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.ChangeBatchesReceived++
		metric.SubscriptionsInspected += uint64(inspected)
		metric.CandidateSubscriptionsSelected += uint64(len(selected))
	})
	for _, group := range selected {
		group.request("invalidate", change.changedAtMS)
	}
}

func (m *subscriptionManager) refreshTenant(project, tenant string) {
	m.mu.Lock()
	groups := make([]*sharedSubscription, 0)
	for _, group := range m.groups {
		if group.project == project && group.tenant == tenant {
			groups = append(groups, group)
		}
	}
	m.mu.Unlock()
	changedAt := epochMillis(time.Now().UTC())
	for _, group := range groups {
		group.request("recover", changedAt)
	}
}

func (m *subscriptionManager) rebindProject(subs []querySubscription) {
	for _, sub := range subs {
		m.detach(sub)
		m.attach(sub)
	}
}

func (m *subscriptionManager) groupKeyAndDependencies(sub querySubscription) (string, []manifest.ReadDependency, bool) {
	current := m.server.runtime.ManifestForProject(sub.project)
	entry, exists := current.Functions[sub.path]
	reads := entry.Dependencies.Reads
	unknown := !exists || len(reads) == 0
	if unknown {
		for _, table := range subscriptionTables(sub.path) {
			reads = append(reads, manifest.ReadDependency{Table: table})
		}
		unknown = len(reads) == 0
	}
	bundleHash := ""
	if current.Bundle != nil {
		bundleHash = current.Bundle.Hash
	}
	userFingerprint := "anonymous"
	if sub.caller.user != nil && sub.caller.user.ID != "" && !entry.Dependencies.ShareByPermissions {
		userFingerprint = sub.caller.user.ID
	}
	canonicalArgs := compactJSON(sub.args)
	keyPayload, _ := json.Marshal(struct {
		Project     string          `json:"project"`
		Tenant      string          `json:"tenant"`
		Path        string          `json:"path"`
		Args        json.RawMessage `json:"args"`
		Permissions string          `json:"permissions"`
		User        string          `json:"user"`
		Bundle      string          `json:"bundle"`
	}{sub.project, sub.tenant, sub.path, canonicalArgs, hashQueryCacheValue(sub.caller.permissions), userFingerprint, bundleHash})
	sum := sha256.Sum256(keyPayload)
	return hex.EncodeToString(sum[:]), reads, unknown
}

func compactJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	var buffer bytes.Buffer
	if json.Compact(&buffer, raw) == nil {
		return buffer.Bytes()
	}
	return append(json.RawMessage(nil), raw...)
}

func (m *subscriptionManager) indexGroupLocked(group *sharedSubscription) {
	if group.unknownDependencies {
		scope := subscriptionScope{project: group.project, tenant: group.tenant}
		if m.broad[scope] == nil {
			m.broad[scope] = map[*sharedSubscription]struct{}{}
		}
		m.broad[scope][group] = struct{}{}
		return
	}
	for _, read := range group.reads {
		key := dependencyKey{project: group.project, tenant: group.tenant, table: read.Table}
		if m.byTable[key] == nil {
			m.byTable[key] = map[*sharedSubscription]struct{}{}
		}
		m.byTable[key][group] = struct{}{}
	}
}

func (m *subscriptionManager) unindexGroupLocked(group *sharedSubscription) {
	for key, groups := range m.byTable {
		delete(groups, group)
		if len(groups) == 0 {
			delete(m.byTable, key)
		}
	}
	for key, groups := range m.broad {
		delete(groups, group)
		if len(groups) == 0 {
			delete(m.broad, key)
		}
	}
}

func (group *sharedSubscription) matches(change tableChange) bool {
	group.mu.Lock()
	rowIDs := group.rowIDs
	group.mu.Unlock()
	if !change.broad && change.operation == "update" && len(change.changedColumns) > 0 {
		relevant := false
		for _, read := range group.reads {
			if !changeContainsTable(change, read.Table) {
				continue
			}
			columns := append(append(append([]string{}, read.Columns...), read.Filters...), read.OrdersBy...)
			if len(columns) == 0 || intersectsStrings(columns, change.changedColumns) {
				relevant = true
				break
			}
		}
		if !relevant {
			return false
		}
	}
	if change.broad || change.operation == "insert" || change.operation == "delete" || len(change.rowIDs) == 0 || len(rowIDs) == 0 {
		return true
	}
	for id := range change.rowIDs {
		if rowIDs[id] {
			return true
		}
	}
	return false
}

func changeContainsTable(change tableChange, table string) bool {
	if len(change.tables) > 0 {
		return change.tables[table]
	}
	return change.table == table
}

func intersectsStrings(left, right []string) bool {
	values := make(map[string]struct{}, len(left))
	for _, value := range left {
		values[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := values[value]; ok {
			return true
		}
	}
	return false
}

func (group *sharedSubscription) request(reason string, changedAtMS float64) {
	group.mu.Lock()
	group.requested++
	group.pendingReason = reason
	if changedAtMS > group.pendingChangedAtMS {
		group.pendingChangedAtMS = changedAtMS
	}
	if group.running {
		group.dirty = true
		group.mu.Unlock()
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.RerunsCoalesced++ })
		return
	}
	group.running = true
	group.mu.Unlock()
	go group.run()
}

func (group *sharedSubscription) run() {
	for {
		group.mu.Lock()
		requested := group.requested
		reason := group.pendingReason
		changedAtMS := group.pendingChangedAtMS
		listeners := group.listenerSnapshotLocked()
		group.dirty = false
		group.mu.Unlock()
		if len(listeners) == 0 || group.ctx.Err() != nil {
			group.finishRun(requested)
			return
		}

		representative, ok := group.firstAuthorizedListener(listeners)
		if !ok {
			group.finishRun(requested)
			return
		}
		startedAt := time.Now().UTC()
		result, err := group.manager.execute(group.ctx, group, representative, reason)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.QueriesRerun++
		})
		if group.ctx.Err() != nil {
			group.finishRun(requested)
			return
		}
		if err != nil {
			group.completeError(err.Error())
		} else {
			group.completeResult(result, reason, changedAtMS, startedAt)
		}

		group.mu.Lock()
		group.completed = requested
		if group.dirty || group.requested > requested {
			group.mu.Unlock()
			continue
		}
		group.running = false
		group.mu.Unlock()
		return
	}
}

func (group *sharedSubscription) finishRun(requested uint64) {
	group.mu.Lock()
	group.completed = requested
	group.running = false
	group.mu.Unlock()
}

func (group *sharedSubscription) listenerSnapshotLocked() []querySubscription {
	listeners := make([]querySubscription, 0, len(group.listeners))
	for _, listener := range group.listeners {
		listeners = append(listeners, listener)
	}
	return listeners
}

func (group *sharedSubscription) firstAuthorizedListener(listeners []querySubscription) (querySubscription, bool) {
	for _, listener := range listeners {
		if listener.ctx.Err() != nil {
			continue
		}
		if listener.conn == nil {
			return listener, true
		}
		if err := listener.conn.revalidateAppAuth(listener.ctx); err != nil {
			if listener.ctx.Err() == nil {
				listener.conn.write(serverMessage{Type: "query.error", ID: listener.id, Error: "authentication is required"})
			}
			continue
		}
		return listener, true
	}
	return querySubscription{}, false
}

func (group *sharedSubscription) completeResult(result any, reason string, changedAtMS float64, startedAt time.Time) {
	payload, err := json.Marshal(explicitNull(result))
	if err != nil {
		group.broadcastError(err.Error())
		return
	}
	hash := sha256.Sum256(payload)
	group.mu.Lock()
	previous := append(json.RawMessage(nil), group.lastResult...)
	unchanged := group.hasHash && hash == group.lastHash
	previousRevision := group.revision
	revision := group.manager.sequence.Add(1)
	group.revision = revision
	group.lastHash = hash
	group.hasHash = true
	group.lastError = ""
	group.rowIDs = resultRowIDs(result)
	if len(payload) <= group.manager.server.config.SharedResultMaxBytes {
		group.lastResult = append(group.lastResult[:0], payload...)
	} else {
		group.lastResult = nil
	}
	listeners := group.listenerSnapshotLocked()
	group.mu.Unlock()

	revisionValue := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: revision}
	if unchanged && len(previous) > 0 {
		message := serverMessage{Type: "query.progress", Path: group.path, Reason: reason, ThroughRevision: revisionValue}
		group.broadcastTo(listeners, message, changedAtMS, startedAt)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.UnchangedResultsSuppressed++
			metric.ProgressMessages++
			metric.ResultBytesBefore += uint64(len(payload))
		})
		return
	}

	cacheRevision := group.manager.server.nextQueryCacheRevision()
	message := serverMessage{Type: "query.result", Path: group.path, Result: json.RawMessage(payload), Reason: reason, CacheScope: group.cacheScope, CacheRevision: cacheRevision, SubscriptionRevision: revisionValue}
	encodedSize := len(payload)
	patched := false
	if len(previous) >= minimumPatchResultBytes {
		if patch, ok := keyedResultPatch(previous, payload); ok {
			patch.SubscriptionRevision = revisionValue
			patch.BaseRevision = &subscriptionRevision{Epoch: group.manager.epoch, Sequence: previousRevision}
			patch.Path = group.path
			patch.Reason = reason
			patch.CacheScope = group.cacheScope
			patch.CacheRevision = cacheRevision
			if encoded, encodeErr := json.Marshal(patch); encodeErr == nil && len(encoded) < len(payload)*7/10 {
				message = patch
				encodedSize = len(encoded)
				patched = true
			}
		}
	}
	group.broadcastTo(listeners, message, changedAtMS, startedAt)
	group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.ResultBytesBefore += uint64(len(payload))
		metric.ResultBytesAfter += uint64(encodedSize)
		if patched {
			metric.Patches++
		} else {
			metric.FullResults++
		}
	})
}

func (group *sharedSubscription) completeError(message string) {
	group.mu.Lock()
	hasSuccessfulResult := group.hasHash
	if !hasSuccessfulResult {
		group.lastError = message
	}
	group.mu.Unlock()
	// A failed refresh never replaces a newer successful snapshot. Initial
	// failures still settle listeners and are replayed to late joiners.
	if !hasSuccessfulResult {
		group.broadcastError(message)
	}
}

func (group *sharedSubscription) broadcastTo(listeners []querySubscription, message serverMessage, changedAtMS float64, startedAt time.Time) {
	for _, listener := range listeners {
		if listener.conn == nil {
			continue
		}
		if !listenerCurrent(listener) {
			continue
		}
		copy := message
		copy.ID = listener.id
		sentAt := time.Now().UTC()
		copy.Trace = &messageTrace{
			ServerChangeCommittedAtMS:     changedAtMS,
			ServerSubscriptionStartedAtMS: epochMillis(startedAt),
			ServerSubscriptionSentAtMS:    epochMillis(sentAt),
			ServerDurationMS:              float64(sentAt.Sub(startedAt).Microseconds()) / 1000,
		}
		listener.conn.write(copy)
		if changedAtMS > 0 {
			latency := epochMillis(sentAt) - changedAtMS
			group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
				metric.ChangeToClientDurationMS += latency
				metric.ChangeToClientSamples++
			})
		}
		group.manager.server.recordTransactionTelemetry(transactionEntryFromTrace(listener.project, listener.tenant, listener.id, "query", listener.path, "server", message.Reason, "ok", "", copy.Trace.(*messageTrace)))
	}
}

func (group *sharedSubscription) broadcastError(message string) {
	group.mu.Lock()
	listeners := group.listenerSnapshotLocked()
	group.mu.Unlock()
	for _, listener := range listeners {
		if listener.conn == nil {
			continue
		}
		if listenerCurrent(listener) {
			listener.conn.write(serverMessage{Type: "query.error", ID: listener.id, Path: listener.path, Error: message})
		}
	}
}

func (group *sharedSubscription) sendFullTo(listener querySubscription, payload json.RawMessage, revision uint64, reason string, changedAtMS float64) {
	if !listenerCurrent(listener) {
		return
	}
	listener.conn.write(serverMessage{
		Type: "query.result", ID: listener.id, Path: listener.path, Result: payload, Reason: reason,
		CacheScope: listener.cacheScope, CacheRevision: group.manager.server.nextQueryCacheRevision(),
		SubscriptionRevision: &subscriptionRevision{Epoch: group.manager.epoch, Sequence: revision},
	})
}

func listenerCurrent(listener querySubscription) bool {
	if listener.conn == nil || listener.ctx.Err() != nil {
		return false
	}
	listener.conn.mu.Lock()
	current, ok := listener.conn.subs[listener.id]
	listener.conn.mu.Unlock()
	return ok && current.token == listener.token
}

func keyedResultPatch(previous, next json.RawMessage) (serverMessage, bool) {
	oldRows, oldOrder, ok := keyedRows(previous)
	if !ok {
		return serverMessage{}, false
	}
	newRows, newOrder, ok := keyedRows(next)
	if !ok {
		return serverMessage{}, false
	}
	inserted := []json.RawMessage{}
	updated := []json.RawMessage{}
	deleted := []string{}
	for id, row := range newRows {
		old, exists := oldRows[id]
		if !exists {
			inserted = append(inserted, row)
		} else if !bytes.Equal(old, row) {
			updated = append(updated, row)
		}
	}
	for id := range oldRows {
		if _, exists := newRows[id]; !exists {
			deleted = append(deleted, id)
		}
	}
	if len(inserted) == 0 && len(updated) == 0 && len(deleted) == 0 && equalStrings(oldOrder, newOrder) {
		return serverMessage{}, false
	}
	sort.Slice(inserted, func(i, j int) bool { return rowID(inserted[i]) < rowID(inserted[j]) })
	sort.Slice(updated, func(i, j int) bool { return rowID(updated[i]) < rowID(updated[j]) })
	sort.Strings(deleted)
	return serverMessage{Type: "query.patch", Inserted: inserted, Updated: updated, Deleted: deleted, Order: newOrder}, true
}

func keyedRows(payload json.RawMessage) (map[string]json.RawMessage, []string, bool) {
	var rows []json.RawMessage
	if json.Unmarshal(payload, &rows) != nil {
		return nil, nil, false
	}
	byID := make(map[string]json.RawMessage, len(rows))
	order := make([]string, 0, len(rows))
	for _, row := range rows {
		id := rowID(row)
		if id == "" {
			return nil, nil, false
		}
		if _, exists := byID[id]; exists {
			return nil, nil, false
		}
		var canonical bytes.Buffer
		if json.Compact(&canonical, row) != nil {
			return nil, nil, false
		}
		byID[id] = canonical.Bytes()
		order = append(order, id)
	}
	return byID, order, true
}

func rowID(row json.RawMessage) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(row, &object) != nil {
		return ""
	}
	var id string
	if json.Unmarshal(object["id"], &id) != nil {
		return ""
	}
	return id
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func normalizedColumns(values []string) []string {
	clean := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			clean = append(clean, value)
		}
	}
	return clean
}
