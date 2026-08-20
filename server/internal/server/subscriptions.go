package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/moduleengine"
)

// Diffing tiny results costs more CPU than it can save on the wire. The
// encoded-size gate below still requires a 30% reduction, so this lower floor
// lets small, fast-growing collection envelopes become patchable without ever
// making their payload larger.
const minimumPatchResultBytes = 512

type subscriptionRevision struct {
	Epoch    string `json:"epoch"`
	Sequence uint64 `json:"sequence"`
}

type dependencyKey struct {
	project string
	tenant  string
	table   string
}

type subscriptionManager struct {
	server *Server
	epoch  string

	mu      sync.Mutex
	groups  map[string]*sharedSubscription
	byTable map[dependencyKey]map[*sharedSubscription]struct{}
	// listenerCount is maintained while mu is held. Recomputing it by walking
	// every group makes distinct-user subscription startup O(n²).
	listenerCount    int
	listeners        *tenantListenerManager
	sequence         atomic.Uint64
	execute          func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error)
	sharedResultMu   sync.Mutex
	sharedResultRuns map[string]*sharedResultExecution
}

type sharedResultExecution struct {
	done      chan struct{}
	result    any
	err       error
	prepareMu sync.Mutex
	prepared  map[string]sharedPreparedResult
}

type sharedPreparedResult struct {
	result any
	err    error
}

// canonicalSharedResult carries immutable work reused by subscriptions that
// share the same canonical result source. Delivery state remains group-local.
type canonicalSharedResult struct {
	payload   json.RawMessage
	hash      [sha256.Size]byte
	queryPerf json.RawMessage
	rowIDs    map[string]bool
	patchMu   sync.Mutex
	patches   map[[sha256.Size]byte]canonicalSharedPatch
}

type canonicalSharedPatch struct {
	message     serverMessage
	encodedSize int
	ok          bool
}

type sharedSubscription struct {
	manager        *subscriptionManager
	key            string
	project        string
	tenant         string
	path           string
	args           json.RawMessage
	caller         callerContext
	cacheScope     string
	reads          []manifest.ReadDependency
	livePlan       *manifest.LiveQueryPlan
	retainSnapshot bool

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	listeners map[*subscriptionToken]querySubscription
	running   bool
	// Correctness is expressed only by these two revisions. Retention timers,
	// listeners, and whether an execution is currently running never make a
	// result current. A snapshot is authoritative exactly when computedRevision
	// has caught up to requiredRevision.
	requiredRevision   uint64
	computedRevision   uint64
	revision           uint64
	pendingReason      string
	pendingChangedAtMS float64
	pendingCommitIDs   map[string]struct{}
	activeCommitIDs    map[string]struct{}
	pendingRequestIDs  map[string]struct{}
	activeRequestIDs   map[string]struct{}
	completedCommitIDs map[string]struct{}
	completedCommits   []string
	lastResult         json.RawMessage
	lastError          string
	lastHash           [sha256.Size]byte
	hasHash            bool
	lastSingleListener *subscriptionToken
	rowIDs             map[string]bool
	idleTimer          *time.Timer
}

func newSubscriptionManager(server *Server) *subscriptionManager {
	epochBytes := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	manager := &subscriptionManager{
		server:           server,
		epoch:            hex.EncodeToString(epochBytes[:8]),
		groups:           map[string]*sharedSubscription{},
		byTable:          map[dependencyKey]map[*sharedSubscription]struct{}{},
		sharedResultRuns: map[string]*sharedResultExecution{},
	}
	manager.listeners = newTenantListenerManager(server)
	manager.execute = func(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64) (any, error) {
		if group.livePlan != nil {
			if _, ok := server.visibilityPlan(group.project, group.livePlan.Table); !ok {
				return server.executeTenantQueryForCallerCached(ctx, group.project, group.tenant, listener.caller, group.path, group.args, group.cacheScope, reason)
			}
			return server.executeStructuredLiveQuery(ctx, group.project, group.tenant, listener.caller, *group.livePlan, group.args)
		}
		return server.executeTenantQueryForCallerCached(ctx, group.project, group.tenant, listener.caller, group.path, group.args, group.cacheScope, reason)
	}
	return manager
}

func (m *subscriptionManager) attach(sub querySubscription) {
	sub.visibilityKey = m.resolveAttachVisibilityKey(sub)
	key, reads, livePlan := m.groupKeyAndDependencies(sub)
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
		retainSnapshot := false
		for _, read := range reads {
			retainSnapshot = retainSnapshot || read.Windowed
		}
		group = &sharedSubscription{
			manager: m, key: key, project: sub.project, tenant: sub.tenant,
			path: sub.path, args: append(json.RawMessage(nil), sub.args...), caller: sub.caller,
			cacheScope: m.executionCacheScope(sub), reads: reads, livePlan: livePlan, retainSnapshot: retainSnapshot,
			ctx: ctx, cancel: cancel, listeners: map[*subscriptionToken]querySubscription{}, running: true,
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
	if _, exists := group.listeners[sub.token]; !exists {
		m.listenerCount++
	}
	group.listeners[sub.token] = sub
	hasSnapshot := len(group.lastResult) > 0 && group.computedRevision >= group.requiredRevision
	lastError := group.lastError
	running := group.running
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
	if !created && running {
		// The active execution broadcasts its first authoritative snapshot to
		// every listener, including this one. A newly created group may also be
		// waiting for its tenant LISTEN connection before that execution starts.
		return
	}
	if listenerReady == nil {
		if created {
			go group.run()
		} else {
			group.request("initial", 0)
		}
		return
	}
	go func() {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-listenerReady:
			go group.run()
		case <-timer.C:
			m.listeners.markNeedsRecovery(sub.project, sub.tenant)
			go group.run()
		case <-group.ctx.Done():
		}
	}()
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
	if _, exists := group.listeners[sub.token]; exists {
		delete(group.listeners, sub.token)
		m.listenerCount--
	}
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
	return len(m.groups), m.listenerCount
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
	candidateTables := tableChangeTables(change)
	for _, table := range appendUniqueStrings(nil, candidateTables...) {
		for group := range m.byTable[dependencyKey{project: change.project, tenant: change.tenant, table: table}] {
			candidates[group] = struct{}{}
		}
	}
	inspected := len(candidates)
	selected := make([]*sharedSubscription, 0, inspected)
	skippedByTable := 0
	for group := range candidates {
		matches, tableMiss := group.matchResult(change)
		if matches {
			selected = append(selected, group)
		} else if tableMiss {
			skippedByTable++
		}
	}
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.ChangeBatchesReceived++
		metric.SubscriptionsInspected += uint64(inspected)
		metric.CandidateSubscriptionsSelected += uint64(len(selected))
		metric.SubscriptionsSkippedByTable += uint64(skippedByTable)
	})
	// Every trigger notification for one mutation is observed after the whole
	// transaction committed. If a group was selected by an earlier table from
	// that commit, its query already sees the final state of all later table
	// notifications. Groups not selected by the earlier table have no matching
	// request and still run when their own dependency arrives.
	dedupID := strings.TrimSpace(change.commitID)
	for _, group := range selected {
		group.requestForCommitBatch("invalidate", change.changedAtMS, change.commitID, dedupID, change)
	}
}

func (m *subscriptionManager) refreshTenant(project, tenant string) {
	m.server.invalidateAllVisibilityContexts(project, tenant)
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

// rebindVisibilityForChange recomputes canonical group fingerprints before a
// visibility dependency can fan a result out. A group that was equivalent at
// revision N may split at N+1 when one member changes team, role, or workspace.
func (m *subscriptionManager) rebindVisibilityForChange(change tableChange) {
	changedTables := tableChangeTables(change)
	m.mu.Lock()
	byToken := map[*subscriptionToken]querySubscription{}
	for _, group := range m.groups {
		if group.project != change.project || group.tenant != change.tenant || group.livePlan == nil {
			continue
		}
		plan, ok := m.server.visibilityPlan(group.project, group.livePlan.Table)
		if !ok || !intersectsStrings(visibilityPlanDependencies(plan), changedTables) {
			continue
		}
		group.mu.Lock()
		for token, subscription := range group.listeners {
			byToken[token] = subscription
		}
		group.mu.Unlock()
	}
	m.mu.Unlock()
	for _, subscription := range byToken {
		m.detach(subscription)
		m.attach(subscription)
	}
}

func (m *subscriptionManager) groupKeyAndDependencies(sub querySubscription) (string, []manifest.ReadDependency, *manifest.LiveQueryPlan) {
	current := m.server.runtime.ManifestForProject(sub.project)
	entry := current.Functions[sub.path]
	reads, plan, registered := m.server.liveQueryDependencies(sub.ctx, sub.project, sub.path)
	if !registered || len(reads) == 0 {
		return "", nil, nil
	}
	bundleHash := ""
	if current.Bundle != nil {
		bundleHash = current.Bundle.Hash
	}
	userFingerprint := "anonymous"
	if sub.visibilityKey != "" {
		userFingerprint = "visibility:" + sub.visibilityKey
	} else if sub.caller.user != nil && sub.caller.user.ID != "" && !entry.Dependencies.ShareByPermissions {
		userFingerprint = sub.caller.user.ID
	}
	canonicalArgs := compactJSON(sub.args)
	executionCacheScope := m.executionCacheScopeForEntry(sub, entry)
	keyPayload, _ := json.Marshal(struct {
		Project     string          `json:"project"`
		Tenant      string          `json:"tenant"`
		Path        string          `json:"path"`
		Args        json.RawMessage `json:"args"`
		Permissions string          `json:"permissions"`
		User        string          `json:"user"`
		Bundle      string          `json:"bundle"`
		CacheScope  string          `json:"cacheScope"`
	}{
		sub.project,
		sub.tenant,
		sub.path,
		canonicalArgs,
		hashQueryCacheValue(sub.caller.permissions),
		userFingerprint,
		bundleHash,
		executionCacheScope,
	})
	sum := sha256.Sum256(keyPayload)
	return hex.EncodeToString(sum[:]), reads, plan
}

func (s *Server) liveQueryDependencies(ctx context.Context, project, path string) ([]manifest.ReadDependency, *manifest.LiveQueryPlan, bool) {
	entry, exists := s.runtime.ManifestForProject(project).Functions[path]
	if exists && entry.Kind == manifest.FunctionKindQuery && entry.Delivery == manifest.DeliveryLive && entry.Dependencies.LiveQueryPlan != nil {
		reads := append([]manifest.ReadDependency(nil), entry.Dependencies.Reads...)
		if plan, ok := s.runtime.ManifestForProject(project).Visibility[entry.Dependencies.LiveQueryPlan.Table]; ok {
			for _, table := range visibilityPlanDependencies(plan) {
				reads = append(reads, manifest.ReadDependency{Table: table})
			}
		}
		return dedupeReadDependencies(reads), entry.Dependencies.LiveQueryPlan, true
	}
	descriptor, exists := s.engineForProject(ctx, project).Describe(path)
	if !exists || descriptor.Kind != moduleengine.KindQuery || descriptor.Delivery != gonvex.DeliveryLive || descriptor.Dependencies.LiveQueryPlan == nil {
		return nil, nil, false
	}
	reads := make([]manifest.ReadDependency, 0, len(descriptor.Dependencies.Reads))
	for _, read := range descriptor.Dependencies.Reads {
		reads = append(reads, manifest.ReadDependency{
			Table: read.Table, Columns: append([]string(nil), read.Columns...), Filters: append([]string(nil), read.Filters...),
			OrdersBy: append([]string(nil), read.OrdersBy...), Windowed: read.Windowed,
		})
	}
	encoded, _ := json.Marshal(descriptor.Dependencies.LiveQueryPlan)
	plan := &manifest.LiveQueryPlan{}
	_ = json.Unmarshal(encoded, plan)
	if visibility, ok := s.runtime.ManifestForProject(project).Visibility[plan.Table]; ok {
		for _, table := range visibilityPlanDependencies(visibility) {
			reads = append(reads, manifest.ReadDependency{Table: table})
		}
	}
	return dedupeReadDependencies(reads), plan, true
}

func dedupeReadDependencies(reads []manifest.ReadDependency) []manifest.ReadDependency {
	byTable := map[string]manifest.ReadDependency{}
	order := []string{}
	for _, read := range reads {
		if strings.TrimSpace(read.Table) == "" {
			continue
		}
		existing, ok := byTable[read.Table]
		if !ok {
			order = append(order, read.Table)
		}
		existing.Table = read.Table
		existing.Columns = appendUniqueStrings(existing.Columns, read.Columns...)
		existing.Filters = appendUniqueStrings(existing.Filters, read.Filters...)
		existing.OrdersBy = appendUniqueStrings(existing.OrdersBy, read.OrdersBy...)
		existing.Windowed = existing.Windowed || read.Windowed
		byTable[read.Table] = existing
	}
	result := make([]manifest.ReadDependency, 0, len(order))
	for _, table := range order {
		result = append(result, byTable[table])
	}
	return result
}

func (m *subscriptionManager) resolveAttachVisibilityKey(sub querySubscription) string {
	entry := m.server.runtime.ManifestForProject(sub.project).Functions[sub.path]
	if entry.Dependencies.LiveQueryPlan != nil {
		if plan, ok := m.server.visibilityPlan(sub.project, entry.Dependencies.LiveQueryPlan.Table); ok {
			resolved, err := m.server.resolveVisibilityContext(sub.ctx, sub.project, sub.tenant, sub.caller, plan, 0)
			if err != nil {
				return "denied:" + visibilityAccountID(sub.caller)
			}
			return resolved.Fingerprint
		}
	}
	return ""
}

func (m *subscriptionManager) executionCacheScope(sub querySubscription) string {
	entry := m.server.runtime.ManifestForProject(sub.project).Functions[sub.path]
	return m.executionCacheScopeForEntry(sub, entry)
}

func (m *subscriptionManager) executionCacheScopeForEntry(sub querySubscription, entry manifest.FunctionEntry) string {
	if entry.Dependencies.LiveQueryPlan != nil {
		if _, ok := m.server.visibilityPlan(sub.project, entry.Dependencies.LiveQueryPlan.Table); ok && sub.visibilityKey != "" {
			return "visibility:" + sub.visibilityKey
		}
	}
	if entry.Dependencies.ShareByPermissions {
		// The result-equivalence contract explicitly excludes identity. Use a
		// permission-derived server cache scope so per-user browser cache scopes
		// do not defeat shared execution. Delivery rewrites this to each
		// listener's own scope below.
		return "permissions:" + hashQueryCacheValue(sub.caller.permissions)
	}
	return sub.cacheScope
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
}

func (group *sharedSubscription) matches(change tableChange) bool {
	matches, _ := group.matchResult(change)
	return matches
}

func (group *sharedSubscription) matchResult(change tableChange) (bool, bool) {
	group.mu.Lock()
	rowIDs := group.rowIDs
	group.mu.Unlock()
	intersected := false
	for _, read := range group.reads {
		if !changeContainsTable(change, read.Table) {
			continue
		}
		intersected = true
		detail := tableDetail(change, read.Table)
		if group.livePlan != nil && group.livePlan.Table == read.Table && !livePlanCouldMatchChange(group.livePlan, group.args, detail) {
			continue
		}
		if detail.operation == "insert" || detail.operation == "delete" || detail.operation == "mixed" {
			return true, false
		}
		if detail.operation == "update" && len(detail.changedColumns) > 0 {
			columns := append(append(append([]string{}, read.Columns...), read.Filters...), read.OrdersBy...)
			if len(columns) > 0 && !intersectsStrings(columns, detail.changedColumns) {
				continue
			}
			// A changed filter can move a row into or out of the result, and a
			// changed ordering column can move an unseen row into a bounded window.
			// Neither case is safe to reject from the previous row-ID snapshot.
			if intersectsStrings(read.Filters, detail.changedColumns) ||
				(read.Windowed && intersectsStrings(read.OrdersBy, detail.changedColumns)) {
				return true, false
			}
		}
		if len(detail.rowIDs) == 0 || len(rowIDs) == 0 {
			return true, false
		}
		for id := range detail.rowIDs {
			if rowIDs[id] {
				return true, false
			}
		}
	}
	return false, !intersected
}

func livePlanCouldMatchChange(plan *manifest.LiveQueryPlan, rawArgs json.RawMessage, detail tableChangeDetail) bool {
	if plan == nil || plan.ServerOnly {
		return true
	}
	args := map[string]any{}
	if len(rawArgs) > 0 && json.Unmarshal(rawArgs, &args) != nil {
		return true
	}
	rows := []json.RawMessage{}
	switch detail.operation {
	case "insert":
		rows = detail.newValues
	case "delete":
		rows = detail.oldValues
	default:
		rows = append(rows, detail.oldValues...)
		rows = append(rows, detail.newValues...)
	}
	if len(rows) == 0 {
		return true
	}
	for _, raw := range rows {
		row := map[string]any{}
		if json.Unmarshal(raw, &row) != nil {
			return true
		}
		if livePlanRowMatches(plan, args, row) {
			return true
		}
	}
	return false
}

func livePlanRowMatches(plan *manifest.LiveQueryPlan, args, row map[string]any) bool {
	if plan.Where != nil && !liveExpressionMatches(plan.Where, args, row) {
		return false
	}
	if plan.Search == nil {
		return true
	}
	search := strings.ToLower(strings.TrimSpace(fmt.Sprint(args[plan.Search.Argument])))
	if search == "" || search == "<nil>" {
		return true
	}
	for _, column := range plan.Search.Columns {
		if strings.Contains(strings.ToLower(fmt.Sprint(row[column])), search) {
			return true
		}
	}
	return false
}

func liveExpressionMatches(expression *manifest.LiveExpression, args, row map[string]any) bool {
	if expression == nil {
		return true
	}
	switch expression.Operator {
	case "and":
		for _, child := range expression.Children {
			if !liveExpressionMatches(child, args, row) {
				return false
			}
		}
		return true
	case "or":
		for _, child := range expression.Children {
			if liveExpressionMatches(child, args, row) {
				return true
			}
		}
		return false
	case "not":
		return len(expression.Children) == 0 || !liveExpressionMatches(expression.Children[0], args, row)
	case "server":
		return true
	}
	left := row[expression.Column]
	right := liveManifestValue(expression.Value, args)
	switch expression.Operator {
	case "eq":
		return liveValuesEqual(left, right)
	case "neq":
		return !liveValuesEqual(left, right)
	case "gt", "gte", "lt", "lte":
		comparison := compareLiveValues(left, right)
		if expression.Operator == "gt" {
			return comparison > 0
		}
		if expression.Operator == "gte" {
			return comparison >= 0
		}
		if expression.Operator == "lt" {
			return comparison < 0
		}
		return comparison <= 0
	case "range":
		return compareLiveValues(left, right) >= 0 && compareLiveValues(left, liveManifestValue(expression.ValueTo, args)) <= 0
	case "contains", "containsInsensitive":
		haystack, needle := fmt.Sprint(left), fmt.Sprint(right)
		if expression.Operator == "containsInsensitive" {
			haystack, needle = strings.ToLower(haystack), strings.ToLower(needle)
		}
		return strings.Contains(haystack, needle)
	case "in":
		if values, ok := right.([]any); ok {
			for _, value := range values {
				if liveValuesEqual(left, value) {
					return true
				}
			}
		}
		return false
	default:
		return true
	}
}

func liveManifestValue(value *manifest.LiveValue, args map[string]any) any {
	if value == nil {
		return nil
	}
	if value.Argument != "" {
		return args[value.Argument]
	}
	return value.Literal
}

func liveValuesEqual(left, right any) bool {
	leftNumber, leftOK := liveNumber(left)
	rightNumber, rightOK := liveNumber(right)
	if leftOK && rightOK {
		return leftNumber == rightNumber
	}
	return fmt.Sprint(left) == fmt.Sprint(right)
}

func compareLiveValues(left, right any) int {
	leftNumber, leftOK := liveNumber(left)
	rightNumber, rightOK := liveNumber(right)
	if leftOK && rightOK {
		if leftNumber < rightNumber {
			return -1
		}
		if leftNumber > rightNumber {
			return 1
		}
		return 0
	}
	return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
}

func liveNumber(value any) (float64, bool) {
	switch current := value.(type) {
	case float64:
		return current, true
	case float32:
		return float64(current), true
	case int:
		return float64(current), true
	case int64:
		return float64(current), true
	case json.Number:
		parsed, err := current.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func tableDetail(change tableChange, table string) tableChangeDetail {
	if detail, ok := change.details[table]; ok {
		return detail
	}
	return tableChangeDetail{
		operation: change.operation, changedColumns: change.changedColumns, rowIDs: change.rowIDs,
		oldValues: change.oldValues, newValues: change.newValues,
	}
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

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func (group *sharedSubscription) request(reason string, changedAtMS float64) {
	group.requestForCommit(reason, changedAtMS, "")
}

func (group *sharedSubscription) requestForCommit(reason string, changedAtMS float64, commitID string) {
	group.requestForCommitBatch(reason, changedAtMS, commitID, commitID)
}

func (group *sharedSubscription) requestForCommitBatch(reason string, changedAtMS float64, commitID, requestID string, changes ...tableChange) {
	commitID = strings.TrimSpace(commitID)
	requestID = strings.TrimSpace(requestID)
	group.mu.Lock()
	targetRevision := uint64(0)
	for _, change := range changes {
		if change.requiredRevision > targetRevision {
			targetRevision = change.requiredRevision
		}
	}
	if requestID != "" && group.commitAlreadyRequestedLocked(requestID) {
		if changedAtMS > group.pendingChangedAtMS {
			group.pendingChangedAtMS = changedAtMS
		}
		group.mu.Unlock()
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.RerunsCoalesced++ })
		return
	}
	if targetRevision > group.requiredRevision {
		group.requiredRevision = targetRevision
	} else if targetRevision == 0 && reason != "initial" {
		// Recovery and legacy/test callers do not carry a database revision. Give
		// them a new local requirement without weakening feed-revision semantics.
		group.requiredRevision++
	}
	group.pendingReason = reason
	if commitID != "" {
		if group.pendingCommitIDs == nil {
			group.pendingCommitIDs = map[string]struct{}{}
		}
		group.pendingCommitIDs[commitID] = struct{}{}
	}
	if requestID != "" {
		if group.pendingRequestIDs == nil {
			group.pendingRequestIDs = map[string]struct{}{}
		}
		group.pendingRequestIDs[requestID] = struct{}{}
	}
	if changedAtMS > group.pendingChangedAtMS {
		group.pendingChangedAtMS = changedAtMS
	}
	if group.running {
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
		targetRevision := group.requiredRevision
		reason := group.pendingReason
		changedAtMS := group.pendingChangedAtMS
		commitIDs := group.pendingCommitIDs
		group.pendingCommitIDs = nil
		group.activeCommitIDs = commitIDs
		requestIDs := group.pendingRequestIDs
		group.pendingRequestIDs = nil
		group.activeRequestIDs = requestIDs
		listeners := group.listenerSnapshotLocked()
		group.mu.Unlock()
		if len(listeners) == 0 || group.ctx.Err() != nil {
			group.finishRun()
			return
		}

		representative, ok := group.firstAuthorizedListener(listeners)
		if !ok {
			group.finishRun()
			return
		}
		startedAt := time.Now().UTC()
		group.manager.server.metrics.recordQueryCommitExecution(group.project, group.tenant, group.key, commitIDs)
		executionCtx := gonvex.WithQueryChange(group.ctx, reason, changedAtMS)
		result, err := group.manager.executeSharedResultSource(executionCtx, group, representative, reason, changedAtMS)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.QueriesRerun++
			metric.ReactiveExecutionPasses++
			metric.ReactiveExecutionDurationMS += float64(time.Since(startedAt).Microseconds()) / 1000
		})
		if group.ctx.Err() != nil {
			group.finishRun()
			return
		}
		completionStarted := time.Now()
		succeeded := err == nil
		if err != nil {
			group.completeError(err.Error())
		} else {
			group.completeResult(result, reason, changedAtMS, startedAt)
		}
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.ResultCompletionPasses++
			metric.ResultCompletionDurationMS += float64(time.Since(completionStarted).Microseconds()) / 1000
		})
		group.mu.Lock()
		group.rememberCompletedCommitsLocked(requestIDs)
		group.activeCommitIDs = nil
		group.activeRequestIDs = nil
		if succeeded && targetRevision > group.computedRevision {
			group.computedRevision = targetRevision
		}
		if succeeded && group.requiredRevision > group.computedRevision {
			group.mu.Unlock()
			if subscriptionRerunCooldown > 0 {
				time.AfterFunc(subscriptionRerunCooldown, group.run)
			} else {
				go group.run()
			}
			return
		}
		group.running = false
		group.mu.Unlock()
		return
	}
}

func (m *subscriptionManager) executeSharedResultSource(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64) (any, error) {
	entry := m.server.runtime.ManifestForProject(group.project).Functions[group.path]
	resultSource := strings.TrimSpace(entry.Dependencies.ShareResultFrom)
	resultField := strings.TrimSpace(entry.Dependencies.ShareResultField)
	if resultSource == "" || resultField == "" {
		return m.executeWithRerunSlot(ctx, group, listener, reason, changedAtMS)
	}

	snapshotKey := any(changedAtMS)
	if commitIDs := sortedStringSet(group.activeCommitIDs); len(commitIDs) > 0 {
		snapshotKey = commitIDs
	}
	resultScope := strings.TrimSpace(group.cacheScope)
	if resultScope == "" {
		switch {
		case listener.visibilityKey != "":
			resultScope = "visibility:" + listener.visibilityKey
		case entry.Dependencies.ShareByPermissions:
			resultScope = "permissions:" + hashQueryCacheValue(listener.caller.permissions)
		default:
			resultScope = "identity:" + visibilityAccountID(listener.caller)
		}
	}
	payload, _ := json.Marshal([]any{
		group.project,
		group.tenant,
		resultSource,
		json.RawMessage(group.args),
		resultScope,
		hashQueryCacheValue(listener.caller.permissions),
		snapshotKey,
	})
	sum := sha256.Sum256(payload)
	key := hex.EncodeToString(sum[:])

	m.sharedResultMu.Lock()
	if active := m.sharedResultRuns[key]; active != nil {
		m.sharedResultMu.Unlock()
		select {
		case <-active.done:
			return active.prepareResult(resultField)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	active := &sharedResultExecution{done: make(chan struct{})}
	m.sharedResultRuns[key] = active
	m.sharedResultMu.Unlock()

	executionStarted := time.Now()
	releaseSlot, acquired := m.acquireExecutionSlot(ctx, reason, group.project, group.tenant)
	if !acquired {
		active.err = ctx.Err()
	} else {
		active.result, active.err = m.server.executeTenantQueryForCallerCached(
			ctx,
			group.project,
			group.tenant,
			listener.caller,
			resultSource,
			group.args,
			group.cacheScope,
			reason,
		)
		releaseSlot()
	}
	executionDurationMS := float64(time.Since(executionStarted).Microseconds()) / 1000
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedResultSourceExecutions++
		metric.SharedResultSourceDurationMS += executionDurationMS
	})
	m.sharedResultMu.Lock()
	close(active.done)
	m.sharedResultMu.Unlock()
	time.AfterFunc(time.Second, func() {
		m.sharedResultMu.Lock()
		if m.sharedResultRuns[key] == active {
			delete(m.sharedResultRuns, key)
		}
		m.sharedResultMu.Unlock()
	})
	return active.prepareResult(resultField)
}

func (execution *sharedResultExecution) prepareResult(field string) (any, error) {
	execution.prepareMu.Lock()
	defer execution.prepareMu.Unlock()
	if prepared, ok := execution.prepared[field]; ok {
		return prepared.result, prepared.err
	}
	result, err := prepareSharedResult(execution.result, field, execution.err)
	if execution.prepared == nil {
		execution.prepared = map[string]sharedPreparedResult{}
	}
	execution.prepared[field] = sharedPreparedResult{result: result, err: err}
	return result, err
}

func prepareSharedResult(value any, field string, executionErr error) (any, error) {
	if executionErr != nil {
		return nil, executionErr
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("shared result source returned %T, want object containing %q", value, field)
	}
	projected, exists := object[field]
	if !exists {
		return nil, fmt.Errorf("shared result source omitted projection %q", field)
	}
	encoded, err := json.Marshal(explicitNull(projected))
	if err != nil {
		return nil, err
	}
	hash, queryPerf := queryResultSemantics(encoded)
	return &canonicalSharedResult{payload: encoded, hash: hash, queryPerf: queryPerf, rowIDs: resultRowIDs(projected)}, nil
}

func (m *subscriptionManager) executeWithRerunSlot(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64) (any, error) {
	release, acquired := m.acquireExecutionSlot(ctx, reason, group.project, group.tenant)
	if !acquired {
		return nil, ctx.Err()
	}
	defer release()
	return m.execute(ctx, group, listener, reason, changedAtMS)
}

// acquireExecutionSlot admits one shared-subscription execution through the
// unified query admission controller. Invalidation and recovery reruns are
// reactive; initial executions are bootstrap hydration.
func (m *subscriptionManager) acquireExecutionSlot(ctx context.Context, reason, project, tenant string) (func(), bool) {
	return m.server.acquireQueryAdmission(ctx, admissionClassForReason(reason), project, tenant)
}

func (group *sharedSubscription) commitAlreadyRequestedLocked(commitID string) bool {
	return requestCoveredBy(group.pendingRequestIDs, commitID) ||
		requestCoveredBy(group.activeRequestIDs, commitID) ||
		requestCoveredBy(group.completedCommitIDs, commitID)
}

func requestCoveredBy(existing map[string]struct{}, requestID string) bool {
	if _, ok := existing[requestID]; ok {
		return true
	}
	commitID, tables, ok := splitCommitTableRequest(requestID)
	if !ok {
		return false
	}
	for candidate := range existing {
		candidateCommit, candidateTables, candidateOK := splitCommitTableRequest(candidate)
		if !candidateOK || candidateCommit != commitID {
			continue
		}
		covered := true
		for table := range tables {
			if _, exists := candidateTables[table]; !exists {
				covered = false
				break
			}
		}
		if covered {
			return true
		}
	}
	return false
}

func splitCommitTableRequest(requestID string) (string, map[string]struct{}, bool) {
	parts := strings.SplitN(requestID, "\x00", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", nil, false
	}
	tables := map[string]struct{}{}
	for _, table := range strings.Split(parts[1], "\x1f") {
		if table != "" {
			tables[table] = struct{}{}
		}
	}
	return parts[0], tables, true
}

func (group *sharedSubscription) rememberCompletedCommitsLocked(commitIDs map[string]struct{}) {
	const retainedCommitIDs = 256
	if len(commitIDs) == 0 {
		return
	}
	if group.completedCommitIDs == nil {
		group.completedCommitIDs = map[string]struct{}{}
	}
	for commitID := range commitIDs {
		if _, exists := group.completedCommitIDs[commitID]; exists {
			continue
		}
		group.completedCommitIDs[commitID] = struct{}{}
		group.completedCommits = append(group.completedCommits, commitID)
	}
	for len(group.completedCommits) > retainedCommitIDs {
		oldest := group.completedCommits[0]
		group.completedCommits = group.completedCommits[1:]
		delete(group.completedCommitIDs, oldest)
	}
}

func (group *sharedSubscription) finishRun() {
	group.mu.Lock()
	group.rememberCompletedCommitsLocked(group.activeRequestIDs)
	group.activeCommitIDs = nil
	group.activeRequestIDs = nil
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
	var sharedResult *canonicalSharedResult
	var payload json.RawMessage
	var hash [sha256.Size]byte
	var queryPerf json.RawMessage
	var rowIDs map[string]bool
	if shared, ok := result.(*canonicalSharedResult); ok {
		sharedResult = shared
		payload, hash, queryPerf, rowIDs = shared.payload, shared.hash, shared.queryPerf, shared.rowIDs
	} else {
		encoded, err := json.Marshal(explicitNull(result))
		if err != nil {
			group.broadcastError(err.Error())
			return
		}
		payload = encoded
		hash, queryPerf = queryResultSemantics(payload)
		rowIDs = resultRowIDs(result)
	}
	group.mu.Lock()
	mutationIDs := make([]string, 0, len(group.activeCommitIDs))
	for commitID := range group.activeCommitIDs {
		if commitID = strings.TrimSpace(commitID); commitID != "" {
			mutationIDs = append(mutationIDs, commitID)
		}
	}
	sort.Strings(mutationIDs)
	// Snapshots are immutable after publication. Keep the previous slice by
	// reference and replace (never overwrite) group.lastResult below; this avoids
	// copying one full result per identity group before every keyed diff.
	previous := group.lastResult
	previousSingleListener := group.lastSingleListener
	unchanged := group.hasHash && hash == group.lastHash
	previousHash := group.lastHash
	previousRevision := group.revision
	listeners := group.listenerSnapshotLocked()
	sameSingleListener := len(listeners) == 1 && previousSingleListener != nil && previousSingleListener == listeners[0].token
	// An unchanged invalidation has no client-visible state transition. Do not
	// emit progress and, crucially, do not advance the server revision: the next
	// real keyed patch must still name the exact revision every client last
	// acknowledged. Initial/cache-revalidation paths retain progress semantics.
	if reason == "invalidate" && unchanged && (len(previous) > 0 || sameSingleListener) {
		group.lastError = ""
		group.rowIDs = rowIDs
		group.mu.Unlock()
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.UnchangedResultsSuppressed++
			metric.ResultBytesBefore += uint64(len(payload))
		})
		return
	}
	revision := group.manager.sequence.Add(1)
	group.revision = revision
	group.lastHash = hash
	group.hasHash = true
	group.lastError = ""
	group.rowIDs = rowIDs
	if len(listeners) == 1 {
		group.lastSingleListener = listeners[0].token
	} else {
		group.lastSingleListener = nil
	}
	// A one-listener group can rerun if a matching listener arrives later and
	// only needs the hash for unchanged-result suppression. Retaining the full
	// payload for every identity-scoped subscription multiplies memory by the
	// user count without improving correctness. Shared groups keep a snapshot
	// for immediate replay and keyed patches.
	if (len(listeners) > 1 || group.retainSnapshot) && len(payload) <= group.manager.server.config.SharedResultMaxBytes {
		if sharedResult != nil {
			// Every admitted group may retain the same immutable encoding. Listener
			// revisions and patch baselines remain group-local slice headers.
			group.lastResult = payload
		} else {
			group.lastResult = append(json.RawMessage(nil), payload...)
		}
	} else {
		group.lastResult = nil
	}
	group.mu.Unlock()

	revisionValue := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: revision}
	if unchanged && (len(previous) > 0 || sameSingleListener) {
		message := serverMessage{Type: "query.progress", Path: group.path, Reason: reason, ThroughRevision: revisionValue, OriginCommandIDs: mutationIDs, QueryPerf: queryPerf}
		group.broadcastTo(listeners, message, changedAtMS, startedAt)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.UnchangedResultsSuppressed++
			metric.ProgressMessages++
			metric.ResultBytesBefore += uint64(len(payload))
		})
		return
	}

	cacheRevision := group.manager.server.nextQueryCacheRevision(hash)
	message := serverMessage{Type: "query.result", Path: group.path, Result: json.RawMessage(payload), Reason: reason, CacheScope: group.cacheScope, CacheRevision: cacheRevision, SubscriptionRevision: revisionValue, OriginCommandIDs: mutationIDs, QueryPerf: queryPerf}
	encodedSize := len(payload)
	patched := false
	if len(previous) >= minimumPatchResultBytes {
		var patch serverMessage
		var patchOK bool
		patchEncodedSize := 0
		if sharedResult != nil {
			cached := sharedResult.keyedPatch(previousHash, previous)
			patch, patchOK = cached.message, cached.ok
			patchEncodedSize = cached.encodedSize
		} else {
			patch, patchOK = keyedResultPatch(previous, payload)
		}
		if patchOK {
			patch.SubscriptionRevision = revisionValue
			patch.BaseRevision = &subscriptionRevision{Epoch: group.manager.epoch, Sequence: previousRevision}
			patch.Path = group.path
			patch.Reason = reason
			patch.OriginCommandIDs = mutationIDs
			patch.CacheScope = group.cacheScope
			patch.CacheRevision = cacheRevision
			patch.FullResult = payload
			if sharedResult != nil {
				message = patch
				// keyedPatch already proved this template is comfortably below the
				// full-result threshold before group-local metadata was attached.
				encodedSize = patchEncodedSize
				patched = true
			} else if encoded, encodeErr := json.Marshal(patch); encodeErr == nil && len(encoded) < len(payload)*7/10 {
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

func (shared *canonicalSharedResult) keyedPatch(previousHash [sha256.Size]byte, previous json.RawMessage) canonicalSharedPatch {
	shared.patchMu.Lock()
	defer shared.patchMu.Unlock()
	if cached, ok := shared.patches[previousHash]; ok {
		return cached
	}
	patch, ok := keyedResultPatch(previous, shared.payload)
	encodedSize := len(shared.payload)
	if ok {
		if encoded, err := json.Marshal(patch); err != nil || len(encoded) >= len(shared.payload)*7/10 {
			ok = false
		} else {
			encodedSize = len(encoded)
		}
	}
	cached := canonicalSharedPatch{message: patch, encodedSize: encodedSize, ok: ok}
	if shared.patches == nil {
		shared.patches = map[[sha256.Size]byte]canonicalSharedPatch{}
	}
	shared.patches[previousHash] = cached
	return cached
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
	deliveryStarted := time.Now()
	type preparedDelivery struct {
		listener querySubscription
		message  serverMessage
	}
	type fanoutKey struct {
		conn        *wsConn
		messageType string
		cacheScope  string
	}
	type deliveryAccounting struct {
		delivery preparedDelivery
		sentAt   time.Time
		trace    *messageTrace
	}
	prepared := make([]preparedDelivery, 0, len(listeners))
	accounting := make([]deliveryAccounting, 0, len(listeners))
	for _, listener := range listeners {
		if listener.conn == nil {
			continue
		}
		if !listenerCurrent(listener) {
			continue
		}
		copy := message
		copy.ID = listener.id
		if copy.Type == "query.pagePatch" && !listener.conn.queryPagePatch {
			copy.Type = "query.result"
			copy.Result = copy.FullResult
			copy.Inserted, copy.Updated, copy.Deleted, copy.Order = nil, nil, nil, nil
		}
		if copy.Type == "query.objectPatch" && !listener.conn.queryObjectPatch {
			copy.Type = "query.result"
			copy.Result = copy.FullResult
			copy.Collections = nil
		}
		if !listener.conn.queryOrderDelta && messageUsesOrderDelta(copy) {
			copy.Type = "query.result"
			copy.Result = copy.FullResult
			copy.Inserted, copy.Updated, copy.Deleted, copy.Order = nil, nil, nil, nil
			copy.Prepend, copy.Append, copy.Collections = nil, nil, nil
		}
		// Compare against the listener's LIVE revision, not the subscribe-time
		// snapshot. The snapshot goes stale the moment the server pushes any
		// newer result: a subscribe-time revision that happens to equal a later
		// payload (seed → delete returns a list to its pre-seed contents, the
		// shape of every CRUD spec) converted the fresh result into a
		// "progress" while the client was rendering the intermediate state —
		// the grid then stayed stale forever.
		if copy.Type == "query.result" && copy.SubscriptionRevision != nil && queryCacheRevisionMatchesHash(currentListenerCacheRevision(listener), group.lastHash) {
			copy = serverMessage{
				Type:             "query.progress",
				ID:               listener.id,
				Path:             group.path,
				Reason:           message.Reason,
				ThroughRevision:  copy.SubscriptionRevision,
				OriginCommandIDs: message.OriginCommandIDs,
				QueryPerf:        message.QueryPerf,
			}
		}
		if copy.Type == "query.result" || copy.Type == "query.patch" || copy.Type == "query.pagePatch" || copy.Type == "query.objectPatch" {
			copy.CacheScope = listener.cacheScope
		}
		prepared = append(prepared, preparedDelivery{listener: listener, message: copy})
	}
	preparedAt := time.Now()
	queueDeliveryAccounting := func(delivery preparedDelivery, sentAt time.Time, trace *messageTrace) {
		listener, copy := delivery.listener, delivery.message
		if (copy.Type == "query.result" || copy.Type == "query.patch" || copy.Type == "query.pagePatch" || copy.Type == "query.objectPatch") && copy.CacheRevision != "" {
			storeListenerCacheRevision(listener, copy.CacheRevision)
		}
		accounting = append(accounting, deliveryAccounting{delivery: delivery, sentAt: sentAt, trace: trace})
	}

	makeTrace := func(copy serverMessage, sentAt time.Time) *messageTrace {
		return &messageTrace{
			ServerChangeCommittedAtMS:     changedAtMS,
			ServerSubscriptionStartedAtMS: epochMillis(startedAt),
			ServerSubscriptionSentAtMS:    epochMillis(sentAt),
			ServerDurationMS:              float64(sentAt.Sub(startedAt).Microseconds()) / 1000,
			QueryPerf:                     copy.QueryPerf,
		}
	}

	batches := map[fanoutKey][]preparedDelivery{}
	for _, delivery := range prepared {
		if delivery.listener.conn.queryFanout {
			key := fanoutKey{conn: delivery.listener.conn, messageType: delivery.message.Type, cacheScope: delivery.message.CacheScope}
			batches[key] = append(batches[key], delivery)
			continue
		}
		sentAt := time.Now().UTC()
		trace := makeTrace(delivery.message, sentAt)
		delivery.message.Trace = trace
		delivery.listener.conn.write(delivery.message)
		queueDeliveryAccounting(delivery, sentAt, trace)
	}
	type fanoutBatch struct {
		key   fanoutKey
		batch []preparedDelivery
	}
	orderedBatches := make([]fanoutBatch, 0, len(batches))
	for key, batch := range batches {
		if len(batch) > 0 {
			orderedBatches = append(orderedBatches, fanoutBatch{key: key, batch: batch})
		}
	}
	sort.Slice(orderedBatches, func(i, j int) bool { return orderedBatches[i].key.conn.id < orderedBatches[j].key.conn.id })
	batchAccounting := make([][]deliveryAccounting, len(orderedBatches))
	workerCount := min(32, len(orderedBatches))
	var batchWG sync.WaitGroup
	for worker := range workerCount {
		batchWG.Add(1)
		go func() {
			defer batchWG.Done()
			for index := worker; index < len(orderedBatches); index += workerCount {
				item := orderedBatches[index]
				key, batch := item.key, item.batch
				sentAt := time.Now().UTC()
				trace := makeTrace(batch[0].message, sentAt)
				if len(batch) == 1 {
					batch[0].message.Trace = trace
					key.conn.write(batch[0].message)
				} else {
					fanout := batch[0].message
					fanout.Type, fanout.QueryType, fanout.ID = "query.fanout", fanout.Type, ""
					fanout.IDs = make([]string, 0, len(batch))
					for _, delivery := range batch {
						fanout.IDs = append(fanout.IDs, delivery.listener.id)
					}
					fanout.Trace = trace
					key.conn.write(fanout)
				}
				local := make([]deliveryAccounting, 0, len(batch))
				for _, delivery := range batch {
					if (delivery.message.Type == "query.result" || delivery.message.Type == "query.patch" || delivery.message.Type == "query.pagePatch" || delivery.message.Type == "query.objectPatch") && delivery.message.CacheRevision != "" {
						storeListenerCacheRevision(delivery.listener, delivery.message.CacheRevision)
					}
					local = append(local, deliveryAccounting{delivery: delivery, sentAt: sentAt, trace: trace})
				}
				batchAccounting[index] = local
			}
		}()
	}
	batchWG.Wait()
	for _, local := range batchAccounting {
		accounting = append(accounting, local...)
	}
	writesAt := time.Now()
	// Telemetry is logical-subscription accounting, not delivery work. Keep it
	// behind every physical WebSocket write so a high duplicate-listener count
	// cannot delay the last client while preserving identical records/metrics.
	var changeLatencyTotal float64
	var changeLatencySamples uint64
	var serverDurationTotal float64
	for _, item := range accounting {
		if changedAtMS > 0 {
			changeLatencyTotal += epochMillis(item.sentAt) - changedAtMS
			changeLatencySamples++
		}
		serverDurationTotal += item.trace.ServerDurationMS
	}
	if changeLatencySamples > 0 {
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.ChangeToClientDurationMS += changeLatencyTotal
			metric.ChangeToClientSamples += changeLatencySamples
		})
	}
	if len(accounting) > 0 {
		first := accounting[0]
		trace := *first.trace
		trace.ServerDurationMS = serverDurationTotal / float64(len(accounting))
		trace.ServerSubscriptionSentAtMS = trace.ServerSubscriptionStartedAtMS + trace.ServerDurationMS
		entry := transactionEntryFromTrace(first.delivery.listener.project, first.delivery.listener.tenant, "", "query", first.delivery.listener.path, "server", message.Reason, "ok", "", &trace)
		entry.LogicalCount = int64(len(accounting))
		group.manager.server.enqueueSubscriptionTelemetry([]transactionTelemetryEntry{entry})
	}
	if message.Reason == "invalidate" {
		prepareDurationMS := float64(preparedAt.Sub(deliveryStarted).Microseconds()) / 1000
		writeDurationMS := float64(writesAt.Sub(preparedAt).Microseconds()) / 1000
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.DeliveryPasses++
			metric.DeliveryPrepareDurationMS += prepareDurationMS
			metric.DeliveryWriteDurationMS += writeDurationMS
		})
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
	revisionValue := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: revision}
	hash, queryPerf := queryResultSemantics(payload)
	var trace *messageTrace
	if len(queryPerf) > 0 {
		trace = &messageTrace{QueryPerf: queryPerf}
	}
	if queryCacheRevisionMatchesHash(currentListenerCacheRevision(listener), hash) {
		listener.conn.write(serverMessage{
			Type: "query.progress", ID: listener.id, Path: listener.path, Reason: reason,
			ThroughRevision: revisionValue, Trace: trace,
		})
		return
	}
	cacheRevision := group.manager.server.nextQueryCacheRevision(hash)
	listener.conn.write(serverMessage{
		Type: "query.result", ID: listener.id, Path: listener.path, Result: payload, Reason: reason,
		CacheScope: listener.cacheScope, CacheRevision: cacheRevision,
		SubscriptionRevision: revisionValue, Trace: trace,
	})
	storeListenerCacheRevision(listener, cacheRevision)
}

// currentListenerCacheRevision reads the listener's LIVE cache revision from
// the connection's subscription map. Group snapshots copy the revision at
// subscribe time; every delivered result advances the client's cache, so the
// snapshot value must never be used for "does the client already have this
// payload" decisions.
func currentListenerCacheRevision(listener querySubscription) string {
	if listener.token == nil {
		return listener.cacheRevision
	}
	revision, _ := listener.token.cacheRevision.Load().(string)
	return revision
}

// storeListenerCacheRevision records the revision most recently DELIVERED to
// this listener so later unchanged-payload checks compare against what the
// client actually holds.
func storeListenerCacheRevision(listener querySubscription, revision string) {
	if revision == "" || listener.token == nil || !listener.token.active.Load() {
		return
	}
	listener.token.cacheRevision.Store(revision)
}

func listenerCurrent(listener querySubscription) bool {
	if listener.conn == nil || listener.token == nil || listener.ctx.Err() != nil {
		return false
	}
	return listener.token.active.Load()
}

func keyedResultPatch(previous, next json.RawMessage) (serverMessage, bool) {
	patchType := "query.patch"
	var envelope map[string]json.RawMessage
	var patchResult any
	if json.Unmarshal(next, &envelope) == nil {
		if envelope["page"] != nil {
			patchType = "query.pagePatch"
			delete(envelope, "page")
			patchResult = envelope
		} else if patch, ok := keyedObjectResultPatch(previous, envelope); ok {
			return patch, true
		}
	}
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
	var order, prepend, append []string
	if !equalStrings(oldOrder, newOrder) {
		if prepend, append, ok = compactOrderDelta(oldOrder, newOrder, deleted, inserted); !ok {
			order, prepend, append = newOrder, nil, nil
		}
	}
	return serverMessage{Type: patchType, Result: patchResult, Inserted: inserted, Updated: updated, Deleted: deleted, Order: order, Prepend: prepend, Append: append}, true
}

// keyedObjectResultPatch handles object-shaped query results whose properties
// are independent keyed row collections (for example a pivot payload containing
// taskUsers, taskTags, and taskCustomFieldValues). It fails closed unless both
// objects have exactly the same property set and every changed property is a
// keyed array. This keeps patch application atomic and avoids inventing merge
// semantics for removed or mutable scalar fields.
func keyedObjectResultPatch(previous json.RawMessage, next map[string]json.RawMessage) (serverMessage, bool) {
	var old map[string]json.RawMessage
	if json.Unmarshal(previous, &old) != nil || len(old) != len(next) {
		return serverMessage{}, false
	}
	collections := make(map[string]keyedCollectionPatch)
	for key, newValue := range next {
		oldValue, exists := old[key]
		if !exists {
			return serverMessage{}, false
		}
		patch, changed, ok := keyedCollectionDiff(oldValue, newValue)
		if !ok {
			if !bytes.Equal(oldValue, newValue) {
				return serverMessage{}, false
			}
			continue
		}
		if changed {
			collections[key] = patch
		}
	}
	if len(collections) == 0 {
		return serverMessage{}, false
	}
	return serverMessage{Type: "query.objectPatch", Collections: collections}, true
}

func keyedCollectionDiff(previous, next json.RawMessage) (keyedCollectionPatch, bool, bool) {
	oldRows, oldOrder, ok := keyedRows(previous)
	if !ok {
		return keyedCollectionPatch{}, false, false
	}
	newRows, newOrder, ok := keyedRows(next)
	if !ok {
		return keyedCollectionPatch{}, false, false
	}
	patch := keyedCollectionPatch{}
	for id, row := range newRows {
		old, exists := oldRows[id]
		if !exists {
			patch.Inserted = append(patch.Inserted, row)
		} else if !bytes.Equal(old, row) {
			patch.Updated = append(patch.Updated, row)
		}
	}
	for id := range oldRows {
		if _, exists := newRows[id]; !exists {
			patch.Deleted = append(patch.Deleted, id)
		}
	}
	orderChanged := !equalStrings(oldOrder, newOrder)
	if len(patch.Inserted) == 0 && len(patch.Updated) == 0 && len(patch.Deleted) == 0 && !orderChanged {
		return keyedCollectionPatch{}, false, true
	}
	sort.Slice(patch.Inserted, func(i, j int) bool { return rowID(patch.Inserted[i]) < rowID(patch.Inserted[j]) })
	sort.Slice(patch.Updated, func(i, j int) bool { return rowID(patch.Updated[i]) < rowID(patch.Updated[j]) })
	sort.Strings(patch.Deleted)
	if orderChanged {
		if prepend, append, compact := compactOrderDelta(oldOrder, newOrder, patch.Deleted, patch.Inserted); compact {
			patch.Prepend, patch.Append = prepend, append
		} else {
			patch.Order = newOrder
		}
	}
	return patch, true, true
}

func compactOrderDelta(oldOrder, newOrder, deleted []string, inserted []json.RawMessage) ([]string, []string, bool) {
	deletedSet := make(map[string]bool, len(deleted))
	for _, id := range deleted {
		deletedSet[id] = true
	}
	base := make([]string, 0, len(oldOrder))
	for _, id := range oldOrder {
		if !deletedSet[id] {
			base = append(base, id)
		}
	}
	insertedSet := make(map[string]bool, len(inserted))
	for _, row := range inserted {
		insertedSet[rowID(row)] = true
	}
	if len(newOrder) < len(base) || len(newOrder)-len(base) != len(insertedSet) {
		return nil, nil, false
	}
	prefixLen := len(newOrder) - len(base)
	if equalStrings(newOrder[prefixLen:], base) {
		prefix := append([]string(nil), newOrder[:prefixLen]...)
		for _, id := range prefix {
			if !insertedSet[id] {
				return nil, nil, false
			}
		}
		return prefix, nil, true
	}
	if equalStrings(newOrder[:len(base)], base) {
		suffix := append([]string(nil), newOrder[len(base):]...)
		for _, id := range suffix {
			if !insertedSet[id] {
				return nil, nil, false
			}
		}
		return nil, suffix, true
	}
	return nil, nil, false
}

func messageUsesOrderDelta(message serverMessage) bool {
	if len(message.Prepend) > 0 || len(message.Append) > 0 {
		return true
	}
	for _, collection := range message.Collections {
		if len(collection.Prepend) > 0 || len(collection.Append) > 0 {
			return true
		}
	}
	return false
}

func keyedRows(payload json.RawMessage) (map[string]json.RawMessage, []string, bool) {
	var rows []json.RawMessage
	if json.Unmarshal(payload, &rows) != nil {
		var envelope map[string]json.RawMessage
		if json.Unmarshal(payload, &envelope) != nil || json.Unmarshal(envelope["page"], &rows) != nil {
			return nil, nil, false
		}
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
	for _, key := range []string{"_id", "id"} {
		var value any
		if json.Unmarshal(object[key], &value) != nil || value == nil {
			continue
		}
		id := strings.TrimSpace(fmt.Sprint(value))
		if id != "" && id != "<nil>" {
			return id
		}
	}
	return ""
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
