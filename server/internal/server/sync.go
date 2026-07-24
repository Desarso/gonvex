package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
)

type syncCursor struct {
	Epoch    string `json:"epoch"`
	Revision uint64 `json:"revision"`
}

type mutationIDContextKey struct{}

func withMutationID(ctx context.Context, mutationID string) context.Context {
	return context.WithValue(ctx, mutationIDContextKey{}, strings.TrimSpace(mutationID))
}

func mutationIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(mutationIDContextKey{}).(string)
	return value
}

type syncSubscription struct {
	conn             *wsConn
	id               string
	path             string
	project          string
	tenant           string
	args             json.RawMessage
	definition       manifest.SyncDefinition
	cursor           syncCursor
	listenerAcquired bool
	visibleKeys      map[string]bool
	mu               sync.Mutex
	closed           bool
}

type syncLogChange struct {
	revision   uint64
	ordinal    int
	mutationID string
	table      string
	rowID      string
	operation  string
	oldValue   json.RawMessage
	newValue   json.RawMessage
}

const (
	defaultSyncRetention = 7 * 24 * time.Hour
	syncPruneInterval    = time.Hour
)

var errSyncCursorExpired = errors.New("sync cursor is older than the retained change log")

func manifestSyncDefinitions(current manifest.Manifest) map[string]manifest.SyncDefinition {
	definitions := map[string]manifest.SyncDefinition{}
	for _, entry := range current.Functions {
		if entry.Kind != manifest.FunctionKindSync || entry.Sync == nil {
			continue
		}
		definition := *entry.Sync
		table := strings.TrimSpace(definition.Table)
		if table == "" {
			continue
		}
		if existing, ok := definitions[table]; ok {
			existing.Columns = appendUniqueStrings(existing.Columns, definition.Columns...)
			for column, argument := range definition.EqualFilters {
				if existing.EqualFilters == nil {
					existing.EqualFilters = map[string]string{}
				}
				existing.EqualFilters[column] = argument
			}
			existing.VisibilityTables = appendUniqueStrings(existing.VisibilityTables, definition.VisibilityTables...)
			existing.ExcludeWhenSet = appendUniqueStrings(existing.ExcludeWhenSet, definition.ExcludeWhenSet...)
			if definition.OrderBy != "" {
				existing.OrderBy = definition.OrderBy
			}
			definitions[table] = existing
			continue
		}
		definition.Columns = appendUniqueStrings(nil, definition.Columns...)
		definitions[table] = definition
	}
	return definitions
}

func syncDefinitionsForSchema(definitions map[string]manifest.SyncDefinition, current manifest.Schema) map[string]manifest.SyncDefinition {
	filtered := map[string]manifest.SyncDefinition{}
	for table, definition := range definitions {
		if _, ok := current.Tables[table]; ok {
			filtered[table] = definition
		}
	}
	return filtered
}

func (s *Server) installTenantSyncLog(ctx context.Context, project, databaseURL string, current manifest.Schema) error {
	definitions := syncDefinitionsForSchema(
		manifestSyncDefinitions(s.runtime.ManifestForProject(project)),
		current,
	)
	if len(definitions) == 0 {
		return nil
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = schemasync.InstallSyncLog(ctx, db, current, definitions)
	return err
}

func (c *wsConn) openSync(ctx context.Context, message clientMessage) {
	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(message.Path) == "" {
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: "sync id and path are required"})
		return
	}
	current := c.server.runtime.ManifestForProject(c.project)
	entry, ok := current.Functions[message.Path]
	if !ok || entry.Kind != manifest.FunctionKindSync || entry.Sync == nil {
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: "sync function is not registered"})
		return
	}
	definition := *entry.Sync
	databaseURL := c.server.databaseURLForTenant(c.project, c.tenant)
	c.server.scheduleSyncLogPrune(c.project, c.tenant, databaseURL)
	base, err := currentSyncCursor(ctx, databaseURL, definition, c.currentCacheScope())
	if err != nil {
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}

	subscription := &syncSubscription{
		conn: c, id: message.ID, path: message.Path, project: c.project, tenant: c.tenant,
		args: append(json.RawMessage(nil), message.Args...), definition: definition, cursor: base,
		visibleKeys: syncKeySet(message.Keys),
	}
	c.closeSync(message.ID)

	if message.Cursor != nil && message.Cursor.Epoch == base.Epoch && message.Cursor.Revision <= base.Revision {
		resumable, resumeErr := syncCursorAvailable(ctx, databaseURL, message.Cursor.Revision, base.Revision)
		if resumeErr != nil {
			c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: resumeErr.Error()})
			return
		}
		if resumable {
			subscription.cursor = *message.Cursor
			c.attachSync(subscription)
			if err := c.server.deliverSync(subscription); err != nil {
				c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
			}
			return
		}
	}

	result, err := c.server.executeTenantQueryForCallerUncached(ctx, c.project, c.tenant, c.caller(), message.Path, message.Args)
	if err != nil {
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	rows, err := syncSnapshotRows(result, definition)
	if err != nil {
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	subscription.visibleKeys = syncRowsKeySet(rows, definition.Key)
	c.attachSync(subscription)
	c.write(serverMessage{
		Type: "sync.snapshot", ID: message.ID, Path: message.Path, Result: rows, Cursor: &base,
		Key: definition.Key, OrderBy: definition.OrderBy, OrderDirection: definition.OrderDirection,
		Mode: definition.Mode, MaxRows: definition.MaxRows, MaxBytes: definition.MaxBytes,
	})
	if err := c.server.deliverSync(subscription); err != nil {
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
	}
}

func (s *Server) scheduleSyncLogPrune(project, tenant, databaseURL string) {
	key := project + "\x00" + tenant
	now := time.Now()
	s.syncPruneMu.Lock()
	if last := s.syncPrunedAt[key]; !last.IsZero() && now.Sub(last) < syncPruneInterval {
		s.syncPruneMu.Unlock()
		return
	}
	s.syncPrunedAt[key] = now
	s.syncPruneMu.Unlock()

	retention := syncRetentionForManifest(s.runtime.ManifestForProject(project))
	go func() {
		if err := pruneSyncLog(context.Background(), databaseURL, retention); err != nil {
			slog.Warn("could not prune Gonvex sync log", "project", project, "tenant", tenant, "error", err)
		}
	}()
}

func syncRetentionForManifest(current manifest.Manifest) time.Duration {
	retention := defaultSyncRetention
	for _, entry := range current.Functions {
		if entry.Kind != manifest.FunctionKindSync || entry.Sync == nil || entry.Sync.RetentionMilliseconds <= 0 {
			continue
		}
		candidate := time.Duration(entry.Sync.RetentionMilliseconds) * time.Millisecond
		if candidate > retention {
			retention = candidate
		}
	}
	return retention
}

func pruneSyncLog(ctx context.Context, databaseURL string, retention time.Duration) error {
	if strings.TrimSpace(databaseURL) == "" {
		return fmt.Errorf("tenant database is not configured")
	}
	if retention <= 0 {
		retention = defaultSyncRetention
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var prunedThrough sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		WITH removed AS (
			DELETE FROM _gonvex_sync_changes
			WHERE created_at < clock_timestamp() - ($1::bigint * interval '1 millisecond')
			RETURNING revision
		)
		SELECT max(revision) FROM removed
	`, retention.Milliseconds()).Scan(&prunedThrough); err != nil {
		return err
	}
	if prunedThrough.Valid {
		if _, err := tx.ExecContext(ctx, `
			UPDATE _gonvex_sync_clock
			SET retained_revision = greatest(retained_revision, $1)
			WHERE singleton = true
		`, prunedThrough.Int64); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func syncSnapshotRows(result any, definition manifest.SyncDefinition) ([]json.RawMessage, error) {
	payload, err := json.Marshal(explicitNull(result))
	if err != nil {
		return nil, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, fmt.Errorf("sync handler must return an array of keyed rows")
	}
	maxRows := definition.MaxRows
	maxBytes := definition.MaxBytes
	totalBytes := int64(0)
	kept := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		if syncRowKey(row, definition.Key) == "" {
			return nil, fmt.Errorf("sync row is missing key %q", definition.Key)
		}
		if maxRows > 0 && len(kept) >= maxRows {
			break
		}
		if maxBytes > 0 && totalBytes+int64(len(row)) > maxBytes {
			break
		}
		totalBytes += int64(len(row))
		kept = append(kept, row)
	}
	return kept, nil
}

func syncRowKey(row json.RawMessage, key string) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(row, &object) != nil {
		return ""
	}
	var value string
	if json.Unmarshal(object[key], &value) == nil {
		return value
	}
	var scalar any
	if json.Unmarshal(object[key], &scalar) == nil && scalar != nil {
		return fmt.Sprint(scalar)
	}
	return ""
}

func currentSyncCursor(ctx context.Context, databaseURL string, definition manifest.SyncDefinition, cacheScope string) (syncCursor, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return syncCursor{}, err
	}
	defer db.Close()
	var databaseEpoch string
	var revision uint64
	if err := db.QueryRowContext(ctx, `SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton = true`).Scan(&databaseEpoch, &revision); err != nil {
		return syncCursor{}, fmt.Errorf("sync storage is not installed: %w", err)
	}
	payload, _ := json.Marshal(struct {
		DatabaseEpoch string                  `json:"databaseEpoch"`
		Definition    manifest.SyncDefinition `json:"definition"`
		Scope         string                  `json:"scope"`
	}{databaseEpoch, definition, cacheScope})
	hash := sha256.Sum256(payload)
	return syncCursor{Epoch: hex.EncodeToString(hash[:16]), Revision: revision}, nil
}

func syncCursorAvailable(ctx context.Context, databaseURL string, revision, current uint64) (bool, error) {
	if revision == current {
		return true, nil
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var retained uint64
	if err := db.QueryRowContext(ctx, `SELECT retained_revision FROM _gonvex_sync_clock WHERE singleton = true`).Scan(&retained); err != nil {
		return false, err
	}
	return revision >= retained, nil
}

func (s *Server) deliverSync(subscription *syncSubscription) error {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || !subscriptionCurrent(subscription) {
		return nil
	}
	if err := subscription.conn.revalidateAppAuth(context.Background()); err != nil {
		subscription.conn.clearAuthentication()
		return nil
	}
	databaseURL := s.databaseURLForTenant(subscription.project, subscription.tenant)
	latest, err := currentSyncCursor(context.Background(), databaseURL, subscription.definition, subscription.conn.currentCacheScope())
	if err != nil {
		return err
	}
	if latest.Epoch != subscription.cursor.Epoch {
		s.resetSyncWhileLocked(subscription, "definition-changed")
		return nil
	}
	if latest.Revision <= subscription.cursor.Revision {
		subscription.conn.write(serverMessage{Type: "sync.ready", ID: subscription.id, Path: subscription.path, Cursor: &latest})
		return nil
	}
	changes, err := readSyncChanges(context.Background(), databaseURL, subscription.cursor.Revision, latest.Revision, subscription.definition.Table)
	if err != nil {
		if errors.Is(err, errSyncCursorExpired) {
			s.resetSyncWhileLocked(subscription, "cursor-expired")
			return nil
		}
		return err
	}
	args := map[string]json.RawMessage{}
	_ = json.Unmarshal(subscription.args, &args)
	if subscription.definition.Mode == "progressive" && len(changes) > 0 {
		if err := s.deliverProgressiveSync(subscription, changes, args, latest); err != nil {
			return err
		}
		subscription.conn.write(serverMessage{Type: "sync.ready", ID: subscription.id, Path: subscription.path, Cursor: &subscription.cursor})
		return nil
	}
	for _, batch := range groupSyncChanges(changes) {
		upserts := map[string]json.RawMessage{}
		deleted := map[string]bool{}
		mutationIDs := map[string]bool{}
		for _, change := range batch.changes {
			oldMatches := syncValueMatches(change.oldValue, subscription.definition, args)
			newMatches := syncValueMatches(change.newValue, subscription.definition, args)
			switch {
			case newMatches:
				upserts[change.rowID] = change.newValue
				delete(deleted, change.rowID)
			case oldMatches:
				delete(upserts, change.rowID)
				deleted[change.rowID] = true
			}
			if change.mutationID != "" {
				mutationIDs[change.mutationID] = true
			}
		}
		cursor := syncCursor{Epoch: latest.Epoch, Revision: batch.revision}
		subscription.conn.write(serverMessage{
			Type: "sync.delta", ID: subscription.id, Path: subscription.path, Cursor: &cursor,
			Upserts: sortedSyncRows(upserts), Deleted: sortedSyncKeys(deleted), MutationIDs: sortedSyncKeys(mutationIDs),
		})
		subscription.cursor = cursor
	}
	if subscription.cursor.Revision < latest.Revision {
		subscription.cursor.Revision = latest.Revision
	}
	subscription.conn.write(serverMessage{Type: "sync.ready", ID: subscription.id, Path: subscription.path, Cursor: &subscription.cursor})
	return nil
}

func (s *Server) resetSyncWhileLocked(subscription *syncSubscription, reason string) {
	subscription.conn.removeSync(subscription.id)
	subscription.closed = true
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	if acquired {
		s.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
	subscription.conn.write(serverMessage{
		Type: "sync.reset", ID: subscription.id, Path: subscription.path, Reason: reason,
	})
}

func (s *Server) deliverProgressiveSync(
	subscription *syncSubscription,
	changes []syncLogChange,
	args map[string]json.RawMessage,
	latest syncCursor,
) error {
	result, err := s.executeTenantQueryForCallerUncached(
		context.Background(),
		subscription.project,
		subscription.tenant,
		subscription.conn.caller(),
		subscription.path,
		subscription.args,
	)
	if err != nil {
		return err
	}
	rows, err := syncSnapshotRows(result, subscription.definition)
	if err != nil {
		return err
	}
	currentRows := map[string]json.RawMessage{}
	currentKeys := map[string]bool{}
	for _, row := range rows {
		key := syncRowKey(row, subscription.definition.Key)
		if key == "" {
			continue
		}
		currentRows[key] = row
		currentKeys[key] = true
	}

	upserts := map[string]json.RawMessage{}
	deleted := map[string]bool{}
	mutationIDs := map[string]bool{}
	for key := range currentKeys {
		if !subscription.visibleKeys[key] {
			upserts[key] = currentRows[key]
		}
	}
	for key := range subscription.visibleKeys {
		if !currentKeys[key] {
			deleted[key] = true
		}
	}
	for _, change := range changes {
		if currentKeys[change.rowID] && syncValueMatches(change.newValue, subscription.definition, args) {
			upserts[change.rowID] = currentRows[change.rowID]
			delete(deleted, change.rowID)
		} else if subscription.visibleKeys[change.rowID] {
			deleted[change.rowID] = true
			delete(upserts, change.rowID)
		}
		if change.mutationID != "" {
			mutationIDs[change.mutationID] = true
		}
	}

	subscription.visibleKeys = currentKeys
	subscription.cursor = latest
	subscription.conn.write(serverMessage{
		Type: "sync.delta", ID: subscription.id, Path: subscription.path, Cursor: &latest,
		Upserts: sortedSyncRows(upserts), Deleted: sortedSyncKeys(deleted), MutationIDs: sortedSyncKeys(mutationIDs),
	})
	return nil
}

func syncKeySet(keys []string) map[string]bool {
	result := map[string]bool{}
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			result[key] = true
		}
	}
	return result
}

func syncRowsKeySet(rows []json.RawMessage, keyField string) map[string]bool {
	result := map[string]bool{}
	for _, row := range rows {
		if key := syncRowKey(row, keyField); key != "" {
			result[key] = true
		}
	}
	return result
}

type syncChangeBatch struct {
	revision uint64
	changes  []syncLogChange
}

func groupSyncChanges(changes []syncLogChange) []syncChangeBatch {
	batches := make([]syncChangeBatch, 0)
	for _, change := range changes {
		if len(batches) == 0 || batches[len(batches)-1].revision != change.revision {
			batches = append(batches, syncChangeBatch{revision: change.revision})
		}
		batches[len(batches)-1].changes = append(batches[len(batches)-1].changes, change)
	}
	return batches
}

func readSyncChanges(ctx context.Context, databaseURL string, after, through uint64, table string) ([]syncLogChange, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var retained uint64
	if err := tx.QueryRowContext(ctx, `SELECT retained_revision FROM _gonvex_sync_clock WHERE singleton = true`).Scan(&retained); err != nil {
		return nil, err
	}
	if after < retained {
		return nil, errSyncCursorExpired
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT revision, ordinal, COALESCE(mutation_id, ''), table_name, row_id, operation,
		       COALESCE(old_value, 'null'::jsonb), COALESCE(new_value, 'null'::jsonb)
		FROM _gonvex_sync_changes
		WHERE revision > $1 AND revision <= $2 AND table_name = $3
		ORDER BY revision, ordinal
	`, after, through, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	changes := make([]syncLogChange, 0)
	for rows.Next() {
		var change syncLogChange
		if err := rows.Scan(
			&change.revision, &change.ordinal, &change.mutationID, &change.table,
			&change.rowID, &change.operation, &change.oldValue, &change.newValue,
		); err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return changes, nil
}

func syncValueMatches(value json.RawMessage, definition manifest.SyncDefinition, args map[string]json.RawMessage) bool {
	if len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return false
	}
	if len(definition.EqualFilters) == 0 {
		return true
	}
	row := map[string]json.RawMessage{}
	if json.Unmarshal(value, &row) != nil {
		return false
	}
	for _, column := range definition.ExcludeWhenSet {
		raw, ok := row[column]
		if ok && len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return false
		}
	}
	for column, argument := range definition.EqualFilters {
		left, leftOK := row[column]
		right, rightOK := args[argument]
		if !leftOK || !rightOK || !jsonScalarEqual(left, right) {
			return false
		}
	}
	return true
}

func jsonScalarEqual(left, right json.RawMessage) bool {
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return bytes.Equal(leftJSON, rightJSON)
}

func sortedSyncRows(values map[string]json.RawMessage) []json.RawMessage {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]json.RawMessage, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, values[key])
	}
	return rows
}

func sortedSyncKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func subscriptionCurrent(subscription *syncSubscription) bool {
	subscription.conn.mu.Lock()
	defer subscription.conn.mu.Unlock()
	return subscription.conn.syncs[subscription.id] == subscription
}

func (c *wsConn) closeSync(id string) {
	c.mu.Lock()
	subscription := c.syncs[id]
	delete(c.syncs, id)
	c.mu.Unlock()
	if subscription != nil {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseSyncListener(subscription)
	}
}

func (c *wsConn) removeSync(id string) {
	c.mu.Lock()
	delete(c.syncs, id)
	c.mu.Unlock()
}

func (c *wsConn) attachSync(subscription *syncSubscription) {
	c.server.subscriptions.listeners.acquire(subscription.project, subscription.tenant)
	subscription.mu.Lock()
	subscription.listenerAcquired = true
	subscription.mu.Unlock()
	c.mu.Lock()
	c.syncs[subscription.id] = subscription
	c.mu.Unlock()
}

func (c *wsConn) closeAllSyncs() {
	c.mu.Lock()
	subscriptions := c.syncs
	c.syncs = map[string]*syncSubscription{}
	c.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseSyncListener(subscription)
	}
}

func (c *wsConn) resetSyncSubscriptions(reason string) {
	c.mu.Lock()
	subscriptions := c.syncs
	c.syncs = map[string]*syncSubscription{}
	c.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseSyncListener(subscription)
		c.write(serverMessage{Type: "sync.reset", ID: subscription.id, Path: subscription.path, Reason: reason})
	}
}

func (s *Server) notifySyncRevision(project, tenant string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project && connection.tenant == tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		subscriptions := make([]*syncSubscription, 0, len(connection.syncs))
		for _, subscription := range connection.syncs {
			subscriptions = append(subscriptions, subscription)
		}
		connection.mu.Unlock()
		for _, subscription := range subscriptions {
			go func(subscription *syncSubscription) {
				if err := s.deliverSync(subscription); err != nil && subscriptionCurrent(subscription) {
					subscription.conn.write(serverMessage{Type: "sync.error", ID: subscription.id, Path: subscription.path, Error: err.Error()})
				}
			}(subscription)
		}
	}
}

func (s *Server) resetSyncsForVisibilityChange(change tableChange) {
	changedTables := tableChangeTables(change)
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == change.project && connection.tenant == change.tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		reset := make([]*syncSubscription, 0)
		for id, subscription := range connection.syncs {
			if intersectsStrings(subscription.definition.VisibilityTables, changedTables) {
				delete(connection.syncs, id)
				reset = append(reset, subscription)
			}
		}
		connection.mu.Unlock()
		for _, subscription := range reset {
			subscription.mu.Lock()
			subscription.closed = true
			subscription.mu.Unlock()
			connection.releaseSyncListener(subscription)
			connection.write(serverMessage{Type: "sync.reset", ID: subscription.id, Path: subscription.path, Reason: "visibility-changed"})
		}
	}
}

func (c *wsConn) releaseSyncListener(subscription *syncSubscription) {
	subscription.mu.Lock()
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	subscription.mu.Unlock()
	if acquired {
		c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
}
