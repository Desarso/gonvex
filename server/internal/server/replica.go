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
	"github.com/google/uuid"
)

type replicaCursor struct {
	Epoch    string `json:"epoch"`
	Revision uint64 `json:"revision"`
}

type replicaClock struct {
	DatabaseEpoch    string
	Revision         uint64
	RetainedRevision uint64
}

type commandIDContextKey struct{}

func withCommandID(ctx context.Context, commandID string) context.Context {
	return context.WithValue(ctx, commandIDContextKey{}, strings.TrimSpace(commandID))
}

func commandIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(commandIDContextKey{}).(string)
	return value
}

type replicaSubscription struct {
	conn             *wsConn
	id               string
	path             string
	project          string
	tenant           string
	args             json.RawMessage
	definition       manifest.ReplicaCollectionDefinition
	cursor           replicaCursor
	listenerAcquired bool
	visibleKeys      map[string]bool
	visibleHashes    map[string]string
	clientDigest     string
	fullIntegrity    bool
	truncated        bool
	// verified means visibleKeys/visibleHashes were derived from an
	// authoritative handler result on this server connection, rather than
	// accepted from an untrusted persisted client cache.
	verified        bool
	mu              sync.Mutex
	closed          bool
	deliveryRunning bool
	deliveryPending bool
}

type replicaLogChange struct {
	revision        uint64
	ordinal         int
	originCommandID string
	table           string
	rowID           string
	operation       string
	changedColumns  []string
	oldValue        json.RawMessage
	newValue        json.RawMessage
}

const (
	defaultSyncRetention   = 7 * 24 * time.Hour
	syncPruneInterval      = time.Hour
	replicaDeliveryTimeout = 30 * time.Second
	// Included in every cursor epoch. Bump whenever the meaning of a resumable
	// cursor becomes stricter so persisted clients are forced through a fresh
	// snapshot instead of inheriting an older, weaker freshness guarantee.
	replicaCursorSemanticsVersion = 3
)

var errReplicaCursorExpired = errors.New("replica cursor is older than the retained change log")

func manifestReplicaCollectionDefinitions(current manifest.Manifest) map[string]manifest.ReplicaCollectionDefinition {
	definitions := map[string]manifest.ReplicaCollectionDefinition{}
	for _, entry := range current.Functions {
		if entry.Delivery != manifest.DeliveryReplica || entry.Replica == nil {
			continue
		}
		definition := replicaCollectionDefinitionWithVisibility(current, entry)
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

func effectiveReplicaCollectionDefinition(entry manifest.FunctionEntry) manifest.ReplicaCollectionDefinition {
	definition := *entry.Replica
	definition.VisibilityTables = appendUniqueStrings(nil, definition.VisibilityTables...)
	return definition
}

func replicaCollectionDefinitionWithVisibility(current manifest.Manifest, entry manifest.FunctionEntry) manifest.ReplicaCollectionDefinition {
	definition := effectiveReplicaCollectionDefinition(entry)
	if plan, ok := current.Visibility[definition.Table]; ok {
		definition.VisibilityTables = appendUniqueStrings(definition.VisibilityTables, visibilityPlanDependencies(plan)...)
		payload, _ := json.Marshal(plan)
		sum := sha256.Sum256(payload)
		definition.VisibilityPlanHash = hex.EncodeToString(sum[:])
	}
	return definition
}

func syncDefinitionsForSchema(definitions map[string]manifest.ReplicaCollectionDefinition, current manifest.Schema) (map[string]manifest.ReplicaCollectionDefinition, error) {
	filtered := map[string]manifest.ReplicaCollectionDefinition{}
	for table, definition := range definitions {
		if _, ok := current.Tables[table]; ok {
			filtered[table] = definition
		}
	}
	sourceDefinitions := make([]manifest.ReplicaCollectionDefinition, 0, len(filtered))
	for _, definition := range filtered {
		sourceDefinitions = append(sourceDefinitions, definition)
	}
	// Visibility dependencies affect the authorized or computed result without
	// necessarily changing the source row. They therefore need durable log
	// triggers too; an in-memory NOTIFY can protect connected clients but cannot
	// prove freshness after an offline reconnect.
	for _, definition := range sourceDefinitions {
		for _, tableName := range definition.VisibilityTables {
			if _, exists := filtered[tableName]; exists {
				continue
			}
			table, exists := current.Tables[tableName]
			if !exists {
				continue
			}
			key := ""
			for columnName, column := range table.Columns {
				if column.PrimaryKey && (key == "" || columnName < key) {
					key = columnName
				}
			}
			if key == "" {
				return nil, fmt.Errorf("sync visibility dependency %q has no primary key for durable invalidation", tableName)
			}
			filtered[tableName] = manifest.ReplicaCollectionDefinition{
				Table:   tableName,
				Key:     key,
				Columns: []string{key},
			}
		}
	}
	return filtered, nil
}

func (s *Server) installTenantSyncLog(ctx context.Context, project, databaseURL string, current manifest.Schema) error {
	if len(current.Tables) == 0 {
		return nil
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = schemasync.InstallSyncLog(ctx, db, current, nil)
	return err
}

func (c *wsConn) openReplica(ctx context.Context, message clientMessage) {
	started := time.Now()
	databaseURL := c.server.databaseURLForTenant(c.project, c.tenant)
	c.server.scheduleSyncLogPrune(c.project, c.tenant, databaseURL)
	clock, err := currentReplicaClock(ctx, databaseURL)
	if err != nil {
		c.server.metrics.recordOperationalLog(c.replicaProtocolLog(message, "open", 0, time.Since(started), err), time.Now().UTC())
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	c.openReplicaWithClock(ctx, message, databaseURL, clock)
}

func (c *wsConn) openReplicaMany(ctx context.Context, opens []replicaOpenRequest) {
	if len(opens) == 0 {
		return
	}
	if len(opens) > 256 {
		c.write(serverMessage{Type: "replica.error", Error: "replica batch cannot contain more than 256 opens"})
		return
	}
	databaseURL := c.server.databaseURLForTenant(c.project, c.tenant)
	c.server.scheduleSyncLogPrune(c.project, c.tenant, databaseURL)
	start, err := currentReplicaClock(ctx, databaseURL)
	if err != nil {
		for _, open := range opens {
			message := clientMessage{ID: open.ID, Path: open.Path, Args: open.Args}
			c.server.metrics.recordOperationalLog(c.replicaProtocolLog(message, "open", 0, 0, err), time.Now().UTC())
			// A batched open still represents independent subscriptions. Route the
			// storage/bootstrap failure to every id so clients can mark each
			// collection failed and activate their query fallback. A single
			// id-less error is dispatched as a system frame and leaves all callers
			// waiting forever.
			c.write(serverMessage{Type: "replica.error", ID: open.ID, Path: open.Path, Error: err.Error()})
		}
		return
	}

	for _, open := range opens {
		c.openReplicaWithClock(ctx, clientMessage{
			ID: open.ID, Path: open.Path, Args: open.Args, Cursor: open.Cursor, Keys: open.Keys, Hashes: open.Hashes,
			Digest: open.Digest, FullIntegrity: open.FullIntegrity,
		}, databaseURL, start)
	}
}

func (c *wsConn) openReplicaWithClock(
	ctx context.Context,
	message clientMessage,
	databaseURL string,
	clock replicaClock,
) {
	started := time.Now()
	phase := "open"
	resultCount := 0
	var protocolErr error
	defer func() {
		c.server.metrics.recordOperationalLog(
			c.replicaProtocolLog(message, phase, resultCount, time.Since(started), protocolErr),
			time.Now().UTC(),
		)
		// Opens are the replica equivalent of a call: without this, replica.*
		// functions show zero traffic in the dashboard's metrics view even
		// while serving every reload.
		if path := strings.TrimSpace(message.Path); path != "" {
			c.server.metrics.recordFunction(c.project, path, "replica", time.Since(started), protocolErr)
		}
	}()
	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(message.Path) == "" {
		protocolErr = errors.New("replica id and path are required")
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	current := c.server.runtime.ManifestForProject(c.project)
	entry, ok := current.Functions[message.Path]
	if !ok || entry.Delivery != manifest.DeliveryReplica || entry.Replica == nil {
		protocolErr = errors.New("replica function is not registered")
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	definition := replicaCollectionDefinitionWithVisibility(current, entry)
	if _, protocolErr = c.server.requiredVisibilityPlan(c.project, definition.Table); protocolErr != nil {
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	base := replicaCursorForClock(clock, definition, c.currentVisibilityScope())

	subscription := &replicaSubscription{
		conn: c, id: message.ID, path: message.Path, project: c.project, tenant: c.tenant,
		args: append(json.RawMessage(nil), message.Args...), definition: definition, cursor: base,
		visibleKeys: replicaKeySet(message.Keys), visibleHashes: cloneStringMap(message.Hashes),
		clientDigest: strings.TrimSpace(message.Digest), fullIntegrity: message.FullIntegrity,
	}
	c.closeReplica(message.ID)

	if message.Cursor != nil && message.Cursor.Epoch == base.Epoch && message.Cursor.Revision <= base.Revision {
		resumable := message.Cursor.Revision >= clock.RetainedRevision
		if resumable {
			phase = "resume"
			subscription.cursor = *message.Cursor
			if err := c.attachReplica(ctx, subscription); err != nil {
				protocolErr = err
				c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
				return
			}
			if err := c.server.deliverReplica(subscription); err != nil {
				protocolErr = err
				c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
			}
			return
		}
	}

	phase = "snapshot"
	release, admitted := c.server.acquireQueryAdmission(ctx, admissionBootstrap, c.project, c.tenant)
	if !admitted {
		protocolErr = ctx.Err()
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	result, snapshotClock, err := c.server.executeStructuredReplicaSnapshot(
		ctx, c.project, c.tenant, c.caller(), databaseURL, definition, message.Args,
	)
	release()
	if err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	// Snapshot rows and this cursor were read from one repeatable-read
	// transaction. No commit can be skipped and no pre-snapshot commit needs to
	// be replayed as a duplicate delta.
	base = replicaCursorForClock(snapshotClock, definition, c.currentVisibilityScope())
	subscription.cursor = base
	rows, truncated, err := replicaSnapshotRows(result, definition)
	if err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	resultCount = len(rows)
	subscription.visibleKeys = replicaRowsKeySet(rows, definition.Key)
	subscription.visibleHashes = replicaRowsHashes(rows, definition.Key)
	subscription.truncated = truncated
	subscription.verified = true
	if err := c.attachReplica(ctx, subscription); err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	c.write(serverMessage{
		Type: "replica.snapshot", ID: message.ID, Path: message.Path, Result: rows, Cursor: &base,
		Key: definition.Key, OrderBy: definition.OrderBy, OrderDirection: definition.OrderDirection,
		Mode: definition.Mode, MaxRows: definition.MaxRows, MaxBytes: definition.MaxBytes,
		Truncated: replicaTruncatedField(truncated),
	})
	if err := c.server.deliverReplica(subscription); err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "replica.error", ID: message.ID, Path: message.Path, Error: err.Error()})
	}
	return
}

func (s *Server) executeStructuredReplicaSnapshot(
	ctx context.Context,
	project, tenant string,
	caller callerContext,
	databaseURL string,
	definition manifest.ReplicaCollectionDefinition,
	rawArgs json.RawMessage,
) (any, replicaClock, error) {
	plan, err := s.requiredVisibilityPlan(project, definition.Table)
	if err != nil {
		return nil, replicaClock{}, err
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, replicaClock{}, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, replicaClock{}, err
	}
	defer tx.Rollback()
	resolved, err := loadVisibilityContextFrom(ctx, tx, project, tenant, caller, plan)
	if err != nil {
		return nil, replicaClock{}, err
	}
	result, err := s.executeStructuredReplicaQueryWith(ctx, project, tenant, definition, rawArgs, plan, resolved, tx)
	if err != nil {
		return nil, replicaClock{}, err
	}
	var clock replicaClock
	if err := tx.QueryRowContext(
		ctx,
		`SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = true`,
	).Scan(&clock.DatabaseEpoch, &clock.Revision, &clock.RetainedRevision); err != nil {
		return nil, replicaClock{}, fmt.Errorf("read snapshot clock: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, replicaClock{}, err
	}
	return result, clock, nil
}

func (c *wsConn) replicaProtocolLog(message clientMessage, phase string, resultCount int, duration time.Duration, protocolErr error) runtimeLogEntry {
	completed := time.Now().UTC()
	c.mu.Lock()
	project, tenant, connectionID, device := c.project, c.tenant, c.id, c.device
	accountID, accountEmail := "", ""
	if c.user != nil {
		accountID, accountEmail = c.user.ID, c.user.Email
	}
	c.mu.Unlock()
	outcome, errorMessage := "ok", ""
	if protocolErr != nil {
		outcome, errorMessage = "error", protocolErr.Error()
	}
	var resultCountValue *int
	if phase == "snapshot" && protocolErr == nil {
		resultCountValue = &resultCount
	}
	path := strings.TrimSpace(message.Path)
	if path == "" {
		path = "replica.open"
	}
	return runtimeLogEntry{
		Time: completed.Format(time.RFC3339Nano), CompletedAt: completed.Format(time.RFC3339Nano),
		StartedAt: completed.Add(-duration).Format(time.RFC3339Nano), ExecutionID: uuid.NewString(), OperationID: message.ID,
		Project: project, Tenant: tenant, AccountID: accountID, AccountEmail: accountEmail,
		ConnectionID: connectionID,
		Browser:      strings.TrimSpace(strings.Join([]string{device.BrowserName, device.BrowserVersion}, " ")),
		DeviceType:   device.DeviceType, Platform: device.Platform,
		Path: path, Kind: "replica", Outcome: outcome, DurationMS: float64(duration.Microseconds()) / 1000,
		Error: errorMessage, Source: "websocket", Reason: phase,
		Request: sanitizeRuntimeLogRequest(message.Args), RequestSizeBytes: len(message.Args), ResultCount: resultCountValue,
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
		if entry.Delivery != manifest.DeliveryReplica || entry.Replica == nil || entry.Replica.RetentionMilliseconds <= 0 {
			continue
		}
		candidate := time.Duration(entry.Replica.RetentionMilliseconds) * time.Millisecond
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

func replicaSnapshotRows(result any, definition manifest.ReplicaCollectionDefinition) ([]json.RawMessage, bool, error) {
	payload, err := json.Marshal(explicitNull(result))
	if err != nil {
		return nil, false, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, false, fmt.Errorf("replica handler must return an array of keyed rows")
	}
	maxRows := definition.MaxRows
	maxBytes := definition.MaxBytes
	totalBytes := int64(0)
	kept := make([]json.RawMessage, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		key := replicaRowKey(row, definition.Key)
		if key == "" {
			return nil, false, fmt.Errorf("replica row is missing key %q", definition.Key)
		}
		if seen[key] {
			return nil, false, fmt.Errorf("replica handler returned duplicate key %q", key)
		}
		seen[key] = true
		if maxRows > 0 && len(kept) >= maxRows {
			return kept, true, nil
		}
		canonical := canonicalReplicaJSON(row)
		if maxBytes > 0 && totalBytes+int64(len(canonical)) > maxBytes {
			return kept, true, nil
		}
		totalBytes += int64(len(canonical))
		kept = append(kept, row)
	}
	return kept, false, nil
}

func replicaTruncatedField(truncated bool) *bool {
	return &truncated
}

func replicaRowKey(row json.RawMessage, key string) string {
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
		switch scalar.(type) {
		case float64, bool:
			// JSON number formatting intentionally matches JavaScript's
			// String(number) boundaries (not fmt.Sprint's scientific-notation
			// threshold), and normalizeReplicaJSON makes -0 become "0".
			return string(canonicalReplicaJSON(object[key]))
		}
	}
	return ""
}

func currentReplicaClock(ctx context.Context, databaseURL string) (replicaClock, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return replicaClock{}, err
	}
	defer db.Close()
	var clock replicaClock
	if err := db.QueryRowContext(
		ctx,
		`SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = true`,
	).Scan(&clock.DatabaseEpoch, &clock.Revision, &clock.RetainedRevision); err != nil {
		return replicaClock{}, fmt.Errorf("sync storage is not installed: %w", err)
	}
	return clock, nil
}

// replicaCursorForClock derives the cursor epoch from the database epoch, the
// replica definition, and the caller's visibility scope (project/tenant/user/
// permissions). The module-generation epoch is deliberately excluded: a resumed
// cursor is never trusted blindly — deliverAuthoritativeReplica re-runs the query
// and repairs drift via deltas — so cursors may safely survive deploys.
func replicaCursorForClock(clock replicaClock, definition manifest.ReplicaCollectionDefinition, visibilityScope string) replicaCursor {
	payload, _ := json.Marshal(struct {
		Semantics     int                                  `json:"semantics"`
		DatabaseEpoch string                               `json:"databaseEpoch"`
		Definition    manifest.ReplicaCollectionDefinition `json:"definition"`
		Scope         string                               `json:"scope"`
	}{replicaCursorSemanticsVersion, clock.DatabaseEpoch, definition, visibilityScope})
	hash := sha256.Sum256(payload)
	return replicaCursor{Epoch: hex.EncodeToString(hash[:16]), Revision: clock.Revision}
}

func currentReplicaCursor(ctx context.Context, databaseURL string, definition manifest.ReplicaCollectionDefinition, visibilityScope string) (replicaCursor, error) {
	clock, err := currentReplicaClock(ctx, databaseURL)
	if err != nil {
		return replicaCursor{}, err
	}
	return replicaCursorForClock(clock, definition, visibilityScope), nil
}

func (s *Server) deliverReplica(subscription *replicaSubscription) error {
	ctx, cancel := context.WithTimeout(context.Background(), replicaDeliveryTimeout)
	defer cancel()

	// Authentication invalidation resets every replica on the connection and takes
	// each subscription lock. Revalidate before taking this subscription lock
	// to avoid self-deadlocking on an expired or revoked session.
	if err := subscription.conn.revalidateAppAuth(ctx); err != nil {
		subscription.conn.clearAuthentication()
		return nil
	}

	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || !subscriptionCurrent(subscription) {
		return nil
	}
	visibilityPlan, planErr := s.requiredVisibilityPlan(subscription.project, subscription.definition.Table)
	if planErr != nil {
		s.failReplicaWhileLocked(subscription, planErr)
		return nil
	}
	databaseURL := s.databaseURLForTenant(subscription.project, subscription.tenant)
	latest, err := currentReplicaCursor(ctx, databaseURL, subscription.definition, subscription.conn.currentVisibilityScope())
	if err != nil {
		return err
	}
	if latest.Epoch != subscription.cursor.Epoch {
		s.resetReplicaWhileLocked(subscription, "definition-changed")
		return nil
	}
	if latest.Revision <= subscription.cursor.Revision {
		if !subscription.verified {
			settled, err := s.deliverAuthoritativeReplica(ctx, subscription, nil, latest)
			if err != nil {
				return err
			}
			if !settled {
				return nil
			}
		}
		s.writeReplicaReady(subscription, replicaReadyServerMessage(subscription, latest))
		return nil
	}
	changes, err := readReplicaChanges(
		ctx,
		databaseURL,
		subscription.cursor.Revision,
		latest.Revision,
		append([]string{subscription.definition.Table}, subscription.definition.VisibilityTables...)...,
	)
	if err != nil {
		if errors.Is(err, errReplicaCursorExpired) {
			s.resetReplicaWhileLocked(subscription, "cursor-expired")
			return nil
		}
		return err
	}
	s.invalidateVisibilityContexts(subscription.project, subscription.tenant, replicaLogChangeTables(changes))
	args := map[string]json.RawMessage{}
	_ = json.Unmarshal(subscription.args, &args)
	visibilityContext, err := s.resolveVisibilityContext(
		ctx, subscription.project, subscription.tenant, subscription.conn.caller(), visibilityPlan, 0,
	)
	if err != nil {
		return err
	}
	if len(changes) == 0 && subscription.verified {
		subscription.cursor = latest
		s.writeReplicaReady(subscription, replicaReadyServerMessage(subscription, latest))
		return nil
	}
	authoritativeReconcile := replicaNeedsAuthoritativeReconcile(subscription.definition, changes)
	if subscription.verified && authoritativeReconcile {
		subscription.conn.write(serverMessage{
			Type: "replica.syncing", ID: subscription.id, Path: subscription.path, Reason: "reconciling",
		})
	}
	if !subscription.verified {
		settled, err := s.deliverAuthoritativeReplica(ctx, subscription, changes, latest)
		if err != nil {
			return err
		}
		if !settled {
			return nil
		}
		s.writeReplicaReady(subscription, replicaReadyServerMessage(subscription, subscription.cursor))
		return nil
	}
	if authoritativeReconcile {
		settled, err := s.deliverAuthoritativeReplica(ctx, subscription, changes, latest)
		if err != nil {
			return err
		}
		if !settled {
			return nil
		}
		s.writeReplicaReady(subscription, replicaReadyServerMessage(subscription, subscription.cursor))
		return nil
	}
	for _, batch := range groupReplicaChanges(changes) {
		upserts := map[string]json.RawMessage{}
		deleted := map[string]bool{}
		originCommandIDs := map[string]bool{}
		for _, change := range batch.changes {
			newMatches := replicaValueMatches(change.newValue, subscription.definition, args)
			newMatches = newMatches && visibilityRawRowMatches(change.newValue, visibilityPlan, visibilityContext)
			switch {
			case newMatches:
				upserts[change.rowID] = change.newValue
				delete(deleted, change.rowID)
				subscription.visibleKeys[change.rowID] = true
				subscription.visibleHashes[change.rowID] = replicaRowHash(change.newValue)
			case subscription.visibleKeys[change.rowID]:
				delete(upserts, change.rowID)
				deleted[change.rowID] = true
				delete(subscription.visibleKeys, change.rowID)
				delete(subscription.visibleHashes, change.rowID)
			}
			if change.originCommandID != "" {
				originCommandIDs[change.originCommandID] = true
			}
		}
		cursor := replicaCursor{Epoch: latest.Epoch, Revision: batch.revision}
		subscription.conn.write(serverMessage{
			Type: "replica.delta", ID: subscription.id, Path: subscription.path, Cursor: &cursor,
			Upserts: sortedReplicaRows(upserts), Deleted: sortedReplicaKeys(deleted), OriginCommandIDs: sortedReplicaKeys(originCommandIDs),
			Digest: replicaHashesDigest(subscription.visibleHashes),
		})
		subscription.cursor = cursor
	}
	if subscription.cursor.Revision < latest.Revision {
		subscription.cursor.Revision = latest.Revision
	}
	s.writeReplicaReady(subscription, replicaReadyServerMessage(subscription, subscription.cursor))
	return nil
}

func replicaReadyServerMessage(subscription *replicaSubscription, cursor replicaCursor) serverMessage {
	return serverMessage{
		Type: "replica.ready", ID: subscription.id, Path: subscription.path, Cursor: &cursor,
		Mode: subscription.definition.Mode, Digest: replicaHashesDigest(subscription.visibleHashes),
		Truncated: replicaTruncatedField(subscription.truncated),
	}
}

func (s *Server) writeReplicaReady(subscription *replicaSubscription, message serverMessage) bool {
	_, ready := s.subscriptions.listeners.whileConnected(
		subscription.project,
		subscription.tenant,
		func() { subscription.conn.writeReplicaReady(message) },
	)
	return ready
}

func (s *Server) resetReplicaWhileLocked(subscription *replicaSubscription, reason string) {
	subscription.conn.removeReplica(subscription.id)
	subscription.closed = true
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	if acquired {
		s.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
	subscription.conn.write(serverMessage{
		Type: "replica.reset", ID: subscription.id, Path: subscription.path, Reason: reason,
	})
}

// failReplicaWhileLocked removes a subscription whose authorization contract
// can no longer be proven. It must not leave an old verified row set attached
// while a visibility plan is missing or malformed.
func (s *Server) failReplicaWhileLocked(subscription *replicaSubscription, err error) {
	subscription.conn.removeReplica(subscription.id)
	subscription.closed = true
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	if acquired {
		s.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
	subscription.conn.write(serverMessage{
		Type: "replica.error", ID: subscription.id, Path: subscription.path, Error: err.Error(),
	})
}

func (s *Server) deliverAuthoritativeReplica(
	ctx context.Context,
	subscription *replicaSubscription,
	changes []replicaLogChange,
	latest replicaCursor,
) (bool, error) {
	release, admitted := s.acquireQueryAdmission(ctx, admissionReactive, subscription.project, subscription.tenant)
	if !admitted {
		return false, ctx.Err()
	}
	result, err := s.executeStructuredReplicaQuery(
		ctx, subscription.project, subscription.tenant, subscription.conn.caller(), subscription.definition, subscription.args,
	)
	release()
	if err != nil {
		return false, err
	}
	rows, truncated, err := replicaSnapshotRows(result, subscription.definition)
	if err != nil {
		return false, err
	}
	subscription.truncated = truncated
	currentRows := map[string]json.RawMessage{}
	currentKeys := map[string]bool{}
	for _, row := range rows {
		key := replicaRowKey(row, subscription.definition.Key)
		if key == "" {
			continue
		}
		currentRows[key] = row
		currentKeys[key] = true
	}
	currentHashes := replicaRowsHashes(rows, subscription.definition.Key)
	currentDigest := replicaHashesDigest(currentHashes)
	if !subscription.verified && subscription.clientDigest != "" && !subscription.fullIntegrity {
		if subscription.clientDigest != currentDigest {
			s.pauseReplicaForIntegrityWhileLocked(subscription)
			return false, nil
		}
		// The compact resume proof matched. No row identifiers or hashes need
		// to cross the wire on the overwhelmingly common unchanged path.
		subscription.visibleKeys = currentKeys
		subscription.visibleHashes = currentHashes
		subscription.verified = true
		subscription.cursor = latest
		return true, nil
	}

	upserts, deleted, originCommandIDs := progressiveReplicaDiff(
		currentRows,
		currentHashes,
		subscription.visibleKeys,
		subscription.visibleHashes,
		changes,
	)
	subscription.visibleKeys = currentKeys
	subscription.visibleHashes = currentHashes
	subscription.verified = true
	subscription.cursor = latest
	if len(upserts) > 0 || len(deleted) > 0 || len(originCommandIDs) > 0 {
		subscription.conn.write(serverMessage{
			Type: "replica.delta", ID: subscription.id, Path: subscription.path, Cursor: &latest,
			Upserts: sortedReplicaRows(upserts), Deleted: sortedReplicaKeys(deleted), OriginCommandIDs: sortedReplicaKeys(originCommandIDs),
			Digest: currentDigest,
		})
	}
	return true, nil
}

func (s *Server) pauseReplicaForIntegrityWhileLocked(subscription *replicaSubscription) {
	subscription.conn.removeReplica(subscription.id)
	subscription.closed = true
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	if acquired {
		s.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
	subscription.conn.write(serverMessage{
		Type: "replica.needHashes", ID: subscription.id, Path: subscription.path,
	})
}

func progressiveReplicaDiff(
	currentRows map[string]json.RawMessage,
	currentHashes map[string]string,
	visibleKeys map[string]bool,
	visibleHashes map[string]string,
	changes []replicaLogChange,
) (map[string]json.RawMessage, map[string]bool, map[string]bool) {
	upserts := map[string]json.RawMessage{}
	deleted := map[string]bool{}
	originCommandIDs := map[string]bool{}
	for key, row := range currentRows {
		if !visibleKeys[key] || visibleHashes[key] != currentHashes[key] {
			upserts[key] = row
		}
	}
	for key := range visibleKeys {
		if _, exists := currentRows[key]; !exists {
			deleted[key] = true
		}
	}
	for _, change := range changes {
		if change.originCommandID != "" {
			originCommandIDs[change.originCommandID] = true
		}
	}
	return upserts, deleted, originCommandIDs
}

func replicaRowsHashes(rows []json.RawMessage, keyField string) map[string]string {
	hashes := make(map[string]string, len(rows))
	for _, row := range rows {
		key := replicaRowKey(row, keyField)
		if key == "" {
			continue
		}
		hashes[key] = replicaRowHash(row)
	}
	return hashes
}

func replicaRowHash(row json.RawMessage) string {
	sum := sha256.Sum256(canonicalReplicaJSON(row))
	return hex.EncodeToString(sum[:])
}

// canonicalReplicaJSON matches the client's stable JSON encoder: recursively
// sorted object keys, JavaScript-compatible JSON numbers, no HTML escaping,
// and escaped U+2028/U+2029. Decoding through float64 intentionally mirrors
// the JsonValue representation a browser obtains from JSON.parse.
func canonicalReplicaJSON(value json.RawMessage) []byte {
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return value
	}
	decoded = normalizeReplicaJSON(decoded)
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(decoded); err != nil {
		return value
	}
	return bytes.TrimSpace(output.Bytes())
}

func normalizeReplicaJSON(value any) any {
	switch current := value.(type) {
	case float64:
		// JSON.stringify(-0) is "0" while encoding/json preserves "-0".
		if current == 0 {
			return float64(0)
		}
		return current
	case []any:
		for index := range current {
			current[index] = normalizeReplicaJSON(current[index])
		}
		return current
	case map[string]any:
		for key, nested := range current {
			current[key] = normalizeReplicaJSON(nested)
		}
		return current
	default:
		return current
	}
}

func replicaHashesDigest(hashes map[string]string) string {
	keys := make([]string, 0, len(hashes))
	for key := range hashes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]string{key, hashes[key]})
	}
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(pairs)
	sum := sha256.Sum256(bytes.TrimSpace(payload.Bytes()))
	return hex.EncodeToString(sum[:])
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		if strings.TrimSpace(key) != "" && strings.TrimSpace(value) != "" {
			result[key] = value
		}
	}
	return result
}

func replicaKeySet(keys []string) map[string]bool {
	result := map[string]bool{}
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			result[key] = true
		}
	}
	return result
}

func replicaRowsKeySet(rows []json.RawMessage, keyField string) map[string]bool {
	result := map[string]bool{}
	for _, row := range rows {
		if key := replicaRowKey(row, keyField); key != "" {
			result[key] = true
		}
	}
	return result
}

type replicaChangeBatch struct {
	revision uint64
	changes  []replicaLogChange
}

// routeReplicaTransaction projects one committed Postgres transaction through
// the Replica Collections already authorized on each connection. It emits one
// atomic client frame regardless of how many tables/collections changed.
func (s *Server) routeReplicaTransaction(project, tenant, databaseEpoch string, batch replicaChangeBatch) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project && connection.tenant == tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		caller := connection.caller()
		visibilityContexts := map[string]*resolvedVisibilityContext{}
		visibilityPlans := map[string]manifest.VisibilityPlan{}
		connection.mu.Lock()
		subscriptions := make([]*replicaSubscription, 0, len(connection.replicas))
		for _, subscription := range connection.replicas {
			subscriptions = append(subscriptions, subscription)
		}
		connection.mu.Unlock()
		if len(subscriptions) == 0 {
			continue
		}
		changes := make([]replicaChangeMessage, 0, len(batch.changes))
		originCommandID := ""
		visibilityPlanErrors := map[string]error{}
		for _, committed := range batch.changes {
			interested := false
			for _, subscription := range subscriptions {
				if subscription.definition.Table == committed.table {
					interested = true
					break
				}
			}
			if !interested {
				continue
			}
			oldVisible, newVisible := false, false
			plan, planned := visibilityPlans[committed.table]
			if !planned {
				planErr, failed := visibilityPlanErrors[committed.table]
				if failed {
					for _, subscription := range subscriptions {
						if subscription.definition.Table != committed.table {
							continue
						}
						subscription.mu.Lock()
						if !subscription.closed {
							s.failReplicaWhileLocked(subscription, planErr)
						}
						subscription.mu.Unlock()
					}
					continue
				}
				var err error
				plan, err = s.requiredVisibilityPlan(project, committed.table)
				if err != nil {
					visibilityPlanErrors[committed.table] = err
					for _, subscription := range subscriptions {
						if subscription.definition.Table != committed.table {
							continue
						}
						subscription.mu.Lock()
						if !subscription.closed {
							s.failReplicaWhileLocked(subscription, err)
						}
						subscription.mu.Unlock()
					}
					continue
				}
				planned = true
				visibilityPlans[committed.table] = plan
				ctx, cancel := context.WithTimeout(context.Background(), replicaDeliveryTimeout)
				// The listener invalidates affected contexts before routing this
				// committed batch. Requiring the batch revision also prevents a
				// delayed LISTEN delivery from reusing a pre-commit context.
				resolved, resolveErr := s.resolveVisibilityContext(ctx, project, tenant, caller, plan, batch.revision)
				cancel()
				if resolveErr == nil {
					visibilityContexts[committed.table] = resolved
				}
			}
			for _, subscription := range subscriptions {
				if subscription.definition.Table != committed.table {
					continue
				}
				subscription.mu.Lock()
				if !subscription.closed && subscription.verified {
					args := map[string]json.RawMessage{}
					_ = json.Unmarshal(subscription.args, &args)
					resolved := visibilityContexts[committed.table]
					oldMatches := replicaValueMatches(committed.oldValue, subscription.definition, args)
					oldMatches = oldMatches && resolved != nil && visibilityRawRowMatches(committed.oldValue, plan, resolved)
					// Only emit an access-removal delete for rows this verified
					// collection actually contained. The independent old-row
					// evaluation proves the old visibility transition; visibleKeys
					// proves client membership for bounded collections.
					oldVisible = oldVisible || (subscription.visibleKeys[committed.rowID] && oldMatches)
					newMatches := replicaValueMatches(committed.newValue, subscription.definition, args)
					newMatches = newMatches && resolved != nil && visibilityRawRowMatches(committed.newValue, plan, resolved)
					newVisible = newVisible || newMatches
				}
				subscription.mu.Unlock()
			}
			if originCommandID == "" {
				originCommandID = strings.TrimSpace(committed.originCommandID)
			}
			operation, visible := visibilityTransitionOperation(oldVisible, newVisible)
			if !visible {
				continue
			}
			change := replicaChangeMessage{
				Entity: committed.table, ID: committed.rowID, Operation: operation,
				ChangedColumns: append([]string(nil), committed.changedColumns...),
			}
			if oldVisible {
				change.OldValue = committed.oldValue
			}
			if newVisible {
				change.NewValue = committed.newValue
			}
			changes = append(changes, change)
		}
		connection.write(serverMessage{
			Type:            "replica.transaction",
			Cursor:          &replicaCursor{Epoch: databaseEpoch, Revision: batch.revision},
			OriginCommandID: originCommandID,
			Changes:         changes,
		})
	}
}

func replicaBatchTables(batch replicaChangeBatch) []string {
	tables := make([]string, 0, len(batch.changes))
	for _, change := range batch.changes {
		tables = append(tables, change.table)
	}
	return appendUniqueStrings(nil, tables...)
}

func replicaLogChangeTables(changes []replicaLogChange) []string {
	tables := make([]string, 0, len(changes))
	for _, change := range changes {
		tables = append(tables, change.table)
	}
	return appendUniqueStrings(nil, tables...)
}

func groupReplicaChanges(changes []replicaLogChange) []replicaChangeBatch {
	batches := make([]replicaChangeBatch, 0)
	for _, change := range changes {
		if len(batches) == 0 || batches[len(batches)-1].revision != change.revision {
			batches = append(batches, replicaChangeBatch{revision: change.revision})
		}
		batches[len(batches)-1].changes = append(batches[len(batches)-1].changes, change)
	}
	return batches
}

func readReplicaChanges(ctx context.Context, databaseURL string, after, through uint64, tables ...string) ([]replicaLogChange, error) {
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
		return nil, errReplicaCursorExpired
	}
	tables = appendUniqueStrings(nil, tables...)
	queryArgs := []any{after, through}
	placeholders := make([]string, 0, len(tables))
	for _, table := range tables {
		queryArgs = append(queryArgs, table)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(queryArgs)))
	}
	tablePredicate := ""
	if len(placeholders) > 0 {
		tablePredicate = " AND table_name IN (" + strings.Join(placeholders, ", ") + ")"
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT revision, ordinal, COALESCE(command_id, ''), table_name, row_id, operation,
		       COALESCE(changed_columns, ARRAY[]::text[]),
		       COALESCE(old_value, 'null'::jsonb), COALESCE(new_value, 'null'::jsonb)
		FROM _gonvex_sync_changes
		WHERE revision > $1 AND revision <= $2`+tablePredicate+`
		ORDER BY revision, ordinal
	`, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	changes := make([]replicaLogChange, 0)
	for rows.Next() {
		var change replicaLogChange
		if err := rows.Scan(
			&change.revision, &change.ordinal, &change.originCommandID, &change.table,
			&change.rowID, &change.operation, &change.changedColumns, &change.oldValue, &change.newValue,
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

func replicaVisibilityChanged(changes []replicaLogChange, sourceTable string) bool {
	for _, change := range changes {
		if change.table != sourceTable {
			return true
		}
	}
	return false
}

func replicaNeedsAuthoritativeReconcile(definition manifest.ReplicaCollectionDefinition, changes []replicaLogChange) bool {
	return definition.Mode == "progressive" ||
		definition.MaxRows > 0 ||
		definition.MaxBytes > 0 ||
		replicaVisibilityChanged(changes, definition.Table)
}

func replicaValueMatches(value json.RawMessage, definition manifest.ReplicaCollectionDefinition, args map[string]json.RawMessage) bool {
	if len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return false
	}
	if len(definition.EqualFilters) == 0 && len(definition.ExcludeWhenSet) == 0 {
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

func sortedReplicaRows(values map[string]json.RawMessage) []json.RawMessage {
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

func sortedReplicaKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func subscriptionCurrent(subscription *replicaSubscription) bool {
	subscription.conn.mu.Lock()
	defer subscription.conn.mu.Unlock()
	return subscription.conn.replicas[subscription.id] == subscription
}

func (c *wsConn) closeReplica(id string) {
	c.mu.Lock()
	subscription := c.replicas[id]
	delete(c.replicas, id)
	c.resolvePendingWatermarksLocked(id, ^uint64(0))
	c.mu.Unlock()
	if subscription != nil {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseReplicaListener(subscription)
	}
}

func (c *wsConn) removeReplica(id string) {
	c.mu.Lock()
	delete(c.replicas, id)
	c.mu.Unlock()
}

func (c *wsConn) attachReplica(ctx context.Context, subscription *replicaSubscription) error {
	ready := c.server.subscriptions.listeners.acquire(subscription.project, subscription.tenant)
	if ready == nil {
		return errors.New("replica listener unavailable; freshness cannot be proven")
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ready:
		case <-ctx.Done():
			c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
			return ctx.Err()
		case <-timer.C:
			c.server.subscriptions.listeners.markNeedsRecovery(subscription.project, subscription.tenant)
			c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
			return errors.New("replica listener did not become ready; freshness cannot be proven")
		}
		next, attached := c.server.subscriptions.listeners.whileConnected(
			subscription.project,
			subscription.tenant,
			func() {
				// The subscription is not published yet, so these writes are
				// private to this goroutine and need no subscription lock.
				subscription.listenerAcquired = true
				c.mu.Lock()
				c.replicas[subscription.id] = subscription
				c.mu.Unlock()
			},
		)
		if attached {
			return nil
		}
		if next == nil {
			c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
			return errors.New("replica listener disappeared; freshness cannot be proven")
		}
		ready = next
	}
}

func (c *wsConn) closeAllReplicas() {
	c.mu.Lock()
	subscriptions := c.replicas
	c.replicas = map[string]*replicaSubscription{}
	c.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseReplicaListener(subscription)
	}
}

func (c *wsConn) resetReplicaSubscriptions(reason string) {
	c.mu.Lock()
	subscriptions := c.replicas
	c.replicas = map[string]*replicaSubscription{}
	c.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseReplicaListener(subscription)
		c.write(serverMessage{Type: "replica.reset", ID: subscription.id, Path: subscription.path, Reason: reason})
	}
}

func (s *Server) notifySyncRevision(
	project string,
	tenant string,
	changedTables []string,
	notifiedDatabaseEpoch string,
	notifiedRevision uint64,
) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project && connection.tenant == tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	if len(changedTables) == 0 || strings.TrimSpace(notifiedDatabaseEpoch) == "" || notifiedRevision == 0 {
		s.scheduleAllReplicaDeliveries(connections)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), replicaDeliveryTimeout)
	clock, err := currentReplicaClock(ctx, s.databaseURLForTenant(project, tenant))
	cancel()
	if err != nil || clock.DatabaseEpoch != notifiedDatabaseEpoch || clock.Revision < notifiedRevision {
		// Filtering is only an optimization. If the shared clock cannot prove the
		// notification belongs to the current database epoch, replay every replica.
		s.scheduleAllReplicaDeliveries(connections)
		return
	}
	clock.Revision = notifiedRevision

	for _, connection := range connections {
		connection.mu.Lock()
		subscriptions := make([]*replicaSubscription, 0, len(connection.replicas))
		for _, subscription := range connection.replicas {
			subscriptions = append(subscriptions, subscription)
		}
		useWatermark := connection.replicaWatermark
		connection.mu.Unlock()
		waiting := make([]string, 0, len(subscriptions))
		deliveries := make([]*replicaSubscription, 0, len(subscriptions))
		advanced := false
		for _, subscription := range subscriptions {
			if replicaDefinitionIntersectsTables(subscription.definition, changedTables) {
				if useWatermark {
					waiting = append(waiting, subscription.id)
				}
				deliveries = append(deliveries, subscription)
				continue
			}
			handled, cursorAdvanced := s.advanceUnchangedReplica(subscription, clock, !useWatermark)
			advanced = advanced || cursorAdvanced
			if !handled {
				if useWatermark {
					waiting = append(waiting, subscription.id)
				}
				deliveries = append(deliveries, subscription)
			}
		}
		if useWatermark && advanced {
			s.writeReplicaWatermark(connection, clock.Revision, waiting)
		}
		for _, subscription := range deliveries {
			s.scheduleReplicaDelivery(subscription)
		}
	}
}

func (s *Server) scheduleAllReplicaDeliveries(connections []*wsConn) {
	for _, connection := range connections {
		connection.mu.Lock()
		subscriptions := make([]*replicaSubscription, 0, len(connection.replicas))
		for _, subscription := range connection.replicas {
			subscriptions = append(subscriptions, subscription)
		}
		connection.mu.Unlock()
		for _, subscription := range subscriptions {
			s.scheduleReplicaDelivery(subscription)
		}
	}
}

func replicaDefinitionIntersectsTables(definition manifest.ReplicaCollectionDefinition, changedTables []string) bool {
	return intersectsStrings(
		append([]string{definition.Table}, definition.VisibilityTables...),
		changedTables,
	)
}

func (s *Server) advanceUnchangedReplica(
	subscription *replicaSubscription,
	clock replicaClock,
	emitReady bool,
) (handled bool, advanced bool) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || !subscriptionCurrent(subscription) {
		return true, false
	}
	// A running delivery may cover an earlier relevant revision. Advancing past
	// it here would acknowledge unseen rows, so let the delivery coalescer replay
	// the full range instead.
	if !subscription.verified || subscription.deliveryRunning || subscription.deliveryPending {
		return false, false
	}
	latest := replicaCursorForClock(clock, subscription.definition, subscription.conn.currentVisibilityScope())
	if latest.Epoch != subscription.cursor.Epoch {
		return false, false
	}
	if subscription.cursor.Revision >= latest.Revision {
		return true, false
	}
	// The table set describes exactly one transaction revision. A lagging cursor
	// has a wider, unknown table range and must use the durable change log.
	if subscription.cursor.Revision+1 != latest.Revision {
		return false, false
	}
	subscription.cursor = latest
	if emitReady {
		s.writeReplicaReady(subscription, replicaReadyServerMessage(subscription, latest))
	}
	return true, true
}

func (s *Server) writeReplicaWatermark(connection *wsConn, revision uint64, waiting []string) bool {
	_, ready := s.subscriptions.listeners.whileConnected(
		connection.project,
		connection.tenant,
		func() { connection.writeReplicaWatermark(revision, waiting) },
	)
	return ready
}

func (s *Server) markTenantReplicasOutOfDate(project, tenant, reason string) {
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
		subscriptions := make([]*replicaSubscription, 0, len(connection.replicas))
		for _, subscription := range connection.replicas {
			subscriptions = append(subscriptions, subscription)
		}
		connection.mu.Unlock()
		for _, subscription := range subscriptions {
			connection.write(serverMessage{
				Type: "replica.syncing", ID: subscription.id, Path: subscription.path, Reason: reason,
			})
		}
	}
}

func beginReplicaDelivery(subscription *replicaSubscription) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		return false
	}
	if subscription.deliveryRunning {
		subscription.deliveryPending = true
		return false
	}
	subscription.deliveryRunning = true
	return true
}

func finishReplicaDelivery(subscription *replicaSubscription) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		subscription.deliveryRunning = false
		subscription.deliveryPending = false
		return false
	}
	if subscription.deliveryPending {
		subscription.deliveryPending = false
		return true
	}
	subscription.deliveryRunning = false
	return false
}

func (s *Server) scheduleReplicaDelivery(subscription *replicaSubscription) {
	if !beginReplicaDelivery(subscription) {
		return
	}
	go func() {
		for {
			if err := s.deliverReplica(subscription); err != nil && subscriptionCurrent(subscription) {
				subscription.conn.write(serverMessage{
					Type: "replica.error", ID: subscription.id, Path: subscription.path, Error: err.Error(),
				})
			}
			if !finishReplicaDelivery(subscription) {
				return
			}
		}
	}()
}

func (s *Server) resetReplicasForVisibilityChange(change tableChange) {
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
		reset := make([]*replicaSubscription, 0)
		reconcile := make([]*replicaSubscription, 0)
		for id, subscription := range connection.replicas {
			if intersectsStrings(subscription.definition.VisibilityTables, changedTables) {
				if subscription.definition.Mode == "progressive" {
					reconcile = append(reconcile, subscription)
					continue
				}
				delete(connection.replicas, id)
				reset = append(reset, subscription)
			}
		}
		connection.mu.Unlock()
		for _, subscription := range reset {
			subscription.mu.Lock()
			subscription.closed = true
			subscription.mu.Unlock()
			connection.releaseReplicaListener(subscription)
			connection.write(serverMessage{Type: "replica.reset", ID: subscription.id, Path: subscription.path, Reason: "visibility-changed"})
		}
		for _, subscription := range reconcile {
			connection.write(serverMessage{
				Type: "replica.syncing", ID: subscription.id, Path: subscription.path, Reason: "visibility-changed",
			})
			s.scheduleReplicaDelivery(subscription)
		}
	}
}

func (s *Server) resetProjectReplicaCollections(project, reason string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		reset := make([]*replicaSubscription, 0, len(connection.replicas))
		for id, subscription := range connection.replicas {
			delete(connection.replicas, id)
			reset = append(reset, subscription)
		}
		connection.mu.Unlock()
		for _, subscription := range reset {
			subscription.mu.Lock()
			subscription.closed = true
			subscription.mu.Unlock()
			connection.releaseReplicaListener(subscription)
			connection.write(serverMessage{Type: "replica.reset", ID: subscription.id, Path: subscription.path, Reason: reason})
		}
	}
}

func (c *wsConn) releaseReplicaListener(subscription *replicaSubscription) {
	subscription.mu.Lock()
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	subscription.mu.Unlock()
	if acquired {
		c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
}
