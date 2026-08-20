package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	controlidentity "github.com/gonvex/gonvex/server/internal/controlplane/identity"
)

// controlPlaneMembershipProjectionTimeout bounds the directory update that runs
// straight after a membership commit. Exceeding it is not a failure: the outbox
// row survives and the change feed drains it.
const controlPlaneMembershipProjectionTimeout = 5 * time.Second

type pendingMemberProjection struct {
	controlidentity.AccountTenantIndex
}

// projectCommittedMemberChanges derives the Control Plane tenant directory
// exclusively from committed tenant.members rows. It is deliberately outside
// reducer execution: tenant Postgres is authoritative and projection failure
// can never roll back a committed business transaction.
func (s *Server) projectCommittedMemberChanges(projectID, tenantID string, batch replicaChangeBatch) {
	for _, change := range batch.changes {
		if change.table != "members" {
			continue
		}
		projection, ok := memberProjectionFromChange(tenantID, change, batch.revision)
		if !ok {
			continue
		}
		if projection.Status != "active" {
			// Existing sockets carry both Account and Member identity, so tenant
			// revocation takes effect without waiting for directory projection.
			s.revokeAppAuthConnections(projectID, projection.AccountID)
			s.revokeAppAuthConnections(projectID, projection.MemberID)
		}
		s.startMembershipProjection(func() {
			s.retryMemberProjection(projectID, projection)
		})
	}
}

// startMembershipProjection binds asynchronous directory work to the server
// lifecycle. A projection must not outlive the runtime that owns its tenant
// database, especially during shutdown or test database cleanup.
func (s *Server) startMembershipProjection(work func()) {
	if s == nil || work == nil {
		return
	}
	s.membershipProjectorMu.Lock()
	if s.membershipProjectorClosing {
		s.membershipProjectorMu.Unlock()
		return
	}
	s.membershipProjectorWG.Add(1)
	s.membershipProjectorMu.Unlock()
	go func() {
		defer s.membershipProjectorWG.Done()
		work()
	}()
}

func memberProjectionFromChange(tenantID string, change replicaLogChange, revision uint64) (controlidentity.AccountTenantIndex, bool) {
	raw := change.newValue
	status := "active"
	if change.operation == "delete" || len(raw) == 0 || string(raw) == "null" {
		raw = change.oldValue
		status = "revoked"
	}
	row := map[string]any{}
	if len(raw) == 0 || json.Unmarshal(raw, &row) != nil {
		return controlidentity.AccountTenantIndex{}, false
	}
	memberID := firstString(row, "id")
	accountID := firstString(row, "account_id", "accountId")
	if value := firstString(row, "status"); value != "" {
		status = strings.ToLower(value)
	}
	memberRevision := firstInt64(row, "membership_revision", "membershipRevision")
	if memberRevision <= 0 {
		memberRevision = int64(revision)
	}
	if strings.TrimSpace(tenantID) == "" || memberID == "" || accountID == "" {
		return controlidentity.AccountTenantIndex{}, false
	}
	return controlidentity.AccountTenantIndex{
		AccountID: accountID, TenantID: tenantID, MemberID: memberID,
		Status: status, TenantMembershipRevision: memberRevision,
	}, true
}

func firstString(row map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := row[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstInt64(row map[string]any, keys ...string) int64 {
	for _, key := range keys {
		switch value := row[key].(type) {
		case float64:
			return int64(value)
		case json.Number:
			parsed, _ := value.Int64()
			return parsed
		case string:
			parsed, _ := strconv.ParseInt(value, 10, 64)
			return parsed
		}
	}
	return 0
}

func (s *Server) retryMemberProjection(projectID string, projection controlidentity.AccountTenantIndex) {
	backoff := 100 * time.Millisecond
	for attempt := 1; attempt <= 8; attempt++ {
		projectionContext := s.ctx
		if projectionContext == nil {
			projectionContext = context.Background()
		}
		ctx, cancel := context.WithTimeout(projectionContext, 5*time.Second)
		db, err := s.pooledProjectRegistry(ctx)
		if err == nil && db != nil {
			err = (controlidentity.MembershipProjector{DB: db}).Upsert(ctx, projection)
		}
		cancel()
		if err == nil {
			return
		}
		if attempt == 8 {
			slog.Error("control-plane member projection exhausted retries",
				"project", projectID, "tenant", projection.TenantID,
				"account", projection.AccountID, "member", projection.MemberID,
				"revision", projection.TenantMembershipRevision, "error", fmt.Sprint(err))
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-projectionContext.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// projectTenantMemberDirectory publishes a committed tenant membership to the
// Control Plane directory. It runs after the tenant commit and never inside it,
// so a directory failure leaves the outbox row for the change feed to retry and
// can never turn a successful membership change into a business error.
func (s *Server) projectTenantMemberDirectory(projectID, tenantID string) {
	projectionContext := s.ctx
	if projectionContext == nil {
		projectionContext = context.Background()
	}
	ctx, cancel := context.WithTimeout(projectionContext, controlPlaneMembershipProjectionTimeout)
	defer cancel()
	if err := s.drainControlPlaneMembershipOutboxContext(ctx, projectID, tenantID); err != nil {
		slog.Warn("control-plane member directory projection deferred to the tenant outbox",
			"project", projectID, "tenant", tenantID, "error", fmt.Sprint(err))
	}
}

// drainControlPlaneMembershipOutbox retries the tenant-local transactional
// outbox. The trigger that writes this table runs in the same Postgres commit
// as members, so a process crash or unavailable Control Plane cannot lose the
// directory update.
func (s *Server) drainControlPlaneMembershipOutbox(projectID, tenantID string) {
	projectionContext := s.ctx
	if projectionContext == nil {
		projectionContext = context.Background()
	}
	ctx, cancel := context.WithTimeout(projectionContext, 30*time.Second)
	defer cancel()
	if err := s.drainControlPlaneMembershipOutboxContext(ctx, projectID, tenantID); err != nil {
		slog.Debug("control-plane member outbox drain did not complete",
			"project", projectID, "tenant", tenantID, "error", fmt.Sprint(err))
	}
}

func (s *Server) drainControlPlaneMembershipOutboxContext(ctx context.Context, projectID, tenantID string) error {
	tenantDB, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return err
	}
	rows, err := tenantDB.QueryContext(ctx, `SELECT account_id, member_id, status, membership_revision
		FROM _gonvex_control_plane_membership_outbox
		ORDER BY membership_revision, account_id LIMIT 1000`)
	if err != nil {
		return err
	}
	pending := []pendingMemberProjection{}
	for rows.Next() {
		item := pendingMemberProjection{}
		item.TenantID = tenantID
		if err := rows.Scan(&item.AccountID, &item.MemberID, &item.Status, &item.TenantMembershipRevision); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	controlDB, err := s.pooledProjectRegistry(ctx)
	if err != nil {
		return err
	}
	if controlDB == nil {
		return fmt.Errorf("control-plane database is unavailable")
	}
	projector := controlidentity.MembershipProjector{DB: controlDB}
	for _, item := range pending {
		if err := projector.Upsert(ctx, item.AccountTenantIndex); err != nil {
			return err
		}
		if _, err := tenantDB.ExecContext(ctx, `DELETE FROM _gonvex_control_plane_membership_outbox
			WHERE account_id = $1 AND membership_revision <= $2`, item.AccountID, item.TenantMembershipRevision); err != nil {
			return err
		}
	}
	return nil
}
