package identity

import (
	"context"
	"fmt"
	"strings"
)

// MembershipProjector writes only the discovery/routing projection. It must
// be called after a committed tenant.members change and retried idempotently.
// It never grants access by itself; request admission must re-check the member
// row in the target tenant database.
type MembershipProjector struct {
	DB Execer
}

func (p MembershipProjector) Upsert(ctx context.Context, membership AccountTenantIndex) error {
	if p.DB == nil {
		return fmt.Errorf("control-plane database is required")
	}
	if strings.TrimSpace(membership.AccountID) == "" || strings.TrimSpace(membership.TenantID) == "" || strings.TrimSpace(membership.MemberID) == "" {
		return fmt.Errorf("accountID, tenantID, and memberID are required")
	}
	if strings.TrimSpace(membership.Status) == "" {
		membership.Status = "active"
	}
	_, err := p.DB.ExecContext(ctx, `INSERT INTO account_tenant_index (
		account_id, tenant_id, member_id, status, tenant_membership_revision, updated_at
	) VALUES ($1, $2, $3, $4, $5, now())
	ON CONFLICT (account_id, tenant_id) DO UPDATE SET
		member_id = EXCLUDED.member_id,
		status = EXCLUDED.status,
		tenant_membership_revision = EXCLUDED.tenant_membership_revision,
		updated_at = now()
	WHERE EXCLUDED.tenant_membership_revision >= account_tenant_index.tenant_membership_revision`, membership.AccountID, membership.TenantID, membership.MemberID, membership.Status, membership.TenantMembershipRevision)
	return err
}

func (p MembershipProjector) Delete(ctx context.Context, accountID, tenantID string) error {
	if p.DB == nil {
		return fmt.Errorf("control-plane database is required")
	}
	if strings.TrimSpace(accountID) == "" || strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("accountID and tenantID are required")
	}
	_, err := p.DB.ExecContext(ctx, `DELETE FROM account_tenant_index WHERE account_id = $1 AND tenant_id = $2`, accountID, tenantID)
	return err
}
