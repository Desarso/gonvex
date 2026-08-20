package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	identity "github.com/gonvex/gonvex/server/pkg/controlplane/identity"
)

type identityTenantTarget struct {
	ProjectID   string
	TenantID    string
	DatabaseURL string
}

func inspectIdentityV2RuntimeMigration(ctx context.Context, control *sql.DB, plan identity.MigrationPlan) error {
	mapping, err := identityPlanMapping(plan)
	if err != nil {
		return err
	}
	if exists, err := relationExists(ctx, control, "gonvex_auth_memberships"); err != nil {
		return err
	} else if exists {
		rows, err := control.QueryContext(ctx, `SELECT DISTINCT user_id FROM gonvex_auth_memberships ORDER BY user_id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var legacyID string
			if err := rows.Scan(&legacyID); err != nil {
				rows.Close()
				return err
			}
			if mapping[legacyID] == "" {
				rows.Close()
				return fmt.Errorf("identity-v2 plan omits legacy membership user %q", legacyID)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	if exists, err := relationExists(ctx, control, "gonvex_auth_users"); err != nil {
		return err
	} else if exists {
		rows, err := control.QueryContext(ctx, `SELECT id FROM gonvex_auth_users ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var legacyID string
			if err := rows.Scan(&legacyID); err != nil {
				rows.Close()
				return err
			}
			if mapping[legacyID] == "" {
				rows.Close()
				return fmt.Errorf("identity-v2 plan omits legacy Control Plane user %q", legacyID)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	for _, table := range []string{"gonvex_auth_codes", "gonvex_auth_sessions", "gonvex_auth_refresh_tokens"} {
		exists, err := relationExists(ctx, control, table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		legacyColumn, err := relationColumnExists(ctx, control, table, "user_id")
		if err != nil {
			return err
		}
		if !legacyColumn {
			canonicalColumn, err := relationColumnExists(ctx, control, table, "account_id")
			if err != nil {
				return err
			}
			if !canonicalColumn {
				return fmt.Errorf("%s has neither legacy user_id nor canonical account_id", table)
			}
			continue
		}
		rows, err := control.QueryContext(ctx, `SELECT DISTINCT user_id FROM `+table+` ORDER BY user_id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var legacyID string
			if err := rows.Scan(&legacyID); err != nil {
				rows.Close()
				return err
			}
			if mapping[legacyID] == "" {
				rows.Close()
				return fmt.Errorf("identity-v2 plan omits %s user %q", table, legacyID)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	targets, err := identityTenantTargets(ctx, control)
	if err != nil {
		return err
	}
	for _, target := range targets {
		tenant, err := sql.Open("pgx", target.DatabaseURL)
		if err != nil {
			return err
		}
		if err := tenant.PingContext(ctx); err != nil {
			tenant.Close()
			return fmt.Errorf("inspect tenant %s/%s: %w", target.ProjectID, target.TenantID, err)
		}
		legacyApplicationUsers, err := relationExists(ctx, tenant, "users")
		if err == nil && legacyApplicationUsers {
			err = fmt.Errorf("legacy application table users still exists; migrate its business profile and foreign-key contract into canonical members before running identity-v2")
		}
		membersExists := false
		if err == nil {
			membersExists, err = relationExists(ctx, tenant, "members")
		}
		legacyColumn := false
		if err == nil && membersExists {
			legacyColumn, err = relationColumnExists(ctx, tenant, "members", "user_id")
		}
		if err == nil && legacyColumn {
			rows, queryErr := tenant.QueryContext(ctx, `SELECT user_id FROM members ORDER BY user_id`)
			if queryErr != nil {
				err = queryErr
			} else {
				for rows.Next() {
					var legacyID string
					if scanErr := rows.Scan(&legacyID); scanErr != nil {
						err = scanErr
						break
					}
					if mapping[legacyID] == "" {
						err = fmt.Errorf("identity-v2 plan omits tenant member %q", legacyID)
						break
					}
				}
				_ = rows.Close()
			}
		} else if err == nil && membersExists {
			for _, column := range []string{"id", "account_id", "status", "membership_revision"} {
				canonicalColumn, columnErr := relationColumnExists(ctx, tenant, "members", column)
				if columnErr != nil {
					err = columnErr
					break
				}
				if !canonicalColumn {
					err = fmt.Errorf("members has neither the legacy user_id shape nor canonical column %s", column)
					break
				}
			}
		}
		_ = tenant.Close()
		if err != nil {
			return fmt.Errorf("inspect tenant %s/%s: %w", target.ProjectID, target.TenantID, err)
		}
	}
	return nil
}

// applyIdentityV2RuntimeSchema is deliberately called only by the explicit
// migration command. Runtime startup refuses these legacy shapes instead of
// trying to infer account merges or rewrite a tenant database in place.
func applyIdentityV2RuntimeSchema(ctx context.Context, control *sql.DB, plan identity.MigrationPlan) error {
	mapping, err := identityPlanMapping(plan)
	if err != nil {
		return err
	}
	targets, err := identityTenantTargets(ctx, control)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := migrateIdentityTenant(ctx, control, target, mapping, plan); err != nil {
			return fmt.Errorf("migrate tenant %s/%s: %w", target.ProjectID, target.TenantID, err)
		}
	}
	// The tenant members committed above are authoritative. The old Control
	// Plane membership rows were only a derived dual-write, so they can now be
	// discarded while the remaining credentials are moved to account_id.
	return migrateControlPlaneAuthTables(ctx, control, mapping)
}

func verifyIdentityV2RuntimeSchema(ctx context.Context, control *sql.DB, plan identity.MigrationPlan) error {
	legacyUsers, err := relationExists(ctx, control, "gonvex_auth_users")
	if err != nil {
		return err
	}
	legacyMemberships, err := relationExists(ctx, control, "gonvex_auth_memberships")
	if err != nil {
		return err
	}
	if legacyUsers || legacyMemberships {
		return fmt.Errorf("identity-v2 verification: legacy Control Plane identity tables remain")
	}
	for _, table := range []string{"gonvex_auth_codes", "gonvex_auth_sessions", "gonvex_auth_refresh_tokens"} {
		legacyColumn, err := relationColumnExists(ctx, control, table, "user_id")
		if err != nil {
			return err
		}
		if legacyColumn {
			return fmt.Errorf("identity-v2 verification: %s.user_id remains", table)
		}
		exists, err := relationExists(ctx, control, table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		var orphaned int
		if err := control.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` value
			LEFT JOIN accounts account ON account.id = value.account_id
			WHERE value.account_id IS NULL OR value.account_id = '' OR account.id IS NULL`).Scan(&orphaned); err != nil {
			return err
		}
		if orphaned != 0 {
			return fmt.Errorf("identity-v2 verification: %s contains %d orphaned account references", table, orphaned)
		}
	}
	targets, err := identityTenantTargets(ctx, control)
	if err != nil {
		return err
	}
	for _, target := range targets {
		tenant, err := sql.Open("pgx", target.DatabaseURL)
		if err != nil {
			return err
		}
		legacyColumn, checkErr := relationColumnExists(ctx, tenant, "members", "user_id")
		if checkErr == nil && legacyColumn {
			checkErr = fmt.Errorf("members.user_id remains")
		}
		if checkErr == nil {
			legacyApplicationUsers, legacyErr := relationExists(ctx, tenant, "users")
			if legacyErr != nil {
				checkErr = legacyErr
			} else if legacyApplicationUsers {
				checkErr = fmt.Errorf("legacy application table users remains")
			}
		}
		type verifiedMember struct {
			memberID, accountID, status string
			revision                    int64
		}
		members := []verifiedMember{}
		if checkErr == nil {
			rows, queryErr := tenant.QueryContext(ctx, `SELECT id, account_id, status, membership_revision
				FROM members ORDER BY id`)
			if queryErr != nil {
				checkErr = queryErr
			} else {
				for rows.Next() {
					var member verifiedMember
					if scanErr := rows.Scan(&member.memberID, &member.accountID, &member.status, &member.revision); scanErr != nil {
						checkErr = scanErr
						break
					}
					if strings.TrimSpace(member.memberID) == "" || strings.TrimSpace(member.accountID) == "" {
						checkErr = fmt.Errorf("members contains an incomplete identity row")
						break
					}
					members = append(members, member)
				}
				if rowsErr := rows.Close(); checkErr == nil && rowsErr != nil {
					checkErr = rowsErr
				}
			}
		}
		_ = tenant.Close()
		if checkErr != nil {
			return fmt.Errorf("identity-v2 verification for tenant %s/%s: %w", target.ProjectID, target.TenantID, checkErr)
		}
		for _, member := range members {
			var accountExists bool
			if err := control.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1)`, member.accountID).Scan(&accountExists); err != nil {
				return err
			}
			if !accountExists {
				return fmt.Errorf("identity-v2 verification for tenant %s/%s: member %q references missing account %q",
					target.ProjectID, target.TenantID, member.memberID, member.accountID)
			}
			var projected bool
			if err := control.QueryRowContext(ctx, `SELECT EXISTS (
				SELECT 1 FROM account_tenant_index
				WHERE account_id = $1 AND tenant_id = $2 AND member_id = $3
					AND status = $4 AND tenant_membership_revision = $5
			)`, member.accountID, target.TenantID, member.memberID, member.status, member.revision).Scan(&projected); err != nil {
				return err
			}
			if !projected {
				return fmt.Errorf("identity-v2 verification for tenant %s/%s: member %q has no matching directory projection",
					target.ProjectID, target.TenantID, member.memberID)
			}
		}
		var checkpointComplete bool
		if err := control.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM identity_migration_checkpoints
			WHERE run_id = $1 AND scope = $2 AND checksum = $3 AND status = 'complete'
		)`, plan.RunID, "tenant:"+target.ProjectID+":"+target.TenantID, plan.Checksum).Scan(&checkpointComplete); err != nil {
			return err
		}
		if !checkpointComplete {
			return fmt.Errorf("identity-v2 verification for tenant %s/%s: migration checkpoint is missing or incomplete",
				target.ProjectID, target.TenantID)
		}
	}
	return nil
}

func identityPlanMapping(plan identity.MigrationPlan) (map[string]string, error) {
	mapping := make(map[string]string, len(plan.Items))
	for _, item := range plan.Items {
		legacyID := strings.TrimSpace(item.Legacy.LegacyUserID)
		accountID := strings.TrimSpace(item.Account.ID)
		if legacyID == "" || accountID == "" {
			return nil, fmt.Errorf("identity-v2 plan contains an incomplete legacy mapping")
		}
		if existing := mapping[legacyID]; existing != "" && existing != accountID {
			return nil, fmt.Errorf("legacy id %q maps to multiple accounts; split the migration by auth realm", legacyID)
		}
		mapping[legacyID] = accountID
	}
	return mapping, nil
}

func migrateControlPlaneAuthTables(ctx context.Context, db *sql.DB, mapping map[string]string) error {
	legacyUsers, err := relationExists(ctx, db, "gonvex_auth_users")
	if err != nil || !legacyUsers {
		return err
	}
	legacyMemberships, err := relationExists(ctx, db, "gonvex_auth_memberships")
	if err != nil {
		return err
	}
	type legacyAccount struct{ legacyID, projectID, accountID string }
	rows, err := db.QueryContext(ctx, `SELECT id, project_id FROM gonvex_auth_users ORDER BY project_id, id`)
	if err != nil {
		return err
	}
	accounts := []legacyAccount{}
	for rows.Next() {
		var item legacyAccount
		if err := rows.Scan(&item.legacyID, &item.projectID); err != nil {
			rows.Close()
			return err
		}
		item.accountID = mapping[item.legacyID]
		if item.accountID == "" {
			rows.Close()
			return fmt.Errorf("legacy Control Plane user %q has no reviewed account mapping", item.legacyID)
		}
		accounts = append(accounts, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range accounts {
		result, err := tx.ExecContext(ctx, `UPDATE accounts SET auth_realm_id = $2, updated_at = now()
			WHERE id = $1 AND (auth_realm_id = '' OR auth_realm_id = $2)`, item.accountID, item.projectID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("account %q is assigned to another auth realm", item.accountID)
		}
	}
	for _, table := range []string{"gonvex_auth_codes", "gonvex_auth_sessions", "gonvex_auth_refresh_tokens"} {
		exists, err := relationExistsTx(ctx, tx, table)
		if err != nil || !exists {
			if err != nil {
				return err
			}
			continue
		}
		legacyColumn, err := relationColumnExistsTx(ctx, tx, table, "user_id")
		if err != nil || !legacyColumn {
			if err != nil {
				return err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN IF NOT EXISTS account_id TEXT`); err != nil {
			return err
		}
		for _, item := range accounts {
			if _, err := tx.ExecContext(ctx, `UPDATE `+table+` SET account_id = $2 WHERE user_id = $1`, item.legacyID, item.accountID); err != nil {
				return err
			}
		}
		var missing int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` WHERE account_id IS NULL OR account_id = ''`).Scan(&missing); err != nil {
			return err
		}
		if missing != 0 {
			return fmt.Errorf("%s has %d rows without reviewed account mappings", table, missing)
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE `+table+` DROP COLUMN user_id`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE `+table+` ALTER COLUMN account_id SET NOT NULL`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE `+table+` ADD CONSTRAINT `+table+`_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE`); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	if legacyMemberships {
		if _, err := tx.ExecContext(ctx, `DROP TABLE gonvex_auth_memberships`); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE gonvex_auth_users`); err != nil {
		return err
	}
	return tx.Commit()
}

func identityTenantTargets(ctx context.Context, db *sql.DB) ([]identityTenantTarget, error) {
	rows, err := db.QueryContext(ctx, `SELECT t.project_id, t.tenant_id,
		CASE WHEN t.database_url <> '' THEN t.database_url ELSE p.database_url END
		FROM gonvex_runtime_tenants t
		JOIN gonvex_runtime_projects p ON p.id = t.project_id
		WHERE t.status = 'active'
		ORDER BY t.project_id, t.tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("discover tenant databases from Control Plane: %w", err)
	}
	defer rows.Close()
	targets := []identityTenantTarget{}
	for rows.Next() {
		var target identityTenantTarget
		if err := rows.Scan(&target.ProjectID, &target.TenantID, &target.DatabaseURL); err != nil {
			return nil, err
		}
		if strings.TrimSpace(target.DatabaseURL) == "" {
			return nil, fmt.Errorf("tenant %s/%s has no database_url in the Control Plane", target.ProjectID, target.TenantID)
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func migrateIdentityTenant(ctx context.Context, control *sql.DB, target identityTenantTarget, mapping map[string]string, plan identity.MigrationPlan) error {
	tenant, err := sql.Open("pgx", target.DatabaseURL)
	if err != nil {
		return err
	}
	defer tenant.Close()
	if err := tenant.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to tenant database: %w", err)
	}
	tx, err := tenant.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	exists, err := relationExistsTx(ctx, tx, "members")
	if err != nil {
		return err
	}
	if !exists {
		if _, err := tx.ExecContext(ctx, canonicalMembersDDL); err != nil {
			return err
		}
	} else {
		legacyColumn, err := relationColumnExistsTx(ctx, tx, "members", "user_id")
		if err != nil {
			return err
		}
		if legacyColumn {
			for _, statement := range []string{
				`ALTER TABLE members ADD COLUMN IF NOT EXISTS id TEXT`,
				`ALTER TABLE members ADD COLUMN IF NOT EXISTS account_id TEXT`,
				`ALTER TABLE members ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'`,
				`ALTER TABLE members ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE members ADD COLUMN IF NOT EXISTS avatar_url TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE members ADD COLUMN IF NOT EXISTS membership_revision BIGINT NOT NULL DEFAULT 1`,
			} {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			rows, err := tx.QueryContext(ctx, `SELECT user_id FROM members ORDER BY user_id`)
			if err != nil {
				return err
			}
			legacyIDs := []string{}
			for rows.Next() {
				var legacyID string
				if err := rows.Scan(&legacyID); err != nil {
					rows.Close()
					return err
				}
				if mapping[legacyID] == "" {
					rows.Close()
					return fmt.Errorf("member %q has no reviewed account mapping", legacyID)
				}
				legacyIDs = append(legacyIDs, legacyID)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			for _, legacyID := range legacyIDs {
				if _, err := tx.ExecContext(ctx, `UPDATE members SET id = $1, account_id = $2 WHERE user_id = $1`, legacyID, mapping[legacyID]); err != nil {
					return err
				}
			}
			var duplicates int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (
				SELECT account_id FROM members GROUP BY account_id HAVING count(*) > 1
			) duplicate_accounts`).Scan(&duplicates); err != nil {
				return err
			}
			if duplicates != 0 {
				return fmt.Errorf("members contains %d duplicate account mappings", duplicates)
			}
			for _, statement := range []string{
				`ALTER TABLE members DROP CONSTRAINT IF EXISTS members_pkey`,
				`ALTER TABLE members DROP COLUMN user_id`,
				`ALTER TABLE members ALTER COLUMN id SET NOT NULL`,
				`ALTER TABLE members ALTER COLUMN account_id SET NOT NULL`,
				`ALTER TABLE members ADD PRIMARY KEY (id)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS members_by_account ON members (account_id)`,
			} {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, account_id, status, membership_revision FROM members`)
	if err != nil {
		return err
	}
	type projection struct {
		memberID, accountID, status string
		revision                    int64
	}
	projections := []projection{}
	for rows.Next() {
		var item projection
		if err := rows.Scan(&item.memberID, &item.accountID, &item.status, &item.revision); err != nil {
			rows.Close()
			return err
		}
		projections = append(projections, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, item := range projections {
		if _, err := control.ExecContext(ctx, `INSERT INTO account_tenant_index (
			account_id, tenant_id, member_id, status, tenant_membership_revision, updated_at
		) VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (account_id, tenant_id) DO UPDATE SET member_id = EXCLUDED.member_id,
			status = EXCLUDED.status, tenant_membership_revision = EXCLUDED.tenant_membership_revision,
			updated_at = now()
		WHERE EXCLUDED.tenant_membership_revision >= account_tenant_index.tenant_membership_revision`,
			item.accountID, target.TenantID, item.memberID, item.status, item.revision); err != nil {
			return fmt.Errorf("project tenant directory: %w", err)
		}
	}
	_, err = control.ExecContext(ctx, `INSERT INTO identity_migration_checkpoints (
		run_id, scope, completed_index, last_legacy_user_id, rows_processed, checksum, status, updated_at
	) VALUES ($1, $2, 0, '', $3, $4, 'complete', now())
	ON CONFLICT (run_id, scope) DO UPDATE SET completed_index = 0, rows_processed = EXCLUDED.rows_processed,
		checksum = EXCLUDED.checksum, status = 'complete', updated_at = now()`,
		plan.RunID, "tenant:"+target.ProjectID+":"+target.TenantID, len(projections), plan.Checksum)
	return err
}

const canonicalMembersDDL = `CREATE TABLE members (
	id TEXT PRIMARY KEY,
	account_id TEXT NOT NULL UNIQUE,
	status TEXT NOT NULL DEFAULT 'active',
	display_name TEXT NOT NULL DEFAULT '',
	avatar_url TEXT NOT NULL DEFAULT '',
	role TEXT NOT NULL DEFAULT 'member',
	permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
	membership_revision BIGINT NOT NULL DEFAULT 1,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

func relationExists(ctx context.Context, db *sql.DB, relation string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, relation).Scan(&exists)
	return exists, err
}

func relationExistsTx(ctx context.Context, tx *sql.Tx, relation string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, relation).Scan(&exists)
	return exists, err
}

func relationColumnExists(ctx context.Context, db *sql.DB, relation, column string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
	)`, relation, column).Scan(&exists)
	return exists, err
}

func relationColumnExistsTx(ctx context.Context, tx *sql.Tx, relation, column string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
	)`, relation, column).Scan(&exists)
	return exists, err
}
