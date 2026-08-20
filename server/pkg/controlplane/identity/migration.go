// Package identity exposes the stable Control Plane identity migration API to
// Gonvex tooling. Runtime implementation details remain in server/internal.
package identity

import (
	"context"

	internal "github.com/gonvex/gonvex/server/internal/controlplane/identity"
)

type LegacyIdentity = internal.LegacyIdentity
type ExistingAccount = internal.ExistingAccount
type MigrationPlan = internal.MigrationPlan
type VerificationResult = internal.VerificationResult
type PostgresMigrationStore = internal.PostgresMigrationStore
type Queryer = internal.Queryer
type Execer = internal.Execer

func PlanIdentityMigration(runID, source string, records []LegacyIdentity, existing []ExistingAccount) (MigrationPlan, error) {
	return internal.PlanIdentityMigration(runID, source, records, existing)
}

func ApplyIdentityMigration(ctx context.Context, store PostgresMigrationStore, plan MigrationPlan, allowUnresolvedCollisions bool) error {
	return internal.ApplyIdentityMigration(ctx, store, plan, allowUnresolvedCollisions)
}

func VerifyIdentityMigration(ctx context.Context, store PostgresMigrationStore, plan MigrationPlan) (VerificationResult, error) {
	return internal.VerifyIdentityMigration(ctx, store, plan)
}

func ValidateIdentityMigrationPlan(plan MigrationPlan) error {
	return internal.ValidateIdentityMigrationPlan(plan)
}

func LoadExistingAccounts(ctx context.Context, db Queryer) ([]ExistingAccount, error) {
	return internal.LoadExistingAccounts(ctx, db)
}

func InstallSchema(ctx context.Context, db Execer) error {
	return internal.InstallSchema(ctx, db)
}
