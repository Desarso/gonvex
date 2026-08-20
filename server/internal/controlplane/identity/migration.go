package identity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// MigrationStore is the persistence boundary for plan/apply/verify. A store
// implementation should use INSERT ... ON CONFLICT and compare-and-set
// checkpoints so rerunning a command is safe after a process or database
// failure. The interface deliberately does not expose tenant authorization;
// that decision belongs to the selected tenant database.
type MigrationStore interface {
	BeginIdentityMigration(context.Context, MigrationRun) error
	SaveCollision(context.Context, string, LegacyAccountResolution) error
	LoadCheckpoint(context.Context, string, string) (MigrationCheckpoint, error)
	ApplyResolution(context.Context, LegacyAccountResolution) error
	SaveCheckpoint(context.Context, MigrationCheckpoint) error
	CompleteIdentityMigration(context.Context, string) error
	VerifyIdentityMigration(context.Context, MigrationPlan) ([]VerificationFinding, error)
}

func PlanIdentityMigration(runID, source string, records []LegacyIdentity, existing []ExistingAccount) (MigrationPlan, error) {
	if runID == "" || source == "" {
		return MigrationPlan{}, fmt.Errorf("runID and source are required")
	}
	resolved := ResolveLegacyAccounts(records, existing)
	items := append([]LegacyAccountResolution(nil), resolved.Items...)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Legacy.Source != items[j].Legacy.Source {
			return items[i].Legacy.Source < items[j].Legacy.Source
		}
		return items[i].Legacy.LegacyUserID < items[j].Legacy.LegacyUserID
	})
	plan := MigrationPlan{RunID: runID, Source: source, Items: items, Collisions: resolved.Collisions, LegacyRows: len(records), AmbiguousCollisions: len(resolved.Collisions)}
	seen := map[string]bool{}
	for _, item := range items {
		seen[item.Account.ID] = true
		switch item.Kind {
		case ResolutionProviderSubject:
			plan.ProviderMatches++
		case ResolutionVerifiedEmail:
			plan.EmailMatches++
		case ResolutionNewAccount:
			plan.NewAccounts++
		}
	}
	plan.UniqueAccounts = len(seen)
	checksum, err := planChecksum(plan)
	if err != nil {
		return MigrationPlan{}, err
	}
	plan.Checksum = checksum
	return plan, nil
}

// ApplyIdentityMigration resumes from each persisted source checkpoint. It
// refuses unresolved identity collisions unless the caller explicitly opts in
// after review; no migration command should silently merge ambiguous users.
func ApplyIdentityMigration(ctx context.Context, store MigrationStore, plan MigrationPlan, allowUnresolvedCollisions bool) error {
	if len(plan.Collisions) > 0 && !allowUnresolvedCollisions {
		return fmt.Errorf("identity migration has %d unresolved collisions", len(plan.Collisions))
	}
	if err := store.BeginIdentityMigration(ctx, MigrationRun{ID: plan.RunID, Source: plan.Source, PlanChecksum: plan.Checksum, Status: "running"}); err != nil {
		return err
	}
	for _, collision := range plan.Collisions {
		if err := store.SaveCollision(ctx, plan.RunID, collision); err != nil {
			return err
		}
	}
	checkpoint, err := store.LoadCheckpoint(ctx, plan.RunID, plan.Source)
	if err != nil {
		return err
	}
	for index, item := range plan.Items {
		if index <= checkpoint.CompletedIndex {
			continue
		}
		if err := store.ApplyResolution(ctx, item); err != nil {
			return err
		}
		checkpoint = MigrationCheckpoint{RunID: plan.RunID, Scope: plan.Source, CompletedIndex: index, LastLegacyUserID: item.Legacy.LegacyUserID, RowsProcessed: index + 1, Checksum: plan.Checksum, Status: "running"}
		if err := store.SaveCheckpoint(ctx, checkpoint); err != nil {
			return err
		}
	}
	checkpoint.CompletedIndex = len(plan.Items) - 1
	checkpoint.RowsProcessed = len(plan.Items)
	checkpoint.Status = "complete"
	if err := store.SaveCheckpoint(ctx, checkpoint); err != nil {
		return err
	}
	return store.CompleteIdentityMigration(ctx, plan.RunID)
}

func VerifyIdentityMigration(ctx context.Context, store MigrationStore, plan MigrationPlan) (VerificationResult, error) {
	findings, err := store.VerifyIdentityMigration(ctx, plan)
	if err != nil {
		return VerificationResult{}, err
	}
	return VerificationResult{PlanChecksum: plan.Checksum, Findings: findings}, nil
}

func planChecksum(plan MigrationPlan) (string, error) {
	payload := struct {
		RunID      string                    `json:"runId"`
		Source     string                    `json:"source"`
		Items      []LegacyAccountResolution `json:"items"`
		Collisions []LegacyAccountResolution `json:"collisions"`
	}{RunID: plan.RunID, Source: plan.Source, Items: plan.Items, Collisions: plan.Collisions}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
