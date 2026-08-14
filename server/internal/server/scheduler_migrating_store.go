package server

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type legacyScheduledJobGuard int

const (
	legacyScheduledJobGuarded legacyScheduledJobGuard = iota
	legacyScheduledJobPending
	legacyScheduledJobCompleted
)

type legacyScheduledJobStore interface {
	scheduledJobStore
	guard(context.Context, string, string) (legacyScheduledJobGuard, error)
}

// migratingScheduledJobStore makes Postgres the durable scheduler source while
// retaining Valkey only as a rolling-upgrade fence. It also drains jobs written
// by the previous Valkey-only runtime. Once every supported runtime version is
// Postgres-backed, the legacy side can be removed without migrating job data.
type migratingScheduledJobStore struct {
	primary *postgresScheduledJobStore
	legacy  legacyScheduledJobStore
}

func newMigratingScheduledJobStore(primary *postgresScheduledJobStore, legacy legacyScheduledJobStore) scheduledJobStore {
	if primary == nil {
		return legacy
	}
	if legacy == nil {
		return primary
	}
	return &migratingScheduledJobStore{primary: primary, legacy: legacy}
}

func (store *migratingScheduledJobStore) enqueue(ctx context.Context, job scheduledJob) error {
	return store.primary.enqueue(ctx, job)
}

func (store *migratingScheduledJobStore) claimDue(ctx context.Context, now time.Time, limit int, owner string) ([]scheduledJob, error) {
	if limit <= 0 {
		return nil, nil
	}
	// Claim up to one worker batch from both stores before choosing what to run.
	// During the rolling migration, this prevents a legacy cron backlog from
	// starving a newly scheduled Postgres one-shot (or vice versa).
	claimed := make([]scheduledJob, 0, limit*2)
	releaseClaimed := func() {
		for _, job := range claimed {
			_ = store.primary.release(ctx, job.ID, job.ClaimToken)
			_ = store.legacy.release(ctx, job.ID, job.ClaimToken)
		}
	}
	legacyJobs, err := store.legacy.claimDue(ctx, now, limit, owner)
	if err != nil {
		return nil, fmt.Errorf("claim legacy scheduled jobs: %w", err)
	}
	releaseLegacy := func() {
		for _, job := range legacyJobs {
			_ = store.legacy.release(ctx, job.ID, job.ClaimToken)
		}
	}
	for _, job := range legacyJobs {
		adoption, err := store.primary.adoptClaim(ctx, job, now)
		if err != nil {
			releaseLegacy()
			releaseClaimed()
			return nil, fmt.Errorf("adopt legacy scheduled job %s: %w", job.ID, err)
		}
		switch adoption {
		case scheduledJobAdopted:
			claimed = append(claimed, job)
		case scheduledJobAlreadyCompleted:
			if err := store.legacy.complete(ctx, job.ID, job.ClaimToken); err != nil {
				releaseLegacy()
				releaseClaimed()
				return nil, fmt.Errorf("discard completed legacy scheduled job %s: %w", job.ID, err)
			}
		default:
			if err := store.legacy.release(ctx, job.ID, job.ClaimToken); err != nil {
				releaseLegacy()
				releaseClaimed()
				return nil, fmt.Errorf("release busy legacy scheduled job %s: %w", job.ID, err)
			}
		}
	}

	primaryJobs, err := store.primary.claimDue(ctx, now, limit, owner)
	if err != nil {
		releaseClaimed()
		return nil, fmt.Errorf("claim postgres scheduled jobs: %w", err)
	}
	releasePrimary := func() {
		for _, job := range primaryJobs {
			_ = store.primary.release(ctx, job.ID, job.ClaimToken)
			_ = store.legacy.release(ctx, job.ID, job.ClaimToken)
		}
	}
	for _, job := range primaryJobs {
		guard, err := store.legacy.guard(ctx, job.ID, job.ClaimToken)
		if err != nil {
			releasePrimary()
			releaseClaimed()
			return nil, fmt.Errorf("guard postgres scheduled job %s: %w", job.ID, err)
		}
		switch guard {
		case legacyScheduledJobGuarded:
			claimed = append(claimed, job)
		case legacyScheduledJobCompleted:
			if err := store.primary.complete(ctx, job.ID, job.ClaimToken); err != nil {
				releasePrimary()
				releaseClaimed()
				return nil, fmt.Errorf("complete postgres copy of legacy job %s: %w", job.ID, err)
			}
		default:
			if err := store.primary.release(ctx, job.ID, job.ClaimToken); err != nil {
				releasePrimary()
				releaseClaimed()
				return nil, fmt.Errorf("release postgres job waiting on legacy execution %s: %w", job.ID, err)
			}
		}
	}
	sort.SliceStable(claimed, func(left, right int) bool {
		leftCron := claimed[left].CronName != ""
		rightCron := claimed[right].CronName != ""
		if leftCron != rightCron {
			return !leftCron
		}
		return claimed[left].ScheduledFor.Before(claimed[right].ScheduledFor)
	})
	if len(claimed) <= limit {
		return claimed, nil
	}
	for _, job := range claimed[limit:] {
		_ = store.primary.release(ctx, job.ID, job.ClaimToken)
		_ = store.legacy.release(ctx, job.ID, job.ClaimToken)
	}
	return claimed[:limit], nil
}

func (store *migratingScheduledJobStore) renew(ctx context.Context, id string, owner string) (bool, error) {
	primaryAlive, err := store.primary.renew(ctx, id, owner)
	if err != nil || !primaryAlive {
		return primaryAlive, err
	}
	legacyAlive, err := store.legacy.renew(ctx, id, owner)
	if err != nil || !legacyAlive {
		return legacyAlive, err
	}
	return true, nil
}

func (store *migratingScheduledJobStore) complete(ctx context.Context, id string, owner string) error {
	// Remove the legacy delivery first. If the process stops before the
	// Postgres update, the retained Valkey dedupe marker tells the next runtime
	// to complete the row without executing the function again.
	if err := store.legacy.complete(ctx, id, owner); err != nil {
		return err
	}
	return store.primary.complete(ctx, id, owner)
}

func (store *migratingScheduledJobStore) release(ctx context.Context, id string, owner string) error {
	primaryErr := store.primary.release(ctx, id, owner)
	legacyErr := store.legacy.release(ctx, id, owner)
	if primaryErr != nil {
		return primaryErr
	}
	return legacyErr
}
