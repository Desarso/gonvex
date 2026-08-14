package server

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testPostgresScheduledJobStore(t *testing.T) (*sql.DB, *postgresScheduledJobStore) {
	t.Helper()
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_scheduler_"+tenantRegistryTestSuffix(t))
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureProjectRegistry(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db, newPostgresScheduledJobStore(func(context.Context) (*sql.DB, error) { return db, nil })
}

func testMigratingScheduledJobStore(t *testing.T) (*sql.DB, *postgresScheduledJobStore, *valkeyScheduledJobStore, scheduledJobStore) {
	t.Helper()
	db, primary := testPostgresScheduledJobStore(t)
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	legacy := newValkeyScheduledJobStore(client)
	return db, primary, legacy, newMigratingScheduledJobStore(primary, legacy)
}

func TestPostgresScheduledJobsPersistPrioritizeAndFenceClaims(t *testing.T) {
	db, firstStore := testPostgresScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 40; index++ {
		if err := firstStore.enqueue(ctx, scheduledJob{
			ID: newScheduledJobID("cron_"), ProjectID: "project-a", TenantID: "tenant-a",
			FunctionPath: "tasks.generate", CronName: "generate", RunAt: now.Add(-time.Hour), ScheduledFor: now.Add(-time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	oneShot := scheduledJob{
		ID: "job-durable", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "assistant.process", RunAt: now, ScheduledFor: now,
	}
	if err := firstStore.enqueue(ctx, oneShot); err != nil {
		t.Fatal(err)
	}

	first, err := firstStore.claimDue(ctx, now, 1, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].ID != oneShot.ID || first[0].ClaimToken == "" {
		t.Fatalf("first claim = %#v", first)
	}
	other := newPostgresScheduledJobStore(func(context.Context) (*sql.DB, error) { return db, nil })
	concurrent, err := other.claimDue(ctx, now, 1, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(concurrent) != 1 || concurrent[0].CronName == "" {
		t.Fatalf("leased one-shot blocked unrelated due cron: %#v", concurrent)
	}
	if err := other.complete(ctx, concurrent[0].ID, concurrent[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.release(ctx, oneShot.ID, first[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := other.claimDue(ctx, now, 1, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != oneShot.ID || reclaimed[0].ClaimToken == first[0].ClaimToken {
		t.Fatalf("reclaimed job = %#v", reclaimed)
	}
	if err := other.complete(ctx, oneShot.ID, reclaimed[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.enqueue(ctx, oneShot); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM gonvex_scheduled_jobs WHERE id = $1`, oneShot.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("completed deterministic job returned to %q", status)
	}
}

func TestMigratingSchedulerDoesNotDuplicateLegacyExecution(t *testing.T) {
	db, primary, legacy, store := testMigratingScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	job := scheduledJob{
		ID: "cron-same-occurrence", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "tasks.generate", CronName: "generate", RunAt: now, ScheduledFor: now,
	}
	if err := primary.enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := legacy.enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	oldClaim, err := legacy.claimDue(ctx, now, 1, "old-runtime")
	if err != nil || len(oldClaim) != 1 {
		t.Fatalf("old claim = %#v, %v", oldClaim, err)
	}
	newClaim, err := store.claimDue(ctx, now, 1, "new-runtime")
	if err != nil {
		t.Fatal(err)
	}
	if len(newClaim) != 0 {
		t.Fatalf("new runtime duplicated active legacy execution: %#v", newClaim)
	}
	if err := legacy.complete(ctx, job.ID, oldClaim[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	newClaim, err = store.claimDue(ctx, now, 1, "new-runtime")
	if err != nil {
		t.Fatal(err)
	}
	if len(newClaim) != 0 {
		t.Fatalf("new runtime duplicated completed legacy execution: %#v", newClaim)
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM gonvex_scheduled_jobs WHERE id = $1`, job.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("postgres copy status = %q", status)
	}
}

func TestMigratingSchedulerDrainsLegacyJobsAndFencesOldRuntime(t *testing.T) {
	_, primary, legacy, store := testMigratingScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	legacyOnly := scheduledJob{
		ID: "job-from-old-runtime", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "tasks.legacy", RunAt: now, ScheduledFor: now,
	}
	if err := legacy.enqueue(ctx, legacyOnly); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.claimDue(ctx, now, 1, "new-runtime")
	if err != nil || len(claimed) != 1 || claimed[0].ID != legacyOnly.ID {
		t.Fatalf("migrated claim = %#v, %v", claimed, err)
	}
	if err := store.complete(ctx, claimed[0].ID, claimed[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	if remaining, err := legacy.claimDue(ctx, now, 1, "old-runtime"); err != nil || len(remaining) != 0 {
		t.Fatalf("completed legacy job remained queued: %#v, %v", remaining, err)
	}

	postgresJob := scheduledJob{
		ID: "cron-from-new-runtime", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "tasks.current", CronName: "current", RunAt: now, ScheduledFor: now,
	}
	if err := primary.enqueue(ctx, postgresJob); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.claimDue(ctx, now, 1, "new-runtime")
	if err != nil || len(claimed) != 1 || claimed[0].ID != postgresJob.ID {
		t.Fatalf("postgres claim = %#v, %v", claimed, err)
	}
	// Reproduce an old overlapping replica deriving the same cron after the new
	// runtime already owns the Postgres row. The shared Valkey claim fence must
	// keep that old replica from executing it.
	if err := legacy.enqueue(ctx, postgresJob); err != nil {
		t.Fatal(err)
	}
	if old, err := legacy.claimDue(ctx, now, 1, "old-runtime"); err != nil || len(old) != 0 {
		t.Fatalf("old runtime bypassed postgres claim fence: %#v, %v", old, err)
	}
	if err := store.complete(ctx, claimed[0].ID, claimed[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	if old, err := legacy.claimDue(ctx, now, 1, "old-runtime"); err != nil || len(old) != 0 {
		t.Fatalf("old runtime retained completed duplicate: %#v, %v", old, err)
	}
}

func TestMigratingSchedulerPrioritizesPostgresOneShotOverLegacyCronBacklog(t *testing.T) {
	_, primary, legacy, store := testMigratingScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 40; index++ {
		if err := legacy.enqueue(ctx, scheduledJob{
			ID: newScheduledJobID("legacy_cron_"), ProjectID: "project-a", TenantID: "tenant-a",
			FunctionPath: "tasks.generate", CronName: "generate", RunAt: now.Add(-time.Hour), ScheduledFor: now.Add(-time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	oneShot := scheduledJob{
		ID: "job-new-one-shot", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "assistant.process", RunAt: now, ScheduledFor: now,
	}
	if err := primary.enqueue(ctx, oneShot); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.claimDue(ctx, now, 1, "new-runtime")
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != oneShot.ID {
		t.Fatalf("postgres one-shot was starved during migration: %#v", claimed)
	}
}

func TestPostgresSchedulerExecutesAcrossRuntimeInstances(t *testing.T) {
	_, _, _, store := testMigratingScheduledJobStore(t)
	producer := newScheduler(nil)
	producer.store = store

	var mu sync.Mutex
	var seen []string
	consumer := newScheduler(func(_ context.Context, job scheduledJob) error {
		mu.Lock()
		seen = append(seen, job.FunctionPath)
		mu.Unlock()
		return nil
	})
	consumer.store = store
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	consumer.start(ctx)

	if _, err := producer.For("project-a", "tenant-a").RunAfter(0, "tasks.fromPostgres", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 1
	})
	mu.Lock()
	defer mu.Unlock()
	if seen[0] != "tasks.fromPostgres" {
		t.Fatalf("executed paths = %#v", seen)
	}
}
