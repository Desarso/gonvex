package server

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"
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

func TestPostgresScheduledJobsPruneOnlyExpiredCompletions(t *testing.T) {
	db, store := testPostgresScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		id        string
		status    string
		completed any
	}{
		{id: "completed-expired", status: "completed", completed: now.Add(-scheduledJobCompletionRetention - time.Hour)},
		{id: "completed-recent", status: "completed", completed: now.Add(-scheduledJobCompletionRetention + time.Hour)},
		{id: "pending-old", status: "pending", completed: nil},
	} {
		if _, err := db.ExecContext(ctx, `INSERT INTO gonvex_scheduled_jobs
			(id, project_id, function_path, run_at, scheduled_for, status, completed_at)
			VALUES ($1, 'project-a', 'tasks.run', $2, $2, $3, $4)`, row.id, now.Add(-90*24*time.Hour), row.status, row.completed); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := store.pruneCompleted(ctx, now.Add(-scheduledJobCompletionRetention), 100)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed rows = %d, want 1", removed)
	}
	for id, want := range map[string]bool{"completed-expired": false, "completed-recent": true, "pending-old": true} {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM gonvex_scheduled_jobs WHERE id = $1)`, id).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != want {
			t.Fatalf("job %s exists = %v, want %v", id, exists, want)
		}
	}
}

func TestPostgresSchedulerExecutesAcrossRuntimeInstances(t *testing.T) {
	_, store := testPostgresScheduledJobStore(t)
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
