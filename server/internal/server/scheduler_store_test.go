package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testScheduledJobStore(t *testing.T) (*miniredis.Miniredis, *redis.Client, *valkeyScheduledJobStore) {
	t.Helper()
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return redisServer, client, newValkeyScheduledJobStore(client)
}

func TestValkeyScheduledJobsAreClaimedOnceAndSurviveWorkerRelease(t *testing.T) {
	_, _, store := testScheduledJobStore(t)
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	job := scheduledJob{
		ID: "job-durable", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now,
	}
	if err := store.enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	first, err := store.claimDue(context.Background(), now, 10, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].ID != job.ID {
		t.Fatalf("first claim = %#v", first)
	}
	second, err := store.claimDue(context.Background(), now, 10, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("concurrent runtime claimed leased job: %#v", second)
	}
	if err := store.release(context.Background(), job.ID, first[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	second, err = store.claimDue(context.Background(), now, 10, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != job.ID {
		t.Fatalf("released job was not reclaimed: %#v", second)
	}
	if err := store.complete(context.Background(), job.ID, second[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	// A lagging rolling replica can still observe the same cron occurrence after
	// the winner completes it. The retained dedupe marker must not recreate it.
	if err := store.enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	remaining, err := store.claimDue(context.Background(), now, 10, "runtime-c")
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("completed job remained scheduled: %#v", remaining)
	}
}

func TestValkeyScheduledJobsSkipLeasedJobsWhenClaimingLaterDueWork(t *testing.T) {
	_, _, store := testScheduledJobStore(t)
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"job-a", "job-b", "job-c"} {
		if err := store.enqueue(context.Background(), scheduledJob{
			ID: id, ProjectID: "project-a", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.claimDue(context.Background(), now, 2, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("first runtime claimed %d jobs, want 2", len(first))
	}
	later, err := store.claimDue(context.Background(), now, 1, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(later) != 1 || later[0].ID != "job-c" {
		t.Fatalf("later due work was starved behind leases: %#v", later)
	}
}

func TestValkeyScheduledJobsPrioritizeOneShotsOverCronBacklog(t *testing.T) {
	_, _, store := testScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 80; index++ {
		job := scheduledJob{
			ID:           newScheduledJobID("cron_"),
			ProjectID:    "project-a",
			TenantID:     "tenant-a",
			FunctionPath: "tasks.generateDueRecurringTasks",
			RunAt:        now.Add(-time.Hour),
			ScheduledFor: now.Add(-time.Hour),
			CronName:     "generate due recurring tasks",
		}
		if err := store.enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	oneShot := scheduledJob{
		ID: "job-assistant", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "assistant.processThread", RunAt: now, ScheduledFor: now,
	}
	if err := store.enqueue(ctx, oneShot); err != nil {
		t.Fatal(err)
	}

	claimed, err := store.claimDue(ctx, now, 1, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != oneShot.ID {
		t.Fatalf("one-shot job was starved behind cron backlog: %#v", claimed)
	}
}

func TestValkeyScheduledJobsPrioritizeOneShotBeyondLegacyScanWindow(t *testing.T) {
	_, client, store := testScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	pipe := client.Pipeline()
	for index := 0; index < 4100; index++ {
		job := scheduledJob{
			ID:           fmt.Sprintf("legacy-cron-%04d", index),
			ProjectID:    "project-a",
			TenantID:     "tenant-a",
			FunctionPath: "tasks.generateDueRecurringTasks",
			RunAt:        now.Add(-time.Hour),
			ScheduledFor: now.Add(-time.Hour),
			CronName:     "generate due recurring tasks",
		}
		payload, err := json.Marshal(payloadForScheduledJob(job))
		if err != nil {
			t.Fatal(err)
		}
		pipe.HSet(ctx, scheduledJobPayloadsKey, job.ID, payload)
		pipe.ZAdd(ctx, scheduledJobsKey, redis.Z{Score: float64(job.RunAt.UnixMilli()), Member: job.ID})
	}
	oneShot := scheduledJob{
		ID: "legacy-job-assistant", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "assistant.processThread", RunAt: now, ScheduledFor: now,
	}
	payload, err := json.Marshal(payloadForScheduledJob(oneShot))
	if err != nil {
		t.Fatal(err)
	}
	pipe.HSet(ctx, scheduledJobPayloadsKey, oneShot.ID, payload)
	pipe.ZAdd(ctx, scheduledJobsKey, redis.Z{Score: float64(oneShot.RunAt.UnixMilli()), Member: oneShot.ID})
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 3; attempt++ {
		claimed, err := store.claimDue(ctx, now, 1, "runtime-a")
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) == 0 {
			continue
		}
		if claimed[0].ID != oneShot.ID {
			t.Fatalf("legacy cron was claimed while a one-shot remained queued: %#v", claimed)
		}
		return
	}
	t.Fatal("one-shot was not found while migrating the legacy cron backlog")
}

func TestValkeyScheduledJobClaimScanIsBounded(t *testing.T) {
	_, client, store := testScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	pipe := client.Pipeline()
	for index := 0; index <= scheduledClaimPageLimit*64; index++ {
		job := scheduledJob{
			ID:           fmt.Sprintf("one-shot-%04d", index),
			ProjectID:    "project-a",
			FunctionPath: "tasks.run",
			RunAt:        now,
			ScheduledFor: now,
		}
		payload, err := json.Marshal(payloadForScheduledJob(job))
		if err != nil {
			t.Fatal(err)
		}
		pipe.HSet(ctx, scheduledJobPayloadsKey, job.ID, payload)
		pipe.ZAdd(ctx, scheduledOneShotJobsKey, redis.Z{Score: float64(now.UnixMilli()), Member: job.ID})
		if index < scheduledClaimPageLimit*64 {
			pipe.Set(ctx, scheduledJobClaimPrefix+job.ID, "another-runtime", scheduledJobLease)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	claimed, err := store.claimDue(ctx, now, 1, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claim scan exceeded its page bound: %#v", claimed)
	}
}

func TestValkeyScheduledJobCompletionRequiresClaimOwnership(t *testing.T) {
	_, _, store := testScheduledJobStore(t)
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	job := scheduledJob{ID: "job-owned", ProjectID: "project-a", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now}
	if err := store.enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.claimDue(context.Background(), now, 1, "runtime-a")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if err := store.complete(context.Background(), job.ID, "not-the-claim-token"); err == nil {
		t.Fatal("completion by a non-owner unexpectedly succeeded")
	}
	if err := store.complete(context.Background(), job.ID, claimed[0].ClaimToken); err != nil {
		t.Fatalf("completion by claim owner failed: %v", err)
	}
}

func TestValkeyMigrationRefreshesExpiredMarkersBeforeOldRuntimeCompletion(t *testing.T) {
	_, client, store := testScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	job := scheduledJob{
		ID: "cron-marker-expired", ProjectID: "project-a", TenantID: "tenant-a",
		FunctionPath: "tasks.generate", CronName: "generate", RunAt: now, ScheduledFor: now,
	}
	if err := store.enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, scheduledJobDedupePrefix+job.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.refreshCompletionMarkers(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.claimDue(ctx, now, 1, "old-runtime")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("old claim = %#v, %v", claimed, err)
	}
	// Reproduce completion by the deployed pre-migration runtime: it removed
	// the queue payload and claim but did not refresh the marker itself.
	pipe := client.Pipeline()
	pipe.ZRem(ctx, scheduledOneShotJobsKey, job.ID)
	pipe.ZRem(ctx, scheduledCronJobsKey, job.ID)
	pipe.ZRem(ctx, scheduledJobsKey, job.ID)
	pipe.HDel(ctx, scheduledJobPayloadsKey, job.ID)
	pipe.Del(ctx, scheduledJobClaimPrefix+job.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	guard, err := store.guard(ctx, job.ID, "new-runtime:1")
	if err != nil {
		t.Fatal(err)
	}
	if guard != legacyScheduledJobCompleted {
		t.Fatalf("guard after old completion = %v, want completed", guard)
	}
}

func TestValkeyScheduledJobReclaimUsesANewFencingToken(t *testing.T) {
	redisServer, _, store := testScheduledJobStore(t)
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	job := scheduledJob{ID: "job-reclaimed", ProjectID: "project-a", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now}
	if err := store.enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	first, err := store.claimDue(context.Background(), now, 1, "runtime-a")
	if err != nil || len(first) != 1 || first[0].ClaimToken == "" {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	redisServer.FastForward(scheduledJobLease + time.Millisecond)
	second, err := store.claimDue(context.Background(), now, 1, "runtime-a")
	if err != nil || len(second) != 1 || second[0].ClaimToken == "" {
		t.Fatalf("second claim = %#v, %v", second, err)
	}
	if first[0].ClaimToken == second[0].ClaimToken {
		t.Fatal("reclaim reused the stale execution's fencing token")
	}
	if err := store.complete(context.Background(), job.ID, first[0].ClaimToken); err == nil {
		t.Fatal("stale execution completed the newer claim")
	}
	if err := store.complete(context.Background(), job.ID, second[0].ClaimToken); err != nil {
		t.Fatalf("new claim completion failed: %v", err)
	}
	if err := store.complete(context.Background(), job.ID, second[0].ClaimToken); err != nil {
		t.Fatalf("retrying an already-applied completion failed: %v", err)
	}
}

func TestValkeyScheduledJobDecodeFailureReleasesEarlierClaims(t *testing.T) {
	_, client, store := testScheduledJobStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"job-a", "job-b"} {
		if err := store.enqueue(ctx, scheduledJob{
			ID: id, ProjectID: "project-a", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.HSet(ctx, scheduledJobPayloadsKey, "job-b", "{invalid-json").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.claimDue(ctx, now, 2, "runtime-a"); err == nil {
		t.Fatal("malformed scheduled payload unexpectedly decoded")
	}
	reclaimed, err := store.claimDue(ctx, now, 1, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != "job-a" {
		t.Fatalf("valid earlier claim was not released: %#v", reclaimed)
	}
}

func TestValkeySchedulerExecutesJobEnqueuedByAnotherReplica(t *testing.T) {
	_, _, store := testScheduledJobStore(t)
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

	if _, err := producer.For("project-a", "tenant-a").RunAfter(0, "tasks.fromOtherReplica", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 1
	})
	mu.Lock()
	defer mu.Unlock()
	if seen[0] != "tasks.fromOtherReplica" {
		t.Fatalf("executed paths = %#v", seen)
	}
}

func TestDistributedIntervalScheduleUsesStableBoundaries(t *testing.T) {
	schedule := intervalSchedule{interval: time.Minute}
	first := schedule.Next(time.Date(2026, 8, 14, 0, 0, 2, 0, time.UTC))
	second := schedule.Next(time.Date(2026, 8, 14, 0, 0, 49, 0, time.UTC))
	want := time.Date(2026, 8, 14, 0, 1, 0, 0, time.UTC)
	if !first.Equal(want) || !second.Equal(want) {
		t.Fatalf("replicas derived %s and %s, want shared boundary %s", first, second, want)
	}
}
