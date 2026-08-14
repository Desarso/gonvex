package server

import (
	"context"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testScheduledJobStore(t *testing.T) (*miniredis.Miniredis, *redis.Client, scheduledJobStore) {
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
	if err := store.release(context.Background(), job.ID, "runtime-a"); err != nil {
		t.Fatal(err)
	}
	second, err = store.claimDue(context.Background(), now, 10, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != job.ID {
		t.Fatalf("released job was not reclaimed: %#v", second)
	}
	if err := store.complete(context.Background(), job.ID, "runtime-b"); err != nil {
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
