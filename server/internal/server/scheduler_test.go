package server

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

type schedulerFinalizationStore struct {
	mu            sync.Mutex
	completeCalls int
	releaseCalls  int
	completeErr   error
}

func (store *schedulerFinalizationStore) enqueue(context.Context, scheduledJob) error { return nil }
func (store *schedulerFinalizationStore) claimDue(context.Context, time.Time, int, string) ([]scheduledJob, error) {
	return nil, nil
}
func (store *schedulerFinalizationStore) renew(context.Context, string, string) (bool, error) {
	return true, nil
}
func (store *schedulerFinalizationStore) complete(context.Context, string, string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.completeCalls++
	return store.completeErr
}
func (store *schedulerFinalizationStore) release(context.Context, string, string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.releaseCalls++
	return nil
}

func TestParseCronExprNext(t *testing.T) {
	mustParse := func(expr string) exprSchedule {
		schedule, err := parseCronExpr(expr)
		if err != nil {
			t.Fatalf("parse %q: %v", expr, err)
		}
		return schedule
	}

	// Every minute.
	base := time.Date(2026, 6, 24, 10, 15, 30, 0, time.UTC)
	if got := mustParse("* * * * *").Next(base); !got.Equal(time.Date(2026, 6, 24, 10, 16, 0, 0, time.UTC)) {
		t.Fatalf("every-minute next = %s", got)
	}

	// Top of every hour.
	if got := mustParse("0 * * * *").Next(base); !got.Equal(time.Date(2026, 6, 24, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("hourly next = %s", got)
	}

	// 09:30 daily, evaluated after that time, rolls to the next day.
	after := time.Date(2026, 6, 24, 9, 31, 0, 0, time.UTC)
	if got := mustParse("30 9 * * *").Next(after); !got.Equal(time.Date(2026, 6, 25, 9, 30, 0, 0, time.UTC)) {
		t.Fatalf("daily next = %s", got)
	}

	// Step values: every 15 minutes.
	if got := mustParse("*/15 * * * *").Next(base); !got.Equal(time.Date(2026, 6, 24, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("step next = %s", got)
	}
}

func TestParseCronExprRejectsBadInput(t *testing.T) {
	for _, expr := range []string{"", "* * * *", "60 * * * *", "* 25 * * *", "* * * * 9", "a * * * *"} {
		if _, err := parseCronExpr(expr); err == nil {
			t.Fatalf("expected error for %q", expr)
		}
	}
}

func TestSchedulerRunsCronAndTracksMetrics(t *testing.T) {
	var runs int64
	done := make(chan struct{})
	sc := newScheduler(func(ctx context.Context, job scheduledJob) error {
		if atomic.AddInt64(&runs, 1) == 1 {
			close(done)
		}
		return nil
	})

	// A cron whose next fire is already due, so the first tick runs it.
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	sc.now = func() time.Time { return now }
	sc.syncCrons("project-a", []gonvex.CronSpec{{
		Name:         "heartbeat",
		Interval:     time.Minute,
		FunctionPath: "system.heartbeat",
	}})
	// Force the cron due by rewinding its next run into the past.
	sc.mu.Lock()
	for _, reg := range sc.crons {
		reg.NextRun = now.Add(-time.Second)
	}
	sc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.start(ctx)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cron did not run within timeout")
	}

	// Let the in-flight job finish recording.
	waitFor(t, func() bool {
		snapshot := sc.snapshot()
		return snapshot.Completed >= 1
	})

	snapshot := sc.snapshot()
	if snapshot.Completed < 1 {
		t.Fatalf("expected at least one completed run, got %+v", snapshot)
	}
	if len(snapshot.Crons) != 1 || snapshot.Crons[0].Name != "heartbeat" {
		t.Fatalf("expected heartbeat cron in snapshot, got %+v", snapshot.Crons)
	}
	if snapshot.Crons[0].Runs < 1 {
		t.Fatalf("expected cron run count to advance, got %+v", snapshot.Crons[0])
	}
}

func TestSchedulerExpandsTenantCronIntoTenantBoundJobs(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	sc := newScheduler(func(ctx context.Context, job scheduledJob) error {
		mu.Lock()
		seen = append(seen, job.TenantID)
		mu.Unlock()
		return nil
	})

	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	sc.now = func() time.Time { return now }
	sc.syncCrons("project-a", []gonvex.CronSpec{{
		Name:         "generate due workplans",
		Interval:     time.Minute,
		FunctionPath: "workplans.generateDueWorkplans",
		PerTenant:    true,
	}}, "tenant-b", "tenant-a", "tenant-a")

	sc.mu.Lock()
	if len(sc.crons) != 2 {
		sc.mu.Unlock()
		t.Fatalf("tenant cron registrations = %d, want 2", len(sc.crons))
	}
	for _, reg := range sc.crons {
		reg.NextRun = now.Add(-time.Second)
	}
	sc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.start(ctx)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 2
	})

	mu.Lock()
	defer mu.Unlock()
	sort.Strings(seen)
	if seen[0] != "tenant-a" || seen[1] != "tenant-b" {
		t.Fatalf("tenant cron jobs ran for %#v", seen)
	}
}

func TestSchedulerDeterministicallyStaggersTenantCrons(t *testing.T) {
	sc := newScheduler(nil)
	now := time.Date(2026, 8, 14, 22, 0, 0, 0, time.UTC)
	sc.now = func() time.Time { return now }
	specs := []gonvex.CronSpec{{
		Name: "reconcile recurrences", Interval: time.Minute,
		FunctionPath: "workplans.reconcileSchedules", PerTenant: true,
	}}
	tenants := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}
	sc.syncCrons("project-a", specs, tenants...)

	first := map[string]time.Time{}
	unique := map[time.Time]bool{}
	for _, reg := range sc.crons {
		first[reg.TenantID] = reg.NextRun
		unique[reg.NextRun] = true
		if reg.NextRun.Before(now) || !reg.NextRun.Before(now.Add(time.Minute)) {
			t.Fatalf("tenant %s next run %s is outside the one-minute window", reg.TenantID, reg.NextRun)
		}
	}
	if len(unique) < 2 {
		t.Fatalf("tenant cron occurrences are aligned at %v", unique)
	}

	sc.syncCrons("project-a", specs, tenants...)
	for _, reg := range sc.crons {
		if reg.NextRun != first[reg.TenantID] {
			t.Fatalf("tenant %s stagger changed from %s to %s", reg.TenantID, first[reg.TenantID], reg.NextRun)
		}
	}
}

func TestStaggeredCronKeepsCanonicalOccurrenceIdentityAndMeasuresActualLag(t *testing.T) {
	canonical := time.Date(2026, 8, 14, 22, 1, 0, 0, time.UTC)
	offset := 20 * time.Second
	actual := canonical.Add(offset)
	reg := &cronRegistration{
		ProjectID: "project-a",
		TenantID:  "tenant-a",
		Spec: gonvex.CronSpec{
			Name: "reconcile recurrences", FunctionPath: "workplans.reconcileSchedules", PerTenant: true,
		},
		Schedule: offsetCronSchedule{base: intervalSchedule{interval: time.Minute}, offset: offset},
		NextRun:  actual,
	}
	sc := newScheduler(func(context.Context, scheduledJob) error { return nil })
	job := sc.cronJobLocked(reg, actual)

	if !job.RunAt.Equal(actual) {
		t.Fatalf("staggered runAt = %s, want %s", job.RunAt, actual)
	}
	if !job.ScheduledFor.Equal(canonical) {
		t.Fatalf("logical occurrence = %s, want canonical %s", job.ScheduledFor, canonical)
	}
	oldRuntimeJob := job
	oldRuntimeJob.RunAt = canonical
	oldRuntimeJob.ScheduledFor = canonical
	if deterministicCronJobID(job) != deterministicCronJobID(oldRuntimeJob) {
		t.Fatalf("stagger changed deterministic cron identity: %q != %q", deterministicCronJobID(job), deterministicCronJobID(oldRuntimeJob))
	}
	sc.crons[reg.key()] = reg
	sc.advancePersistedCron(job, actual)
	if !reg.NextRun.Equal(actual.Add(time.Minute)) {
		t.Fatalf("staggered cron advanced to %s, want %s", reg.NextRun, actual.Add(time.Minute))
	}

	sc.running = 1
	sc.now = func() time.Time { return actual }
	sc.execute(context.Background(), job, actual)
	if sc.lastLagMS != 0 {
		t.Fatalf("intentional stagger counted as scheduler lag: %.2fms", sc.lastLagMS)
	}
}

func TestSchedulerRunAfterEnqueuesOneShot(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	sc := newScheduler(func(ctx context.Context, job scheduledJob) error {
		mu.Lock()
		seen = append(seen, job.FunctionPath)
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.start(ctx)

	handle := sc.For("project-a", "tenant-a")
	id, err := handle.RunAfter(10*time.Millisecond, "emails.sendReminder", map[string]any{"to": "a@example.com"})
	if err != nil {
		t.Fatalf("RunAfter: %v", err)
	}
	if id == "" {
		t.Fatal("expected a job id")
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 1 && seen[0] == "emails.sendReminder"
	})
}

func TestInMemorySchedulerPrioritizesOneShotsOverCronBacklog(t *testing.T) {
	now := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	started := make(chan scheduledJob, 1)
	sc := newScheduler(func(_ context.Context, job scheduledJob) error {
		started <- job
		return nil
	})
	sc.now = func() time.Time { return now }
	sc.maxConcurrent = 1
	sc.jobs = append(sc.jobs,
		scheduledJob{
			ID: "cron-overdue", ProjectID: "project-a", TenantID: "tenant-a",
			FunctionPath: "tasks.generateDueRecurringTasks", CronName: "generate due recurring tasks",
			RunAt: now.Add(-time.Hour), ScheduledFor: now.Add(-time.Hour),
		},
		scheduledJob{
			ID: "job-assistant", ProjectID: "project-a", TenantID: "tenant-a",
			FunctionPath: "assistant.processThread", RunAt: now, ScheduledFor: now,
		},
	)

	sc.dispatchDue(context.Background())
	select {
	case job := <-started:
		if job.ID != "job-assistant" {
			t.Fatalf("one-shot job was starved behind cron backlog: %#v", job)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not dispatch a due job")
	}
}

func TestSchedulerHandleEncodesArgs(t *testing.T) {
	var mu sync.Mutex
	var captured json.RawMessage
	sc := newScheduler(func(ctx context.Context, job scheduledJob) error {
		mu.Lock()
		captured = job.Args
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.start(ctx)

	if _, err := sc.For("p", "t").RunAfter(0, "x.y", map[string]int{"n": 3}); err != nil {
		t.Fatalf("RunAfter: %v", err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(captured) > 0
	})
	mu.Lock()
	defer mu.Unlock()
	if string(captured) != `{"n":3}` {
		t.Fatalf("unexpected encoded args: %s", string(captured))
	}
}

func TestSchedulerCompletesSuccessfulJobWhenParentContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &schedulerFinalizationStore{}
	sc := newScheduler(func(context.Context, scheduledJob) error {
		cancel()
		return nil
	})
	sc.store = store
	sc.running = 1
	now := time.Now()
	sc.execute(ctx, scheduledJob{
		ID: "job-success", ClaimToken: "claim-1", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now,
	}, now)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.completeCalls != 1 || store.releaseCalls != 0 {
		t.Fatalf("complete calls = %d, release calls = %d", store.completeCalls, store.releaseCalls)
	}
	if snapshot := sc.snapshot(); snapshot.Completed != 1 || snapshot.Failed != 0 {
		t.Fatalf("successful canceled-parent execution recorded as %+v", snapshot)
	}
}

func TestSchedulerCompletesFailedJobWithoutRetryingForever(t *testing.T) {
	store := &schedulerFinalizationStore{}
	sc := newScheduler(func(context.Context, scheduledJob) error {
		return errors.New("authentication required")
	})
	sc.store = store
	sc.running = 1
	now := time.Now()
	sc.execute(context.Background(), scheduledJob{
		ID: "job-terminal-error", ClaimToken: "claim-1", FunctionPath: "assistant.processThread", RunAt: now, ScheduledFor: now,
	}, now)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.completeCalls != 1 || store.releaseCalls != 0 {
		t.Fatalf("complete calls = %d, release calls = %d", store.completeCalls, store.releaseCalls)
	}
	if snapshot := sc.snapshot(); snapshot.Completed != 0 || snapshot.Failed != 1 {
		t.Fatalf("failed execution recorded as %+v", snapshot)
	}
}

func TestSchedulerCompletesDeterministicFailureEvenWhenParentIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &schedulerFinalizationStore{}
	sc := newScheduler(func(context.Context, scheduledJob) error {
		cancel()
		return errors.New("authentication required")
	})
	sc.store = store
	sc.running = 1
	now := time.Now()
	sc.execute(ctx, scheduledJob{
		ID: "job-terminal-race", ClaimToken: "claim-1", FunctionPath: "assistant.processThread", RunAt: now, ScheduledFor: now,
	}, now)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.completeCalls != 1 || store.releaseCalls != 0 {
		t.Fatalf("complete calls = %d, release calls = %d", store.completeCalls, store.releaseCalls)
	}
}

func TestSchedulerReleasesInterruptedFailedJobForRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &schedulerFinalizationStore{}
	sc := newScheduler(func(ctx context.Context, _ scheduledJob) error { return ctx.Err() })
	sc.store = store
	sc.running = 1
	now := time.Now()
	sc.execute(ctx, scheduledJob{
		ID: "job-interrupted", ClaimToken: "claim-1", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now,
	}, now)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.completeCalls != 0 || store.releaseCalls != 1 {
		t.Fatalf("complete calls = %d, release calls = %d", store.completeCalls, store.releaseCalls)
	}
}

func TestSchedulerRetriesAndRecordsCompletionFailure(t *testing.T) {
	store := &schedulerFinalizationStore{completeErr: errors.New("database unavailable")}
	sc := newScheduler(func(context.Context, scheduledJob) error { return nil })
	sc.store = store
	sc.running = 1
	now := time.Now()
	sc.execute(context.Background(), scheduledJob{
		ID: "job-finalize-failure", ClaimToken: "claim-1", FunctionPath: "tasks.run", RunAt: now, ScheduledFor: now,
	}, now)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.completeCalls != 3 || store.releaseCalls != 0 {
		t.Fatalf("complete calls = %d, release calls = %d", store.completeCalls, store.releaseCalls)
	}
	if snapshot := sc.snapshot(); snapshot.Completed != 0 || snapshot.Failed != 1 {
		t.Fatalf("completion failure recorded as %+v", snapshot)
	}
}

func TestSchedulerRejectsInvalidTargetBeforePersistence(t *testing.T) {
	sc := newScheduler(nil)
	sc.validateTarget = func(projectID, functionPath string) error {
		if projectID != "project-a" || functionPath != "reports.read" {
			t.Fatalf("validation input = %q %q", projectID, functionPath)
		}
		return errors.New("query targets are forbidden")
	}
	if _, err := sc.For("project-a", "tenant-a").RunAfter(0, "reports.read", map[string]any{}); err == nil {
		t.Fatal("expected target validation error")
	}
	if len(sc.jobs) != 0 {
		t.Fatal("invalid target reached the scheduler queue")
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
