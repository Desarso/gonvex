package server

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

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
