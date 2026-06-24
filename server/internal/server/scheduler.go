package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

const (
	schedulerTickInterval   = 500 * time.Millisecond
	schedulerMaxConcurrent  = 16
	schedulerRecentRunLimit = 30
)

// scheduledExecutor runs a single scheduled job. The server provides an
// implementation that dispatches to the mutation or action execution path.
type scheduledExecutor func(ctx context.Context, job scheduledJob) error

// scheduledJob is one unit of deferred work: either a one-shot job enqueued via
// ctx.Scheduler, or a single fire of a registered cron.
type scheduledJob struct {
	ID           string
	ProjectID    string
	TenantID     string
	FunctionPath string
	Args         json.RawMessage
	RunAt        time.Time
	ScheduledFor time.Time
	CronName     string
}

type cronRegistration struct {
	ProjectID  string
	Spec       gonvex.CronSpec
	Schedule   cronSchedule
	NextRun    time.Time
	LastRun    time.Time
	LastStatus string
	Runs       int64
	Failures   int64
}

func (c *cronRegistration) key() string {
	return c.ProjectID + "\x00" + c.Spec.Name
}

type schedulerBucket struct {
	Completed  int64
	Failed     int64
	TotalLagMS float64
	LagSamples int64
	MaxRunning int
}

type schedulerRun struct {
	Time       string  `json:"time"`
	Project    string  `json:"project,omitempty"`
	Function   string  `json:"function"`
	Cron       string  `json:"cron,omitempty"`
	Outcome    string  `json:"outcome"`
	LagMS      float64 `json:"lagMs"`
	DurationMS float64 `json:"durationMs"`
	Error      string  `json:"error,omitempty"`
}

// scheduler is the in-process job runner that powers the dashboard's
// scheduler/concurrency panels. Jobs and crons live in memory; crons are
// re-derived from each project's compiled app on every manifest sync.
type scheduler struct {
	mu            sync.Mutex
	now           func() time.Time
	exec          scheduledExecutor
	maxConcurrent int
	logger        *slog.Logger

	jobs    []scheduledJob
	crons   map[string]*cronRegistration
	running int
	queued  int
	idSeq   int64

	completed int64
	failed    int64
	lastLagMS float64

	buckets map[int64]*schedulerBucket
	recent  []schedulerRun

	wake chan struct{}
}

func newScheduler(exec scheduledExecutor) *scheduler {
	return &scheduler{
		now:           time.Now,
		exec:          exec,
		maxConcurrent: schedulerMaxConcurrent,
		logger:        slog.Default().With("component", "scheduler"),
		crons:         map[string]*cronRegistration{},
		buckets:       map[int64]*schedulerBucket{},
		wake:          make(chan struct{}, 1),
	}
}

// For binds the scheduler to a project/tenant so functions can enqueue
// follow-up work through ctx.Scheduler.
func (sc *scheduler) For(projectID, tenantID string) gonvex.Scheduler {
	return schedulerHandle{sc: sc, projectID: projectID, tenantID: tenantID}
}

type schedulerHandle struct {
	sc        *scheduler
	projectID string
	tenantID  string
}

func (h schedulerHandle) RunAfter(delay time.Duration, functionPath string, args any) (string, error) {
	if delay < 0 {
		delay = 0
	}
	return h.RunAt(h.sc.now().Add(delay), functionPath, args)
}

func (h schedulerHandle) RunAt(at time.Time, functionPath string, args any) (string, error) {
	functionPath = strings.TrimSpace(functionPath)
	if functionPath == "" {
		return "", fmt.Errorf("scheduler: function path is required")
	}
	raw, err := encodeSchedulerArgs(args)
	if err != nil {
		return "", err
	}
	return h.sc.enqueue(scheduledJob{
		ProjectID:    h.projectID,
		TenantID:     h.tenantID,
		FunctionPath: functionPath,
		Args:         raw,
		RunAt:        at,
		ScheduledFor: at,
	}), nil
}

func encodeSchedulerArgs(args any) (json.RawMessage, error) {
	if args == nil {
		return nil, nil
	}
	if raw, ok := args.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(args)
}

func (sc *scheduler) enqueue(job scheduledJob) string {
	sc.mu.Lock()
	sc.idSeq++
	job.ID = fmt.Sprintf("job_%d", sc.idSeq)
	if job.ScheduledFor.IsZero() {
		job.ScheduledFor = job.RunAt
	}
	sc.jobs = append(sc.jobs, job)
	sc.mu.Unlock()
	sc.signal()
	return job.ID
}

func (sc *scheduler) signal() {
	select {
	case sc.wake <- struct{}{}:
	default:
	}
}

// syncCrons replaces the cron registrations for a project with the crons
// declared by its compiled app. Existing run statistics for unchanged crons are
// preserved so the dashboard history survives a resync.
func (sc *scheduler) syncCrons(projectID string, specs []gonvex.CronSpec) {
	now := sc.now()
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Drop existing crons for this project, remembering their stats.
	previous := map[string]*cronRegistration{}
	for key, reg := range sc.crons {
		if reg.ProjectID == projectID {
			previous[reg.Spec.Name] = reg
			delete(sc.crons, key)
		}
	}

	for _, spec := range specs {
		schedule, err := scheduleForSpec(spec)
		if err != nil {
			sc.logger.Warn("skip cron with invalid schedule", "project", projectID, "cron", spec.Name, "error", err)
			continue
		}
		reg := &cronRegistration{ProjectID: projectID, Spec: spec, Schedule: schedule}
		reg.NextRun = schedule.Next(now)
		if prior, ok := previous[spec.Name]; ok {
			reg.Runs = prior.Runs
			reg.Failures = prior.Failures
			reg.LastRun = prior.LastRun
			reg.LastStatus = prior.LastStatus
		}
		if reg.NextRun.IsZero() {
			sc.logger.Warn("cron has no upcoming run", "project", projectID, "cron", spec.Name)
			continue
		}
		sc.crons[reg.key()] = reg
	}
	sc.signal()
}

func scheduleForSpec(spec gonvex.CronSpec) (cronSchedule, error) {
	if strings.TrimSpace(spec.Expression) != "" {
		return parseCronExpr(spec.Expression)
	}
	if spec.Interval > 0 {
		return intervalSchedule{interval: spec.Interval}, nil
	}
	return nil, fmt.Errorf("cron %q has neither interval nor expression", spec.Name)
}

func (sc *scheduler) start(ctx context.Context) {
	go sc.run(ctx)
}

func (sc *scheduler) run(ctx context.Context) {
	ticker := time.NewTicker(schedulerTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-sc.wake:
		}
		sc.dispatchDue(ctx)
	}
}

func (sc *scheduler) dispatchDue(ctx context.Context) {
	now := sc.now()
	sc.mu.Lock()
	sc.observeRunningLocked(now)

	var due []scheduledJob
	remaining := sc.jobs[:0]
	for _, job := range sc.jobs {
		if job.RunAt.After(now) {
			remaining = append(remaining, job)
			continue
		}
		due = append(due, job)
	}
	sc.jobs = remaining

	for _, reg := range sc.crons {
		if reg.NextRun.IsZero() || reg.NextRun.After(now) {
			continue
		}
		due = append(due, sc.cronJobLocked(reg, now))
		reg.NextRun = reg.Schedule.Next(now)
	}

	sort.Slice(due, func(left, right int) bool {
		return due[left].ScheduledFor.Before(due[right].ScheduledFor)
	})

	var start []scheduledJob
	queued := 0
	for _, job := range due {
		if sc.running >= sc.maxConcurrent {
			// Backpressure: leave it due (RunAt is already in the past) so it
			// runs on the next tick. This shows up as queued + rising lag.
			sc.jobs = append(sc.jobs, job)
			queued++
			continue
		}
		sc.running++
		start = append(start, job)
	}
	sc.queued = queued
	sc.mu.Unlock()

	for _, job := range start {
		go sc.execute(ctx, job, now)
	}
}

func (sc *scheduler) cronJobLocked(reg *cronRegistration, now time.Time) scheduledJob {
	sc.idSeq++
	return scheduledJob{
		ID:           fmt.Sprintf("cron_%d", sc.idSeq),
		ProjectID:    reg.ProjectID,
		FunctionPath: reg.Spec.FunctionPath,
		Args:         reg.Spec.Args,
		RunAt:        reg.NextRun,
		ScheduledFor: reg.NextRun,
		CronName:     reg.Spec.Name,
	}
}

func (sc *scheduler) execute(ctx context.Context, job scheduledJob, dispatchedAt time.Time) {
	lag := dispatchedAt.Sub(job.ScheduledFor)
	if lag < 0 {
		lag = 0
	}
	start := sc.now()
	var err error
	if sc.exec != nil {
		err = sc.exec(ctx, job)
	} else {
		err = fmt.Errorf("scheduler executor not configured")
	}
	duration := sc.now().Sub(start)
	sc.finish(job, lag, duration, err)
}

func (sc *scheduler) finish(job scheduledJob, lag, duration time.Duration, err error) {
	now := sc.now()
	lagMS := float64(lag.Microseconds()) / 1000
	durationMS := float64(duration.Microseconds()) / 1000
	outcome := "ok"
	errorMessage := ""
	if err != nil {
		outcome = "error"
		errorMessage = err.Error()
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.running > 0 {
		sc.running--
	}
	sc.lastLagMS = lagMS
	bucket := sc.bucketLocked(now)
	bucket.TotalLagMS += lagMS
	bucket.LagSamples++
	if err != nil {
		sc.failed++
		bucket.Failed++
	} else {
		sc.completed++
		bucket.Completed++
	}

	if job.CronName != "" {
		if reg, ok := sc.crons[job.ProjectID+"\x00"+job.CronName]; ok {
			reg.Runs++
			reg.LastRun = now
			reg.LastStatus = outcome
			if err != nil {
				reg.Failures++
			}
		}
	}

	sc.recent = append(sc.recent, schedulerRun{
		Time:       now.Format(time.RFC3339Nano),
		Project:    job.ProjectID,
		Function:   job.FunctionPath,
		Cron:       job.CronName,
		Outcome:    outcome,
		LagMS:      lagMS,
		DurationMS: durationMS,
		Error:      errorMessage,
	})
	if len(sc.recent) > schedulerRecentRunLimit {
		sc.recent = sc.recent[len(sc.recent)-schedulerRecentRunLimit:]
	}
}

func (sc *scheduler) observeRunningLocked(now time.Time) {
	bucket := sc.bucketLocked(now)
	if sc.running > bucket.MaxRunning {
		bucket.MaxRunning = sc.running
	}
}

func (sc *scheduler) bucketLocked(now time.Time) *schedulerBucket {
	key := bucketKey(now)
	bucket := sc.buckets[key]
	if bucket == nil {
		bucket = &schedulerBucket{}
		sc.buckets[key] = bucket
	}
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for existing := range sc.buckets {
		if existing < oldest {
			delete(sc.buckets, existing)
		}
	}
	return bucket
}

type schedulerSnapshot struct {
	Running   int                     `json:"running"`
	Queued    int                     `json:"queued"`
	Scheduled int                     `json:"scheduled"`
	Completed int64                   `json:"completed"`
	Failed    int64                   `json:"failed"`
	LagMS     float64                 `json:"lagMs"`
	Crons     []schedulerCronSnapshot `json:"crons"`
	Recent    []schedulerRun          `json:"recent"`
	Series    []schedulerPoint        `json:"series"`
}

type schedulerCronSnapshot struct {
	Name     string `json:"name"`
	Project  string `json:"project,omitempty"`
	Function string `json:"function"`
	Schedule string `json:"schedule"`
	NextRun  string `json:"nextRun,omitempty"`
	LastRun  string `json:"lastRun,omitempty"`
	Status   string `json:"status,omitempty"`
	Runs     int64  `json:"runs"`
	Failures int64  `json:"failures"`
}

type schedulerPoint struct {
	Time       string  `json:"time"`
	Completed  int64   `json:"completed"`
	Failed     int64   `json:"failed"`
	AvgLagMS   float64 `json:"avgLagMs"`
	MaxRunning int     `json:"maxRunning"`
}

func (sc *scheduler) snapshot() schedulerSnapshot {
	now := sc.now()
	sc.mu.Lock()
	defer sc.mu.Unlock()

	crons := make([]schedulerCronSnapshot, 0, len(sc.crons))
	for _, reg := range sc.crons {
		nextRun := ""
		if !reg.NextRun.IsZero() {
			nextRun = reg.NextRun.Format(time.RFC3339Nano)
		}
		lastRun := ""
		if !reg.LastRun.IsZero() {
			lastRun = reg.LastRun.Format(time.RFC3339Nano)
		}
		crons = append(crons, schedulerCronSnapshot{
			Name:     reg.Spec.Name,
			Project:  reg.ProjectID,
			Function: reg.Spec.FunctionPath,
			Schedule: describeSchedule(reg.Spec),
			NextRun:  nextRun,
			LastRun:  lastRun,
			Status:   reg.LastStatus,
			Runs:     reg.Runs,
			Failures: reg.Failures,
		})
	}
	sort.Slice(crons, func(left, right int) bool { return crons[left].Name < crons[right].Name })

	recent := make([]schedulerRun, len(sc.recent))
	copy(recent, sc.recent)
	sort.Slice(recent, func(left, right int) bool { return recent[left].Time > recent[right].Time })

	return schedulerSnapshot{
		Running:   sc.running,
		Queued:    sc.queued,
		Scheduled: len(sc.jobs) + len(sc.crons),
		Completed: sc.completed,
		Failed:    sc.failed,
		LagMS:     sc.lastLagMS,
		Crons:     crons,
		Recent:    recent,
		Series:    sc.seriesLocked(now),
	}
}

func (sc *scheduler) seriesLocked(now time.Time) []schedulerPoint {
	points := make([]schedulerPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := sc.buckets[bucketStart.UnixMilli()]
		point := schedulerPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.Completed = bucket.Completed
			point.Failed = bucket.Failed
			point.MaxRunning = bucket.MaxRunning
			if bucket.LagSamples > 0 {
				point.AvgLagMS = bucket.TotalLagMS / float64(bucket.LagSamples)
			}
		}
		points = append(points, point)
	}
	return points
}

func describeSchedule(spec gonvex.CronSpec) string {
	if strings.TrimSpace(spec.Expression) != "" {
		return spec.Expression
	}
	return "every " + spec.Interval.String()
}
