package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// deferredScheduler preserves Convex's commit ordering: jobs scheduled inside
// a mutation become visible to the scheduler only after that mutation commits.
// Without this buffer, a zero-delay job can race the transaction and observe
// the row that triggered it as missing.
type deferredScheduler struct {
	base gonvex.Scheduler
	now  func() time.Time
	seq  int
	jobs []deferredScheduledJob
}

type deferredScheduledJob struct {
	at           time.Time
	functionPath string
	args         json.RawMessage
}

func newDeferredScheduler(base gonvex.Scheduler) *deferredScheduler {
	return &deferredScheduler{base: base, now: time.Now}
}

func (scheduler *deferredScheduler) RunAfter(delay time.Duration, functionPath string, args any) (string, error) {
	if delay < 0 {
		delay = 0
	}
	return scheduler.RunAt(scheduler.now().Add(delay), functionPath, args)
}

func (scheduler *deferredScheduler) RunAt(at time.Time, functionPath string, args any) (string, error) {
	functionPath = strings.TrimSpace(functionPath)
	if functionPath == "" {
		return "", fmt.Errorf("scheduler: function path is required")
	}
	raw, err := encodeSchedulerArgs(args)
	if err != nil {
		return "", err
	}
	scheduler.seq++
	scheduler.jobs = append(scheduler.jobs, deferredScheduledJob{
		at:           at,
		functionPath: functionPath,
		args:         append(json.RawMessage(nil), raw...),
	})
	return fmt.Sprintf("deferred_job_%d", scheduler.seq), nil
}

func (scheduler *deferredScheduler) flush() error {
	for _, job := range scheduler.jobs {
		if _, err := scheduler.base.RunAt(job.at, job.functionPath, job.args); err != nil {
			return err
		}
	}
	scheduler.jobs = nil
	return nil
}
