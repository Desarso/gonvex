package gonvex

import (
	"encoding/json"
	"fmt"
	"time"
)

// Scheduler lets Reducers and Actions enqueue follow-up work that the runtime
// runs later, equivalent to the Convex scheduler. It is available on every
// RuntimeContext as ctx.Scheduler.
type Scheduler interface {
	// RunAfter schedules functionPath to run once after delay has elapsed.
	// It returns the scheduled job id.
	RunAfter(delay time.Duration, functionPath string, args any) (string, error)
	// RunAt schedules functionPath to run once at the given time.
	RunAt(at time.Time, functionPath string, args any) (string, error)
}

// CronSpec is the host-side recurring-job contract emitted by a module artifact.
// Exactly one of Interval or Expression is set. PerTenant selects one schedule
// for each tenant in the project.
type CronSpec struct {
	Name         string
	FunctionPath string
	Interval     time.Duration
	Expression   string
	Args         json.RawMessage
	PerTenant    bool
}

// schedulerUnavailable is the default ctx.Scheduler used when a function runs
// outside the runtime (e.g. unit tests). It fails loudly rather than silently
// dropping scheduled work.
type schedulerUnavailable struct{}

func (schedulerUnavailable) RunAfter(time.Duration, string, any) (string, error) {
	return "", fmt.Errorf("gonvex: scheduler is not available in this context")
}

func (schedulerUnavailable) RunAt(time.Time, string, any) (string, error) {
	return "", fmt.Errorf("gonvex: scheduler is not available in this context")
}
