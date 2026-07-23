package gonvex

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Scheduler lets mutations and actions enqueue follow-up work that the runtime
// runs later, equivalent to the Convex scheduler. It is available on every
// RuntimeContext as ctx.Scheduler.
type Scheduler interface {
	// RunAfter schedules functionPath to run once after delay has elapsed.
	// It returns the scheduled job id.
	RunAfter(delay time.Duration, functionPath string, args any) (string, error)
	// RunAt schedules functionPath to run once at the given time.
	RunAt(at time.Time, functionPath string, args any) (string, error)
}

// CronSpec describes a recurring job registered by a project. Exactly one of
// Interval or Expression is set: Interval for app.Cron, Expression (a standard
// 5-field cron string) for app.CronExpr.
type CronSpec struct {
	Name         string
	FunctionPath string
	Interval     time.Duration
	Expression   string
	Args         json.RawMessage
	PerTenant    bool
}

// Cron registers a recurring job that runs functionPath every interval. The
// referenced function must be a mutation or action. args is encoded once at
// registration time and replayed on every run.
func (a *App) Cron(name string, interval time.Duration, functionPath string, args any) {
	a.registerCron(CronSpec{Name: name, Interval: interval, FunctionPath: functionPath, Args: encodeCronArgs(name, args)})
}

// TenantCron registers a recurring job that runs once for every tenant in the
// project. Each invocation is bound to that tenant's database and TenantID.
func (a *App) TenantCron(name string, interval time.Duration, functionPath string, args any) {
	a.registerCron(CronSpec{
		Name:         name,
		Interval:     interval,
		FunctionPath: functionPath,
		Args:         encodeCronArgs(name, args),
		PerTenant:    true,
	})
}

// CronExpr registers a recurring job driven by a standard 5-field cron
// expression (minute hour day-of-month month day-of-week).
func (a *App) CronExpr(name string, expression string, functionPath string, args any) {
	a.registerCron(CronSpec{Name: name, Expression: expression, FunctionPath: functionPath, Args: encodeCronArgs(name, args)})
}

// TenantCronExpr is TenantCron driven by a standard 5-field cron expression.
func (a *App) TenantCronExpr(name string, expression string, functionPath string, args any) {
	a.registerCron(CronSpec{
		Name:         name,
		Expression:   expression,
		FunctionPath: functionPath,
		Args:         encodeCronArgs(name, args),
		PerTenant:    true,
	})
}

// Crons returns a copy of every cron registered on the app.
func (a *App) Crons() []CronSpec {
	a.mu.RLock()
	defer a.mu.RUnlock()
	crons := make([]CronSpec, len(a.crons))
	copy(crons, a.crons)
	return crons
}

func (a *App) registerCron(spec CronSpec) {
	spec.Name = strings.TrimSpace(spec.Name)
	spec.FunctionPath = strings.TrimSpace(spec.FunctionPath)
	if spec.Name == "" {
		panic("gonvex: cron name is required")
	}
	if spec.FunctionPath == "" {
		panic(fmt.Sprintf("gonvex: cron %q requires a function path", spec.Name))
	}
	if spec.Interval <= 0 && strings.TrimSpace(spec.Expression) == "" {
		panic(fmt.Sprintf("gonvex: cron %q requires an interval or expression", spec.Name))
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, existing := range a.crons {
		if existing.Name == spec.Name {
			panic(fmt.Sprintf("gonvex: cron %q already registered", spec.Name))
		}
	}
	a.crons = append(a.crons, spec)
}

func encodeCronArgs(name string, args any) json.RawMessage {
	if args == nil {
		return nil
	}
	if raw, ok := args.(json.RawMessage); ok {
		return raw
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		panic(fmt.Sprintf("gonvex: cron %q args are not JSON-encodable: %v", name, err))
	}
	return encoded
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
