package moduleengine

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

type recordingScheduler struct {
	afterDelay time.Duration
	at         time.Time
	path       string
	args       any
}

func (scheduler *recordingScheduler) RunAfter(delay time.Duration, path string, args any) (string, error) {
	scheduler.afterDelay = delay
	scheduler.path = path
	scheduler.args = args
	return "scheduled-after", nil
}

func (scheduler *recordingScheduler) RunAt(at time.Time, path string, args any) (string, error) {
	scheduler.at = at
	scheduler.path = path
	scheduler.args = args
	return "scheduled-at", nil
}

type recordingActionOutbox struct {
	path string
	args any
}

func (o *recordingActionOutbox) Enqueue(_ context.Context, path string, args any) (string, error) {
	o.path = path
	o.args = args
	return "outbox-1", nil
}

func TestReducerHostCallsEnqueueActionInCurrentTransaction(t *testing.T) {
	outbox := &recordingActionOutbox{}
	reducer := &gonvex.ReducerCtx{RuntimeContext: gonvex.RuntimeContext{
		Tx:     &sql.Tx{},
		Outbox: outbox,
	}}
	if granted := reducerCapabilities(reducer); !granted.ActionOutbox {
		t.Fatal("transactional reducer was not granted the durable Action outbox")
	}

	dispatcher := newReducerHostCalls(reducer)
	result, err := dispatcher.dispatch(context.Background(), hostCallPayload{
		Kind:     hostCallActionEnqueue,
		Function: "notifications.deliver",
		Args:     json.RawMessage(`{"notificationId":"notification-1"}`),
	})
	if err != nil {
		t.Fatalf("enqueue Action: %v", err)
	}
	if string(result) != `"outbox-1"` {
		t.Fatalf("outbox id = %s, want %q", result, "outbox-1")
	}
	if outbox.path != "notifications.deliver" {
		t.Fatalf("Action path = %q", outbox.path)
	}
	args, ok := outbox.args.(map[string]any)
	if !ok || args["notificationId"] != "notification-1" {
		t.Fatalf("Action args = %#v", outbox.args)
	}
}

func TestReducerWithoutTransactionCannotEnqueueAction(t *testing.T) {
	reducer := &gonvex.ReducerCtx{RuntimeContext: gonvex.RuntimeContext{Outbox: &recordingActionOutbox{}}}
	if granted := reducerCapabilities(reducer); granted.ActionOutbox {
		t.Fatal("non-transactional reducer was granted the durable Action outbox")
	}
}

func TestReducerHostCallsUseCurrentTransactionalScheduler(t *testing.T) {
	scheduler := &recordingScheduler{}
	reducer := &gonvex.ReducerCtx{RuntimeContext: gonvex.RuntimeContext{
		Tx:        &sql.Tx{},
		Scheduler: scheduler,
	}}
	if granted := reducerCapabilities(reducer); !granted.Scheduler {
		t.Fatal("transactional reducer was not granted the scheduler")
	}

	result, err := newReducerHostCalls(reducer).dispatch(context.Background(), hostCallPayload{
		Kind:     hostCallScheduleAfter,
		DelayMS:  2500,
		Function: "notifications.deliver",
		Args:     json.RawMessage(`{"notificationId":"notification-1"}`),
	})
	if err != nil {
		t.Fatalf("schedule after: %v", err)
	}
	if string(result) != `"scheduled-after"` {
		t.Fatalf("scheduled id = %s", result)
	}
	if scheduler.afterDelay != 2500*time.Millisecond || scheduler.path != "notifications.deliver" {
		t.Fatalf("scheduled call = (%s, %q)", scheduler.afterDelay, scheduler.path)
	}
}

func TestActionHostCallsCanScheduleAt(t *testing.T) {
	scheduler := &recordingScheduler{}
	runtimeCtx := &gonvex.RuntimeContext{Scheduler: scheduler}
	if granted := actionCapabilities(runtimeCtx); !granted.Scheduler {
		t.Fatal("action was not granted the scheduler")
	}

	const atUnixMS int64 = 1_800_000_000_123
	result, err := newActionHostCalls(runtimeCtx).dispatch(context.Background(), hostCallPayload{
		Kind:     hostCallScheduleAt,
		AtUnixMS: atUnixMS,
		Function: "reports.generate",
		Args:     json.RawMessage(`null`),
	})
	if err != nil {
		t.Fatalf("schedule at: %v", err)
	}
	if string(result) != `"scheduled-at"` || scheduler.at.UnixMilli() != atUnixMS {
		t.Fatalf("scheduled result = %s at %d", result, scheduler.at.UnixMilli())
	}
}

func TestQueryHostCallsRejectScheduler(t *testing.T) {
	query := &gonvex.QueryCtx{}
	if granted := queryCapabilities(query); granted.Scheduler {
		t.Fatal("query was granted the scheduler")
	}
	_, err := newQueryHostCalls(query).dispatch(context.Background(), hostCallPayload{
		Kind:     hostCallScheduleAfter,
		DelayMS:  1,
		Function: "jobs.run",
	})
	if err == nil {
		t.Fatal("query scheduler call unexpectedly succeeded")
	}
}
