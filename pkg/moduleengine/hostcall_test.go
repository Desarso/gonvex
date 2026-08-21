package moduleengine

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	declared := ActionCapabilities{Scheduler: true}
	granted, _, _, err := actionCapabilities(runtimeCtx, Descriptor{Path: "reports.run", Kind: KindAction, ActionCapabilities: declared})
	if err != nil || !granted.Scheduler {
		t.Fatal("action was not granted the scheduler")
	}

	const atUnixMS int64 = 1_800_000_000_123
	result, err := newActionHostCalls(runtimeCtx, declared).dispatch(context.Background(), hostCallPayload{
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

type recordingQueryAPI struct {
	path string
	args any
}

func (q *recordingQueryAPI) Call(_ context.Context, path string, args any) (any, error) {
	q.path, q.args = path, args
	return map[string]any{"ok": true}, nil
}

func TestAgentActionInvokesOnlyDeclaredQueryTool(t *testing.T) {
	queries := &recordingQueryAPI{}
	runtimeCtx := &gonvex.RuntimeContext{Queries: queries, AgentActionsEnabled: true}
	declared := ActionCapabilities{Tools: map[string]ActionToolBinding{
		"searchTasks": {Kind: KindQuery, Function: "agents.searchTasks"},
	}}
	result, err := newActionHostCalls(runtimeCtx, declared).dispatch(context.Background(), hostCallPayload{
		Kind: hostCallToolInvoke,
		Tool: "searchTasks",
		Args: json.RawMessage(`{"search":"freezer"}`),
	})
	if err != nil {
		t.Fatalf("invoke declared tool: %v", err)
	}
	if queries.path != "agents.searchTasks" || string(result) != `{"ok":true}` {
		t.Fatalf("query call = %q %#v, result = %s", queries.path, queries.args, result)
	}
	if _, err := newActionHostCalls(runtimeCtx, declared).dispatch(context.Background(), hostCallPayload{Kind: hostCallToolInvoke, Tool: "anythingElse"}); err == nil {
		t.Fatal("undeclared tool unexpectedly succeeded")
	}
}

func TestAgentActionRequiresOperatorEnablement(t *testing.T) {
	_, _, _, err := actionCapabilities(&gonvex.RuntimeContext{}, Descriptor{Path: "agents.run", Kind: KindAction, ActionProfile: "agent"})
	if err == nil {
		t.Fatal("agent action was enabled without operator opt-in")
	}
}

func TestActionFetchRequiresAnExactDeclaredOrigin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(server.Close)
	payload, _ := json.Marshal(map[string]any{"url": server.URL + "/v1/test", "method": "GET"})
	if _, err := runFetch(context.Background(), payload, nil); err == nil {
		t.Fatal("fetch without a declared origin unexpectedly succeeded")
	}
	if _, err := runFetch(context.Background(), payload, []string{server.URL}); err != nil {
		t.Fatalf("fetch to declared origin: %v", err)
	}
}

func TestActionHostCallsRecheckDeclaredCapabilities(t *testing.T) {
	dispatcher := newActionHostCalls(&gonvex.RuntimeContext{
		Scheduler: &recordingScheduler{},
	}, ActionCapabilities{})
	for _, call := range []hostCallPayload{
		{Kind: hostCallScheduleAfter, Function: "jobs.run", DelayMS: 1},
		{Kind: hostCallFetch, Request: json.RawMessage(`{"url":"https://example.test"}`)},
		{Kind: hostCallStorage, Operation: "getMetadata", Payload: json.RawMessage(`{}`)},
	} {
		if _, err := dispatcher.dispatch(context.Background(), call); err == nil {
			t.Fatalf("undeclared Action capability %q unexpectedly reached its host service", call.Kind)
		}
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
