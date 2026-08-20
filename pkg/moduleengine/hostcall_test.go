package moduleengine

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

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
