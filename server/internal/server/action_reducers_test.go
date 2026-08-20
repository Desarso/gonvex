package server

import "testing"

func TestActionReducerIdempotencyKeyIsStableAndCallSpecific(t *testing.T) {
	first := actionReducerIdempotencyKey("outbox-1", 1, "tasks.finish", []byte(`{"id":"task-1"}`))
	replay := actionReducerIdempotencyKey("outbox-1", 1, "tasks.finish", []byte(`{"id":"task-1"}`))
	other := actionReducerIdempotencyKey("outbox-1", 2, "tasks.finish", []byte(`{"id":"task-1"}`))
	if first != replay {
		t.Fatalf("expected an Action replay to reuse its child Reducer key")
	}
	if first == other {
		t.Fatalf("expected distinct calls to receive distinct keys")
	}
}
