package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
)

type actionReducerCaller struct {
	server   *Server
	project  string
	tenant   string
	caller   callerContext
	parent   string
	mu       sync.Mutex
	sequence uint64
}

func (r *actionReducerCaller) Call(ctx context.Context, path string, args any) (any, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("encode Reducer %q arguments: %w", path, err)
	}
	// An outbox Action is at-least-once. Derive the child command identity from
	// the durable outbox operation and call contents so replay after a crash
	// returns the original committed Reducer result instead of executing twice.
	r.mu.Lock()
	r.sequence++
	sequence := r.sequence
	r.mu.Unlock()
	key := actionReducerIdempotencyKey(r.parent, sequence, path, raw)
	ctx = withMutationID(ctx, key)
	ctx = withMutationIdempotency(ctx, key, r.caller.subject())
	return r.server.executeTenantMutationForCaller(ctx, r.project, r.tenant, r.caller, path, raw)
}

func actionReducerIdempotencyKey(parent string, sequence uint64, path string, raw json.RawMessage) string {
	prefix := fmt.Sprintf("%s\x00%d\x00%s\x00", parent, sequence, path)
	digest := sha256.Sum256(append(append([]byte(prefix), raw...), '\n'))
	return fmt.Sprintf("outbox:%x", digest[:])
}
