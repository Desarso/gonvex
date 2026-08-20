package gonvex

import (
	"context"
	"fmt"
)

// ActionOutbox atomically records external work inside the current Reducer
// transaction. The runtime invokes the named Action after commit and retries
// failed deliveries from the durable row.
type ActionOutbox interface {
	Enqueue(ctx context.Context, actionPath string, args any) (string, error)
}

type actionOutboxUnavailable struct{}

func (actionOutboxUnavailable) Enqueue(context.Context, string, any) (string, error) {
	return "", fmt.Errorf("gonvex: ActionOutbox is only available inside a transactional Reducer")
}
func UnavailableActionOutbox() ActionOutbox { return actionOutboxUnavailable{} }
