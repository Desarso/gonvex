package gonvex

import (
	"context"
	"fmt"
)

// ReducerAPI is the only durable-state bridge exposed to Actions.
type ReducerAPI interface {
	Call(ctx context.Context, path string, args any) (any, error)
}

type reducersUnavailable struct{}

func (reducersUnavailable) Call(context.Context, string, any) (any, error) {
	return nil, fmt.Errorf("gonvex: Reducers are only callable from an Action context")
}

func UnavailableReducers() ReducerAPI { return reducersUnavailable{} }
