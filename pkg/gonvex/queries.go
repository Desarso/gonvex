package gonvex

import (
	"context"
	"fmt"
)

// QueryAPI is the host-owned bridge used by declared Action tools. It is not
// exposed directly to module JavaScript: moduleengine resolves the tool name
// to one signed internal Query before calling this interface.
type QueryAPI interface {
	Call(ctx context.Context, path string, args any) (any, error)
}

type queriesUnavailable struct{}

func (queriesUnavailable) Call(context.Context, string, any) (any, error) {
	return nil, fmt.Errorf("gonvex: Queries are only callable through a declared Action tool")
}

func UnavailableQueries() QueryAPI { return queriesUnavailable{} }
