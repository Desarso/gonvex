package moduleengine

import (
	"encoding/json"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

type compositeTestEngine struct {
	descriptors map[string]Descriptor
	queries     int
}

func (e *compositeTestEngine) Runtime() string { return "test" }
func (e *compositeTestEngine) Describe(path string) (Descriptor, bool) {
	d, ok := e.descriptors[path]
	return d, ok
}
func (e *compositeTestEngine) Descriptors() map[string]Descriptor { return e.descriptors }
func (e *compositeTestEngine) Crons() []gonvex.CronSpec           { return nil }
func (e *compositeTestEngine) InvokeQuery(*gonvex.QueryCtx, Invocation) (Result, error) {
	e.queries++
	return Result{Value: "query"}, nil
}
func (e *compositeTestEngine) InvokeReducer(*gonvex.ReducerCtx, Invocation) (Result, error) {
	return Result{}, nil
}
func (e *compositeTestEngine) InvokeInternalReducer(*gonvex.ReducerCtx, Invocation) (Result, error) {
	return Result{}, nil
}
func (e *compositeTestEngine) InvokeAction(*gonvex.ActionCtx, Invocation) (Result, error) {
	return Result{}, nil
}
func TestCompositeEngineRoutesUniquePaths(t *testing.T) {
	goEngine := &compositeTestEngine{descriptors: map[string]Descriptor{"go.read": {Path: "go.read", Kind: KindQuery}}}
	tsEngine := &compositeTestEngine{descriptors: map[string]Descriptor{"ts.read": {Path: "ts.read", Kind: KindQuery}}}
	composite, err := NewCompositeEngine(goEngine, tsEngine)
	if err != nil {
		t.Fatalf("NewCompositeEngine: %v", err)
	}
	if _, err := composite.InvokeQuery(nil, Invocation{Path: "ts.read", Args: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if tsEngine.queries != 1 || goEngine.queries != 0 {
		t.Fatalf("routing go=%d ts=%d", goEngine.queries, tsEngine.queries)
	}
}

func TestCompositeEngineRejectsDuplicatePaths(t *testing.T) {
	descriptor := Descriptor{Path: "shared", Kind: KindQuery}
	_, err := NewCompositeEngine(
		&compositeTestEngine{descriptors: map[string]Descriptor{"shared": descriptor}},
		&compositeTestEngine{descriptors: map[string]Descriptor{"shared": descriptor}},
	)
	if err == nil {
		t.Fatal("duplicate path was accepted")
	}
}
