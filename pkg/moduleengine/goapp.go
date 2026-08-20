package moduleengine

import (
	"fmt"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// GoRuntime is the Runtime() name reported by compiled Go modules.
const GoRuntime = "go"

// GoAppEngine adapts a compiled Go module — the *gonvex.App a project bundle
// registers through its plugin Register symbol — to ModuleEngine. It owns no
// state: lookup, argument decoding, dispatch and error shaping all stay in
// pkg/gonvex, so wrapping an App changes nothing about how it executes.
type GoAppEngine struct {
	app *gonvex.App
}

var _ ModuleEngine = (*GoAppEngine)(nil)

// NewGoAppEngine wraps app. Every method tolerates a nil engine or nil App and
// behaves like a module with no registered functions, but callers that decide
// whether a project has a module loaded should return an untyped nil
// ModuleEngine rather than a wrapped nil, so `engine != nil` keeps meaning
// "a module is loaded".
func NewGoAppEngine(app *gonvex.App) *GoAppEngine {
	return &GoAppEngine{app: app}
}

// App returns the wrapped application. It preserves compatibility for host code
// and tests that still need the Go type directly; engines for other languages
// have no equivalent, which is why it is not part of ModuleEngine.
func (e *GoAppEngine) App() *gonvex.App {
	if e == nil {
		return nil
	}
	return e.app
}

func (e *GoAppEngine) Runtime() string { return GoRuntime }

func (e *GoAppEngine) Describe(path string) (Descriptor, bool) {
	if e == nil || e.app == nil {
		return Descriptor{}, false
	}
	function, ok := e.app.Lookup(path)
	if !ok {
		return Descriptor{}, false
	}
	return describeFunction(function), true
}

func (e *GoAppEngine) Descriptors() map[string]Descriptor {
	if e == nil || e.app == nil {
		return map[string]Descriptor{}
	}
	functions := e.app.Functions()
	descriptors := make(map[string]Descriptor, len(functions))
	for path, function := range functions {
		descriptors[path] = describeFunction(function)
	}
	return descriptors
}

func (e *GoAppEngine) Crons() []gonvex.CronSpec {
	if e == nil || e.app == nil {
		return nil
	}
	return e.app.Crons()
}

func (e *GoAppEngine) InvokeQuery(ctx *gonvex.QueryCtx, call Invocation) (Result, error) {
	if e == nil || e.app == nil {
		return Result{}, notRegistered(call.Path)
	}
	value, err := e.app.ExecuteQuery(ctx, call.Path, call.Args)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func (e *GoAppEngine) InvokeReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error) {
	if e == nil || e.app == nil {
		return Result{}, notRegistered(call.Path)
	}
	value, err := e.app.ExecuteReducer(ctx, call.Path, call.Args)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func (e *GoAppEngine) InvokeInternalReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error) {
	if e == nil || e.app == nil {
		return Result{}, notRegistered(call.Path)
	}
	value, err := e.app.ExecuteInternalReducer(ctx, call.Path, call.Args)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func (e *GoAppEngine) InvokeAction(ctx *gonvex.ActionCtx, call Invocation) (Result, error) {
	if e == nil || e.app == nil {
		return Result{}, notRegistered(call.Path)
	}
	value, err := e.app.ExecuteAction(ctx, call.Path, call.Args)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func describeFunction(function gonvex.Function) Descriptor {
	return Descriptor{
		Path:         function.Path,
		Kind:         Kind(function.Kind),
		Internal:     function.Internal,
		Delivery:     function.Delivery,
		Dependencies: function.Dependencies,
		Replica:      function.Replica,
	}
}

// notRegistered mirrors the dispatch error pkg/gonvex returns for an unknown
// path so an engine without a module fails exactly like an empty one.
func notRegistered(path string) error {
	return &gonvex.DispatchError{
		Code:    "not_found",
		Path:    path,
		Message: fmt.Sprintf("function %q is not registered", path),
	}
}
