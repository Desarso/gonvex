package moduleengine

import (
	"fmt"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// CompositeEngine routes one project's functions across its compiled Go
// module and its language-neutral module artifact. Paths must be unique across
// both engines; silently shadowing a function would make deployment order part
// of application semantics.
type CompositeEngine struct {
	goEngine     ModuleEngine
	moduleEngine ModuleEngine
	descriptors  map[string]Descriptor
}

var _ ModuleEngine = (*CompositeEngine)(nil)

func NewCompositeEngine(goEngine, moduleEngine ModuleEngine) (*CompositeEngine, error) {
	if goEngine == nil && moduleEngine == nil {
		return nil, nil
	}
	result := &CompositeEngine{
		goEngine: goEngine, moduleEngine: moduleEngine,
		descriptors: map[string]Descriptor{},
	}
	for _, engine := range []ModuleEngine{goEngine, moduleEngine} {
		if engine == nil {
			continue
		}
		for path, descriptor := range engine.Descriptors() {
			if _, exists := result.descriptors[path]; exists {
				return nil, fmt.Errorf("duplicate module function path %q across Go and TypeScript modules", path)
			}
			result.descriptors[path] = descriptor
		}
	}
	return result, nil
}

func (e *CompositeEngine) Runtime() string { return "go+module" }

func (e *CompositeEngine) Describe(path string) (Descriptor, bool) {
	if e == nil {
		return Descriptor{}, false
	}
	descriptor, ok := e.descriptors[path]
	return descriptor, ok
}

func (e *CompositeEngine) Descriptors() map[string]Descriptor {
	if e == nil {
		return map[string]Descriptor{}
	}
	result := make(map[string]Descriptor, len(e.descriptors))
	for path, descriptor := range e.descriptors {
		result[path] = descriptor
	}
	return result
}

func (e *CompositeEngine) Crons() []gonvex.CronSpec {
	if e == nil {
		return nil
	}
	var result []gonvex.CronSpec
	if e.goEngine != nil {
		result = append(result, e.goEngine.Crons()...)
	}
	if e.moduleEngine != nil {
		result = append(result, e.moduleEngine.Crons()...)
	}
	return result
}

func (e *CompositeEngine) engine(path string) (ModuleEngine, error) {
	if e == nil {
		return nil, notRegistered(path)
	}
	if _, ok := e.descriptors[path]; !ok {
		return nil, notRegistered(path)
	}
	if e.goEngine != nil {
		if _, ok := e.goEngine.Describe(path); ok {
			return e.goEngine, nil
		}
	}
	return e.moduleEngine, nil
}

func (e *CompositeEngine) InvokeQuery(ctx *gonvex.QueryCtx, call Invocation) (Result, error) {
	engine, err := e.engine(call.Path)
	if err != nil {
		return Result{}, err
	}
	return engine.InvokeQuery(ctx, call)
}
func (e *CompositeEngine) InvokeReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error) {
	engine, err := e.engine(call.Path)
	if err != nil {
		return Result{}, err
	}
	return engine.InvokeReducer(ctx, call)
}
func (e *CompositeEngine) InvokeInternalReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error) {
	engine, err := e.engine(call.Path)
	if err != nil {
		return Result{}, err
	}
	return engine.InvokeInternalReducer(ctx, call)
}
func (e *CompositeEngine) InvokeAction(ctx *gonvex.ActionCtx, call Invocation) (Result, error) {
	engine, err := e.engine(call.Path)
	if err != nil {
		return Result{}, err
	}
	return engine.InvokeAction(ctx, call)
}
