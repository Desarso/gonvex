// Package moduleengine defines the seam between the Gonvex runtime host and the
// modules that execute a project's functions.
//
// Projects execute through language-neutral module hosts. The current
// production path is the TypeScript artifact host; future Rust/Wasm hosts can
// implement this seam without coupling the server to an application language.
//
// Invocations are deliberately narrow — a registered function path plus its
// JSON-encoded arguments — so an engine that crosses a process or language
// boundary can forward them verbatim. Everything a handler needs from the host
// (database handles, storage, identity, scheduler) travels on
// the *gonvex context types, which stay the host capability bundle no matter
// which engine runs the handler.
package moduleengine

import (
	"encoding/json"
	"fmt"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// Kind classifies a module function. The values match manifest.FunctionKind so
// a descriptor and a serialized module manifest share one vocabulary.
type Kind string

const (
	KindQuery   Kind = "query"
	KindReducer Kind = "reducer"
	KindAction  Kind = "action"
)

// Descriptor is the language-neutral view of one registered function: the
// routing facts the host needs before it invokes anything. Richer declarative
// metadata (structured query plans, subscription sharing rules, sync definitions)
// keeps travelling in the project manifest, which is already engine-agnostic.
type Descriptor struct {
	Path string
	Kind Kind
	// Internal marks a Reducer callable only by the host, scheduler, or Action.
	Internal bool
	// Delivery distinguishes one-shot Queries, Live Queries, and Replica
	// Collections without inventing extra executable function kinds.
	Delivery gonvex.DeliveryMode
	// Dependencies and Replica are executable delivery contracts exposed by
	// every engine. Live Query dependencies are derived from the structured
	// plan; keeping the resulting plan on the descriptor lets the host route
	// subscriptions without reaching back into application code.
	Dependencies       gonvex.FunctionDependencies
	Replica            *gonvex.ReplicaCollectionDefinition
	ActionProfile      string
	ActionCapabilities ActionCapabilities
}

type ActionCapabilities struct {
	NetworkOrigins []string
	Secrets        []string
	Tools          map[string]ActionToolBinding
	Scheduler      bool
	Storage        bool
}

type ActionToolBinding struct {
	Kind     Kind
	Function string
}

// Invocation is one call into a module: which function to run and its
// JSON-encoded arguments. Args is passed through untouched, so an engine backed
// by another language forwards the same bytes its module already expects.
type Invocation struct {
	Path string
	Args json.RawMessage
}

// Result carries a handler's decoded return value. The Rust/V8 module host
// crosses a language-neutral wire boundary before returning this host shape.
type Result struct {
	Value any
}

// ReducerInvoker is the shape shared by ModuleEngine's two reducer entry
// points, so host code can accept either one.
type ReducerInvoker func(*gonvex.ReducerCtx, Invocation) (Result, error)

// ReducerExec adapts a typed reducer entry point to the (ctx, path, args)
// callback shape used by host helpers that wrap a reducer in a database
// transaction. It exists so transaction management stays independent of the
// seam's request/result types.
func ReducerExec(invoke ReducerInvoker) func(*gonvex.ReducerCtx, string, json.RawMessage) (any, error) {
	return func(ctx *gonvex.ReducerCtx, path string, args json.RawMessage) (any, error) {
		result, err := invoke(ctx, Invocation{Path: path, Args: args})
		return result.Value, err
	}
}

// ModuleEngine executes the registered functions of one loaded project module.
// Implementations are long-lived and shared across requests: the host resolves
// an engine per project and reuses it until the module is replaced.
type ModuleEngine interface {
	// Runtime names the implementation (for example "v8" or "wasm") for
	// logs, metrics and debugging.
	Runtime() string

	// Describe reports the registered function at path. It is a pure lookup on
	// hot routing paths and must never execute module code.
	Describe(path string) (Descriptor, bool)

	// Descriptors returns every registered function keyed by path.
	Descriptors() map[string]Descriptor

	// Crons returns the recurring jobs the module registered. CronSpec is plain
	// data, so an engine in another language reports schedules the same way.
	Crons() []gonvex.CronSpec

	// The Invoke methods dispatch one call of the matching kind and return the
	// handler's error unwrapped, so the host keeps today's error surface: a
	// *gonvex.DispatchError for an unknown path, a wrong kind or undecodable
	// arguments, and the handler's own error otherwise.
	InvokeQuery(ctx *gonvex.QueryCtx, call Invocation) (Result, error)
	InvokeReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error)
	InvokeInternalReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error)
	InvokeAction(ctx *gonvex.ActionCtx, call Invocation) (Result, error)
}

func notRegistered(path string) error {
	return &gonvex.DispatchError{Code: "not_found", Path: path, Message: fmt.Sprintf("function %q is not registered", path)}
}
