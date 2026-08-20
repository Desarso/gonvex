// Package moduleengine defines the seam between the Gonvex runtime host and the
// modules that execute a project's functions.
//
// Every project today is a compiled Go plugin that registers handlers on a
// *gonvex.App; GoAppEngine adapts that App to ModuleEngine so the host
// dispatches against an interface instead of against the Go type. Future
// language-neutral engines (an out-of-process Rust module host, a V8 isolate
// pool) implement the same interface and coexist with compiled Go modules: the
// host resolves one engine per project and never learns which language served
// the call.
//
// Invocations are deliberately narrow — a registered function path plus its
// JSON-encoded arguments — so an engine that crosses a process or language
// boundary can forward them verbatim. Everything a handler needs from the host
// (database handles, storage, ephemeral state, identity, scheduler) travels on
// the *gonvex context types, which stay the host capability bundle no matter
// which engine runs the handler.
package moduleengine

import (
	"encoding/json"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// Kind classifies a registered function. The values match gonvex.FunctionKind
// and manifest.FunctionKind so a descriptor, an in-process Go registration and
// a serialized manifest entry all share one vocabulary.
type Kind string

const (
	KindQuery   Kind = "query"
	KindReducer Kind = "reducer"
	KindAction  Kind = "action"
	KindHTTP    Kind = "http"
)

// Descriptor is the language-neutral view of one registered function: the
// routing facts the host needs before it invokes anything. Richer declarative
// metadata (read/write tables, subscription sharing rules, sync definitions)
// keeps travelling in the project manifest, which is already engine-agnostic.
type Descriptor struct {
	Path string
	Kind Kind
	// Public marks an HTTP function that may run without a Gonvex session
	// because the module authenticates the request itself.
	Public bool
	// Internal marks a Reducer callable only by the host, scheduler, or Action.
	Internal bool
	// Delivery distinguishes one-shot Queries, Live Queries, and Replica
	// Collections without inventing extra executable function kinds.
	Delivery gonvex.DeliveryMode
	// Dependencies and Replica are executable delivery contracts exposed by
	// every engine. Keeping them on the descriptor lets the host route Live
	// Queries and Replica Collections without reaching back into a compiled Go
	// application.
	Dependencies gonvex.FunctionDependencies
	Replica      *gonvex.ReplicaCollectionDefinition
}

// Invocation is one call into a module: which function to run and its
// JSON-encoded arguments. Args is passed through untouched, so an engine backed
// by another language forwards the same bytes its module already expects.
type Invocation struct {
	Path string
	Args json.RawMessage
}

// Result carries a handler's return value. A Go module returns a live Go value;
// an out-of-process engine decodes its response before returning, so the host
// sees one shape regardless of the module's language.
type Result struct {
	Value any
}

// HTTPInvocation is an HTTP function call. The request is already normalized to
// the transport-independent gonvex.HTTPRequest the host builds from net/http.
type HTTPInvocation struct {
	Path    string
	Request gonvex.HTTPRequest
}

// HTTPResult carries the response an HTTP function returned.
type HTTPResult struct {
	Response gonvex.HTTPResponse
}

// ReducerInvoker is the shape shared by ModuleEngine's two reducer entry
// points, so host code can accept either one.
type ReducerInvoker func(*gonvex.ReducerCtx, Invocation) (Result, error)

// ReducerExec adapts a typed reducer entry point to the (ctx, path, args)
// callback shape used by host helpers that wrap a mutation in a database
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
	// Runtime names the implementation ("go" today, later "rust" or "v8") for
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
	InvokeHTTP(ctx *gonvex.HTTPContext, call HTTPInvocation) (HTTPResult, error)
}
