package moduleengine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
)

// RemoteEngine serves one project's module through the module host. Routing
// facts — which functions exist, of which kind, with which delivery — are
// answered locally from the manifest the artifact shipped with, so Describe and
// Descriptors stay pure lookups on hot paths and never cross a process
// boundary. Only Invoke talks to the host.
type RemoteEngine struct {
	host        *RemoteHost
	moduleID    string
	identity    string
	artifact    artifactPayload
	descriptors map[string]Descriptor

	mu         sync.Mutex
	epoch      uint64
	generation uint64
}

var _ ModuleEngine = (*RemoteEngine)(nil)

// NewRemoteEngine projects a manifest's module artifact onto the module host.
// The JavaScript bundle is decoded and hash-verified here, before anything is
// sent, so a corrupt artifact fails as a manifest error at the caller rather
// than as a load error one process away.
func NewRemoteEngine(host *RemoteHost, projectID string, artifact manifest.ModuleArtifact) (*RemoteEngine, error) {
	if host == nil || !host.Available() {
		return nil, fmt.Errorf("moduleengine: project %q ships a %s module but no module host is configured", projectID, artifact.NormalizedLanguage())
	}
	artifact = artifact.Normalize()
	if err := artifact.Validate(); err != nil {
		return nil, fmt.Errorf("moduleengine: project %q module artifact is not executable: %w", projectID, err)
	}
	code, err := artifact.DecodeJavaScript()
	if err != nil {
		return nil, fmt.Errorf("moduleengine: project %q module artifact is not executable: %w", projectID, err)
	}

	paths := make([]string, 0, len(artifact.Functions))
	for path := range artifact.Functions {
		paths = append(paths, path)
	}
	// Deterministic order keeps a reload's payload byte-identical for an
	// unchanged artifact, which makes host-side logs comparable.
	sort.Strings(paths)

	descriptors := make(map[string]Descriptor, len(paths))
	functions := make([]functionPayload, 0, len(paths))
	for _, path := range paths {
		function := artifact.Functions[path]
		descriptors[path] = descriptorFromModuleFunction(path, function)
		functions = append(functions, functionPayload{
			Path:     path,
			Kind:     string(function.Kind),
			Internal: function.Internal,
			Delivery: string(function.Delivery),
			Handler:  function.Handler,
			Export:   function.Export,
			File:     function.File,
			Args:     function.Args,
			Result:   function.Result,
			Metadata: moduleFunctionMetadata(function),
		})
	}

	return &RemoteEngine{
		host:     host,
		moduleID: projectID,
		identity: artifact.Identity(),
		artifact: artifactPayload{
			Language:     artifact.NormalizedLanguage(),
			Entrypoint:   artifact.Entrypoint,
			ArtifactHash: artifact.Identity(),
			JavaScript: javaScriptPayload{
				Path: artifact.JavaScript.Path,
				Hash: strings.ToLower(strings.TrimSpace(artifact.JavaScript.Hash)),
				// Re-encoded from the verified bytes rather than forwarded, so
				// what the host verifies is exactly what was verified here.
				Code: base64.StdEncoding.EncodeToString(code),
			},
			Functions: functions,
		},
		descriptors: descriptors,
	}, nil
}

func moduleFunctionMetadata(function manifest.ModuleFunction) map[string]any {
	metadata := map[string]any{}
	if function.Offline != nil {
		metadata["offline"] = function.Offline
	}
	if function.Optimistic != nil {
		metadata["optimistic"] = function.Optimistic
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

// Identity is the artifact's content hash. A sync whose module hash is
// unchanged reuses the loaded engine instead of publishing a new generation.
func (e *RemoteEngine) Identity() string { return e.identity }

// Generation is the module generation the host most recently activated for this
// engine, or zero before its first activation.
func (e *RemoteEngine) Generation() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.generation
}

func (e *RemoteEngine) Runtime() string { return RemoteRuntime }

func (e *RemoteEngine) Describe(path string) (Descriptor, bool) {
	if e == nil {
		return Descriptor{}, false
	}
	descriptor, ok := e.descriptors[path]
	return descriptor, ok
}

func (e *RemoteEngine) Descriptors() map[string]Descriptor {
	if e == nil {
		return map[string]Descriptor{}
	}
	descriptors := make(map[string]Descriptor, len(e.descriptors))
	for path, descriptor := range e.descriptors {
		descriptors[path] = descriptor
	}
	return descriptors
}

// Crons returns nothing: the module artifact has no schedule surface yet, and
// inventing one here would let the scheduler register jobs the module cannot
// actually run.
func (e *RemoteEngine) Crons() []gonvex.CronSpec { return nil }

// Activate publishes this engine's artifact to the module host and makes it the
// generation new calls use. It is the operation a manifest sync must succeed at
// before the engine is installed, so a failed load never becomes a live module.
func (e *RemoteEngine) Activate(ctx context.Context) error {
	_, _, err := e.ensure(ctx)
	return err
}

// ensure guarantees the module is loaded and active in the current host
// session. A host that restarted hands out a new session epoch, and the engine
// republishes itself into it — the runtime keeps serving without a client ever
// reconnecting.
func (e *RemoteEngine) ensure(ctx context.Context) (*session, uint64, error) {
	current, err := e.host.Session(ctx)
	if err != nil {
		return nil, 0, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.generation != 0 && e.epoch == current.epoch {
		return current, e.generation, nil
	}

	request, cancel := context.WithTimeout(ctx, e.host.requestTimeout())
	defer cancel()
	frame, err := current.request(request, loadOp{
		Op:       "load",
		ModuleID: e.moduleID,
		Artifact: e.artifact,
	}, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("moduleengine: module host failed to load project %q: %w", e.moduleID, err)
	}
	var loaded loadedResponse
	if err := json.Unmarshal(frame.Payload, &loaded); err != nil {
		return nil, 0, fmt.Errorf("moduleengine: module host sent an unreadable load result for project %q: %w", e.moduleID, err)
	}

	// Load warms the isolates; activate is the atomic swap. Splitting them is
	// what makes a generation swap invisible to connected clients: nothing
	// points at the new generation until it is known to run.
	activateCtx, cancelActivate := context.WithTimeout(ctx, e.host.requestTimeout())
	defer cancelActivate()
	activated, err := current.request(activateCtx, activateOp{
		Op:         "activate",
		ModuleID:   e.moduleID,
		Generation: loaded.Generation,
		DrainMS:    uint64(e.host.drainTimeout() / time.Millisecond),
	}, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("moduleengine: module host failed to activate project %q generation %d: %w", e.moduleID, loaded.Generation, err)
	}
	var result activatedResponse
	if err := json.Unmarshal(activated.Payload, &result); err != nil {
		return nil, 0, fmt.Errorf("moduleengine: module host sent an unreadable activation result for project %q: %w", e.moduleID, err)
	}

	e.epoch = current.epoch
	e.generation = result.Generation
	return current, e.generation, nil
}

func (e *RemoteEngine) InvokeQuery(ctx *gonvex.QueryCtx, call Invocation) (Result, error) {
	if ctx == nil {
		ctx = &gonvex.QueryCtx{}
	}
	descriptor, err := e.expect(call.Path, KindQuery, false)
	if err != nil {
		return Result{}, err
	}
	dispatcher := newQueryHostCalls(ctx)
	defer dispatcher.close()
	value, err := e.invoke(ctx.Context, invocationFor(&ctx.RuntimeContext, queryCapabilities(ctx)), descriptor, call.Args, dispatcher)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func (e *RemoteEngine) InvokeReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error) {
	if ctx == nil {
		ctx = &gonvex.ReducerCtx{}
	}
	descriptor, err := e.expect(call.Path, KindReducer, false)
	if err != nil {
		return Result{}, err
	}
	return e.invokeReducer(ctx, descriptor, call)
}

func (e *RemoteEngine) InvokeInternalReducer(ctx *gonvex.ReducerCtx, call Invocation) (Result, error) {
	if ctx == nil {
		ctx = &gonvex.ReducerCtx{}
	}
	descriptor, err := e.expect(call.Path, KindReducer, true)
	if err != nil {
		return Result{}, err
	}
	return e.invokeReducer(ctx, descriptor, call)
}

func (e *RemoteEngine) invokeReducer(ctx *gonvex.ReducerCtx, descriptor Descriptor, call Invocation) (Result, error) {
	dispatcher := newReducerHostCalls(ctx)
	defer dispatcher.close()
	value, err := e.invoke(ctx.Context, invocationFor(&ctx.RuntimeContext, reducerCapabilities(ctx)), descriptor, call.Args, dispatcher)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

func (e *RemoteEngine) InvokeAction(ctx *gonvex.ActionCtx, call Invocation) (Result, error) {
	if ctx == nil {
		ctx = &gonvex.ActionCtx{}
	}
	descriptor, err := e.expect(call.Path, KindAction, false)
	if err != nil {
		return Result{}, err
	}
	dispatcher := newActionHostCalls(&ctx.RuntimeContext)
	defer dispatcher.close()
	value, err := e.invoke(ctx.Context, invocationFor(&ctx.RuntimeContext, actionCapabilities(&ctx.RuntimeContext)), descriptor, call.Args, dispatcher)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: value}, nil
}

// expect resolves a path and enforces the same visibility rules a compiled Go
// module enforces: an unknown path, a wrong kind, and an internal reducer
// reached from the public entry point all fail before anything is dispatched.
func (e *RemoteEngine) expect(path string, kind Kind, internal bool) (Descriptor, error) {
	descriptor, ok := e.Describe(path)
	if !ok {
		return Descriptor{}, notRegistered(path)
	}
	if descriptor.Kind != kind {
		return Descriptor{}, &gonvex.DispatchError{
			Code:    "wrong_kind",
			Path:    path,
			Message: fmt.Sprintf("function %q is %s, not %s", path, descriptor.Kind, kind),
		}
	}
	if kind == KindReducer {
		if internal && !descriptor.Internal {
			return Descriptor{}, &gonvex.DispatchError{
				Code:    "not_found",
				Path:    path,
				Message: fmt.Sprintf("internal reducer %q is not registered", path),
			}
		}
		if !internal && descriptor.Internal {
			return Descriptor{}, &gonvex.DispatchError{
				Code:    "not_found",
				Path:    path,
				Message: fmt.Sprintf("reducer %q is internal", path),
			}
		}
	}
	return descriptor, nil
}

func (e *RemoteEngine) invoke(
	ctx context.Context,
	invocationCtx invocationContext,
	descriptor Descriptor,
	args json.RawMessage,
	dispatcher hostCallDispatcher,
) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	current, generation, err := e.ensure(ctx)
	if err != nil {
		return nil, err
	}

	call, cancel := e.callContext(ctx)
	defer cancel()
	if deadline, ok := call.Deadline(); ok {
		invocationCtx.DeadlineUnixMS = deadline.UnixMilli()
	}
	frame, err := current.request(call, invokeOp{
		Op:         "invoke",
		ModuleID:   e.moduleID,
		Generation: generation,
		Function:   descriptor.Path,
		Kind:       normalizeKind(descriptor.Kind),
		Args:       string(args),
		Context:    invocationCtx,
	}, dispatcher)
	if err != nil {
		return nil, e.dispatchError(descriptor.Path, err)
	}
	var invoked invokedResponse
	if err := json.Unmarshal(frame.Payload, &invoked); err != nil {
		return nil, fmt.Errorf("moduleengine: module host sent an unreadable result for %q: %w", descriptor.Path, err)
	}
	if strings.TrimSpace(invoked.Value) == "" {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal([]byte(invoked.Value), &value); err != nil {
		return nil, fmt.Errorf("moduleengine: %q returned a value that is not JSON: %w", descriptor.Path, err)
	}
	return value, nil
}

// callContext bounds one invocation. A caller-supplied deadline wins whenever
// it is tighter than the host's ceiling; neither may lengthen the other.
func (e *RemoteEngine) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ceiling := e.host.requestTimeout()
	if ceiling <= 0 {
		return context.WithCancel(ctx)
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < ceiling {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, ceiling)
}

// dispatchError maps the host's error vocabulary onto the errors host code
// already branches on, so a module in another language fails the same way a Go
// module does.
func (e *RemoteEngine) dispatchError(path string, err error) error {
	var hostErr *HostError
	if !errors.As(err, &hostErr) {
		return err
	}
	switch hostErr.Code {
	case codeFunctionNotFound, codeModuleNotLoaded:
		return &gonvex.DispatchError{Code: "not_found", Path: path, Message: hostErr.Message, Err: err}
	case codeWrongFunctionKind:
		return &gonvex.DispatchError{Code: "wrong_kind", Path: path, Message: hostErr.Message, Err: err}
	case codeBadRequest, codeInvalidArtifact, codeArtifactHashMismatch:
		return &gonvex.DispatchError{Code: "invalid_args", Path: path, Message: hostErr.Message, Err: err}
	default:
		return fmt.Errorf("moduleengine: %q failed: %w", path, err)
	}
}

func descriptorFromModuleFunction(path string, function manifest.ModuleFunction) Descriptor {
	descriptor := Descriptor{
		Path:     path,
		Kind:     Kind(function.Kind),
		Internal: function.Internal,
		Delivery: gonvex.DeliveryMode(function.Delivery),
	}
	if descriptor.Delivery == "" && descriptor.Kind == KindQuery {
		descriptor.Delivery = gonvex.DeliveryOneShot
	}
	descriptor.Dependencies = dependenciesFromManifest(function.Dependencies)
	if function.Replica != nil {
		descriptor.Replica = replicaFromManifest(*function.Replica)
	}
	return descriptor
}

func dependenciesFromManifest(dependencies manifest.FunctionDependencies) gonvex.FunctionDependencies {
	converted := gonvex.FunctionDependencies{
		ShareByPermissions:  dependencies.ShareByPermissions,
		ShareResultFrom:     dependencies.ShareResultFrom,
		ShareResultField:    dependencies.ShareResultField,
		NonOptimisticReason: dependencies.NonOptimisticReason,
	}
	for _, read := range dependencies.Reads {
		converted.Reads = append(converted.Reads, gonvex.ReadDependency{
			Table:    read.Table,
			Columns:  append([]string(nil), read.Columns...),
			Filters:  append([]string(nil), read.Filters...),
			OrdersBy: append([]string(nil), read.OrdersBy...),
			Windowed: read.Windowed,
		})
	}
	if reducer := dependencies.OptimisticReducer; reducer != nil {
		converted.OptimisticReducer = &gonvex.OptimisticReducerDefinition{
			Entity:     reducer.Entity,
			RowIDPath:  append([]string(nil), reducer.RowIDPath...),
			FieldsPath: append([]string(nil), reducer.FieldsPath...),
		}
	}
	if projection := dependencies.OptimisticProjection; projection != nil {
		converted.OptimisticProjection = &gonvex.OptimisticProjectionDefinition{
			Entity:     projection.Entity,
			Key:        projection.Key,
			ResultPath: append([]string(nil), projection.ResultPath...),
		}
	}
	if plan := dependencies.LiveQueryPlan; plan != nil {
		converted.LiveQueryPlan = liveQueryPlanFromManifest(*plan)
	}
	return converted
}

func liveQueryPlanFromManifest(plan manifest.LiveQueryPlan) *gonvex.LiveQueryPlan {
	converted := &gonvex.LiveQueryPlan{
		Table:      plan.Table,
		Key:        plan.Key,
		Columns:    append([]string(nil), plan.Columns...),
		ResultPath: append([]string(nil), plan.ResultPath...),
		ServerOnly: plan.ServerOnly,
	}
	converted.Where = liveExpressionFromManifest(plan.Where)
	if plan.Search != nil {
		converted.Search = &gonvex.LiveSearch{
			Argument: plan.Search.Argument,
			Columns:  append([]string(nil), plan.Search.Columns...),
		}
	}
	if plan.Sort != nil {
		converted.Sort = &gonvex.LiveSort{
			ColumnArgument:    plan.Sort.ColumnArgument,
			DirectionArgument: plan.Sort.DirectionArgument,
			AllowedColumns:    append([]string(nil), plan.Sort.AllowedColumns...),
			DefaultColumn:     plan.Sort.DefaultColumn,
			DefaultDirection:  plan.Sort.DefaultDirection,
		}
	}
	if plan.Window != nil {
		converted.Window = &gonvex.LiveWindow{
			OffsetArgument: plan.Window.OffsetArgument,
			LimitArgument:  plan.Window.LimitArgument,
			DefaultLimit:   plan.Window.DefaultLimit,
			MaxLimit:       plan.Window.MaxLimit,
		}
	}
	return converted
}

func liveExpressionFromManifest(expression *manifest.LiveExpression) *gonvex.LiveExpression {
	if expression == nil {
		return nil
	}
	converted := &gonvex.LiveExpression{
		Operator: expression.Operator,
		Column:   expression.Column,
		Value:    liveValueFromManifest(expression.Value),
		ValueTo:  liveValueFromManifest(expression.ValueTo),
	}
	for _, child := range expression.Children {
		converted.Children = append(converted.Children, liveExpressionFromManifest(child))
	}
	return converted
}

func liveValueFromManifest(value *manifest.LiveValue) *gonvex.LiveValue {
	if value == nil {
		return nil
	}
	return &gonvex.LiveValue{Argument: value.Argument, Literal: value.Literal}
}

func replicaFromManifest(replica manifest.ReplicaCollectionDefinition) *gonvex.ReplicaCollectionDefinition {
	converted := &gonvex.ReplicaCollectionDefinition{
		Table:            replica.Table,
		Key:              replica.Key,
		Columns:          append([]string(nil), replica.Columns...),
		ExcludeWhenSet:   append([]string(nil), replica.ExcludeWhenSet...),
		VisibilityTables: append([]string(nil), replica.VisibilityTables...),
		OrderBy:          replica.OrderBy,
		OrderDirection:   replica.OrderDirection,
		Mode:             replica.Mode,
		MaxRows:          replica.MaxRows,
		MaxBytes:         replica.MaxBytes,
		Retention:        time.Duration(replica.RetentionMilliseconds) * time.Millisecond,
	}
	if len(replica.EqualFilters) > 0 {
		converted.EqualFilters = make(map[string]string, len(replica.EqualFilters))
		for column, argument := range replica.EqualFilters {
			converted.EqualFilters[column] = argument
		}
	}
	return converted
}
