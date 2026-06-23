package gonvex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
)

type FunctionKind string

const (
	FunctionKindQuery            FunctionKind = "query"
	FunctionKindMutation         FunctionKind = "mutation"
	FunctionKindAction           FunctionKind = "action"
	FunctionKindHTTP             FunctionKind = "http"
	FunctionKindInternalMutation FunctionKind = "internalMutation"
	FunctionKindLiveGrid         FunctionKind = "liveGrid"
)

type App struct {
	mu        sync.RWMutex
	functions map[string]Function
}

type Function struct {
	Path       string
	Kind       FunctionKind
	Handler    any
	ArgType    reflect.Type
	ResultType reflect.Type

	handlerValue reflect.Value
}

type User struct {
	ID    string
	Email string
}

type RuntimeContext struct {
	context.Context

	ProjectID   string
	TenantID    string
	User        *User
	Permissions map[string]any
	DatabaseURL string
	DB          *sql.DB
	LandlordDB  *sql.DB
	TenantDB    *sql.DB
	Tx          *sql.Tx
	Storage     StorageAPI
	Logger      *slog.Logger
}

type QueryCtx struct {
	RuntimeContext
}

type MutationCtx struct {
	RuntimeContext
}

type ActionCtx struct {
	RuntimeContext
}

type HTTPContext struct {
	RuntimeContext
}

type DispatchError struct {
	Code    string
	Path    string
	Message string
	Err     error
}

func (e *DispatchError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "gonvex dispatch error"
}

func (e *DispatchError) Unwrap() error {
	return e.Err
}

func NewApp() *App {
	return &App{functions: map[string]Function{}}
}

func (a *App) Query(path string, handler any) {
	a.register(FunctionKindQuery, path, handler)
}

func (a *App) Mutation(path string, handler any) {
	a.register(FunctionKindMutation, path, handler)
}

func (a *App) Action(path string, handler any) {
	a.register(FunctionKindAction, path, handler)
}

func (a *App) HTTP(path string, handler any) {
	a.register(FunctionKindHTTP, path, handler)
}

func (a *App) InternalMutation(path string, handler any) {
	a.register(FunctionKindInternalMutation, path, handler)
}

func (a *App) LiveGrid(path string, handler any) {
	a.register(FunctionKindLiveGrid, path, handler)
}

func (a *App) Functions() map[string]Function {
	a.mu.RLock()
	defer a.mu.RUnlock()
	functions := make(map[string]Function, len(a.functions))
	for path, function := range a.functions {
		functions[path] = function
	}
	return functions
}

func (a *App) Lookup(path string) (Function, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	function, ok := a.functions[path]
	return function, ok
}

func (a *App) ExecuteQuery(ctx *QueryCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &QueryCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindQuery, FunctionKindLiveGrid)
}

func (a *App) ExecuteMutation(ctx *MutationCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &MutationCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindMutation)
}

func (a *App) ExecuteInternalMutation(ctx *MutationCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &MutationCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindInternalMutation)
}

func (a *App) ExecuteAction(ctx *ActionCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &ActionCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindAction)
}

func (a *App) register(kind FunctionKind, path string, handler any) {
	path = strings.TrimSpace(path)
	if path == "" {
		panic("gonvex: function path is required")
	}

	function, err := newFunction(kind, path, handler)
	if err != nil {
		panic(err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.functions == nil {
		a.functions = map[string]Function{}
	}
	if existing, ok := a.functions[path]; ok {
		panic(fmt.Sprintf("gonvex: function %q already registered as %s", path, existing.Kind))
	}
	a.functions[path] = function
}

func (a *App) execute(path string, rawArgs json.RawMessage, ctx any, allowedKinds ...FunctionKind) (any, error) {
	function, ok := a.Lookup(path)
	if !ok {
		return nil, &DispatchError{Code: "not_found", Path: path, Message: fmt.Sprintf("function %q is not registered", path)}
	}
	if !kindAllowed(function.Kind, allowedKinds) {
		return nil, &DispatchError{Code: "wrong_kind", Path: path, Message: fmt.Sprintf("function %q is %s, not %s", path, function.Kind, joinKinds(allowedKinds))}
	}

	if normalizer, ok := ctx.(interface{ normalize() }); ok {
		normalizer.normalize()
	}

	arg, err := decodeArg(function.ArgType, rawArgs)
	if err != nil {
		return nil, &DispatchError{Code: "invalid_args", Path: path, Message: fmt.Sprintf("invalid args for %q: %v", path, err), Err: err}
	}

	results := function.handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx), arg})
	if errValue := results[1]; !errValue.IsNil() {
		return nil, errValue.Interface().(error)
	}
	return results[0].Interface(), nil
}

func newFunction(kind FunctionKind, path string, handler any) (Function, error) {
	if handler == nil {
		return Function{}, fmt.Errorf("gonvex: handler for %q is nil", path)
	}
	value := reflect.ValueOf(handler)
	typ := value.Type()
	if typ.Kind() != reflect.Func {
		return Function{}, fmt.Errorf("gonvex: handler for %q must be a function", path)
	}
	if typ.NumIn() != 2 || typ.NumOut() != 2 {
		return Function{}, fmt.Errorf("gonvex: handler for %q must be func(ctx, args) (result, error)", path)
	}
	expectedCtx := ctxTypeForKind(kind)
	if !typ.In(0).AssignableTo(expectedCtx) {
		return Function{}, fmt.Errorf("gonvex: handler for %q must accept %s as its first argument", path, expectedCtx)
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if !typ.Out(1).Implements(errorType) {
		return Function{}, fmt.Errorf("gonvex: handler for %q must return error as its second result", path)
	}
	return Function{
		Path:         path,
		Kind:         kind,
		Handler:      handler,
		ArgType:      typ.In(1),
		ResultType:   typ.Out(0),
		handlerValue: value,
	}, nil
}

func ctxTypeForKind(kind FunctionKind) reflect.Type {
	switch kind {
	case FunctionKindMutation, FunctionKindInternalMutation:
		return reflect.TypeOf((*MutationCtx)(nil))
	case FunctionKindAction:
		return reflect.TypeOf((*ActionCtx)(nil))
	case FunctionKindHTTP:
		return reflect.TypeOf((*HTTPContext)(nil))
	default:
		return reflect.TypeOf((*QueryCtx)(nil))
	}
}

func decodeArg(argType reflect.Type, raw json.RawMessage) (reflect.Value, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return reflect.Zero(argType), nil
	}
	if argType.Kind() == reflect.Interface {
		var value any
		if err := decodeJSON(raw, &value, false); err != nil {
			return reflect.Value{}, err
		}
		if value == nil {
			return reflect.Zero(argType), nil
		}
		return reflect.ValueOf(value), nil
	}

	target := reflect.New(argType)
	value := target
	disallowUnknown := argType.Kind() == reflect.Struct
	if argType.Kind() == reflect.Ptr {
		value = reflect.New(argType.Elem())
		disallowUnknown = argType.Elem().Kind() == reflect.Struct
	}
	if err := decodeJSON(raw, value.Interface(), disallowUnknown); err != nil {
		return reflect.Value{}, err
	}
	if argType.Kind() == reflect.Ptr {
		return value, nil
	}
	return target.Elem(), nil
}

func decodeJSON(raw json.RawMessage, target any, disallowUnknown bool) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func kindAllowed(kind FunctionKind, allowed []FunctionKind) bool {
	for _, allowedKind := range allowed {
		if kind == allowedKind {
			return true
		}
	}
	return false
}

func joinKinds(kinds []FunctionKind) string {
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, string(kind))
	}
	return strings.Join(parts, " or ")
}

func (c *RuntimeContext) normalize() {
	if c.Context == nil {
		c.Context = context.Background()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.TenantID == "" {
		c.TenantID = c.ProjectID
	}
	if c.Storage == nil {
		c.Storage = storageUnavailable{}
	}
}

func (c *QueryCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *MutationCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *ActionCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *HTTPContext) normalize() {
	c.RuntimeContext.normalize()
}

type Schema struct{}
type Table struct{}

type ColumnOption func(*columnOptions)

type columnOptions struct {
	nullable bool
}

func Nullable(options *columnOptions) {
	options.nullable = true
}

func (s *Schema) Table(name string, define func(*Table)) {}
func (s *Schema) LandlordTable(name string, define func(*Table)) {
	s.Table(name, define)
}
func (s *Schema) TenantTable(name string, define func(*Table)) {
	s.Table(name, define)
}

func (t *Table) ID(name string)                               {}
func (t *Table) String(name string, options ...ColumnOption)  {}
func (t *Table) Text(name string, options ...ColumnOption)    {}
func (t *Table) Int(name string, options ...ColumnOption)     {}
func (t *Table) Int64(name string, options ...ColumnOption)   {}
func (t *Table) Float64(name string, options ...ColumnOption) {}
func (t *Table) Bool(name string, options ...ColumnOption)    {}
func (t *Table) Time(name string, options ...ColumnOption)    {}
func (t *Table) JSON(name string, options ...ColumnOption)    {}

func (t *Table) Index(name string, columns ...string)        {}
func (t *Table) UniqueIndex(name string, columns ...string)  {}
func (t *Table) TrigramIndex(name string, columns ...string) {}
