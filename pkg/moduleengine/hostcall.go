package moduleengine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// Bounds on the host operations a module may ask for. They are host policy, so
// a module cannot raise them by asking differently.
const (
	maxFetchResponseBytes = 8 << 20
	defaultFetchTimeout   = 30 * time.Second
	defaultRowKeyColumn   = "id"
)

// invocationFor projects a runtime context onto the wire. It carries who is
// calling and what they may reach; it deliberately carries no database URL, no
// connection, and no credential, because the module never opens anything
// itself.
func invocationFor(runtimeCtx *gonvex.RuntimeContext, granted capabilities) invocationContext {
	invocation := invocationContext{
		Capabilities: granted,
		NowUnixMS:    time.Now().UTC().UnixMilli(),
	}
	if runtimeCtx == nil {
		return invocation
	}
	invocation.ProjectID = runtimeCtx.ProjectID
	invocation.TenantID = runtimeCtx.TenantID
	invocation.OperationID = runtimeCtx.OperationID
	if runtimeCtx.Tenant != nil {
		invocation.Tenant = &tenantIdentity{
			ID:        runtimeCtx.Tenant.ID,
			ProjectID: runtimeCtx.Tenant.ProjectID,
			Name:      runtimeCtx.Tenant.Name,
		}
	} else if runtimeCtx.TenantID != "" {
		invocation.Tenant = &tenantIdentity{ID: runtimeCtx.TenantID, ProjectID: runtimeCtx.ProjectID}
	}
	account := runtimeCtx.Auth.Account
	if account != nil {
		invocation.Account = &accountIdentity{
			ID:        account.ID,
			Email:     account.Email,
			Name:      account.Name,
			AvatarURL: account.AvatarURL,
		}
	}
	if member := runtimeCtx.Member; member != nil {
		invocation.Member = &memberIdentity{
			ID:          member.ID,
			AccountID:   member.AccountID,
			Status:      member.Status,
			Role:        member.Role,
			DisplayName: member.DisplayName,
			Permissions: member.Permissions,
		}
	}
	if runtimeCtx.Member != nil && len(runtimeCtx.Member.Permissions) > 0 {
		invocation.Permissions = runtimeCtx.Member.Permissions
	}
	return invocation
}

// Capability grants. The module host intersects each grant with what the
// function kind may ever reach, so these can only narrow the structural rule:
// a Query reads, a Reducer reads and writes inside the caller's transaction,
// and an Action reaches the network, storage and reducers but never a table.
func queryCapabilities(ctx *gonvex.QueryCtx) capabilities {
	if ctx == nil {
		return capabilities{}
	}
	return capabilities{DBRead: ctx.DB != nil || ctx.TenantDB != nil || ctx.Tx != nil}
}

func reducerCapabilities(ctx *gonvex.ReducerCtx) capabilities {
	if ctx == nil {
		return capabilities{}
	}
	// Writes exist only when the host is holding a transaction for this call.
	// Without one there is nothing atomic to write into, so the module is told
	// so by absence rather than by a failure halfway through.
	inTransaction := ctx.Tx != nil
	return capabilities{
		DBRead:       inTransaction,
		DBWrite:      inTransaction,
		ActionOutbox: inTransaction && ctx.Outbox != nil,
	}
}

func actionCapabilities(runtimeCtx *gonvex.RuntimeContext) capabilities {
	if runtimeCtx == nil {
		return capabilities{}
	}
	return capabilities{
		RunReducer: runtimeCtx.Reducers != nil,
		Network:    true,
		Storage:    runtimeCtx.Storage != nil,
	}
}

// queryHostCalls serves a Query's reads. It opens one read-only transaction
// lazily and keeps it for the whole invocation: Postgres then refuses a write
// no matter what statement the module sends, and every read in one Query sees
// the same snapshot.
type queryHostCalls struct {
	ctx *gonvex.QueryCtx

	mu sync.Mutex
	tx *sql.Tx
}

func newQueryHostCalls(ctx *gonvex.QueryCtx) *queryHostCalls {
	return &queryHostCalls{ctx: ctx}
}

func (h *queryHostCalls) dispatch(ctx context.Context, call hostCallPayload) (json.RawMessage, error) {
	switch call.Kind {
	case hostCallDBQuery:
		h.mu.Lock()
		defer h.mu.Unlock()
		runner, err := h.runner(ctx)
		if err != nil {
			return nil, err
		}
		if err := requireReadOnlyStatement(call.Statement); err != nil {
			return nil, err
		}
		return runQuery(ctx, runner, call.Statement, call.Parameters)
	default:
		return nil, fmt.Errorf("a query may not call %q", call.Kind)
	}
}

func (h *queryHostCalls) runner(ctx context.Context) (sqlRunner, error) {
	if h.tx != nil {
		return h.tx, nil
	}
	if h.ctx == nil {
		return nil, fmt.Errorf("no database is available to this query")
	}
	if h.ctx.Tx != nil {
		return h.ctx.Tx, nil
	}
	database := h.ctx.DB
	if database == nil {
		database = h.ctx.TenantDB
	}
	if database == nil {
		return nil, fmt.Errorf("no database is available to this query")
	}
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open a read-only transaction: %w", err)
	}
	h.tx = tx
	return tx, nil
}

func (h *queryHostCalls) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.tx != nil {
		// Read-only work has nothing to commit; rolling back releases the
		// snapshot as soon as the invocation is done with it.
		_ = h.tx.Rollback()
		h.tx = nil
	}
}

// reducerHostCalls serves a Reducer's reads and writes against the exact
// transaction the Go host opened for this call. Nothing else is reachable: the
// same transaction commits the module's writes and the host's bookkeeping, or
// neither commits.
type reducerHostCalls struct {
	ctx *gonvex.ReducerCtx
	mu  sync.Mutex
}

func newReducerHostCalls(ctx *gonvex.ReducerCtx) *reducerHostCalls {
	return &reducerHostCalls{ctx: ctx}
}

func (h *reducerHostCalls) dispatch(ctx context.Context, call hostCallPayload) (json.RawMessage, error) {
	// One transaction is not safe for concurrent use, so a module that fires
	// several database calls at once has them serialized rather than refused.
	h.mu.Lock()
	defer h.mu.Unlock()
	tx, err := h.transaction()
	if err != nil {
		return nil, err
	}
	switch call.Kind {
	case hostCallDBQuery:
		if err := requireSingleStatement(call.Statement); err != nil {
			return nil, err
		}
		return runQuery(ctx, tx, call.Statement, call.Parameters)
	case hostCallDBInsert:
		return runInsert(ctx, tx, call)
	case hostCallDBUpdate:
		return runUpdate(ctx, tx, call)
	case hostCallDBDelete:
		return runDelete(ctx, tx, call)
	case hostCallActionEnqueue:
		return h.enqueueAction(ctx, call)
	default:
		return nil, fmt.Errorf("a reducer may not call %q", call.Kind)
	}
}

func (h *reducerHostCalls) enqueueAction(ctx context.Context, call hostCallPayload) (json.RawMessage, error) {
	if h.ctx == nil || h.ctx.Outbox == nil {
		return nil, fmt.Errorf("the durable Action outbox is unavailable to this reducer")
	}
	path := strings.TrimSpace(call.Function)
	if path == "" {
		return nil, fmt.Errorf("actions.enqueue requires an Action path")
	}
	args, err := decodeJSONValue(call.Args)
	if err != nil {
		return nil, fmt.Errorf("Action %q arguments are not valid JSON: %w", path, err)
	}
	id, err := h.ctx.Outbox.Enqueue(ctx, path, args)
	if err != nil {
		return nil, err
	}
	return encodeJSONValue(id)
}

func (h *reducerHostCalls) transaction() (*sql.Tx, error) {
	if h.ctx == nil || h.ctx.Tx == nil {
		return nil, fmt.Errorf("this reducer is running without a database transaction")
	}
	return h.ctx.Tx, nil
}

func (h *reducerHostCalls) close() {}

// actionHostCalls serves the non-transactional surface: reducers, the network,
// and storage. An Action never reaches an application table directly, so there
// is deliberately no database branch here at all.
type actionHostCalls struct {
	ctx *gonvex.RuntimeContext
}

func newActionHostCalls(ctx *gonvex.RuntimeContext) *actionHostCalls {
	return &actionHostCalls{ctx: ctx}
}

func (h *actionHostCalls) dispatch(ctx context.Context, call hostCallPayload) (json.RawMessage, error) {
	if h.ctx == nil {
		return nil, fmt.Errorf("this action has no host context")
	}
	switch call.Kind {
	case hostCallRunReducer:
		return h.runReducer(ctx, call)
	case hostCallFetch:
		return runFetch(ctx, call.Request)
	case hostCallStorage:
		return runStorage(h.ctx.Storage, call.Operation, call.Payload)
	default:
		return nil, fmt.Errorf("an action may not call %q", call.Kind)
	}
}

func (h *actionHostCalls) runReducer(ctx context.Context, call hostCallPayload) (json.RawMessage, error) {
	if h.ctx.Reducers == nil {
		return nil, fmt.Errorf("reducers are not available to this action")
	}
	path := strings.TrimSpace(call.Function)
	if path == "" {
		return nil, fmt.Errorf("runReducer requires a reducer path")
	}
	args, err := decodeJSONValue(call.Args)
	if err != nil {
		return nil, fmt.Errorf("runReducer arguments are not valid JSON: %w", err)
	}
	value, err := h.ctx.Reducers.Call(ctx, path, args)
	if err != nil {
		return nil, err
	}
	return encodeJSONValue(value)
}

func (h *actionHostCalls) close() {}

// sqlRunner is the subset of database/sql both a pool and a transaction expose.
type sqlRunner interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func runQuery(ctx context.Context, runner sqlRunner, statement string, parameters json.RawMessage) (json.RawMessage, error) {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return nil, fmt.Errorf("a database query requires a statement")
	}
	values, err := decodeParameters(parameters)
	if err != nil {
		return nil, err
	}
	rows, err := runner.QueryContext(ctx, statement, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// runInsert builds the statement from a table name and a JSON object. The
// module never writes SQL for a table change, so there is no place for it to
// interpolate a value: identifiers are validated and quoted here, and values
// are always bound.
func runInsert(ctx context.Context, runner sqlRunner, call hostCallPayload) (json.RawMessage, error) {
	table, err := quoteIdentifier(call.Table)
	if err != nil {
		return nil, err
	}
	row, err := decodeObject(call.Row, "row")
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, fmt.Errorf("insert into %s requires at least one column", call.Table)
	}
	columns := sortedKeys(row)
	quoted := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	values := make([]any, 0, len(columns))
	for index, column := range columns {
		name, err := quoteIdentifier(column)
		if err != nil {
			return nil, err
		}
		value, err := toSQLValue(row[column])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", column, err)
		}
		quoted = append(quoted, name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", index+1))
		values = append(values, value)
	}
	statement := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING *",
		table, strings.Join(quoted, ", "), strings.Join(placeholders, ", "),
	)
	rows, err := runner.QueryContext(ctx, statement, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFirstRow(rows)
}

func runUpdate(ctx context.Context, runner sqlRunner, call hostCallPayload) (json.RawMessage, error) {
	table, err := quoteIdentifier(call.Table)
	if err != nil {
		return nil, err
	}
	key, err := quoteIdentifier(keyColumn(call.Key))
	if err != nil {
		return nil, err
	}
	patch, err := decodeObject(call.Patch, "patch")
	if err != nil {
		return nil, err
	}
	if len(patch) == 0 {
		return nil, fmt.Errorf("update of %s requires at least one column", call.Table)
	}
	assignments := make([]string, 0, len(patch))
	values := make([]any, 0, len(patch)+1)
	for index, column := range sortedKeys(patch) {
		name, err := quoteIdentifier(column)
		if err != nil {
			return nil, err
		}
		value, err := toSQLValue(patch[column])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", column, err)
		}
		assignments = append(assignments, fmt.Sprintf("%s = $%d", name, index+1))
		values = append(values, value)
	}
	id, err := decodeKeyValue(call.ID)
	if err != nil {
		return nil, err
	}
	values = append(values, id)
	statement := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = $%d RETURNING *",
		table, strings.Join(assignments, ", "), key, len(values),
	)
	rows, err := runner.QueryContext(ctx, statement, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFirstRow(rows)
}

func runDelete(ctx context.Context, runner sqlRunner, call hostCallPayload) (json.RawMessage, error) {
	table, err := quoteIdentifier(call.Table)
	if err != nil {
		return nil, err
	}
	key, err := quoteIdentifier(keyColumn(call.Key))
	if err != nil {
		return nil, err
	}
	id, err := decodeKeyValue(call.ID)
	if err != nil {
		return nil, err
	}
	statement := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, key)
	result, err := runner.ExecContext(ctx, statement, id)
	if err != nil {
		return nil, err
	}
	deleted := int64(0)
	if affected, err := result.RowsAffected(); err == nil {
		deleted = affected
	}
	return json.Marshal(map[string]any{"deleted": deleted})
}

func keyColumn(key string) string {
	if trimmed := strings.TrimSpace(key); trimmed != "" {
		return trimmed
	}
	return defaultRowKeyColumn
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	// Deterministic column order makes the generated statement stable, which
	// matters for prepared-statement caching and for readable logs.
	sort.Strings(keys)
	return keys
}

func scanFirstRow(rows *sql.Rows) (json.RawMessage, error) {
	decoded, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	var records []json.RawMessage
	if err := json.Unmarshal(decoded, &records); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return json.RawMessage("null"), nil
	}
	return records[0], nil
}

func scanRows(rows *sql.Rows) (json.RawMessage, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	records := make([]map[string]any, 0, 16)
	for rows.Next() {
		cells := make([]any, len(columns))
		targets := make([]any, len(columns))
		for index := range cells {
			targets[index] = &cells[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		record := make(map[string]any, len(columns))
		for index, column := range columns {
			databaseType := ""
			if index < len(types) && types[index] != nil {
				databaseType = strings.ToUpper(types[index].DatabaseTypeName())
			}
			record[column] = toJSONValue(cells[index], databaseType)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(records)
}

// toJSONValue makes a driver value JSON-representable. JSON columns keep their
// structure, byte slices become text, and timestamps become RFC 3339 so a
// module reads a value it can compare and re-send.
func toJSONValue(value any, databaseType string) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		if databaseType == "JSON" || databaseType == "JSONB" {
			if json.Valid(typed) {
				return json.RawMessage(append([]byte(nil), typed...))
			}
		}
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return value
	}
}

// decodeParameters turns the JSON array a module sent into bound values.
func decodeParameters(parameters json.RawMessage) ([]any, error) {
	if len(bytes.TrimSpace(parameters)) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(parameters))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("query parameters are not valid JSON: %w", err)
	}
	if decoded == nil {
		return nil, nil
	}
	list, ok := decoded.([]any)
	if !ok {
		return nil, fmt.Errorf("query parameters must be an array")
	}
	values := make([]any, 0, len(list))
	for index, item := range list {
		value, err := toSQLValue(item)
		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", index+1, err)
		}
		values = append(values, value)
	}
	return values, nil
}

func decodeObject(payload json.RawMessage, name string) (map[string]any, error) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return map[string]any{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("%s is not valid JSON: %w", name, err)
	}
	if decoded == nil {
		return map[string]any{}, nil
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", name)
	}
	return object, nil
}

func decodeKeyValue(payload json.RawMessage) (any, error) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, fmt.Errorf("a row id is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("row id is not valid JSON: %w", err)
	}
	switch decoded.(type) {
	case string, json.Number:
		return toSQLValue(decoded)
	default:
		return nil, fmt.Errorf("a row id must be a string or a number")
	}
}

func decodeJSONValue(payload json.RawMessage) (any, error) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func encodeJSONValue(value any) (json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// toSQLValue converts a decoded JSON value into something a driver accepts.
// Objects and arrays are re-encoded as JSON text so json/jsonb columns work
// without the module knowing the column type.
func toSQLValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case bool, string:
		return typed, nil
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer, nil
		}
		number, err := typed.Float64()
		if err != nil {
			return nil, fmt.Errorf("%s is not a representable number", typed.String())
		}
		return number, nil
	case float64:
		return typed, nil
	case int64:
		return typed, nil
	case map[string]any, []any:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return nil, fmt.Errorf("value cannot be encoded as JSON: %w", err)
		}
		return string(encoded), nil
	default:
		return nil, fmt.Errorf("value of type %T cannot be bound", value)
	}
}

// quoteIdentifier validates and quotes a table or column name. Only plain
// identifiers, optionally schema-qualified, are accepted: a name that cannot be
// spelled without quoting tricks is rejected rather than escaped.
func quoteIdentifier(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("an identifier is required")
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("identifier %q is not a table or column name", name)
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if !validIdentifier(part) {
			return "", fmt.Errorf("identifier %q is not a table or column name", name)
		}
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func validIdentifier(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for index, character := range name {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character == '_':
		case index > 0 && character >= '0' && character <= '9':
		case index > 0 && character == '$':
		default:
			return false
		}
	}
	return true
}

// requireReadOnlyStatement is defence in depth behind the read-only
// transaction: a Query's reads are refused by Postgres anyway, but rejecting an
// obvious write here gives the module a clear error instead of a driver one.
func requireReadOnlyStatement(statement string) error {
	if err := requireSingleStatement(statement); err != nil {
		return err
	}
	head := strings.ToLower(firstWord(statement))
	switch head {
	case "select", "with", "values", "table", "explain", "show":
		return nil
	default:
		return fmt.Errorf("a query may only read; %q is not a read statement", head)
	}
}

// requireSingleStatement rejects a batch: one host call is one statement, so a
// second statement can never ride along behind a semicolon.
func requireSingleStatement(statement string) error {
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return fmt.Errorf("a database statement is required")
	}
	inSingle, inDouble, inLineComment, inBlockComment := false, false, false, false
	runes := []rune(trimmed)
	for index := 0; index < len(runes); index++ {
		character := runes[index]
		next := rune(0)
		if index+1 < len(runes) {
			next = runes[index+1]
		}
		switch {
		case inLineComment:
			if character == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			if character == '*' && next == '/' {
				inBlockComment = false
				index++
			}
		case inSingle:
			if character == '\'' {
				if next == '\'' {
					index++
					continue
				}
				inSingle = false
			}
		case inDouble:
			if character == '"' {
				inDouble = false
			}
		case character == '-' && next == '-':
			inLineComment = true
			index++
		case character == '/' && next == '*':
			inBlockComment = true
			index++
		case character == '\'':
			inSingle = true
		case character == '"':
			inDouble = true
		case character == ';':
			// A trailing semicolon is ordinary; anything after one is a batch.
			if strings.TrimSpace(string(runes[index+1:])) != "" {
				return fmt.Errorf("a database statement may not contain more than one statement")
			}
		}
	}
	if inSingle || inDouble || inBlockComment {
		return fmt.Errorf("a database statement has an unterminated quote or comment")
	}
	return nil
}

func firstWord(statement string) string {
	trimmed := strings.TrimLeft(statement, " \t\r\n(")
	for index, character := range trimmed {
		if character == ' ' || character == '\t' || character == '\r' || character == '\n' || character == '(' {
			return trimmed[:index]
		}
	}
	return trimmed
}

// fetchRequest is the module SDK's fetch, normalized by the bootstrap.
type fetchRequest struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    *string           `json:"body"`
}

// runFetch performs an Action's outbound request. It is bounded on time and on
// response size, and it refuses anything that is not plain HTTP: a module's
// network reach must not become a way to read the host's filesystem.
func runFetch(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var request fetchRequest
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, fmt.Errorf("fetch requires a request")
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("fetch request is not valid JSON: %w", err)
	}
	url := strings.TrimSpace(request.URL)
	if !strings.HasPrefix(strings.ToLower(url), "http://") && !strings.HasPrefix(strings.ToLower(url), "https://") {
		return nil, fmt.Errorf("fetch only supports http and https URLs")
	}
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	if method == "" {
		method = http.MethodGet
	}

	call, cancel := context.WithTimeout(ctx, defaultFetchTimeout)
	defer cancel()
	var body io.Reader
	if request.Body != nil {
		body = strings.NewReader(*request.Body)
	}
	outbound, err := http.NewRequestWithContext(call, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("fetch request is not usable: %w", err)
	}
	for name, value := range request.Headers {
		outbound.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(outbound)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	limited := io.LimitReader(response.Body, maxFetchResponseBytes+1)
	content, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(content) > maxFetchResponseBytes {
		return nil, fmt.Errorf("fetch response exceeds the %d byte limit", maxFetchResponseBytes)
	}
	headers := make(map[string]string, len(response.Header))
	for name := range response.Header {
		headers[strings.ToLower(name)] = response.Header.Get(name)
	}
	return json.Marshal(map[string]any{
		"status":     response.StatusCode,
		"statusText": response.Status,
		"url":        url,
		"headers":    headers,
		"body":       string(content),
	})
}

// runStorage maps the module SDK's storage surface onto the host's StorageAPI.
// Bytes cross the boundary base64-encoded because the op boundary is JSON.
func runStorage(storage gonvex.StorageAPI, operation string, payload json.RawMessage) (json.RawMessage, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage is not available to this function")
	}
	var request struct {
		FileID        string `json:"fileId"`
		TTLMS         int64  `json:"ttlMs"`
		ContentBase64 string `json:"contentBase64"`
		ContentType   string `json:"contentType"`
		Size          int64  `json:"size"`
		Visibility    string `json:"visibility"`
		OwnerID       string `json:"ownerId"`
		ExpiresMS     int64  `json:"expiresMs"`
	}
	if len(bytes.TrimSpace(payload)) > 0 {
		if err := json.Unmarshal(payload, &request); err != nil {
			return nil, fmt.Errorf("storage payload is not valid JSON: %w", err)
		}
	}
	options := gonvex.UploadOptions{
		ContentType: request.ContentType,
		Size:        request.Size,
		Visibility:  gonvex.FileVisibility(request.Visibility),
		OwnerID:     request.OwnerID,
		Expires:     time.Duration(request.ExpiresMS) * time.Millisecond,
	}

	switch strings.TrimSpace(operation) {
	case "generateUploadUrl":
		target, err := storage.GenerateUploadURL(options)
		if err != nil {
			return nil, err
		}
		return json.Marshal(target)
	case "getUrl":
		url, err := storage.GetURL(request.FileID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(url)
	case "generateDownloadUrl":
		ttl := time.Duration(request.TTLMS) * time.Millisecond
		url, err := storage.GenerateDownloadURL(request.FileID, ttl)
		if err != nil {
			return nil, err
		}
		return json.Marshal(url)
	case "getMetadata":
		metadata, err := storage.GetMetadata(request.FileID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(metadata)
	case "delete":
		if err := storage.Delete(request.FileID); err != nil {
			return nil, err
		}
		return json.RawMessage("null"), nil
	case "store":
		content, err := base64.StdEncoding.DecodeString(request.ContentBase64)
		if err != nil {
			return nil, fmt.Errorf("storage content is not valid base64: %w", err)
		}
		metadata, err := storage.Store(content, options)
		if err != nil {
			return nil, err
		}
		return json.Marshal(metadata)
	default:
		return nil, fmt.Errorf("unknown storage operation %q", operation)
	}
}
