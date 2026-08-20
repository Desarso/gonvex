package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

var visibilityIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var errVisibilityChangedDuringLoad = errors.New("visibility changed while its context was loading")

// resolvedVisibilityContext is the immutable authorization input used by both
// row routing and canonical Live Query grouping. It is built from the tenant
// database, cached, and discarded whenever one of the plan's dependency tables
// commits a newer revision.
type resolvedVisibilityContext struct {
	ScopeKey     string
	Revision     uint64
	Fingerprint  string
	Direct       map[string]string
	Role         string
	Permissions  map[string]any
	Sets         map[string]map[string]struct{}
	Dependencies map[string]struct{}
}

func visibilityPlanDependencies(plan manifest.VisibilityPlan) []string {
	values := map[string]struct{}{}
	for _, set := range plan.Sets {
		if table := strings.TrimSpace(set.Table); table != "" {
			values[table] = struct{}{}
		}
		for _, join := range set.Joins {
			if table := strings.TrimSpace(join.Table); table != "" {
				values[table] = struct{}{}
			}
		}
	}
	if visibilityPlanUsesIdentity(plan) {
		values["members"] = struct{}{}
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func visibilityPlanUsesIdentity(plan manifest.VisibilityPlan) bool {
	if visibilityExpressionUsesIdentity(plan.Where) {
		return true
	}
	for _, set := range plan.Sets {
		for _, constraint := range set.Where {
			if constraint.Context == "account.id" || constraint.Context == "member.id" {
				return true
			}
		}
	}
	return false
}

func visibilityExpressionUsesIdentity(expression *manifest.VisibilityExpression) bool {
	if expression == nil {
		return false
	}
	if expression.Operator == "permission" || expression.Operator == "role" || expression.Context != "" {
		return true
	}
	for _, child := range expression.Children {
		if visibilityExpressionUsesIdentity(child) {
			return true
		}
	}
	return false
}

func (s *Server) visibilityPlan(project, table string) (manifest.VisibilityPlan, bool) {
	current := s.runtime.ManifestForProject(project)
	plan, ok := current.Visibility[strings.TrimSpace(table)]
	return plan, ok
}

func (s *Server) requireVisibilityPlans(current manifest.Manifest) error {
	if current.Module != nil && current.Module.Visibility != nil {
		manifestPayload, _ := json.Marshal(current.Visibility)
		artifactPayload, _ := json.Marshal(current.Module.Visibility)
		if string(manifestPayload) != string(artifactPayload) {
			return fmt.Errorf("manifest and module artifact visibility plans disagree")
		}
	}
	for path, entry := range current.Functions {
		if entry.Kind != manifest.FunctionKindQuery {
			continue
		}
		table := ""
		key := ""
		switch entry.Delivery {
		case manifest.DeliveryReplica:
			if entry.Replica != nil {
				table = strings.TrimSpace(entry.Replica.Table)
				key = strings.TrimSpace(entry.Replica.Key)
			}
		case manifest.DeliveryLive:
			if entry.Dependencies.LiveQueryPlan != nil {
				table = strings.TrimSpace(entry.Dependencies.LiveQueryPlan.Table)
				key = strings.TrimSpace(entry.Dependencies.LiveQueryPlan.Key)
			}
		default:
			continue
		}
		if table == "" {
			return fmt.Errorf("query %q has no source table", path)
		}
		plan, ok := current.Visibility[table]
		if !ok {
			return fmt.Errorf("query %q requires an explicit visibility plan for table %q", path, table)
		}
		if err := validateVisibilityPlan(table, plan); err != nil {
			return fmt.Errorf("query %q visibility: %w", path, err)
		}
		if plan.Key != key {
			return fmt.Errorf("query %q key %q does not match visibility key %q", path, key, plan.Key)
		}
	}
	return nil
}

func validateVisibilityPlan(table string, plan manifest.VisibilityPlan) error {
	if strings.TrimSpace(plan.Table) != table {
		return fmt.Errorf("plan table %q does not match source table %q", plan.Table, table)
	}
	if err := validateVisibilityIdentifier(plan.Table); err != nil {
		return err
	}
	if err := validateVisibilityIdentifier(plan.Key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	if plan.Where == nil {
		return fmt.Errorf("plan must declare where; use operator public for intentionally public rows")
	}
	for name, set := range plan.Sets {
		if err := validateVisibilityIdentifier(name); err != nil {
			return fmt.Errorf("invalid set name: %w", err)
		}
		if err := validateVisibilityIdentifier(set.Table); err != nil {
			return fmt.Errorf("set %q table: %w", name, err)
		}
		if err := validateVisibilityIdentifier(set.Select); err != nil {
			return fmt.Errorf("set %q select: %w", name, err)
		}
		seenTables := map[string]bool{set.Table: true}
		for _, join := range set.Joins {
			if err := validateVisibilityIdentifier(join.Table); err != nil {
				return fmt.Errorf("set %q join table: %w", name, err)
			}
			if seenTables[join.Table] {
				return fmt.Errorf("set %q uses table %q more than once", name, join.Table)
			}
			seenTables[join.Table] = true
			if err := validateVisibilityIdentifier(join.LeftColumn); err != nil {
				return fmt.Errorf("set %q join left column: %w", name, err)
			}
			if err := validateVisibilityIdentifier(join.RightColumn); err != nil {
				return fmt.Errorf("set %q join right column: %w", name, err)
			}
		}
		for _, constraint := range set.Where {
			if constraint.Table != "" && !seenTables[constraint.Table] {
				return fmt.Errorf("set %q constraint references table %q outside the set", name, constraint.Table)
			}
			if err := validateVisibilityIdentifier(constraint.Column); err != nil {
				return fmt.Errorf("set %q constraint column: %w", name, err)
			}
			if !validVisibilityContextKey(constraint.Context) {
				return fmt.Errorf("set %q has unsupported context %q", name, constraint.Context)
			}
		}
	}
	return validateVisibilityExpression(plan.Where, plan.Sets)
}

func validateVisibilityExpression(expression *manifest.VisibilityExpression, sets map[string]manifest.VisibilitySet) error {
	if expression == nil {
		return fmt.Errorf("missing expression")
	}
	switch expression.Operator {
	case "public":
		return nil
	case "permission", "role":
		if strings.TrimSpace(expression.Value) == "" {
			return fmt.Errorf("%s expression requires value", expression.Operator)
		}
	case "eqContext":
		if err := validateVisibilityIdentifier(expression.Column); err != nil {
			return err
		}
		if !validVisibilityContextKey(expression.Context) {
			return fmt.Errorf("unsupported context %q", expression.Context)
		}
	case "inSet":
		if err := validateVisibilityIdentifier(expression.Column); err != nil {
			return err
		}
		if _, ok := sets[expression.Set]; !ok {
			return fmt.Errorf("unknown visibility set %q", expression.Set)
		}
	case "and", "or":
		if len(expression.Children) == 0 {
			return fmt.Errorf("%s expression requires children", expression.Operator)
		}
	case "not":
		if len(expression.Children) != 1 {
			return fmt.Errorf("not expression requires exactly one child")
		}
	default:
		return fmt.Errorf("unsupported visibility operator %q", expression.Operator)
	}
	for _, child := range expression.Children {
		if err := validateVisibilityExpression(child, sets); err != nil {
			return err
		}
	}
	return nil
}

func validateVisibilityIdentifier(value string) error {
	if !visibilityIdentifierPattern.MatchString(strings.TrimSpace(value)) {
		return fmt.Errorf("invalid SQL identifier %q", value)
	}
	return nil
}

func validVisibilityContextKey(value string) bool {
	switch strings.TrimSpace(value) {
	case "account.id", "member.id", "tenant.id":
		return true
	default:
		return false
	}
}

func quoteVisibilityIdentifier(value string) (string, error) {
	if err := validateVisibilityIdentifier(value); err != nil {
		return "", err
	}
	return `"` + value + `"`, nil
}

func visibilityContextCacheKey(project, tenant string, caller callerContext, plan manifest.VisibilityPlan) string {
	payload, _ := json.Marshal(struct {
		Project string                  `json:"project"`
		Tenant  string                  `json:"tenant"`
		Account string                  `json:"account"`
		Member  string                  `json:"member"`
		Plan    manifest.VisibilityPlan `json:"plan"`
	}{project, tenant, visibilityAccountID(caller), visibilityMemberID(caller), plan})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func visibilityAccountID(caller callerContext) string {
	if caller.user == nil {
		return ""
	}
	return strings.TrimSpace(caller.user.ID)
}

func visibilityMemberID(caller callerContext) string {
	if caller.member == nil {
		return ""
	}
	return strings.TrimSpace(caller.member.ID)
}

func (s *Server) resolveVisibilityContext(ctx context.Context, project, tenant string, caller callerContext, plan manifest.VisibilityPlan, requiredRevision uint64) (*resolvedVisibilityContext, error) {
	if err := validateVisibilityPlan(plan.Table, plan); err != nil {
		return nil, err
	}
	key := visibilityContextCacheKey(project, tenant, caller, plan)
	scope := project + "\x00" + tenant
	projectScope := project + "\x00*"
	for attempt := 0; attempt < 3; attempt++ {
		s.visibilityMu.Lock()
		cached := s.visibilityContexts[key]
		if cached != nil && cached.Revision >= requiredRevision {
			s.visibilityMu.Unlock()
			return cached, nil
		}
		epoch := s.visibilityEpochs[scope] + s.visibilityEpochs[projectScope]
		s.visibilityMu.Unlock()

		value, err, _ := s.visibilityLoads.Do(key+":"+strconv.FormatUint(requiredRevision, 10)+":"+strconv.FormatUint(epoch, 10), func() (any, error) {
			s.visibilityMu.Lock()
			cached := s.visibilityContexts[key]
			if cached != nil && cached.Revision >= requiredRevision {
				s.visibilityMu.Unlock()
				return cached, nil
			}
			if s.visibilityEpochs[scope]+s.visibilityEpochs[projectScope] != epoch {
				s.visibilityMu.Unlock()
				return nil, errVisibilityChangedDuringLoad
			}
			s.visibilityMu.Unlock()
			resolved, loadErr := s.loadVisibilityContext(ctx, project, tenant, caller, plan)
			if loadErr != nil {
				return nil, loadErr
			}
			if resolved.Revision < requiredRevision {
				return nil, fmt.Errorf("visibility context revision %d is behind required revision %d", resolved.Revision, requiredRevision)
			}
			s.visibilityMu.Lock()
			defer s.visibilityMu.Unlock()
			if s.visibilityEpochs[scope]+s.visibilityEpochs[projectScope] != epoch {
				return nil, errVisibilityChangedDuringLoad
			}
			s.visibilityContexts[key] = resolved
			return resolved, nil
		})
		if errors.Is(err, errVisibilityChangedDuringLoad) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return value.(*resolvedVisibilityContext), nil
	}
	return nil, errVisibilityChangedDuringLoad
}

func (s *Server) loadVisibilityContext(ctx context.Context, project, tenant string, caller callerContext, plan manifest.VisibilityPlan) (*resolvedVisibilityContext, error) {
	databaseURL := s.databaseURLForTenant(project, tenant)
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolved := &resolvedVisibilityContext{
		ScopeKey: project + "\x00" + tenant,
		Direct: map[string]string{
			"account.id": visibilityAccountID(caller),
			"member.id":  visibilityMemberID(caller),
			"tenant.id":  strings.TrimSpace(tenant),
		},
		Permissions:  map[string]any{},
		Sets:         map[string]map[string]struct{}{},
		Dependencies: map[string]struct{}{},
	}
	if visibilityPlanUsesIdentity(plan) {
		accountID := resolved.Direct["account.id"]
		if accountID == "" {
			return nil, fmt.Errorf("visibility requires an authenticated account")
		}
		var (
			memberID       string
			role           string
			rawPermissions []byte
		)
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(NULLIF(id, ''), user_id), role, permissions
			FROM members
			WHERE (account_id = $1 OR user_id = $1) AND status = 'active'
		`, accountID).Scan(&memberID, &role, &rawPermissions); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("active tenant member for account %q not found", accountID)
			}
			return nil, err
		}
		resolved.Direct["member.id"] = memberID
		resolved.Role = role
		if len(rawPermissions) > 0 {
			if err := json.Unmarshal(rawPermissions, &resolved.Permissions); err != nil {
				return nil, err
			}
		}
		resolved.Permissions["role"] = role
		resolved.Dependencies["members"] = struct{}{}
	}
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM _gonvex_sync_clock WHERE singleton = true`).Scan(&resolved.Revision); err != nil {
		return nil, fmt.Errorf("read visibility revision: %w", err)
	}
	setNames := make([]string, 0, len(plan.Sets))
	for name := range plan.Sets {
		setNames = append(setNames, name)
	}
	sort.Strings(setNames)
	for _, name := range setNames {
		set := plan.Sets[name]
		query, args, buildErr := compileVisibilitySet(set, resolved.Direct)
		if buildErr != nil {
			return nil, fmt.Errorf("visibility set %q: %w", name, buildErr)
		}
		rows, queryErr := tx.QueryContext(ctx, query, args...)
		if queryErr != nil {
			return nil, fmt.Errorf("visibility set %q: %w", name, queryErr)
		}
		values := map[string]struct{}{}
		for rows.Next() {
			var value any
			if scanErr := rows.Scan(&value); scanErr != nil {
				rows.Close()
				return nil, scanErr
			}
			values[visibilityScalar(value)] = struct{}{}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return nil, rowsErr
		}
		rows.Close()
		resolved.Sets[name] = values
		resolved.Dependencies[set.Table] = struct{}{}
		for _, join := range set.Joins {
			resolved.Dependencies[join.Table] = struct{}{}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	resolved.Fingerprint = visibilityFingerprint(plan, resolved)
	return resolved, nil
}

func compileVisibilitySet(set manifest.VisibilitySet, direct map[string]string) (string, []any, error) {
	args := []any{}
	query, err := compileVisibilitySetUsing(set, direct, func(value any) string {
		args = append(args, value)
		return "$" + strconv.Itoa(len(args))
	})
	return query, args, err
}

func compileVisibilitySetUsing(set manifest.VisibilitySet, direct map[string]string, argument func(any) string) (string, error) {
	baseTable, err := quoteVisibilityIdentifier(set.Table)
	if err != nil {
		return "", err
	}
	selected, err := quoteVisibilityIdentifier(set.Select)
	if err != nil {
		return "", err
	}
	aliases := map[string]string{set.Table: "v0"}
	query := `SELECT DISTINCT v0.` + selected + ` FROM ` + baseTable + ` AS v0`
	previousAlias := "v0"
	for index, join := range set.Joins {
		table, tableErr := quoteVisibilityIdentifier(join.Table)
		left, leftErr := quoteVisibilityIdentifier(join.LeftColumn)
		right, rightErr := quoteVisibilityIdentifier(join.RightColumn)
		if tableErr != nil || leftErr != nil || rightErr != nil {
			return "", fmt.Errorf("invalid join")
		}
		alias := "v" + strconv.Itoa(index+1)
		query += ` JOIN ` + table + ` AS ` + alias + ` ON ` + previousAlias + `.` + left + ` = ` + alias + `.` + right
		aliases[join.Table] = alias
		previousAlias = alias
	}
	predicates := []string{}
	for _, constraint := range set.Where {
		alias := "v0"
		if constraint.Table != "" {
			alias = aliases[constraint.Table]
		}
		column, columnErr := quoteVisibilityIdentifier(constraint.Column)
		if columnErr != nil || alias == "" {
			return "", fmt.Errorf("invalid constraint")
		}
		predicates = append(predicates, fmt.Sprintf(`%s.%s = %s`, alias, column, argument(direct[constraint.Context])))
	}
	if len(predicates) > 0 {
		query += ` WHERE ` + strings.Join(predicates, ` AND `)
	}
	return query, nil
}

func visibilityFingerprint(plan manifest.VisibilityPlan, resolved *resolvedVisibilityContext) string {
	directKeys := map[string]struct{}{}
	permissionKeys := map[string]struct{}{}
	rolesUsed := false
	visibilityFingerprintInputs(plan.Where, directKeys, permissionKeys, &rolesUsed)
	direct := map[string]string{}
	for key := range directKeys {
		direct[key] = resolved.Direct[key]
	}
	permissions := map[string]any{}
	for key := range permissionKeys {
		permissions[key] = resolved.Permissions[key]
	}
	role := ""
	if rolesUsed {
		role = resolved.Role
	}
	setValues := map[string][]string{}
	for name, values := range resolved.Sets {
		items := make([]string, 0, len(values))
		for value := range values {
			items = append(items, value)
		}
		sort.Strings(items)
		setValues[name] = items
	}
	payload, _ := json.Marshal(struct {
		Plan        manifest.VisibilityPlan `json:"plan"`
		Direct      map[string]string       `json:"direct"`
		Role        string                  `json:"role"`
		Permissions map[string]any          `json:"permissions"`
		Sets        map[string][]string     `json:"sets"`
	}{plan, direct, role, permissions, setValues})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func visibilityFingerprintInputs(expression *manifest.VisibilityExpression, direct, permissions map[string]struct{}, rolesUsed *bool) {
	if expression == nil {
		return
	}
	switch expression.Operator {
	case "permission":
		permissions[expression.Value] = struct{}{}
	case "role":
		*rolesUsed = true
	case "eqContext":
		direct[expression.Context] = struct{}{}
	}
	for _, child := range expression.Children {
		visibilityFingerprintInputs(child, direct, permissions, rolesUsed)
	}
}

func (s *Server) invalidateVisibilityContexts(project, tenant string, changedTables []string) {
	if len(changedTables) == 0 {
		return
	}
	scope := project + "\x00" + tenant
	s.visibilityMu.Lock()
	s.visibilityEpochs[scope]++
	for key, resolved := range s.visibilityContexts {
		if resolved.ScopeKey != scope {
			continue
		}
		for _, table := range changedTables {
			if _, ok := resolved.Dependencies[table]; ok {
				delete(s.visibilityContexts, key)
				break
			}
		}
	}
	s.visibilityMu.Unlock()
}

func (s *Server) invalidateAllVisibilityContexts(project, tenant string) {
	scope := project + "\x00" + tenant
	s.visibilityMu.Lock()
	s.visibilityEpochs[scope]++
	for key, resolved := range s.visibilityContexts {
		if resolved.ScopeKey == scope {
			delete(s.visibilityContexts, key)
		}
	}
	s.visibilityMu.Unlock()
}

func (s *Server) invalidateProjectVisibilityContexts(project string) {
	prefix := project + "\x00"
	s.visibilityMu.Lock()
	s.visibilityEpochs[project+"\x00*"]++
	for key, resolved := range s.visibilityContexts {
		if strings.HasPrefix(resolved.ScopeKey, prefix) {
			delete(s.visibilityContexts, key)
		}
	}
	s.visibilityMu.Unlock()
}

func visibilityExpressionMatches(expression *manifest.VisibilityExpression, row map[string]any, resolved *resolvedVisibilityContext) bool {
	if expression == nil || resolved == nil {
		return false
	}
	switch expression.Operator {
	case "public":
		return true
	case "permission":
		return visibilityTruthy(resolved.Permissions[expression.Value])
	case "role":
		return resolved.Role == expression.Value
	case "eqContext":
		return visibilityScalar(row[expression.Column]) == visibilityScalar(resolved.Direct[expression.Context])
	case "inSet":
		_, ok := resolved.Sets[expression.Set][visibilityScalar(row[expression.Column])]
		return ok
	case "and":
		for _, child := range expression.Children {
			if !visibilityExpressionMatches(child, row, resolved) {
				return false
			}
		}
		return len(expression.Children) > 0
	case "or":
		for _, child := range expression.Children {
			if visibilityExpressionMatches(child, row, resolved) {
				return true
			}
		}
		return false
	case "not":
		return len(expression.Children) == 1 && !visibilityExpressionMatches(expression.Children[0], row, resolved)
	default:
		return false
	}
}

func visibilityTruthy(value any) bool {
	switch current := value.(type) {
	case bool:
		return current
	case string:
		parsed, err := strconv.ParseBool(current)
		return err == nil && parsed
	case float64:
		return current != 0
	default:
		return false
	}
}

func visibilityScalar(value any) string {
	switch current := value.(type) {
	case nil:
		return "null"
	case []byte:
		return string(current)
	case string:
		return current
	case json.Number:
		return current.String()
	default:
		encoded, err := json.Marshal(current)
		if err != nil {
			return fmt.Sprint(current)
		}
		return string(encoded)
	}
}

func visibilityRawRowMatches(raw json.RawMessage, plan manifest.VisibilityPlan, resolved *resolvedVisibilityContext) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	row := map[string]any{}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&row) != nil {
		return false
	}
	return visibilityExpressionMatches(plan.Where, row, resolved)
}

func visibilityTransitionOperation(oldVisible, newVisible bool) (string, bool) {
	switch {
	case oldVisible && newVisible:
		return "update", true
	case !oldVisible && newVisible:
		return "insert", true
	case oldVisible && !newVisible:
		return "delete", true
	default:
		return "", false
	}
}

func memberChangeIdentities(batch syncChangeBatch) map[string]struct{} {
	identities := map[string]struct{}{}
	for _, change := range batch.changes {
		if change.table != "members" {
			continue
		}
		for _, raw := range []json.RawMessage{change.oldValue, change.newValue} {
			row := map[string]any{}
			if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &row) != nil {
				continue
			}
			for _, field := range []string{"id", "member_id", "memberId", "account_id", "accountId", "user_id", "userId"} {
				if identity := strings.TrimSpace(fmt.Sprint(row[field])); identity != "" && identity != "<nil>" {
					identities[identity] = struct{}{}
				}
			}
		}
	}
	return identities
}

// refreshCommittedMemberConnections removes stale tenant-local identity from
// live sockets whenever its authoritative members row changes. A valid session
// can authenticate again and receive the new role/permissions; a revoked member
// fails the tenant database admission check.
func (s *Server) refreshCommittedMemberConnections(project, tenant string, batch syncChangeBatch) {
	identities := memberChangeIdentities(batch)
	if len(identities) == 0 {
		return
	}
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project && connection.tenant == tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := false
		if connection.user != nil {
			_, matches = identities[connection.user.ID]
		}
		if connection.member != nil && !matches {
			_, memberMatches := identities[connection.member.ID]
			_, accountMatches := identities[connection.member.AccountID]
			matches = memberMatches || accountMatches
		}
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{
				Type: "auth.error", ID: "membership-changed",
				Error: "tenant membership changed; authenticate again",
			})
		}
	}
}

type visibilitySQLBuilder struct {
	args []any
}

func (builder *visibilitySQLBuilder) argument(value any) string {
	if number, ok := value.(json.Number); ok {
		if integer, err := number.Int64(); err == nil {
			value = integer
		} else if decimal, err := number.Float64(); err == nil {
			value = decimal
		}
	}
	builder.args = append(builder.args, value)
	return "$" + strconv.Itoa(len(builder.args))
}

func compileVisibilitySQL(expression *manifest.VisibilityExpression, plan manifest.VisibilityPlan, direct map[string]string, permissions map[string]any, role string, builder *visibilitySQLBuilder, rowAlias string) (string, error) {
	if expression == nil {
		return "FALSE", nil
	}
	switch expression.Operator {
	case "public":
		return "TRUE", nil
	case "permission":
		return compileMemberAttributeSQL(direct, builder,
			`lower(COALESCE(_gonvex_member.permissions ->> `+builder.argument(expression.Value)+`::text, '')) IN ('true', '1')`), nil
	case "role":
		return compileMemberAttributeSQL(direct, builder,
			`_gonvex_member.role = `+builder.argument(expression.Value)), nil
	case "eqContext":
		column, err := quoteVisibilityIdentifier(expression.Column)
		if err != nil {
			return "", err
		}
		return rowAlias + "." + column + " = " + builder.argument(direct[expression.Context]), nil
	case "inSet":
		column, err := quoteVisibilityIdentifier(expression.Column)
		if err != nil {
			return "", err
		}
		set, ok := plan.Sets[expression.Set]
		if !ok {
			return "", fmt.Errorf("unknown visibility set %q", expression.Set)
		}
		subquery, err := compileVisibilitySetWithBuilder(set, direct, builder)
		if err != nil {
			return "", err
		}
		return rowAlias + "." + column + " IN (" + subquery + ")", nil
	case "and", "or":
		parts := make([]string, 0, len(expression.Children))
		for _, child := range expression.Children {
			part, err := compileVisibilitySQL(child, plan, direct, permissions, role, builder, rowAlias)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+part+")")
		}
		if len(parts) == 0 {
			return "FALSE", nil
		}
		return strings.Join(parts, " "+strings.ToUpper(expression.Operator)+" "), nil
	case "not":
		if len(expression.Children) != 1 {
			return "FALSE", nil
		}
		part, err := compileVisibilitySQL(expression.Children[0], plan, direct, permissions, role, builder, rowAlias)
		if err != nil {
			return "", err
		}
		return "NOT (" + part + ")", nil
	default:
		return "FALSE", fmt.Errorf("unsupported visibility operator %q", expression.Operator)
	}
}

func compileMemberAttributeSQL(direct map[string]string, builder *visibilitySQLBuilder, predicate string) string {
	account := builder.argument(direct["account.id"])
	return `EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE ` +
		`(_gonvex_member.account_id = ` + account + ` OR _gonvex_member.user_id = ` + account + `) ` +
		`AND _gonvex_member.status = 'active' AND (` + predicate + `))`
}

func compileActiveMemberSQL(direct map[string]string, builder *visibilitySQLBuilder) string {
	return compileMemberAttributeSQL(direct, builder, "TRUE")
}

func compileVisibilitySetWithBuilder(set manifest.VisibilitySet, direct map[string]string, builder *visibilitySQLBuilder) (string, error) {
	return compileVisibilitySetUsing(set, direct, builder.argument)
}

func (s *Server) executeStructuredReplicaQuery(ctx context.Context, project, tenant string, caller callerContext, definition manifest.ReplicaCollectionDefinition, rawArgs json.RawMessage) (any, error) {
	plan, ok := s.visibilityPlan(project, definition.Table)
	if !ok {
		return nil, fmt.Errorf("visibility plan required for replica table %q", definition.Table)
	}
	resolved, err := s.resolveVisibilityContext(ctx, project, tenant, caller, plan, 0)
	if err != nil {
		return nil, err
	}
	args := map[string]any{}
	if len(rawArgs) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(rawArgs)))
		decoder.UseNumber()
		if err := decoder.Decode(&args); err != nil {
			return nil, err
		}
	}
	builder := &visibilitySQLBuilder{}
	predicates := make([]string, 0, 1+len(definition.EqualFilters)+len(definition.ExcludeWhenSet))
	visibilityPredicate, err := compileVisibilitySQL(plan.Where, plan, resolved.Direct, resolved.Permissions, resolved.Role, builder, "r")
	if err != nil {
		return nil, err
	}
	predicates = append(predicates, visibilityPredicate)
	if visibilityPlanUsesIdentity(plan) {
		predicates = append(predicates, compileActiveMemberSQL(resolved.Direct, builder))
	}
	for _, columnName := range definition.ExcludeWhenSet {
		column, err := quoteVisibilityIdentifier(columnName)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, "r."+column+" IS NULL")
	}
	filterColumns := make([]string, 0, len(definition.EqualFilters))
	for column := range definition.EqualFilters {
		filterColumns = append(filterColumns, column)
	}
	sort.Strings(filterColumns)
	for _, columnName := range filterColumns {
		column, err := quoteVisibilityIdentifier(columnName)
		if err != nil {
			return nil, err
		}
		argument := definition.EqualFilters[columnName]
		value, exists := args[argument]
		if !exists {
			return nil, fmt.Errorf("replica filter argument %q is required", argument)
		}
		predicates = append(predicates, "r."+column+" = "+builder.argument(value))
	}
	limit := 0
	if definition.MaxRows > 0 {
		limit = definition.MaxRows + 1
	}
	return s.queryVisibleRows(ctx, project, tenant, definition.Table, definition.Columns, predicates, definition.OrderBy, definition.OrderDirection, 0, limit, builder.args)
}

func (s *Server) executeStructuredLiveQuery(ctx context.Context, project, tenant string, caller callerContext, plan manifest.LiveQueryPlan, rawArgs json.RawMessage) (any, error) {
	visibility, ok := s.visibilityPlan(project, plan.Table)
	if !ok {
		return nil, fmt.Errorf("visibility plan required for live query table %q", plan.Table)
	}
	resolved, err := s.resolveVisibilityContext(ctx, project, tenant, caller, visibility, 0)
	if err != nil {
		return nil, err
	}
	args := map[string]any{}
	if len(rawArgs) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(rawArgs)))
		decoder.UseNumber()
		if err := decoder.Decode(&args); err != nil {
			return nil, err
		}
	}
	builder := &visibilitySQLBuilder{}
	predicates := []string{}
	visibilityPredicate, err := compileVisibilitySQL(visibility.Where, visibility, resolved.Direct, resolved.Permissions, resolved.Role, builder, "r")
	if err != nil {
		return nil, err
	}
	predicates = append(predicates, visibilityPredicate)
	if visibilityPlanUsesIdentity(visibility) {
		predicates = append(predicates, compileActiveMemberSQL(resolved.Direct, builder))
	}
	if plan.Where != nil {
		predicate, compileErr := compileLiveExpressionSQL(plan.Where, args, builder, "r")
		if compileErr != nil {
			return nil, compileErr
		}
		predicates = append(predicates, predicate)
	}
	if plan.Search != nil {
		search := strings.TrimSpace(fmt.Sprint(args[plan.Search.Argument]))
		if search != "" && search != "<nil>" {
			parts := make([]string, 0, len(plan.Search.Columns))
			for _, columnName := range plan.Search.Columns {
				column, columnErr := quoteVisibilityIdentifier(columnName)
				if columnErr != nil {
					return nil, columnErr
				}
				parts = append(parts, "strpos(lower(COALESCE(r."+column+"::text, '')), lower("+builder.argument(search)+"::text)) > 0")
			}
			if len(parts) > 0 {
				predicates = append(predicates, "("+strings.Join(parts, " OR ")+")")
			}
		}
	}
	orderBy, direction := "", ""
	if plan.Sort != nil {
		orderBy = plan.Sort.DefaultColumn
		if candidate := strings.TrimSpace(fmt.Sprint(args[plan.Sort.ColumnArgument])); candidate != "" && stringInSlice(candidate, plan.Sort.AllowedColumns) {
			orderBy = candidate
		}
		direction = strings.ToLower(plan.Sort.DefaultDirection)
		if candidate := strings.ToLower(strings.TrimSpace(fmt.Sprint(args[plan.Sort.DirectionArgument]))); candidate == "asc" || candidate == "desc" {
			direction = candidate
		}
	}
	offset, limit := 0, 0
	if plan.Window != nil {
		offset = visibilityIntArgument(args[plan.Window.OffsetArgument], 0)
		limit = visibilityIntArgument(args[plan.Window.LimitArgument], plan.Window.DefaultLimit)
		if limit <= 0 {
			limit = plan.Window.DefaultLimit
		}
		if plan.Window.MaxLimit > 0 && limit > plan.Window.MaxLimit {
			limit = plan.Window.MaxLimit
		}
	}
	columns := append([]string(nil), plan.Columns...)
	if len(columns) == 0 {
		return nil, fmt.Errorf("live query for %q must declare columns", plan.Table)
	}
	if !stringInSlice(plan.Key, columns) {
		columns = append(columns, plan.Key)
	}
	rows, err := s.queryVisibleRows(ctx, project, tenant, plan.Table, columns, predicates, orderBy, direction, offset, limit, builder.args)
	if err != nil {
		return nil, err
	}
	return visibilityResultAtPath(rows, plan.ResultPath), nil
}

func (s *Server) queryVisibleRows(ctx context.Context, project, tenant, tableName string, columnNames, predicates []string, orderBy, direction string, offset, limit int, args []any) ([]any, error) {
	table, err := quoteVisibilityIdentifier(tableName)
	if err != nil {
		return nil, err
	}
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("query table %q must declare columns", tableName)
	}
	columns := make([]string, 0, len(columnNames))
	seen := map[string]bool{}
	for _, columnName := range columnNames {
		if seen[columnName] {
			continue
		}
		seen[columnName] = true
		column, columnErr := quoteVisibilityIdentifier(columnName)
		if columnErr != nil {
			return nil, columnErr
		}
		columns = append(columns, "r."+column+" AS "+column)
	}
	query := `SELECT row_to_json(_gonvex_visible_row)::text FROM (SELECT ` + strings.Join(columns, ", ") + ` FROM ` + table + ` AS r`
	if len(predicates) > 0 {
		query += ` WHERE ` + strings.Join(predicates, ` AND `)
	}
	if orderBy != "" {
		column, columnErr := quoteVisibilityIdentifier(orderBy)
		if columnErr != nil {
			return nil, columnErr
		}
		if direction != "asc" {
			direction = "desc"
		}
		query += ` ORDER BY r.` + column + ` ` + strings.ToUpper(direction)
	}
	if limit > 0 {
		args = append(args, limit)
		query += ` LIMIT $` + strconv.Itoa(len(args))
	}
	if offset > 0 {
		args = append(args, offset)
		query += ` OFFSET $` + strconv.Itoa(len(args))
	}
	query += `) AS _gonvex_visible_row`
	databaseURL := s.databaseURLForTenant(project, tenant)
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []any{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value any
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func compileLiveExpressionSQL(expression *manifest.LiveExpression, args map[string]any, builder *visibilitySQLBuilder, rowAlias string) (string, error) {
	if expression == nil {
		return "TRUE", nil
	}
	switch expression.Operator {
	case "and", "or":
		parts := make([]string, 0, len(expression.Children))
		for _, child := range expression.Children {
			part, err := compileLiveExpressionSQL(child, args, builder, rowAlias)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+part+")")
		}
		if len(parts) == 0 {
			return "FALSE", nil
		}
		return strings.Join(parts, " "+strings.ToUpper(expression.Operator)+" "), nil
	case "not":
		if len(expression.Children) != 1 {
			return "", fmt.Errorf("live not expression requires one child")
		}
		part, err := compileLiveExpressionSQL(expression.Children[0], args, builder, rowAlias)
		if err != nil {
			return "", err
		}
		return "NOT (" + part + ")", nil
	case "server":
		return "", fmt.Errorf("server-only arbitrary reactive predicates are not supported")
	}
	column, err := quoteVisibilityIdentifier(expression.Column)
	if err != nil {
		return "", err
	}
	left := rowAlias + "." + column
	value := liveSQLValue(expression.Value, args)
	switch expression.Operator {
	case "eq":
		return left + " = " + builder.argument(value), nil
	case "neq":
		return left + " <> " + builder.argument(value), nil
	case "gt", "gte", "lt", "lte":
		operators := map[string]string{"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
		return left + " " + operators[expression.Operator] + " " + builder.argument(value), nil
	case "range":
		return left + " BETWEEN " + builder.argument(value) + " AND " + builder.argument(liveSQLValue(expression.ValueTo, args)), nil
	case "contains":
		return "strpos(COALESCE(" + left + "::text, ''), " + builder.argument(fmt.Sprint(value)) + "::text) > 0", nil
	case "containsInsensitive":
		return "strpos(lower(COALESCE(" + left + "::text, '')), lower(" + builder.argument(fmt.Sprint(value)) + "::text)) > 0", nil
	case "in":
		values, ok := value.([]any)
		if !ok || len(values) == 0 {
			return "FALSE", nil
		}
		placeholders := make([]string, 0, len(values))
		for _, item := range values {
			placeholders = append(placeholders, builder.argument(item))
		}
		return left + " IN (" + strings.Join(placeholders, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported live query operator %q", expression.Operator)
	}
}

func liveSQLValue(value *manifest.LiveValue, args map[string]any) any {
	if value == nil {
		return nil
	}
	if value.Argument != "" {
		return args[value.Argument]
	}
	return value.Literal
}

func visibilityIntArgument(value any, fallback int) int {
	switch current := value.(type) {
	case json.Number:
		parsed, err := strconv.Atoi(current.String())
		if err == nil && parsed >= 0 {
			return parsed
		}
	case float64:
		if current >= 0 {
			return int(current)
		}
	case int:
		if current >= 0 {
			return current
		}
	case string:
		parsed, err := strconv.Atoi(current)
		if err == nil && parsed >= 0 {
			return parsed
		}
	}
	return fallback
}

func stringInSlice(value string, values []string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func visibilityResultAtPath(rows []any, path []string) any {
	if len(path) == 0 {
		return rows
	}
	root := map[string]any{}
	cursor := root
	for index, part := range path {
		if index == len(path)-1 {
			cursor[part] = rows
			break
		}
		next := map[string]any{}
		cursor[part] = next
		cursor = next
	}
	return root
}
