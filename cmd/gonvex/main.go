package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

const defaultRuntimeURL = "http://localhost:8080"

type projectSettings struct {
	ProjectID  string
	RuntimeURL string
	Key        string
	DryRun     bool
}

type gonvexConfig struct {
	Project string `json:"project"`
	Runtime string `json:"runtime"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		printHelp()
		return nil
	}

	switch args[0] {
	case "dev":
		return runDev(args[1:])
	case "migrate":
		return runMigrate(args[1:])
	default:
		printHelp()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runDev(args []string) error {
	flags := flag.NewFlagSet("dev", flag.ContinueOnError)
	project := flags.String("project", ".", "project root containing gonvex/ backend functions")
	runtimeURL := flags.String("runtime-url", "", "gonvex runtime URL")
	projectID := flags.String("project-id", "", "gonvex project ID")
	key := flags.String("key", "", "gonvex project key")
	once := flags.Bool("once", false, "generate and sync once, then exit")
	dryRun := flags.Bool("dry-run", false, "show pending SQL migrations without changing the runtime")
	if err := flags.Parse(args); err != nil {
		return err
	}
	childCommand := flags.Args()
	if *once && len(childCommand) > 0 {
		return fmt.Errorf("--once cannot be used with a child command")
	}

	root, err := filepath.Abs(*project)
	if err != nil {
		return err
	}
	settings := loadProjectSettings(root)
	if *runtimeURL != "" {
		settings.RuntimeURL = *runtimeURL
	}
	if *projectID != "" {
		settings.ProjectID = *projectID
	}
	if *key != "" {
		settings.Key = *key
	}
	settings.DryRun = *dryRun
	if *dryRun && !*once {
		return fmt.Errorf("--dry-run requires --once")
	}
	if len(childCommand) > 0 {
		return runDevWithCommand(root, settings, childCommand)
	}

	return watchProject(context.Background(), root, settings, *once)
}

func runDevWithCommand(root string, settings projectSettings, childCommand []string) error {
	// The first sync commonly races the runtime's own startup: `make dev`
	// launches the runtime and this command together, and the runtime needs a
	// few seconds to compile and bind. Failing on the first refused connection
	// takes the whole dev session down, so wait for the runtime to accept the
	// sync before starting the child process. Only connection-level failures
	// are retried — a rejected sync (bad key, invalid schema) fails fast.
	if err := syncWithStartupRetry(root, settings); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchErrors := make(chan error, 1)
	go func() {
		watchErrors <- watchProject(ctx, root, settings, false)
	}()

	command := exec.CommandContext(ctx, childCommand[0], childCommand[1:]...)
	command.Dir = root
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Stdin = os.Stdin

	if err := command.Start(); err != nil {
		cancel()
		return err
	}

	commandErr := command.Wait()
	cancel()

	select {
	case watchErr := <-watchErrors:
		if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
			return watchErr
		}
	default:
	}

	return commandErr
}

// devStartupSyncTimeout bounds how long the first sync waits for a runtime that
// is still starting. Long enough for a cold `go build` of the runtime binary,
// short enough that a genuinely absent runtime still fails the session.
var devStartupSyncTimeout = 60 * time.Second

func syncWithStartupRetry(root string, settings projectSettings) error {
	deadline := time.Now().Add(devStartupSyncTimeout)
	announced := false
	for attempt := 0; ; attempt++ {
		err := watchProject(context.Background(), root, settings, true)
		if err == nil || !runtimeUnreachable(err) || time.Now().After(deadline) {
			return err
		}
		if !announced {
			fmt.Printf("[gonvex] waiting for runtime at %s ...\n", settings.RuntimeURL)
			announced = true
		}
		time.Sleep(time.Second)
	}
}

// runtimeUnreachable reports whether the sync failed because nothing is
// listening yet, as opposed to the runtime rejecting the request.
func runtimeUnreachable(err error) bool {
	if err == nil {
		return false
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

func watchProject(ctx context.Context, root string, settings projectSettings, once bool) error {
	backendDir := filepath.Join(root, "gonvex")
	if err := os.MkdirAll(backendDir, 0o755); err != nil {
		return err
	}

	var lastFingerprint string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		files, err := goFiles(backendDir)
		if err != nil {
			return err
		}

		watchedFiles := append([]string(nil), files...)
		migrationFiles, err := sqlFiles(filepath.Join(root, "migrations"))
		if err != nil {
			return err
		}
		watchedFiles = append(watchedFiles, migrationFiles...)
		fingerprint, err := fingerprint(watchedFiles)
		if err != nil {
			return err
		}

		if fingerprint != lastFingerprint {
			lastFingerprint = fingerprint
			m, err := buildManifest(root, files, settings.ProjectID)
			if err != nil {
				return err
			}
			if err := writeBindings(root, m); err != nil {
				return err
			}
			if err := syncRuntime(settings, m); err != nil {
				fmt.Printf("[gonvex] runtime sync failed: %v\n", err)
				if once {
					return err
				}
			} else if settings.DryRun {
				fmt.Printf("[gonvex] inspected pending migrations for project %s\n", settings.ProjectID)
			} else {
				fmt.Printf("[gonvex] synced project %s to %s\n", settings.ProjectID, settings.RuntimeURL)
			}
			fmt.Printf("[gonvex] generated %d function binding(s)\n", len(m.Functions))
		}

		if once {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func goFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "_generated" {
			return filepath.SkipDir
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func sqlFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			return filepath.SkipDir
		}
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			files = append(files, path)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil, nil
	}
	return files, err
}

func fingerprint(files []string) (string, error) {
	parts := make([]string, 0, len(files))
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%s:%d", file, info.ModTime().UnixNano()))
	}
	return strings.Join(parts, "|"), nil
}

func buildManifest(root string, files []string, projectID string) (manifest.Manifest, error) {
	functions := map[string]manifest.FunctionEntry{}
	schema := manifest.EmptySchema()
	bundleFiles := map[string]string{}
	packageName := ""
	for _, file := range files {
		source, err := os.ReadFile(file)
		if err != nil {
			return manifest.Manifest{}, err
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return manifest.Manifest{}, err
		}
		rel = filepath.ToSlash(rel)
		bundleRel := strings.TrimPrefix(rel, "gonvex/")
		bundleFiles[path.Join("app", bundleRel)] = projectbundle.EncodeFile(source)
		if packageName == "" {
			packageName = projectbundle.DetectPackageName(string(source))
		}

		entries, err := parseRegistrations(root, file)
		if err != nil {
			return manifest.Manifest{}, err
		}
		for path, entry := range entries {
			functions[path] = entry
		}

		parsedSchema, err := parseSchema(file)
		if err != nil {
			return manifest.Manifest{}, err
		}
		parsedSchema = parsedSchema.Normalize()
		for name, table := range parsedSchema.ControlPlaneTables {
			schema.ControlPlaneTables[name] = table
		}
		for name, table := range parsedSchema.TenantTables {
			schema.TenantTables[name] = table
		}
		for name, table := range parsedSchema.Tables {
			schema.Tables[name] = table
		}
	}
	migrationRoot := filepath.Join(root, "migrations")
	if err := filepath.WalkDir(migrationRoot, func(file string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) && file == migrationRoot {
				return filepath.SkipDir
			}
			return walkErr
		}
		if entry.IsDir() {
			if file != migrationRoot {
				return fmt.Errorf("migrations directory must not contain subdirectories: %s", file)
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			return fmt.Errorf("migrations directory may contain only .sql files: %s", entry.Name())
		}
		contents, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		bundleFiles[path.Join("migrations", entry.Name())] = projectbundle.EncodeFile(contents)
		return nil
	}); err != nil && !os.IsNotExist(err) {
		return manifest.Manifest{}, err
	}
	schema = schema.Normalize()

	return manifest.Manifest{
		Project:     projectID,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Functions:   functions,
		Schema:      schema,
		Bundle: &manifest.SourceBundle{
			Hash:        projectbundle.HashFiles(bundleFiles),
			ModulePath:  projectbundle.DefaultModulePath(projectID),
			PackageName: packageName,
			Files:       bundleFiles,
		},
		NotifySchemaVersion: manifest.NotifySchemaVersion,
	}, nil
}

func parseRegistrations(root string, file string) (map[string]manifest.FunctionEntry, error) {
	source, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), file, source, 0)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(root, file)
	if err != nil {
		return nil, err
	}
	entries := map[string]manifest.FunctionEntry{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if !ok || receiver.Name != "app" || !registrationKind(selector.Sel.Name) {
			return true
		}
		pathValue, ok := stringLiteral(call.Args[0])
		if !ok {
			return true
		}
		handler, ok := call.Args[1].(*ast.Ident)
		if !ok {
			return true
		}
		entry := manifest.FunctionEntry{Kind: functionKind(selector.Sel.Name), Handler: handler.Name, File: rel}
		entry.Internal = selector.Sel.Name == "InternalReducer"
		switch selector.Sel.Name {
		case "LiveQuery":
			entry.Delivery = manifest.DeliveryLive
		case "ReplicaCollection":
			entry.Delivery = manifest.DeliveryReplica
		default:
			if entry.Kind == manifest.FunctionKindQuery {
				entry.Delivery = manifest.DeliveryOneShot
			}
		}
		optionStart := 2
		if selector.Sel.Name == "ReplicaCollection" && len(call.Args) >= 3 {
			definition := &manifest.ReplicaCollectionDefinition{}
			if parseReplicaOption(call.Args[2], definition) {
				entry.Replica = definition
			}
			optionStart = 3
		}
		for _, option := range call.Args[optionStart:] {
			parseDependencyOption(option, &entry.Dependencies)
		}
		entries[pathValue] = entry
		return true
	})

	return entries, nil
}

func registrationKind(name string) bool {
	switch name {
	case "Query", "LiveQuery", "ReplicaCollection", "Reducer", "Action", "InternalReducer":
		return true
	default:
		return false
	}
}

func parseReplicaOption(expression ast.Expr, definition *manifest.ReplicaCollectionDefinition) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "gonvex" && selector.Sel.Name == "ReplicaTable" {
		values := stringArguments(call.Args)
		if len(values) == 0 {
			return false
		}
		definition.Table = values[0]
		definition.Key = "id"
		definition.Mode = "eager"
		definition.EqualFilters = map[string]string{}
		return true
	}
	if !parseReplicaOption(selector.X, definition) {
		return false
	}
	values := stringArguments(call.Args)
	switch selector.Sel.Name {
	case "Key":
		if len(values) > 0 {
			definition.Key = values[0]
		}
	case "Columns":
		definition.Columns = values
	case "EqualArg":
		if len(values) > 0 {
			argument := values[0]
			if len(values) > 1 {
				argument = values[1]
			}
			if definition.EqualFilters == nil {
				definition.EqualFilters = map[string]string{}
			}
			definition.EqualFilters[values[0]] = argument
		}
	case "ExcludeWhenSet":
		definition.ExcludeWhenSet = values
	case "VisibilityDependsOn":
		definition.VisibilityTables = values
	case "OrderBy":
		if len(values) > 0 {
			definition.OrderBy = values[0]
		}
		if len(values) > 1 {
			definition.OrderDirection = strings.ToLower(values[1])
		}
	case "Eager":
		definition.Mode = "eager"
	case "Progressive":
		definition.Mode = "progressive"
	case "Budget":
		if len(call.Args) > 0 {
			definition.MaxRows = intLiteral(call.Args[0])
		}
		if len(call.Args) > 1 {
			definition.MaxBytes = int64(intLiteral(call.Args[1]))
		}
	}
	return true
}

func intLiteral(expression ast.Expr) int {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.INT {
		return 0
	}
	value, _ := strconv.Atoi(literal.Value)
	return value
}

func stringLiteral(expression ast.Expr) (string, bool) {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	var value string
	if err := json.Unmarshal([]byte(literal.Value), &value); err != nil {
		return "", false
	}
	return value, true
}

func parseDependencyOption(expression ast.Expr, dependencies *manifest.FunctionDependencies) {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "gonvex" {
		switch selector.Sel.Name {
		case "LivePlan":
			if len(call.Args) > 0 {
				if plan := parseLiveQueryPlan(call.Args[0]); plan != nil {
					dependencies.LiveQueryPlan = plan
					read := manifest.ReadDependency{Table: plan.Table, Columns: append([]string(nil), plan.Columns...), Windowed: plan.Window != nil}
					if plan.Search != nil {
						read.Filters = append(read.Filters, plan.Search.Columns...)
					}
					read.Filters = append(read.Filters, manifestExpressionColumns(plan.Where)...)
					if plan.Sort != nil {
						read.OrdersBy = append(read.OrdersBy, plan.Sort.AllowedColumns...)
					}
					dependencies.Reads = append(dependencies.Reads, read)
				}
			}
		case "ShareByPermissions":
			dependencies.ShareByPermissions = true
		case "OnlineOnlyNonOptimistic":
			values := stringArguments(call.Args)
			if len(values) > 0 {
				dependencies.NonOptimisticReason = values[0]
			}
		case "ShareResultFrom":
			values := stringArguments(call.Args)
			if len(values) > 0 {
				dependencies.ShareResultFrom = values[0]
			}
			if len(values) > 1 {
				dependencies.ShareResultField = values[1]
			}
		}
		return
	}
}

func manifestExpressionColumns(expression *manifest.LiveExpression) []string {
	if expression == nil {
		return nil
	}
	columns := []string{}
	if strings.TrimSpace(expression.Column) != "" {
		columns = append(columns, expression.Column)
	}
	for _, child := range expression.Children {
		columns = append(columns, manifestExpressionColumns(child)...)
	}
	return columns
}

func parseLiveQueryPlan(expression ast.Expr) *manifest.LiveQueryPlan {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return nil
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "gonvex" && selector.Sel.Name == "LiveTable" {
		values := stringArguments(call.Args)
		if len(values) == 0 {
			return nil
		}
		return &manifest.LiveQueryPlan{Table: values[0], Key: "id"}
	}
	plan := parseLiveQueryPlan(selector.X)
	if plan == nil {
		return nil
	}
	values := stringArguments(call.Args)
	switch selector.Sel.Name {
	case "EntityKey":
		if len(values) > 0 {
			plan.Key = values[0]
		}
	case "Select":
		plan.Columns = values
	case "ResultRowsAt":
		plan.ResultPath = values
	case "Filter":
		if len(call.Args) > 0 {
			plan.Where = parseLiveExpression(call.Args[0])
		}
	case "SearchArg":
		if len(values) > 0 {
			plan.Search = &manifest.LiveSearch{Argument: values[0], Columns: append([]string(nil), values[1:]...)}
		}
	case "SortArgs":
		if len(values) >= 4 {
			plan.Sort = &manifest.LiveSort{ColumnArgument: values[0], DirectionArgument: values[1], DefaultColumn: values[2], DefaultDirection: strings.ToLower(values[3]), AllowedColumns: append([]string(nil), values[4:]...)}
		}
	case "WindowArgs":
		if len(values) >= 2 {
			plan.Window = &manifest.LiveWindow{OffsetArgument: values[0], LimitArgument: values[1]}
			if len(call.Args) > 2 {
				plan.Window.DefaultLimit = intLiteral(call.Args[2])
			}
			if len(call.Args) > 3 {
				plan.Window.MaxLimit = intLiteral(call.Args[3])
			}
		}
	case "OnlineOnly":
		plan.ServerOnly = true
	}
	return plan
}

func parseLiveExpression(expression ast.Expr) *manifest.LiveExpression {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return nil
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	operators := map[string]string{
		"Eq": "eq", "Neq": "neq", "GreaterThan": "gt", "GreaterOrEqual": "gte",
		"LessThan": "lt", "LessOrEqual": "lte", "In": "in", "Contains": "contains",
		"ContainsInsensitive": "containsInsensitive", "Range": "range", "All": "and",
		"Any": "or", "Not": "not", "ServerExpression": "server",
	}
	operator := operators[selector.Sel.Name]
	if operator == "" {
		return nil
	}
	result := &manifest.LiveExpression{Operator: operator}
	if operator == "and" || operator == "or" || operator == "not" {
		for _, argument := range call.Args {
			if child := parseLiveExpression(argument); child != nil {
				result.Children = append(result.Children, child)
			}
		}
		return result
	}
	if len(call.Args) > 0 {
		if value, ok := stringLiteral(call.Args[0]); ok {
			result.Column = value
		}
	}
	if len(call.Args) > 1 {
		result.Value = parseLiveValue(call.Args[1])
	}
	if len(call.Args) > 2 {
		result.ValueTo = parseLiveValue(call.Args[2])
	}
	return result
}

func parseLiveValue(expression ast.Expr) *manifest.LiveValue {
	if call, ok := expression.(*ast.CallExpr); ok {
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && len(call.Args) > 0 {
			switch selector.Sel.Name {
			case "Arg":
				if value, ok := stringLiteral(call.Args[0]); ok {
					return &manifest.LiveValue{Argument: value}
				}
			case "Literal":
				return &manifest.LiveValue{Literal: goLiteralValue(call.Args[0])}
			}
		}
	}
	return nil
}

func goLiteralValue(expression ast.Expr) any {
	switch value := expression.(type) {
	case *ast.BasicLit:
		switch value.Kind {
		case token.STRING:
			decoded, _ := strconv.Unquote(value.Value)
			return decoded
		case token.INT:
			decoded, _ := strconv.ParseInt(value.Value, 0, 64)
			return decoded
		case token.FLOAT:
			decoded, _ := strconv.ParseFloat(value.Value, 64)
			return decoded
		}
	case *ast.Ident:
		if value.Name == "true" {
			return true
		}
		if value.Name == "false" {
			return false
		}
	}
	return nil
}

func stringArguments(arguments []ast.Expr) []string {
	values := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		if value, ok := stringLiteral(argument); ok && strings.TrimSpace(value) != "" {
			values = append(values, strings.TrimSpace(value))
		}
	}
	return values
}

func parseSchema(file string) (manifest.Schema, error) {
	source, err := os.ReadFile(file)
	if err != nil {
		return manifest.Schema{}, err
	}

	schema := manifest.EmptySchema()
	tablePattern := regexp.MustCompile(`(?s)s\.(ControlPlaneTable|TenantTable|Table)\(\s*"([^"]+)"\s*,\s*func\([^)]*\)\s*\{(.*?)\n\s*\}\s*\)`)
	columnPattern := regexp.MustCompile(`t\.(ID|String|Text|Int|Int64|Float64|Bool|Time|JSON)\(\s*"([^"]+)"([^)]*)\)`)
	indexPattern := regexp.MustCompile(`t\.(Index|UniqueIndex|TrigramIndex)\(\s*"([^"]+)"([^)]*)\)`)

	for _, tableMatch := range tablePattern.FindAllStringSubmatch(string(source), -1) {
		scope := tableMatch[1]
		name := tableMatch[2]
		table := manifest.Table{
			Columns: map[string]manifest.Column{},
			Indexes: map[string]manifest.Index{},
		}
		body := tableMatch[3]

		for _, columnMatch := range columnPattern.FindAllStringSubmatch(body, -1) {
			kind := columnMatch[1]
			name := columnMatch[2]
			table.Columns[name] = manifest.Column{
				Type:       columnType(kind),
				Nullable:   strings.Contains(columnMatch[3], "gonvex.Nullable"),
				PrimaryKey: kind == "ID",
			}
		}

		for _, indexMatch := range indexPattern.FindAllStringSubmatch(body, -1) {
			table.Indexes[indexMatch[2]] = manifest.Index{
				Columns: stringArgs(indexMatch[3]),
				Unique:  indexMatch[1] == "UniqueIndex",
				Kind:    indexKind(indexMatch[1]),
			}
		}

		switch scope {
		case "ControlPlaneTable":
			schema.ControlPlaneTables[name] = table
		case "TenantTable", "Table":
			schema.TenantTables[name] = table
			schema.Tables[name] = table
		}
	}

	return schema.Normalize(), nil
}

func indexKind(method string) string {
	if method == "TrigramIndex" {
		return "trigram"
	}
	return ""
}

func columnType(kind string) string {
	switch kind {
	case "ID":
		return "id"
	case "String":
		return "string"
	case "Text":
		return "text"
	case "Int":
		return "int"
	case "Int64":
		return "int64"
	case "Float64":
		return "float64"
	case "Bool":
		return "bool"
	case "Time":
		return "time"
	case "JSON":
		return "json"
	default:
		return strings.ToLower(kind)
	}
}

func stringArgs(input string) []string {
	pattern := regexp.MustCompile(`"([^"]+)"`)
	matches := pattern.FindAllStringSubmatch(input, -1)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		values = append(values, match[1])
	}
	return values
}

func functionKind(raw string) manifest.FunctionKind {
	switch raw {
	case "Query":
		return manifest.FunctionKindQuery
	case "Reducer":
		return manifest.FunctionKindReducer
	case "Action":
		return manifest.FunctionKindAction
	case "InternalReducer":
		return manifest.FunctionKindReducer
	case "LiveQuery", "ReplicaCollection":
		return manifest.FunctionKindQuery
	default:
		return manifest.FunctionKind(raw)
	}
}

func writeBindings(root string, m manifest.Manifest) error {
	dir := filepath.Join(root, "gonvex", "_generated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(dir, "landlord")); err != nil {
		return err
	}

	bindingManifest := m.Normalize()
	normalizedSchema := bindingManifest.Schema
	bindingManifest.Bundle = nil
	manifestJSON, err := json.MarshalIndent(bindingManifest, "", "  ")
	if err != nil {
		return err
	}

	files := map[string]string{
		"manifest.json": string(manifestJSON) + "\n",
		"api.ts":        renderAPI(m),
		"client.ts":     "// Generated by gonvex dev. Do not edit.\nexport { GonvexClient } from \"@gonvex/client\";\n",
		"react.ts":      "// Generated by gonvex dev. Do not edit.\nexport { GonvexProvider, useLiveQuery, useQuery, useReducer, useReplicaCollection, useReplicaSelector } from \"@gonvex/react\";\n",
		"types.ts":      "// Generated by gonvex dev. Do not edit.\nexport type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n",
		"schema.ts":     renderSchemaBinding(normalizedSchema),
	}

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			return err
		}
	}

	for _, scoped := range []struct {
		dir    string
		schema manifest.Schema
	}{
		{dir: "control-plane", schema: normalizedSchema.ControlPlaneSchema()},
		{dir: "tenant", schema: normalizedSchema.TenantSchema()},
	} {
		scopedDir := filepath.Join(dir, scoped.dir)
		if err := os.MkdirAll(scopedDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(scopedDir, "schema.ts"), []byte(renderScopedSchemaBinding(scoped.dir, scoped.schema)), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(scopedDir, "tables.ts"), []byte(renderTablesBinding(scoped.schema.Tables)), 0o644); err != nil {
			return err
		}
	}

	return nil
}

func renderSchemaBinding(schema manifest.Schema) string {
	schema = schema.Normalize()
	payload, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		payload = []byte("{}")
	}
	return "// Generated by gonvex dev. Do not edit.\nexport const schema = " + string(payload) + " as const;\n\n" +
		"export const controlPlane = { tables: schema.controlPlaneTables ?? {} } as const;\n" +
		"export const tenant = { tables: schema.tenantTables ?? schema.tables } as const;\n" +
		"export const tables = tenant.tables;\n"
}

func renderScopedSchemaBinding(scope string, schema manifest.Schema) string {
	payload, err := json.MarshalIndent(schema.Normalize(), "", "  ")
	if err != nil {
		payload = []byte("{}")
	}
	const header = "// Generated by gonvex dev. Do not edit.\n"
	switch scope {
	case "control-plane":
		return header + "export const schema = " + string(payload) + " as const;\n\n" +
			"export const controlPlane = schema;\n" +
			"export const tables = schema.tables;\n"
	default:
		return header + "export const schema = " + string(payload) + " as const;\n\n" +
			"export const tenant = schema;\n" +
			"export const tables = schema.tables;\n"
	}
}

func renderTablesBinding(tables map[string]manifest.Table) string {
	payload, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		payload = []byte("{}")
	}
	return "// Generated by gonvex dev. Do not edit.\nexport const tables = " + string(payload) + " as const;\n"
}

func renderAPI(m manifest.Manifest) string {
	var builder strings.Builder
	builder.WriteString("// Generated by gonvex dev. Do not edit.\n\n")
	builder.WriteString("export const api = {\n")
	paths := make([]string, 0, len(m.Functions))
	for path := range m.Functions {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		entry := m.Functions[path]
		builder.WriteString(fmt.Sprintf("  %q: { kind: %q, path: %q },\n", path, entry.Kind, path))
	}
	builder.WriteString("} as const;\n\n")
	builder.WriteString("export type Api = typeof api;\n")
	return builder.String()
}

func syncRuntime(settings projectSettings, m manifest.Manifest) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}

	endpoint := strings.TrimRight(settings.RuntimeURL, "/") + "/dev/sync"
	if settings.DryRun {
		endpoint += "?dryRun=true"
	}
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("x-gonvex-project-id", settings.ProjectID)
	if settings.Key != "" {
		request.Header.Set("authorization", "Bearer "+settings.Key)
		request.Header.Set("x-gonvex-key", settings.Key)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode > 299 {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("runtime returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	if settings.DryRun {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		var report any
		if err := json.Unmarshal(body, &report); err != nil {
			return fmt.Errorf("decode dry-run response: %w", err)
		}
		formatted, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Printf("[gonvex] migration dry run:\n%s\n", formatted)
	}
	return nil
}

func loadProjectSettings(root string) projectSettings {
	loadDotEnv(filepath.Join(root, ".env.local"))
	loadDotEnv(filepath.Join(root, ".env"))
	config := loadGonvexConfig(filepath.Join(root, "gonvex.json"))
	projectID := env("GONVEX_PROJECT_ID", env("GONVEX_PROJECT", config.Project))
	if projectID == "" {
		projectID = filepath.Base(root)
	}
	return projectSettings{
		ProjectID:  projectID,
		RuntimeURL: env("GONVEX_RUNTIME_URL", fallback(config.Runtime, defaultRuntimeURL)),
		Key:        env("GONVEX_PROJECT_KEY", env("GONVEX_DEPLOY_KEY", env("GONVEX_KEY", ""))),
	}
}

func loadGonvexConfig(path string) gonvexConfig {
	file, err := os.Open(path)
	if err != nil {
		return gonvexConfig{}
	}
	defer file.Close()

	var config gonvexConfig
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return gonvexConfig{}
	}
	return config
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		os.Setenv(key, strings.Trim(strings.TrimSpace(value), `"'`))
	}
}

func fallback(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func printHelp() {
	fmt.Println("Usage:")
	fmt.Println("  gonvex dev [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>] [--once] [--dry-run] [-- <command>]")
	fmt.Println("  gonvex migrate identity-v2 (--plan | --apply | --verify) [options]")
}
