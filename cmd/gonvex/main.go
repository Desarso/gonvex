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
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	if err := watchProject(context.Background(), root, settings, true); err != nil {
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
		for name, table := range parsedSchema.LandlordTables {
			schema.LandlordTables[name] = table
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
		optionStart := 2
		if selector.Sel.Name == "Sync" && len(call.Args) >= 3 {
			definition := &manifest.SyncDefinition{}
			if parseSyncOption(call.Args[2], definition) {
				entry.Sync = definition
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
	case "Query", "Mutation", "Action", "HTTP", "PublicHTTP", "InternalMutation", "LiveGrid", "Sync":
		return true
	default:
		return false
	}
}

func parseSyncOption(expression ast.Expr, definition *manifest.SyncDefinition) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "gonvex" && selector.Sel.Name == "SyncTable" {
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
	if !parseSyncOption(selector.X, definition) {
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

type dependencyOptionTarget struct {
	kind  string
	start int
}

func parseDependencyOption(expression ast.Expr, dependencies *manifest.FunctionDependencies) dependencyOptionTarget {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return dependencyOptionTarget{}
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return dependencyOptionTarget{}
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "gonvex" {
		switch selector.Sel.Name {
		case "Reads":
			start := len(dependencies.Reads)
			for _, value := range stringArguments(call.Args) {
				dependencies.Reads = append(dependencies.Reads, manifest.ReadDependency{Table: value})
			}
			return dependencyOptionTarget{kind: "read", start: start}
		case "Writes":
			start := len(dependencies.Writes)
			for _, value := range stringArguments(call.Args) {
				dependencies.Writes = append(dependencies.Writes, manifest.WriteDependency{Table: value})
			}
			return dependencyOptionTarget{kind: "write", start: start}
		case "ReadsEphemeral":
			dependencies.ReadsEphemeral = true
		case "WritesEphemeral":
			dependencies.WritesEphemeral = true
		case "ShareByPermissions":
			dependencies.ShareByPermissions = true
		case "ShareByVisibility":
			values := stringArguments(call.Args)
			if len(values) > 0 {
				dependencies.ShareByVisibility = values[0]
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
		return dependencyOptionTarget{}
	}

	target := parseDependencyOption(selector.X, dependencies)
	values := stringArguments(call.Args)
	switch target.kind {
	case "read":
		for index := target.start; index < len(dependencies.Reads); index++ {
			switch selector.Sel.Name {
			case "Columns":
				dependencies.Reads[index].Columns = values
			case "Filters":
				dependencies.Reads[index].Filters = values
			case "OrdersBy":
				dependencies.Reads[index].OrdersBy = values
			case "Windowed":
				dependencies.Reads[index].Windowed = true
			case "Predicate":
				if len(values) > 0 {
					dependencies.Reads[index].Predicate = values[0]
				}
			}
		}
	case "write":
		if selector.Sel.Name == "Columns" {
			for index := target.start; index < len(dependencies.Writes); index++ {
				dependencies.Writes[index].Columns = values
			}
		}
	}
	return target
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
	tablePattern := regexp.MustCompile(`(?s)s\.(LandlordTable|TenantTable|Table)\(\s*"([^"]+)"\s*,\s*func\([^)]*\)\s*\{(.*?)\n\s*\}\s*\)`)
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
		case "LandlordTable":
			schema.LandlordTables[name] = table
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
	case "Mutation":
		return manifest.FunctionKindMutation
	case "Action":
		return manifest.FunctionKindAction
	case "HTTP", "PublicHTTP":
		return manifest.FunctionKindHTTP
	case "InternalMutation":
		return manifest.FunctionKindInternalMutation
	case "LiveGrid":
		return manifest.FunctionKindLiveGrid
	case "Sync":
		return manifest.FunctionKindSync
	default:
		return manifest.FunctionKind(raw)
	}
}

func writeBindings(root string, m manifest.Manifest) error {
	dir := filepath.Join(root, "gonvex", "_generated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	bindingManifest := m
	bindingManifest.Bundle = nil
	manifestJSON, err := json.MarshalIndent(bindingManifest, "", "  ")
	if err != nil {
		return err
	}

	files := map[string]string{
		"manifest.json": string(manifestJSON) + "\n",
		"api.ts":        renderAPI(m),
		"client.ts":     "// Generated by gonvex dev. Do not edit.\nexport { GonvexClient } from \"@gonvex/client\";\n",
		"react.ts":      "// Generated by gonvex dev. Do not edit.\nexport { GonvexProvider, useMutation, useQuery, useSync, useSyncSelector } from \"@gonvex/react\";\n",
		"types.ts":      "// Generated by gonvex dev. Do not edit.\nexport type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n",
		"schema.ts":     renderSchemaBinding(m.Schema.Normalize()),
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
		{dir: "landlord", schema: m.Schema.LandlordSchema()},
		{dir: "tenant", schema: m.Schema.TenantSchema()},
	} {
		scopedDir := filepath.Join(dir, scoped.dir)
		if err := os.MkdirAll(scopedDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(scopedDir, "schema.ts"), []byte(renderSchemaBinding(scoped.schema)), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(scopedDir, "tables.ts"), []byte(renderTablesBinding(scoped.schema.Tables)), 0o644); err != nil {
			return err
		}
	}

	return nil
}

func renderSchemaBinding(schema manifest.Schema) string {
	payload, err := json.MarshalIndent(schema.Normalize(), "", "  ")
	if err != nil {
		payload = []byte("{}")
	}
	return "// Generated by gonvex dev. Do not edit.\nexport const schema = " + string(payload) + " as const;\n"
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
	fmt.Println("Usage: gonvex dev [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>] [--once] [--dry-run] [-- <command>]")
}
