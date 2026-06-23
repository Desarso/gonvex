package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
)

const defaultRuntimeURL = "http://localhost:8080"

type projectSettings struct {
	ProjectID  string
	RuntimeURL string
	Key        string
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

		fingerprint, err := fingerprint(files)
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
	for _, file := range files {
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
	schema = schema.Normalize()

	return manifest.Manifest{
		Project:     projectID,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Functions:   functions,
		Schema:      schema,
	}, nil
}

func parseRegistrations(root string, file string) (map[string]manifest.FunctionEntry, error) {
	source, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	pattern := regexp.MustCompile(`app\.(Query|Mutation|Action|HTTP|InternalMutation|LiveGrid)\(\s*"([^"]+)"\s*,\s*([A-Za-z_][A-Za-z0-9_]*)`)
	entries := map[string]manifest.FunctionEntry{}
	for _, match := range pattern.FindAllStringSubmatch(string(source), -1) {
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return nil, err
		}
		entries[match[2]] = manifest.FunctionEntry{
			Kind:    functionKind(match[1]),
			Handler: match[3],
			File:    rel,
		}
	}

	return entries, nil
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
	case "HTTP":
		return manifest.FunctionKindHTTP
	case "InternalMutation":
		return manifest.FunctionKindInternalMutation
	case "LiveGrid":
		return manifest.FunctionKindLiveGrid
	default:
		return manifest.FunctionKind(raw)
	}
}

func writeBindings(root string, m manifest.Manifest) error {
	dir := filepath.Join(root, "gonvex", "_generated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	manifestJSON, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	files := map[string]string{
		"manifest.json": string(manifestJSON) + "\n",
		"api.ts":        renderAPI(m),
		"client.ts":     "// Generated by gonvex dev. Do not edit.\nexport { GonvexClient } from \"@gonvex/client\";\n",
		"react.ts":      "// Generated by gonvex dev. Do not edit.\nexport { GonvexProvider, useMutation, useQuery } from \"@gonvex/react\";\n",
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

	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(settings.RuntimeURL, "/")+"/dev/sync", bytes.NewReader(payload))
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
	fmt.Println("Usage: gonvex dev [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>] [--once] [-- <command>]")
}
