//go:build windows

package projectbundle

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

func (l *Loader) compileAndRegister(projectDir string, packageName string, _ string) (*gonvex.App, error) {
	interpreter := interp.New(interp.Options{})
	interpreter.Use(stdlib.Symbols)
	interpreter.Use(gonvexHostSymbols())

	appDir := filepath.Join(projectDir, "app")
	sourceFiles, err := sourceFiles(appDir)
	if err != nil {
		return nil, fmt.Errorf("read app sources: %w", err)
	}
	sortSourceFiles(sourceFiles)
	combined, err := mergePackageSources(appDir, sourceFiles, packageName)
	if err != nil {
		return nil, err
	}
	if _, err := interpreter.Eval(combined); err != nil {
		return nil, fmt.Errorf("interpret project sources: %w", err)
	}

	app := gonvex.NewApp()
	registerValue, err := interpreter.Eval(fmt.Sprintf("%s.Register", packageName))
	if err != nil {
		registerValue, err = interpreter.Eval("app.Register")
		if err != nil {
			return nil, fmt.Errorf("resolve Register: %w", err)
		}
	}
	reflect.ValueOf(registerValue.Interface()).Call([]reflect.Value{reflect.ValueOf(app)})

	if len(app.Functions()) == 0 {
		return nil, fmt.Errorf("project bundle registered zero functions")
	}
	return app, nil
}

func gonvexHostSymbols() map[string]map[string]reflect.Value {
	return map[string]map[string]reflect.Value{
		"github.com/gonvex/gonvex/pkg/gonvex/gonvex": {
			"NewApp":      reflect.ValueOf(gonvex.NewApp),
			"App":         reflect.ValueOf((*gonvex.App)(nil)),
			"QueryCtx":    reflect.ValueOf((*gonvex.QueryCtx)(nil)),
			"MutationCtx": reflect.ValueOf((*gonvex.MutationCtx)(nil)),
			"ActionCtx":   reflect.ValueOf((*gonvex.ActionCtx)(nil)),
			"User":        reflect.ValueOf((*gonvex.User)(nil)),
			"Schema":      reflect.ValueOf(gonvex.Schema{}),
			"Table":       reflect.ValueOf(gonvex.Table{}),
			"Nullable":    reflect.ValueOf(gonvex.Nullable),
		},
	}
}

func sortSourceFiles(files []string) {
	priority := map[string]int{
		"db.go":       0,
		"schema.go":   1,
		"register.go": 99,
	}
	sort.SliceStable(files, func(i, j int) bool {
		left := priority[files[i]]
		right := priority[files[j]]
		if left != right {
			return left < right
		}
		return files[i] < files[j]
	})
}

func sourceFiles(appDir string) ([]string, error) {
	files := []string{}
	err := filepath.WalkDir(appDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		rel, err := filepath.Rel(appDir, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	return files, err
}

func mergePackageSources(appDir string, files []string, packageName string) (string, error) {
	importLines := map[string]bool{}
	bodies := []string{}
	for _, name := range files {
		if name == "plugin_main.go" {
			continue
		}
		source, err := os.ReadFile(filepath.Join(appDir, filepath.FromSlash(name)))
		if err != nil {
			return "", fmt.Errorf("read %s: %w", name, err)
		}
		imports, body, err := splitPackageSource(string(source))
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", name, err)
		}
		for _, item := range imports {
			importLines[item] = true
		}
		bodies = append(bodies, "// source: "+name+"\n"+body)
	}

	var builder strings.Builder
	builder.WriteString("package ")
	builder.WriteString(packageName)
	builder.WriteString("\n\n")
	if len(importLines) > 0 {
		lines := make([]string, 0, len(importLines))
		for line := range importLines {
			lines = append(lines, line)
		}
		sort.Strings(lines)
		builder.WriteString("import (\n")
		for _, line := range lines {
			builder.WriteString("\t")
			builder.WriteString(line)
			builder.WriteString("\n")
		}
		builder.WriteString(")\n\n")
	}
	builder.WriteString(strings.Join(bodies, "\n\n"))
	return builder.String(), nil
}

func splitPackageSource(source string) ([]string, string, error) {
	lines := strings.Split(source, "\n")
	index := 0
	for index < len(lines) {
		line := strings.TrimSpace(lines[index])
		if line == "" || strings.HasPrefix(line, "//") {
			index++
			continue
		}
		if strings.HasPrefix(line, "package ") {
			index++
		}
		break
	}

	imports := []string{}
	for index < len(lines) {
		line := strings.TrimSpace(lines[index])
		if line == "" || strings.HasPrefix(line, "//") {
			index++
			continue
		}
		if strings.HasPrefix(line, "import ") {
			if strings.Contains(line, "(") {
				index++
				for index < len(lines) {
					item := strings.TrimSpace(lines[index])
					if item == "" || strings.HasPrefix(item, "//") {
						index++
						continue
					}
					if item == ")" {
						index++
						break
					}
					imports = append(imports, strings.TrimSuffix(item, ","))
					index++
				}
				continue
			}
			imports = append(imports, strings.TrimPrefix(line, "import "))
			index++
			continue
		}
		break
	}
	return imports, strings.Join(lines[index:], "\n"), nil
}
