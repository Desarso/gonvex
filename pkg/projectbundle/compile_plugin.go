//go:build !windows

package projectbundle

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"reflect"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

func (l *Loader) compileAndRegister(projectDir string, _ string, hash string) (*gonvex.App, error) {
	pluginPath := filepath.Join(projectDir, "gonvex_project_"+safeHashPrefix(hash)+".so")
	if err := runGoBuild(projectDir, pluginPath); err != nil {
		return nil, err
	}

	handle, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("open project plugin: %w", err)
	}
	symbol, err := handle.Lookup("Register")
	if err != nil {
		return nil, fmt.Errorf("lookup Register symbol: %w", err)
	}

	registerFn, ok := symbol.(func(*gonvex.App))
	if !ok {
		value := reflect.ValueOf(symbol)
		if value.Kind() != reflect.Func {
			return nil, fmt.Errorf("Register symbol is %T, expected func(*gonvex.App)", symbol)
		}
		fnType := value.Type()
		if fnType.NumIn() != 1 || fnType.NumOut() != 0 {
			return nil, fmt.Errorf("Register has invalid signature: %s", fnType)
		}
	}

	app := gonvex.NewApp()
	switch fn := symbol.(type) {
	case func(*gonvex.App):
		fn(app)
	default:
		reflect.ValueOf(symbol).Call([]reflect.Value{reflect.ValueOf(app)})
	}
	if len(app.Functions()) == 0 {
		return nil, fmt.Errorf("project bundle registered zero functions")
	}
	return app, nil
}

func runGoBuild(projectDir string, outputPath string) error {
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", outputPath, ".")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("compile project bundle: %w: %s", err, string(output))
	}
	return nil
}
