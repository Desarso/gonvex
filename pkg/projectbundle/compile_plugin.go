//go:build !windows

package projectbundle

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"reflect"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// compiledPluginPath is the persistent cache location for a bundle's compiled
// .so, keyed by both content and the exact runtime binary. Go plugins are tied
// to their host package build IDs; an incompatible plugin must never be passed
// to plugin.Open because the failed attempt can prevent a compatible rebuild
// from loading in that process.
func (l *Loader) compiledPluginPath(hash string) string {
	return filepath.Join(
		l.cacheDir,
		"compiled",
		"gonvex_plugin_"+safeHashPrefix(hash)+"_"+l.runtimeFingerprint+".so",
	)
}

func (l *Loader) compileAndRegister(projectDir string, _ string, hash string) (*gonvex.App, error) {
	// Fast path: reuse a previously-compiled plugin for this exact bundle if it
	// exists and is compatible with the running binary.
	cached := l.compiledPluginPath(hash)
	l.removeIncompatibleCompiledPlugins(hash, cached)
	if _, err := os.Stat(cached); err == nil {
		if app, err := registerFromPlugin(cached); err == nil {
			return app, nil
		}
		// Stale (e.g. built against an older runtime binary) or corrupt: drop it
		// and fall through to a fresh compile.
		_ = os.Remove(cached)
	}

	pluginPath := filepath.Join(projectDir, "gonvex_project_"+safeHashPrefix(hash)+".so")
	if err := runGoBuild(projectDir, pluginPath); err != nil {
		return nil, err
	}
	// Persist the freshly-compiled plugin for future restarts (best effort: a
	// copy failure just means we recompile next time).
	_ = copyFile(pluginPath, cached)
	return registerFromPlugin(pluginPath)
}

func (l *Loader) removeIncompatibleCompiledPlugins(hash string, current string) {
	pattern := filepath.Join(l.cacheDir, "compiled", "gonvex_plugin_"+safeHashPrefix(hash)+"*.so")
	matches, _ := filepath.Glob(pattern)
	for _, candidate := range matches {
		if candidate != current {
			_ = os.Remove(candidate)
		}
	}
}

// registerFromPlugin opens a compiled plugin, invokes its Register symbol, and
// returns the resulting App.
func registerFromPlugin(pluginPath string) (*gonvex.App, error) {
	handle, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("open project plugin: %w", err)
	}
	symbol, err := handle.Lookup("Register")
	if err != nil {
		return nil, fmt.Errorf("lookup Register symbol: %w", err)
	}

	_, ok := symbol.(func(*gonvex.App))
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

// copyFile atomically copies src to dst, creating parent dirs as needed.
func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func runGoBuild(projectDir string, outputPath string) error {
	tidy := exec.Command("go", "mod", "tidy")
	tidy.Dir = projectDir
	tidy.Env = append(os.Environ(), "CGO_ENABLED=1")
	if output, err := tidy.CombinedOutput(); err != nil {
		return fmt.Errorf("tidy project bundle module: %w: %s", err, string(output))
	}

	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", outputPath, ".")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("compile project bundle: %w: %s", err, string(output))
	}
	return nil
}
