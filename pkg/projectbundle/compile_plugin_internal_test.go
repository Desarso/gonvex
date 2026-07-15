//go:build !windows

package projectbundle

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompiledPluginPathIsScopedToRuntimeBinary(t *testing.T) {
	first := &Loader{cacheDir: t.TempDir(), runtimeFingerprint: "runtime-a"}
	second := &Loader{cacheDir: first.cacheDir, runtimeFingerprint: "runtime-b"}
	firstPath := first.compiledPluginPath("abcdef1234567890")
	secondPath := second.compiledPluginPath("abcdef1234567890")
	if firstPath == secondPath {
		t.Fatal("different runtime binaries reused the same compiled plugin path")
	}
	if filepath.Base(firstPath) != "gonvex_plugin_abcdef123456_runtime-a.so" {
		t.Fatalf("unexpected runtime-scoped plugin path: %s", firstPath)
	}
}

func TestRemoveIncompatibleCompiledPluginsPreservesCurrentRuntime(t *testing.T) {
	cacheDir := t.TempDir()
	loader := &Loader{cacheDir: cacheDir, runtimeFingerprint: "runtime-new"}
	current := loader.compiledPluginPath("abcdef1234567890")
	old := filepath.Join(cacheDir, "compiled", "gonvex_plugin_abcdef123456_runtime-old.so")
	legacy := filepath.Join(cacheDir, "compiled", "gonvex_plugin_abcdef123456.so")
	for _, path := range []string{current, old, legacy} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("plugin"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	loader.removeIncompatibleCompiledPlugins("abcdef1234567890", current)
	if _, err := os.Stat(current); err != nil {
		t.Fatalf("current runtime plugin was removed: %v", err)
	}
	for _, path := range []string{old, legacy} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("incompatible plugin still exists: %s", path)
		}
	}
}
