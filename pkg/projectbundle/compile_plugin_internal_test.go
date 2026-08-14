//go:build !windows

package projectbundle

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompiledPluginPathIsScopedToRuntimeBinary(t *testing.T) {
	first := &Loader{cacheDir: t.TempDir(), runtimeFingerprint: "runtime-a"}
	second := &Loader{cacheDir: first.cacheDir, runtimeFingerprint: "runtime-b"}
	firstPath := first.compiledPluginPath("project-a", "abcdef1234567890")
	secondPath := second.compiledPluginPath("project-a", "abcdef1234567890")
	if firstPath == secondPath {
		t.Fatal("different runtime binaries reused the same compiled plugin path")
	}
	want := "gonvex_plugin_" + projectCacheKey("project-a") + "_abcdef123456_runtime-a.so"
	if filepath.Base(firstPath) != want {
		t.Fatalf("unexpected runtime-scoped plugin path: %s", firstPath)
	}
}

func TestCompiledPluginPathUsesCollisionResistantProjectIdentity(t *testing.T) {
	loader := &Loader{cacheDir: t.TempDir(), runtimeFingerprint: "runtime"}
	first := loader.compiledPluginPath("team/a", "abcdef1234567890")
	second := loader.compiledPluginPath("team?a", "abcdef1234567890")
	if first == second {
		t.Fatalf("distinct project IDs collided: %s", first)
	}
}

func TestRemoveIncompatibleCompiledPluginsPreservesCurrentRuntime(t *testing.T) {
	cacheDir := t.TempDir()
	loader := &Loader{cacheDir: cacheDir, runtimeFingerprint: "runtime-new"}
	current := loader.compiledPluginPath("project-a", "abcdef1234567890")
	old := filepath.Join(cacheDir, "compiled", "gonvex_plugin_abcdef123456_runtime-oldx.so")
	legacy := filepath.Join(cacheDir, "compiled", "gonvex_plugin_abcdef123456.so")
	projectOld := filepath.Join(cacheDir, "compiled", "gonvex_plugin_"+projectCacheKey("project-a")+"_abcdef123456_runtime-old.so")
	prefixCollision := filepath.Join(cacheDir, "compiled", "gonvex_plugin_abcdef123456-project_abcdef123456_runtime-new.so")
	for _, path := range []string{current, old, legacy, projectOld, prefixCollision} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("plugin"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	loader.removeIncompatibleCompiledPlugins("project-a", "abcdef1234567890", current)
	if _, err := os.Stat(current); err != nil {
		t.Fatalf("current runtime plugin was removed: %v", err)
	}
	for _, path := range []string{old, legacy, projectOld} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("incompatible plugin still exists: %s", path)
		}
	}
	if _, err := os.Stat(prefixCollision); err != nil {
		t.Fatalf("another project's prefix-colliding plugin was removed: %v", err)
	}
}

func TestPruneCompiledPluginsBoundsEachProject(t *testing.T) {
	cacheDir := t.TempDir()
	loader := &Loader{cacheDir: cacheDir, runtimeFingerprint: "runtime"}
	var current string
	for generation := 0; generation < 7; generation++ {
		path := loader.compiledPluginPath("project-a", fmt.Sprintf("%012d", generation))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("plugin"), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(int64(generation), 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
		current = path
	}
	other := loader.compiledPluginPath("project-b", "000000000001")
	if err := os.WriteFile(other, []byte("plugin"), 0o600); err != nil {
		t.Fatal(err)
	}

	loader.pruneCompiledPlugins("project-a", current)
	projectA, _ := filepath.Glob(filepath.Join(cacheDir, "compiled", "gonvex_plugin_"+projectCacheKey("project-a")+"_*.so"))
	if len(projectA) != compiledPluginGenerationsPerProject {
		t.Fatalf("project-a cache has %d entries, want %d", len(projectA), compiledPluginGenerationsPerProject)
	}
	if _, err := os.Stat(current); err != nil {
		t.Fatalf("current plugin was pruned: %v", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("another project's plugin was pruned: %v", err)
	}
}
