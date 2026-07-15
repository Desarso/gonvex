package projectbundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
)

var safeProjectID = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// godanticImportPath is the module the AI-assistant project bundle imports to
// embed the godantic agent. When a bundle imports it, renderGoMod wires a
// require+replace to the copy vendored at <moduleRoot>/third_party/godantic.
const godanticImportPath = "github.com/Desarso/godantic"

type Loader struct {
	cacheDir           string
	moduleRoot         string
	runtimeFingerprint string
	apps               map[string]*loadedProject
}

var (
	runtimeFingerprintOnce  sync.Once
	runtimeFingerprintValue string
)

type loadedProject struct {
	hash string
	app  *gonvex.App
}

func NewLoader(cacheDir string, moduleRoot string) *Loader {
	if strings.TrimSpace(cacheDir) == "" {
		cacheDir = filepath.Join(os.TempDir(), "gonvex-project-bundles")
	}
	return &Loader{
		cacheDir:           cacheDir,
		moduleRoot:         strings.TrimSpace(moduleRoot),
		runtimeFingerprint: currentRuntimeFingerprint(),
		apps:               map[string]*loadedProject{},
	}
}

// currentRuntimeFingerprint scopes compiled Go plugins to the exact host
// binary they were built against. plugin.Open can poison a process after it
// attempts to load an incompatible plugin, so cache compatibility must be
// decided before opening the file rather than by retrying after an error.
func currentRuntimeFingerprint() string {
	runtimeFingerprintOnce.Do(func() {
		executable, err := os.Executable()
		if err == nil {
			file, openErr := os.Open(executable)
			if openErr == nil {
				hasher := sha256.New()
				if _, copyErr := io.Copy(hasher, file); copyErr == nil {
					runtimeFingerprintValue = hex.EncodeToString(hasher.Sum(nil))[:12]
				}
				_ = file.Close()
			}
		}
		if runtimeFingerprintValue == "" {
			fallback := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
			sum := sha256.Sum256([]byte(fallback))
			runtimeFingerprintValue = hex.EncodeToString(sum[:])[:12]
		}
	})
	return runtimeFingerprintValue
}

func (l *Loader) AppForProject(projectID string) *gonvex.App {
	if projectID == "" {
		return nil
	}
	if loaded, ok := l.apps[projectID]; ok && loaded != nil && loaded.app != nil {
		return loaded.app
	}
	return nil
}

func (l *Loader) Load(projectID string, bundle manifest.SourceBundle) (*gonvex.App, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, fmt.Errorf("project id is required to load a bundle")
	}
	if bundle.Hash == "" {
		return nil, fmt.Errorf("bundle hash is required")
	}
	if len(bundle.Files) == 0 {
		return nil, fmt.Errorf("bundle has no source files")
	}
	if cached, ok := l.apps[projectID]; ok && cached.hash == bundle.Hash && cached.app != nil {
		return cached.app, nil
	}

	modulePath := strings.TrimSpace(bundle.ModulePath)
	if modulePath == "" {
		modulePath = defaultModulePath(projectID)
	}
	packageName := strings.TrimSpace(bundle.PackageName)
	if packageName == "" {
		packageName = "app"
	}

	projectDir := filepath.Join(l.cacheDir, sanitizeProjectID(projectID))
	if err := os.RemoveAll(projectDir); err != nil {
		return nil, fmt.Errorf("reset project bundle cache: %w", err)
	}
	appDir := filepath.Join(projectDir, "app")
	if err := os.MkdirAll(appDir, 0o755); err != nil {
		return nil, fmt.Errorf("create project bundle dir: %w", err)
	}

	needsGodantic := false
	for relPath, encoded := range bundle.Files {
		content, err := decodeFile(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", relPath, err)
		}
		if strings.Contains(string(content), godanticImportPath) {
			needsGodantic = true
		}
		target := filepath.Join(projectDir, filepath.FromSlash(relPath))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return nil, fmt.Errorf("create dir for %s: %w", relPath, err)
		}
		if err := os.WriteFile(target, content, 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", relPath, err)
		}
	}

	buildModulePath := modulePath + "/" + safeHashPrefix(bundle.Hash)
	if err := os.WriteFile(filepath.Join(projectDir, "plugin_main.go"), []byte(renderPluginMain(buildModulePath, packageName)), 0o644); err != nil {
		return nil, fmt.Errorf("write plugin main: %w", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(renderGoMod(buildModulePath, bundle.GoVersion, l.moduleRoot, needsGodantic)), 0o644); err != nil {
		return nil, fmt.Errorf("write go.mod: %w", err)
	}

	app, err := l.compileAndRegister(projectDir, packageName, bundle.Hash)
	if err != nil {
		return nil, err
	}
	l.apps[projectID] = &loadedProject{hash: bundle.Hash, app: app}
	return app, nil
}

func HashFiles(files map[string]string) string {
	hasher := sha256.New()
	keys := sortedKeys(files)
	for _, path := range keys {
		hasher.Write([]byte(path))
		hasher.Write([]byte{0})
		hasher.Write([]byte(files[path]))
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func DetectPackageName(source string) string {
	match := regexp.MustCompile(`(?m)^package\s+([A-Za-z_][A-Za-z0-9_]*)`).FindStringSubmatch(source)
	if len(match) == 2 {
		return match[1]
	}
	return "app"
}

func DefaultModulePath(projectID string) string {
	return defaultModulePath(projectID)
}

func defaultModulePath(projectID string) string {
	return "gonvexapp/" + sanitizeProjectID(projectID)
}

func sanitizeProjectID(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return "project"
	}
	return safeProjectID.ReplaceAllString(projectID, "-")
}

func safeHashPrefix(hash string) string {
	hash = strings.TrimSpace(hash)
	if len(hash) > 12 {
		hash = hash[:12]
	}
	if hash == "" {
		return "bundle"
	}
	return hash
}

func renderPluginMain(modulePath string, _ string) string {
	return fmt.Sprintf(`package main

import (
	gonvexpkg "github.com/gonvex/gonvex/pkg/gonvex"
	apppkg "%s/app"
)

func Register(app *gonvexpkg.App) {
	apppkg.Register(app)
}
`, modulePath)
}

func renderGoMod(modulePath string, goVersion string, moduleRoot string, includeGodantic bool) string {
	if strings.TrimSpace(goVersion) == "" {
		goVersion = "1.22"
	}
	builder := strings.Builder{}
	builder.WriteString("module ")
	builder.WriteString(modulePath)
	builder.WriteString("\n\ngo ")
	builder.WriteString(goVersion)
	builder.WriteString("\n\nrequire github.com/gonvex/gonvex v0.0.0\n")
	if moduleRoot != "" {
		builder.WriteString("\nreplace github.com/gonvex/gonvex => ")
		builder.WriteString(filepath.ToSlash(moduleRoot))
		builder.WriteString("\n")
	}
	// Optional godantic wiring: only when the bundle imports godantic AND a
	// vendored copy exists at <moduleRoot>/third_party/godantic. This lets gonvex
	// actions embed the godantic agent (the AI assistant) while leaving projects
	// that don't use it untouched. go mod tidy resolves it from the local replace.
	if includeGodantic && moduleRoot != "" {
		godanticRoot := filepath.Join(moduleRoot, "third_party", "godantic")
		if _, err := os.Stat(filepath.Join(godanticRoot, "go.mod")); err == nil {
			builder.WriteString("\nrequire github.com/Desarso/godantic v0.0.0\n")
			builder.WriteString("\nreplace github.com/Desarso/godantic => ")
			builder.WriteString(filepath.ToSlash(godanticRoot))
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func sortedKeys(files map[string]string) []string {
	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
