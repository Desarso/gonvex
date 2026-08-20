package runtime

import (
	"sync"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/moduleengine"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

type Runtime struct {
	mu        sync.RWMutex
	manifest  manifest.Manifest
	manifests map[string]manifest.Manifest
	loader    *projectbundle.Loader
	// engines memoizes the module engine serving each project. Engines are
	// meant to be long-lived — a future out-of-process engine owns a worker
	// pool or a set of isolates — so the wrapper is reused until the loader
	// swaps in a differently compiled module for that project.
	engines map[string]loadedEngine
}

// loadedEngine pairs an engine with the loaded module it was built from, so a
// replaced bundle invalidates the memoized engine.
type loadedEngine struct {
	app    *gonvex.App
	engine moduleengine.ModuleEngine
}

func New() *Runtime {
	return NewWithLoader(projectbundle.NewLoader("", ""))
}

func NewWithLoader(loader *projectbundle.Loader) *Runtime {
	if loader == nil {
		loader = projectbundle.NewLoader("", "")
	}
	return &Runtime{
		manifest: manifest.Manifest{
			Functions: map[string]manifest.FunctionEntry{},
			Schema:    manifest.EmptySchema(),
		},
		manifests: map[string]manifest.Manifest{},
		loader:    loader,
		engines:   map[string]loadedEngine{},
	}
}

func (r *Runtime) SyncManifest(next manifest.Manifest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if next.Bundle != nil && len(next.Bundle.Files) > 0 {
		if _, err := r.loader.Load(next.Project, *next.Bundle); err != nil {
			return err
		}
	}
	r.manifest = next
	if next.Project != "" {
		r.manifests[next.Project] = next
	}
	return nil
}

// AppForProject returns the compiled Go module loaded for projectID. It stays
// the compatibility entry point for host code and tests that need the Go type
// itself; dispatch resolves EngineForProject instead.
func (r *Runtime) AppForProject(projectID string) *gonvex.App {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loader.AppForProject(projectID)
}

// EngineForProject returns the module engine serving projectID, or nil when no
// module is loaded for it. Compiled Go bundles are wrapped in a GoAppEngine;
// engines for other languages resolve here too once their loaders exist, which
// is the one place the host learns which implementation runs a project.
func (r *Runtime) EngineForProject(projectID string) moduleengine.ModuleEngine {
	app := r.AppForProject(projectID)
	if app == nil {
		return nil
	}
	r.mu.RLock()
	loaded, ok := r.engines[projectID]
	r.mu.RUnlock()
	if ok && loaded.app == app {
		return loaded.engine
	}
	// Build under the write lock so concurrent resolvers of the same module
	// converge on one engine instance rather than racing to replace each other.
	r.mu.Lock()
	defer r.mu.Unlock()
	if loaded, ok := r.engines[projectID]; ok && loaded.app == app {
		return loaded.engine
	}
	engine := moduleengine.NewGoAppEngine(app)
	if r.engines == nil {
		r.engines = map[string]loadedEngine{}
	}
	r.engines[projectID] = loadedEngine{app: app, engine: engine}
	return engine
}

func (r *Runtime) ProjectIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.manifests))
	for projectID := range r.manifests {
		if projectID != "" {
			ids = append(ids, projectID)
		}
	}
	return ids
}

func (r *Runtime) Manifest() manifest.Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest
}

func (r *Runtime) ManifestForProject(projectID string) manifest.Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if projectID != "" {
		if m, ok := r.manifests[projectID]; ok {
			return m
		}
		return emptyManifest(projectID)
	}
	return r.manifest
}

func emptyManifest(projectID string) manifest.Manifest {
	return manifest.Manifest{
		Project:   projectID,
		Functions: map[string]manifest.FunctionEntry{},
		Schema:    manifest.EmptySchema(),
	}
}
