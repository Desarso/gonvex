package runtime

import (
	"context"
	"fmt"
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
	// moduleHost is the one out-of-process module host this runtime talks to,
	// shared by every project. It is nil when none is configured, which leaves
	// compiled Go modules working exactly as before.
	moduleHost *moduleengine.RemoteHost
	// engines memoizes the module engine serving each project. Engines are
	// long-lived — a remote engine owns a module generation and its warm
	// isolates — so the wrapper is reused until the loader swaps in a
	// differently compiled module or a newer artifact is published.
	engines map[string]loadedEngine
}

// loadedEngine pairs an engine with the module it was built from, so a replaced
// bundle or a replaced artifact invalidates the memoized engine. Exactly one of
// app and remote is set.
type loadedEngine struct {
	app    *gonvex.App
	remote *moduleengine.RemoteEngine
	engine moduleengine.ModuleEngine
	// identity is the module artifact's content hash for a remote engine. An
	// unchanged artifact reuses the active generation instead of publishing a
	// new one for every sync.
	identity string
}

func New() *Runtime {
	return NewWithLoader(projectbundle.NewLoader("", ""))
}

func NewWithLoader(loader *projectbundle.Loader) *Runtime {
	return NewWithModuleHost(loader, nil)
}

// NewWithModuleHost builds a runtime that can also serve module artifacts
// through host. A nil or unconfigured host keeps every Go project working and
// makes a TypeScript manifest fail its sync with a clear error instead of
// silently loading nothing.
func NewWithModuleHost(loader *projectbundle.Loader, host *moduleengine.RemoteHost) *Runtime {
	if loader == nil {
		loader = projectbundle.NewLoader("", "")
	}
	return &Runtime{
		manifest: manifest.Manifest{
			Functions: map[string]manifest.FunctionEntry{},
			Schema:    manifest.EmptySchema(),
		},
		manifests:  map[string]manifest.Manifest{},
		loader:     loader,
		moduleHost: host,
		engines:    map[string]loadedEngine{},
	}
}

// ModuleHost returns the configured module host, or nil. The server owns its
// shutdown, so the runtime only hands it out.
func (r *Runtime) ModuleHost() *moduleengine.RemoteHost {
	if r == nil {
		return nil
	}
	return r.moduleHost
}

func (r *Runtime) SyncManifest(next manifest.Manifest) error {
	return r.SyncManifestContext(context.Background(), next)
}

// SyncManifestContext installs a project's module. A manifest carrying a
// TypeScript module artifact is published to the module host and activated
// before it is installed, so a module that fails to load or to warm leaves the
// previously active generation serving traffic. Everything else keeps the Go
// bundle flow untouched.
func (r *Runtime) SyncManifestContext(ctx context.Context, next manifest.Manifest) error {
	// Only the module artifact is normalized here: the manifest itself is
	// stored exactly as the sync delivered it, which is what every existing
	// reader of ManifestForProject expects.
	if next.Module != nil {
		normalized := next.Module.Normalize()
		next.Module = &normalized
	}

	var installed *loadedEngine
	switch {
	case next.UsesModuleHost():
		engine, err := r.activateModule(ctx, next)
		if err != nil {
			return err
		}
		installed = engine
	case next.Bundle != nil && len(next.Bundle.Files) > 0:
		if _, err := r.loader.Load(next.Project, *next.Bundle); err != nil {
			return err
		}
	case next.Module != nil:
		// A module artifact in a language this runtime has no engine for is a
		// deployment error, not something to quietly ignore.
		return fmt.Errorf("project %q ships a %s module, which this runtime cannot execute", next.Project, next.Module.NormalizedLanguage())
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if installed != nil {
		r.engines[next.Project] = *installed
	} else if next.Project != "" {
		// Switching a project back to a Go bundle must not leave a remote engine
		// shadowing it.
		if previous, ok := r.engines[next.Project]; ok && previous.remote != nil {
			delete(r.engines, next.Project)
		}
	}
	r.manifest = next
	if next.Project != "" {
		r.manifests[next.Project] = next
	}
	return nil
}

// activateModule publishes the manifest's artifact to the module host. The load
// and the activation both happen before the runtime installs anything, and both
// happen outside the runtime lock: a generation swap must not stall the request
// paths that are reading descriptors while it runs.
func (r *Runtime) activateModule(ctx context.Context, next manifest.Manifest) (*loadedEngine, error) {
	if next.Project == "" {
		return nil, fmt.Errorf("a project id is required to load a module artifact")
	}
	if r.moduleHost == nil || !r.moduleHost.Available() {
		return nil, fmt.Errorf(
			"project %q ships a %s module but no module host is configured; set GONVEX_MODULE_HOST_BINARY or GONVEX_MODULE_HOST_ENDPOINT",
			next.Project, next.Module.NormalizedLanguage(),
		)
	}

	identity := next.Module.Identity()
	r.mu.RLock()
	existing, ok := r.engines[next.Project]
	r.mu.RUnlock()
	if ok && existing.remote != nil && identity != "" && existing.identity == identity {
		// The same artifact is already the active generation. Republishing it
		// would retire warm isolates to replace them with identical ones.
		if err := existing.remote.Activate(ctx); err != nil {
			return nil, err
		}
		return &existing, nil
	}

	engine, err := moduleengine.NewRemoteEngine(r.moduleHost, next.Project, *next.Module)
	if err != nil {
		return nil, err
	}
	if err := engine.Activate(ctx); err != nil {
		return nil, fmt.Errorf("project %q module was not activated: %w", next.Project, err)
	}
	return &loadedEngine{remote: engine, engine: engine, identity: identity}, nil
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
// module is loaded for it. A project whose manifest shipped a module artifact
// resolves to the engine that dispatches into the module host; compiled Go
// bundles resolve to a GoAppEngine. This is the one place the host learns which
// implementation runs a project, and it never learns the module's language.
func (r *Runtime) EngineForProject(projectID string) moduleengine.ModuleEngine {
	r.mu.RLock()
	loaded, ok := r.engines[projectID]
	r.mu.RUnlock()
	if ok && loaded.remote != nil {
		return loaded.remote
	}

	app := r.AppForProject(projectID)
	if app == nil {
		return nil
	}
	if ok && loaded.app == app && loaded.engine != nil {
		return loaded.engine
	}
	// Build under the write lock so concurrent resolvers of the same module
	// converge on one engine instance rather than racing to replace each other.
	r.mu.Lock()
	defer r.mu.Unlock()
	if loaded, ok := r.engines[projectID]; ok {
		if loaded.remote != nil {
			return loaded.remote
		}
		if loaded.app == app && loaded.engine != nil {
			return loaded.engine
		}
	}
	engine := moduleengine.NewGoAppEngine(app)
	if r.engines == nil {
		r.engines = map[string]loadedEngine{}
	}
	r.engines[projectID] = loadedEngine{app: app, engine: engine}
	return engine
}

// ModuleGeneration reports the module-host generation currently serving
// projectID, or zero when the project is not served by a module artifact.
func (r *Runtime) ModuleGeneration(projectID string) uint64 {
	r.mu.RLock()
	loaded, ok := r.engines[projectID]
	r.mu.RUnlock()
	if !ok || loaded.remote == nil {
		return 0
	}
	return loaded.remote.Generation()
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

// Close releases the module host within a bound. Compiled Go modules have
// nothing to release: a plugin cannot be unloaded.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil || r.moduleHost == nil {
		return nil
	}
	return r.moduleHost.Close(ctx)
}

func emptyManifest(projectID string) manifest.Manifest {
	return manifest.Manifest{
		Project:   projectID,
		Functions: map[string]manifest.FunctionEntry{},
		Schema:    manifest.EmptySchema(),
	}
}
