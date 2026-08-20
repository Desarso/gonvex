package runtime

import (
	"context"
	"fmt"
	"sync"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/moduleengine"
)

type Runtime struct {
	mu        sync.RWMutex
	manifest  manifest.Manifest
	manifests map[string]manifest.Manifest
	// moduleHost is the one out-of-process TypeScript module host this runtime
	// talks to, shared by every project.
	moduleHost *moduleengine.RemoteHost
	// engines memoizes the module generation serving each project.
	engines map[string]loadedEngine
}

type loadedEngine struct {
	remote   *moduleengine.RemoteEngine
	engine   moduleengine.ModuleEngine
	identity string
}

// ModuleHostHealth describes whether the Rust process required by active
// TypeScript projects can currently accept calls. A runtime with no active
// projects leaves the host lazy and remains ready.
type ModuleHostHealth struct {
	Required       bool   `json:"required"`
	Ready          bool   `json:"ready"`
	Configured     bool   `json:"configured"`
	Managed        bool   `json:"managed"`
	Started        bool   `json:"started"`
	Running        bool   `json:"running"`
	Connected      bool   `json:"connected"`
	ActiveProjects int    `json:"activeProjects"`
	Epoch          uint64 `json:"epoch,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

func New() *Runtime {
	return NewWithModuleHost(nil)
}

// NewWithModuleHost builds the TypeScript-only application runtime.
func NewWithModuleHost(host *moduleengine.RemoteHost) *Runtime {
	return &Runtime{
		manifest: manifest.Manifest{
			Functions: map[string]manifest.FunctionEntry{},
			Schema:    manifest.EmptySchema(),
		},
		manifests:  map[string]manifest.Manifest{},
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

// ModuleHostHealth reports module-host liveness without starting the lazy
// host. Once at least one TypeScript project is active, losing the Rust host
// makes the runtime unhealthy until the next module invocation reconnects it.
func (r *Runtime) ModuleHostHealth() ModuleHostHealth {
	if r == nil {
		return ModuleHostHealth{Ready: true}
	}
	r.mu.RLock()
	activeProjects := len(r.engines)
	r.mu.RUnlock()

	var status moduleengine.HostStatus
	if r.moduleHost != nil {
		status = r.moduleHost.Status()
	}
	required := activeProjects > 0
	health := ModuleHostHealth{
		Required:       required,
		Ready:          !required || status.Ready,
		Configured:     status.Configured,
		Managed:        status.Managed,
		Started:        status.Started,
		Running:        status.Running,
		Connected:      status.Connected,
		ActiveProjects: activeProjects,
		Epoch:          status.Epoch,
	}
	if health.Ready {
		return health
	}
	switch {
	case !status.Configured:
		health.Reason = "not-configured"
	case status.Closed:
		health.Reason = "shut-down"
	case status.Managed && status.Started && !status.Running:
		health.Reason = "process-exited"
	default:
		health.Reason = "not-connected"
	}
	return health
}

func (r *Runtime) SyncManifest(next manifest.Manifest) error {
	return r.SyncManifestContext(context.Background(), next)
}

// SyncManifestContext publishes and atomically activates a TypeScript module.
// A module that fails to load or warm leaves the previous generation serving.
func (r *Runtime) SyncManifestContext(ctx context.Context, next manifest.Manifest) error {
	if next.Module == nil {
		return fmt.Errorf("project %q must ship a TypeScript module artifact", next.Project)
	}
	normalized := *next.Module
	next.Module = &normalized
	if err := next.Module.Validate(); err != nil {
		return fmt.Errorf("project %q module is invalid: %w", next.Project, err)
	}
	loaded, err := r.activateModule(ctx, next)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.engines[next.Project] = *loaded
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

// EngineForProject returns the active TypeScript module engine for projectID.
func (r *Runtime) EngineForProject(projectID string) moduleengine.ModuleEngine {
	r.mu.RLock()
	loaded, ok := r.engines[projectID]
	r.mu.RUnlock()
	if ok {
		return loaded.engine
	}
	return nil
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

// Close releases the module host within a bound.
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
