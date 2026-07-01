package runtime

import (
	"sync"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

type Runtime struct {
	mu        sync.RWMutex
	manifest  manifest.Manifest
	manifests map[string]manifest.Manifest
	loader    *projectbundle.Loader
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

func (r *Runtime) AppForProject(projectID string) *gonvex.App {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loader.AppForProject(projectID)
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
