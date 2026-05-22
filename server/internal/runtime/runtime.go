package runtime

import (
	"sync"

	"github.com/gonvex/gonvex/pkg/manifest"
)

type Runtime struct {
	mu       sync.RWMutex
	manifest manifest.Manifest
}

func New() *Runtime {
	return &Runtime{
		manifest: manifest.Manifest{
			Functions: map[string]manifest.FunctionEntry{},
			Schema:    manifest.EmptySchema(),
		},
	}
}

func (r *Runtime) SyncManifest(next manifest.Manifest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifest = next
}

func (r *Runtime) Manifest() manifest.Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest
}
