package runtime

import (
	"testing"

	"github.com/gonvex/gonvex/pkg/moduleengine"
)

func TestModuleHostHealthAllowsUnusedLazyHost(t *testing.T) {
	runtime := NewWithModuleHost(moduleengine.NewRemoteHost(moduleengine.HostOptions{
		Binary: "/path/that/does/not/need/to/start",
	}))

	health := runtime.ModuleHostHealth()
	if health.Required || !health.Ready || !health.Configured || health.Started || health.Connected {
		t.Fatalf("unexpected lazy host health: %+v", health)
	}
}

func TestModuleHostHealthRequiresLiveHostForActiveProject(t *testing.T) {
	runtime := NewWithModuleHost(nil)
	runtime.engines["whagons"] = loadedEngine{}

	health := runtime.ModuleHostHealth()
	if !health.Required || health.Ready || health.Configured || health.ActiveProjects != 1 {
		t.Fatalf("unexpected required host health: %+v", health)
	}
	if health.Reason != "not-configured" {
		t.Fatalf("reason = %q, want not-configured", health.Reason)
	}
}
