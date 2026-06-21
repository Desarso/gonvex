package projectbundle_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

func TestLoadProjectBundle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go plugin bundles are not supported on Windows; use Linux/macOS runtime for plugin-backed sync tests")
	}

	moduleRoot, err := gonvexModuleRoot()
	if err != nil {
		t.Fatal(err)
	}

	source, err := os.ReadFile(filepath.Join("testdata", "app", "register.go"))
	if err != nil {
		t.Fatal(err)
	}

	bundle := manifest.SourceBundle{
		Hash:        "test",
		ModulePath:  "gonvexapp/test-project",
		PackageName: "app",
		Files: map[string]string{
			"app/register.go": projectbundle.EncodeFile(source),
		},
	}
	bundle.Hash = projectbundle.HashFiles(bundle.Files)

	loader := projectbundle.NewLoader(t.TempDir(), moduleRoot)
	app, err := loader.Load("test-project", bundle)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	if _, ok := app.Lookup("sample.echo"); !ok {
		t.Fatalf("expected sample.echo to be registered")
	}
}

func gonvexModuleRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrInvalid
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..")), nil
}
