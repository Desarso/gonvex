package projectbundle_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

func TestLoadProjectBundleWindowsInterpreter(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows interpreter test")
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
