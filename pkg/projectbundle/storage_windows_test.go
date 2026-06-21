//go:build windows

package projectbundle_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

const storageAppSource = `package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
	app.Mutation("files.createUploadUrl", CreateUploadURL)
}

type uploadArgs struct {
	ContentType string ` + "`json:\"contentType\"`" + `
}

func CreateUploadURL(ctx *gonvex.MutationCtx, args uploadArgs) (gonvex.UploadTarget, error) {
	return ctx.Storage.GenerateUploadURL(gonvex.UploadOptions{
		ContentType: args.ContentType,
		Visibility:  gonvex.FileVisibilityPrivate,
	})
}
`

// TestInterpretedStorageHandler proves the yaegi interpreter can resolve the
// gonvex storage symbols (UploadOptions, UploadTarget, FileVisibility) and
// dispatch through the ctx.Storage interface field on the embedded
// RuntimeContext. With no storage configured, the call must surface
// ErrStorageNotConfigured rather than panicking on a nil interface.
func TestInterpretedStorageHandler(t *testing.T) {
	moduleRoot, err := gonvexModuleRoot()
	if err != nil {
		t.Fatal(err)
	}

	bundle := manifest.SourceBundle{
		ModulePath:  "gonvexapp/storage-project",
		PackageName: "app",
		Files: map[string]string{
			"app/register.go": projectbundle.EncodeFile([]byte(storageAppSource)),
		},
	}
	bundle.Hash = projectbundle.HashFiles(bundle.Files)

	loader := projectbundle.NewLoader(t.TempDir(), moduleRoot)
	app, err := loader.Load("storage-project", bundle)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}

	if _, ok := app.Lookup("files.createUploadUrl"); !ok {
		t.Fatalf("expected files.createUploadUrl to be registered")
	}

	// No Storage set on the context -> normalize installs the not-configured
	// fallback, so the interpreted handler should return ErrStorageNotConfigured.
	_, err = app.ExecuteMutation(&gonvex.MutationCtx{}, "files.createUploadUrl", json.RawMessage(`{"contentType":"text/plain"}`))
	if !errors.Is(err, gonvex.ErrStorageNotConfigured) {
		t.Fatalf("expected ErrStorageNotConfigured, got %v", err)
	}
}
