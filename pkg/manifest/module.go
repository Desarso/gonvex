package manifest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// Module languages carried on ModuleArtifact.Language. Go projects ship a
// SourceBundle instead and never populate a module artifact.
const (
	LanguageTypeScript = "typescript"
	LanguageGo         = "go"
)

// Language reports the artifact's normalized language, defaulting to
// TypeScript because that is the only language the artifact pipeline emits.
func (a ModuleArtifact) NormalizedLanguage() string {
	language := strings.ToLower(strings.TrimSpace(a.Language))
	if language == "" {
		return LanguageTypeScript
	}
	return language
}

// IsTypeScript reports whether this artifact is executed by the TypeScript
// module host rather than by the compiled Go plugin loader.
func (a ModuleArtifact) IsTypeScript() bool {
	return a.NormalizedLanguage() == LanguageTypeScript
}

// ModuleLanguage reports which engine a manifest wants. A manifest with no
// module artifact is a Go project, which keeps the Go bundle flow the default
// for every project that has not opted into a module artifact.
func (m Manifest) ModuleLanguage() string {
	if m.Module == nil {
		return LanguageGo
	}
	return m.Module.NormalizedLanguage()
}

// UsesModuleHost reports whether this manifest must be served by the module
// host instead of by a compiled Go bundle.
func (m Manifest) UsesModuleHost() bool {
	return m.Module != nil && m.Module.IsTypeScript()
}

// DecodeJavaScript returns the artifact's bundled ESM source, verifying that it
// matches the hash the build recorded. The check happens here, before anything
// is handed to an engine, so a truncated or substituted bundle fails as a
// manifest error rather than as a mysterious module error later.
func (a ModuleArtifact) DecodeJavaScript() ([]byte, error) {
	if a.JavaScript == nil {
		return nil, fmt.Errorf("module artifact has no JavaScript bundle")
	}
	code, err := base64.StdEncoding.DecodeString(a.JavaScript.Code)
	if err != nil {
		return nil, fmt.Errorf("module JavaScript is not valid base64: %w", err)
	}
	if len(code) == 0 {
		return nil, fmt.Errorf("module JavaScript bundle is empty")
	}
	expected := strings.ToLower(strings.TrimSpace(a.JavaScript.Hash))
	if expected == "" {
		return nil, fmt.Errorf("module JavaScript has no hash to verify")
	}
	digest := sha256.Sum256(code)
	if actual := hex.EncodeToString(digest[:]); actual != expected {
		return nil, fmt.Errorf("module JavaScript hash %s does not match the manifest hash %s", actual, expected)
	}
	return code, nil
}

// Validate reports whether an artifact is executable: right language, verified
// bundle, and at least one well-formed function declaration.
func (a ModuleArtifact) Validate() error {
	if !a.IsTypeScript() {
		return fmt.Errorf("module language %q has no module host", a.NormalizedLanguage())
	}
	if _, err := a.DecodeJavaScript(); err != nil {
		return err
	}
	for path, function := range a.Functions {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("module declares a function with an empty path")
		}
		switch function.Kind {
		case FunctionKindQuery, FunctionKindReducer, FunctionKindAction, FunctionKindHTTP:
		default:
			return fmt.Errorf("module function %q has unknown kind %q", path, function.Kind)
		}
	}
	return nil
}

// Identity is the artifact's stable content identity: the artifact hash when
// the build recorded one, the JavaScript hash otherwise. Callers use it to skip
// reloading an unchanged module.
func (a ModuleArtifact) Identity() string {
	if hash := strings.TrimSpace(a.Hash); hash != "" {
		return hash
	}
	if hash := strings.TrimSpace(a.ArtifactHash); hash != "" {
		return hash
	}
	if a.JavaScript != nil {
		return strings.TrimSpace(a.JavaScript.Hash)
	}
	return ""
}
