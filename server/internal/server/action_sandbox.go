package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/datafiles"
	gonvexsandbox "github.com/gonvex/gonvex/server/internal/sandbox"
)

type actionSandbox struct {
	manager   *gonvexsandbox.Manager
	dataFiles *datafiles.Manager
	storage   gonvex.StorageAPI
	scope     gonvexsandbox.Scope
}

type storageOpener interface {
	Open(string) (io.ReadCloser, error)
}

func (sandbox *actionSandbox) Call(ctx context.Context, operation string, payload json.RawMessage, duckdb bool) (any, error) {
	if sandbox == nil || sandbox.manager == nil || !sandbox.manager.Enabled() {
		return nil, errors.New("sandbox is disabled by runtime policy")
	}
	switch operation {
	case "create":
		var request struct {
			TTLMS int64 `json:"ttlMs"`
		}
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		return sandbox.manager.Create(sandbox.scope, duckdb, time.Duration(request.TTLMS)*time.Millisecond)
	case "run":
		var request struct {
			SandboxID, Code string
			TimeoutMS       int64 `json:"timeoutMs"`
		}
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		return sandbox.manager.Run(sandbox.scope, request.SandboxID, request.Code, time.Duration(request.TimeoutMS)*time.Millisecond)
	case "cancel", "status":
		var request struct{ SandboxID, ExecutionID string }
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		if operation == "cancel" {
			return sandbox.manager.Cancel(sandbox.scope, request.SandboxID, request.ExecutionID)
		}
		return sandbox.manager.Status(sandbox.scope, request.SandboxID, request.ExecutionID)
	case "readFile":
		var request struct{ SandboxID, Path string }
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		return sandbox.manager.ReadFile(sandbox.scope, request.SandboxID, request.Path)
	case "readText":
		var request struct{ SandboxID, Path string }
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		value, err := sandbox.manager.ReadFile(sandbox.scope, request.SandboxID, request.Path)
		if err != nil {
			return nil, err
		}
		encoded, _ := value["contentBase64"].(string)
		content, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		return string(content), nil
	case "writeFile":
		var request struct{ SandboxID, Path, ContentBase64 string }
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		content, err := base64.StdEncoding.DecodeString(request.ContentBase64)
		if err != nil {
			return nil, fmt.Errorf("sandbox file contentBase64 is invalid: %w", err)
		}
		return sandbox.manager.WriteFile(sandbox.scope, request.SandboxID, request.Path, content)
	case "writeText":
		var request struct{ SandboxID, Path, Content string }
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		return sandbox.manager.WriteFile(sandbox.scope, request.SandboxID, request.Path, []byte(request.Content))
	case "importFile":
		if !duckdb {
			return nil, errors.New("sandbox importFile requires capabilities.sandbox.duckdb")
		}
		var request struct{ SandboxID, FileID, Filename string }
		if err := strictJSON(payload, &request); err != nil {
			return nil, err
		}
		return sandbox.importFile(ctx, request.SandboxID, request.FileID, request.Filename)
	default:
		return nil, fmt.Errorf("unsupported sandbox operation %q", operation)
	}
}

func (sandbox *actionSandbox) importFile(ctx context.Context, sandboxID, fileID, filename string) (any, error) {
	if sandbox.storage == nil {
		return nil, gonvex.ErrStorageNotConfigured
	}
	meta, err := sandbox.storage.GetMetadata(strings.TrimSpace(fileID))
	if err != nil {
		return nil, err
	}
	if meta.TenantID != sandbox.scope.TenantID {
		return nil, gonvex.ErrForbidden
	}
	if meta.Visibility == gonvex.FileVisibilityPrivate && meta.OwnerID != sandbox.scope.AccountID {
		return nil, gonvex.ErrForbidden
	}
	if meta.Status != gonvex.FileStatusUploaded {
		return nil, gonvex.ErrFileNotFound
	}
	opener, ok := sandbox.storage.(storageOpener)
	if !ok {
		return nil, errors.New("configured storage cannot stream files to the sandbox")
	}
	if strings.TrimSpace(filename) == "" {
		return nil, errors.New("sandbox importFile requires the original filename")
	}
	fileKey, err := datafiles.FileKeyFor(meta.ID, filename)
	if err != nil {
		return nil, err
	}
	path, imported, err := sandbox.dataFiles.Ensure(ctx, datafiles.Scope{ProjectID: sandbox.scope.ProjectID, TenantID: sandbox.scope.TenantID}, fileKey, func(context.Context) (io.ReadCloser, string, error) {
		reader, openErr := opener.Open(meta.ID)
		return reader, filename, openErr
	})
	if err != nil {
		return nil, err
	}
	tables := make([]gonvexsandbox.Table, 0, len(imported.Tables))
	for _, table := range imported.Tables {
		tables = append(tables, gonvexsandbox.Table{TableName: table.TableName, RowCount: table.RowCount, Columns: append([]string(nil), table.Columns...)})
	}
	return sandbox.manager.AttachDuckDB(sandbox.scope, sandboxID, path, fileKey, tables)
}

func strictJSON(payload json.RawMessage, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid sandbox request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("invalid sandbox request: trailing JSON")
	}
	return nil
}
