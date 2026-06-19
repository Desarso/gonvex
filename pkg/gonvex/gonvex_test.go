package gonvex

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type echoArgs struct {
	Name string `json:"name"`
}

func TestAppDispatchesQuery(t *testing.T) {
	app := NewApp()
	app.Query("hello.echo", func(ctx *QueryCtx, args echoArgs) (map[string]string, error) {
		return map[string]string{
			"name":    args.Name,
			"project": ctx.ProjectID,
			"tenant":  ctx.TenantID,
		}, nil
	})

	result, err := app.ExecuteQuery(&QueryCtx{RuntimeContext: RuntimeContext{
		Context:   context.Background(),
		ProjectID: "project-a",
	}}, "hello.echo", json.RawMessage(`{"name":"Ada"}`))
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}

	payload, ok := result.(map[string]string)
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	if payload["name"] != "Ada" || payload["project"] != "project-a" || payload["tenant"] != "project-a" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestAppDispatchRejectsUnknownArgFields(t *testing.T) {
	app := NewApp()
	app.Query("hello.echo", func(ctx *QueryCtx, args echoArgs) (string, error) {
		return args.Name, nil
	})

	_, err := app.ExecuteQuery(&QueryCtx{}, "hello.echo", json.RawMessage(`{"name":"Ada","extra":true}`))
	if err == nil {
		t.Fatal("expected invalid args error")
	}
	var dispatchErr *DispatchError
	if !errors.As(err, &dispatchErr) {
		t.Fatalf("expected DispatchError, got %T", err)
	}
	if dispatchErr.Code != "invalid_args" {
		t.Fatalf("expected invalid_args, got %q", dispatchErr.Code)
	}
}

func TestAppDispatchRejectsWrongKind(t *testing.T) {
	app := NewApp()
	app.Mutation("hello.echo", func(ctx *MutationCtx, args echoArgs) (string, error) {
		return args.Name, nil
	})

	_, err := app.ExecuteQuery(&QueryCtx{}, "hello.echo", json.RawMessage(`{"name":"Ada"}`))
	if err == nil {
		t.Fatal("expected wrong kind error")
	}
	var dispatchErr *DispatchError
	if !errors.As(err, &dispatchErr) {
		t.Fatalf("expected DispatchError, got %T", err)
	}
	if dispatchErr.Code != "wrong_kind" {
		t.Fatalf("expected wrong_kind, got %q", dispatchErr.Code)
	}
}

func TestAppRegisterRejectsInvalidSignature(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected invalid registration to panic")
		}
	}()

	NewApp().Query("bad.signature", func(args echoArgs) (string, error) {
		return args.Name, nil
	})
}
