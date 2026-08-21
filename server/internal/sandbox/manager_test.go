package sandbox

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestManagerScopesFilesAndExecutionsToCaller(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test worker is a POSIX script")
	}
	root := t.TempDir()
	worker := filepath.Join(root, "worker")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"ok\":true,\"result\":{\"answer\":42},\"error\":\"\",\"logs\":[]}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := New(Config{Enabled: true, Root: filepath.Join(root, "sandboxes"), WorkerBinary: worker, AllowUnconfined: true, DefaultTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	scope := Scope{ProjectID: "project", TenantID: "tenant-a", AccountID: "account-a"}
	handle, err := manager.Create(scope, false, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.WriteFile(scope, handle.SandboxID, "input/data.json", []byte(`[{"value":1}]`)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReadFile(Scope{ProjectID: "project", TenantID: "tenant-a", AccountID: "account-b"}, handle.SandboxID, "input/data.json"); err == nil {
		t.Fatal("another account read the workspace")
	}
	execution, err := manager.Run(scope, handle.SandboxID, "return 42", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for execution.Status == "queued" || execution.Status == "running" {
		if time.Now().After(deadline) {
			t.Fatal("execution did not finish")
		}
		time.Sleep(10 * time.Millisecond)
		execution, err = manager.Status(scope, handle.SandboxID, execution.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if execution.Status != "succeeded" {
		t.Fatalf("execution = %#v", execution)
	}
}

func TestManagerRejectsTraversal(t *testing.T) {
	if _, err := safeFilePath(t.TempDir(), "../secret"); err == nil {
		t.Fatal("path traversal accepted")
	}
}

func TestManagerBoundsTotalWorkspacesAndExecutionHistory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test worker is a POSIX script")
	}
	root := t.TempDir()
	worker := filepath.Join(root, "worker")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"ok\":true,\"result\":1,\"error\":\"\",\"logs\":[]}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := New(Config{
		Enabled: true, Root: filepath.Join(root, "sandboxes"), WorkerBinary: worker,
		AllowUnconfined: true, MaxTotalSandboxes: 1, MaxExecutions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	scope := Scope{ProjectID: "project", TenantID: "tenant", AccountID: "account"}
	handle, err := manager.Create(scope, false, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(Scope{ProjectID: "project", TenantID: "other", AccountID: "other"}, false, time.Minute); err == nil {
		t.Fatal("global sandbox limit was not enforced")
	}
	first, err := manager.Run(scope, handle.SandboxID, "return 1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first = waitForTerminal(t, manager, scope, first)
	second, err := manager.Run(scope, handle.SandboxID, "return 2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTerminal(t, manager, scope, second)
	if _, err := manager.Status(scope, handle.SandboxID, first.ExecutionID); err == nil {
		t.Fatal("old execution result was retained past the history limit")
	}
}

func waitForTerminal(t *testing.T, manager *Manager, scope Scope, execution ExecutionStatus) ExecutionStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for execution.Status == "queued" || execution.Status == "running" {
		if time.Now().After(deadline) {
			t.Fatal("execution did not finish")
		}
		time.Sleep(10 * time.Millisecond)
		var err error
		execution, err = manager.Status(scope, execution.SandboxID, execution.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
	}
	return execution
}

func TestManagerCancelsAnUncooperativeWorker(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test worker is a POSIX script")
	}
	root := t.TempDir()
	worker := filepath.Join(root, "worker")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\ncat >/dev/null\nwhile :; do :; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := New(Config{Enabled: true, Root: filepath.Join(root, "sandboxes"), WorkerBinary: worker, AllowUnconfined: true, DefaultTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	scope := Scope{ProjectID: "project", TenantID: "tenant", AccountID: "account"}
	handle, err := manager.Create(scope, false, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := manager.Run(scope, handle.SandboxID, "for (;;) {}", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		execution, err = manager.Status(scope, handle.SandboxID, execution.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if execution.Status == "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("execution never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := manager.Cancel(scope, handle.SandboxID, execution.ExecutionID); err != nil {
		t.Fatal(err)
	}
	for {
		execution, err = manager.Status(scope, handle.SandboxID, execution.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if execution.Status == "cancelled" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution was not cancelled: %#v", execution)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
