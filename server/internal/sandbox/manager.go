// Package sandbox owns ephemeral TypeScript analysis workspaces. Application
// Actions reach it through a capability-scoped bridge; workers never receive a
// database credential, project secret, object-store credential, or host path.
package sandbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Enabled           bool
	Root              string
	WorkerBinary      string
	AllowUnconfined   bool
	MaxConcurrent     int
	MaxSandboxes      int
	MaxTotalSandboxes int
	MaxExecutions     int
	DefaultTTL        time.Duration
	MaxTTL            time.Duration
	DefaultTimeout    time.Duration
	MaxTimeout        time.Duration
	MaxCodeBytes      int
	MaxFileBytes      int64
	MaxWorkspaceBytes int64
	MaxOutputBytes    int
	MaxRows           int
	MaxHeapBytes      int64
	DuckDBMemoryBytes int64
	WorkerUID         int
	WorkerGID         int
}

func (cfg Config) withDefaults() Config {
	if strings.TrimSpace(cfg.Root) == "" {
		cfg.Root = filepath.Join(os.TempDir(), "gonvex-sandboxes")
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 2
	}
	if cfg.MaxSandboxes <= 0 {
		cfg.MaxSandboxes = 4
	}
	if cfg.MaxTotalSandboxes <= 0 {
		cfg.MaxTotalSandboxes = 128
	}
	if cfg.MaxExecutions <= 0 {
		cfg.MaxExecutions = 16
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 30 * time.Minute
	}
	if cfg.MaxTTL <= 0 {
		cfg.MaxTTL = 2 * time.Hour
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 30 * time.Second
	}
	if cfg.MaxTimeout <= 0 {
		cfg.MaxTimeout = 2 * time.Minute
	}
	if cfg.MaxCodeBytes <= 0 {
		cfg.MaxCodeBytes = 512 << 10
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = 64 << 20
	}
	if cfg.MaxWorkspaceBytes <= 0 {
		cfg.MaxWorkspaceBytes = 256 << 20
	}
	if cfg.MaxOutputBytes <= 0 {
		cfg.MaxOutputBytes = 8 << 20
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 500
	}
	if cfg.MaxHeapBytes <= 0 {
		cfg.MaxHeapBytes = 64 << 20
	}
	if cfg.DuckDBMemoryBytes <= 0 {
		cfg.DuckDBMemoryBytes = 128 << 20
	}
	if cfg.WorkerUID <= 0 {
		cfg.WorkerUID = 65534
	}
	if cfg.WorkerGID <= 0 {
		cfg.WorkerGID = 65534
	}
	return cfg
}

type Scope struct {
	ProjectID string
	TenantID  string
	AccountID string
}

func (scope Scope) validate() error {
	if strings.TrimSpace(scope.ProjectID) == "" || strings.TrimSpace(scope.TenantID) == "" || strings.TrimSpace(scope.AccountID) == "" {
		return errors.New("sandbox requires an authenticated account and tenant")
	}
	return nil
}

type Handle struct {
	SandboxID string `json:"sandboxId"`
	ExpiresAt int64  `json:"expiresAt"`
	DuckDB    bool   `json:"duckdb"`
}

type LogLine struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type ExecutionStatus struct {
	SandboxID   string    `json:"sandboxId"`
	ExecutionID string    `json:"executionId"`
	Status      string    `json:"status"`
	StartedAt   int64     `json:"startedAt,omitempty"`
	FinishedAt  int64     `json:"finishedAt,omitempty"`
	Result      any       `json:"result,omitempty"`
	Error       string    `json:"error,omitempty"`
	Logs        []LogLine `json:"logs"`
}

type Table struct {
	TableName string   `json:"tableName"`
	RowCount  int64    `json:"rowCount"`
	Columns   []string `json:"columns"`
}

type Import struct {
	Alias  string  `json:"alias"`
	Path   string  `json:"path"`
	Tables []Table `json:"tables"`
}

type Manager struct {
	cfg       Config
	ctx       context.Context
	cancel    context.CancelFunc
	admission chan struct{}

	mu         sync.Mutex
	workspaces map[string]*workspace
}

type workspace struct {
	id         string
	scope      Scope
	root       string
	duckdb     bool
	createdAt  time.Time
	expiresAt  time.Time
	imports    []Import
	executions map[string]*execution
	active     string
	ioBusy     bool
}

type execution struct {
	id              string
	status          string
	startedAt       time.Time
	finishedAt      time.Time
	result          any
	err             string
	logs            []LogLine
	cancel          context.CancelFunc
	cancelRequested bool
}

type workerRequest struct {
	Version           int      `json:"version"`
	Root              string   `json:"root"`
	AllowUnconfined   bool     `json:"allowUnconfined"`
	Code              string   `json:"code"`
	DuckDB            bool     `json:"duckdb"`
	Imports           []Import `json:"imports,omitempty"`
	MaxHeapBytes      int64    `json:"maxHeapBytes"`
	MaxFileBytes      int64    `json:"maxFileBytes"`
	MaxWorkspaceBytes int64    `json:"maxWorkspaceBytes"`
	MaxOutputBytes    int      `json:"maxOutputBytes"`
	MaxRows           int      `json:"maxRows"`
	DuckDBMemoryBytes int64    `json:"duckdbMemoryBytes"`
	TimeoutMS         int64    `json:"timeoutMs"`
	WorkerUID         int      `json:"workerUid"`
	WorkerGID         int      `json:"workerGid"`
}

type workerResponse struct {
	OK     bool      `json:"ok"`
	Result any       `json:"result"`
	Error  string    `json:"error"`
	Logs   []LogLine `json:"logs"`
}

func New(cfg Config) (*Manager, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		return &Manager{cfg: cfg, ctx: ctx, cancel: cancel, admission: make(chan struct{}, cfg.MaxConcurrent), workspaces: map[string]*workspace{}}, nil
	}
	binary := strings.TrimSpace(cfg.WorkerBinary)
	if binary == "" {
		var err error
		binary, err = exec.LookPath("gonvex-sandbox-worker")
		if err != nil {
			return nil, errors.New("sandbox is enabled but gonvex-sandbox-worker is not configured or on PATH")
		}
	}
	absolute, err := filepath.Abs(binary)
	if err != nil {
		return nil, fmt.Errorf("sandbox worker path: %w", err)
	}
	if info, err := os.Stat(absolute); err != nil || info.IsDir() || info.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("sandbox worker %q is not executable", absolute)
	}
	cfg.WorkerBinary = absolute
	if runtime.GOOS == "linux" && os.Geteuid() != 0 && !cfg.AllowUnconfined {
		return nil, errors.New("sandbox requires root for chroot isolation; set GONVEX_SANDBOX_ALLOW_UNCONFINED=true only for local development")
	}
	absoluteRoot, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("sandbox root: %w", err)
	}
	cfg.Root = absoluteRoot
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{cfg: cfg, ctx: ctx, cancel: cancel, admission: make(chan struct{}, cfg.MaxConcurrent), workspaces: map[string]*workspace{}}
	go manager.janitor()
	return manager, nil
}

func (m *Manager) Enabled() bool { return m != nil && m.cfg.Enabled }

func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.cancel()
	m.mu.Lock()
	roots := make([]string, 0, len(m.workspaces))
	for _, workspace := range m.workspaces {
		for _, run := range workspace.executions {
			if run.cancel != nil {
				run.cancel()
			}
		}
		roots = append(roots, workspace.root)
	}
	m.workspaces = map[string]*workspace{}
	m.mu.Unlock()
	for _, root := range roots {
		_ = os.RemoveAll(root)
	}
}

func (m *Manager) Create(scope Scope, duckdb bool, ttl time.Duration) (Handle, error) {
	if !m.Enabled() {
		return Handle{}, errors.New("sandbox is disabled by runtime policy")
	}
	if err := scope.validate(); err != nil {
		return Handle{}, err
	}
	if ttl <= 0 {
		ttl = m.cfg.DefaultTTL
	}
	if ttl > m.cfg.MaxTTL {
		return Handle{}, fmt.Errorf("sandbox ttl exceeds %s", m.cfg.MaxTTL)
	}
	now := time.Now().UTC()
	id, err := randomID("sbx")
	if err != nil {
		return Handle{}, err
	}
	root := filepath.Join(m.cfg.Root, id)

	m.mu.Lock()
	if len(m.workspaces) >= m.cfg.MaxTotalSandboxes {
		m.mu.Unlock()
		return Handle{}, errors.New("runtime sandbox limit reached")
	}
	count := 0
	for _, candidate := range m.workspaces {
		if candidate.scope == scope && now.Before(candidate.expiresAt) {
			count++
		}
	}
	if count >= m.cfg.MaxSandboxes {
		m.mu.Unlock()
		return Handle{}, fmt.Errorf("sandbox limit reached for this account and tenant")
	}
	workspace := &workspace{id: id, scope: scope, root: root, duckdb: duckdb, createdAt: now, expiresAt: now.Add(ttl), executions: map[string]*execution{}}
	m.workspaces[id] = workspace
	m.mu.Unlock()
	rollback := func(err error) (Handle, error) {
		m.mu.Lock()
		delete(m.workspaces, id)
		m.mu.Unlock()
		_ = os.RemoveAll(root)
		return Handle{}, err
	}
	if err := os.MkdirAll(filepath.Join(root, "files"), 0o700); err != nil {
		return rollback(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "imports"), 0o700); err != nil {
		return rollback(err)
	}
	if err := m.chownTree(root); err != nil {
		return rollback(err)
	}
	return Handle{SandboxID: id, ExpiresAt: workspace.expiresAt.UnixMilli(), DuckDB: duckdb}, nil
}

func (m *Manager) Run(scope Scope, sandboxID, code string, timeout time.Duration) (ExecutionStatus, error) {
	if len(code) == 0 {
		return ExecutionStatus{}, errors.New("sandbox code is required")
	}
	if len(code) > m.cfg.MaxCodeBytes {
		return ExecutionStatus{}, fmt.Errorf("sandbox code exceeds %d bytes", m.cfg.MaxCodeBytes)
	}
	if timeout <= 0 {
		timeout = m.cfg.DefaultTimeout
	}
	if timeout > m.cfg.MaxTimeout {
		return ExecutionStatus{}, fmt.Errorf("sandbox timeout exceeds %s", m.cfg.MaxTimeout)
	}
	id, err := randomID("run")
	if err != nil {
		return ExecutionStatus{}, err
	}
	m.mu.Lock()
	workspace, err := m.ownedLocked(scope, sandboxID)
	if err != nil {
		m.mu.Unlock()
		return ExecutionStatus{}, err
	}
	if workspace.active != "" || workspace.ioBusy {
		m.mu.Unlock()
		return ExecutionStatus{}, errors.New("sandbox already has an active execution")
	}
	for len(workspace.executions) >= m.cfg.MaxExecutions {
		oldestID := ""
		var oldest time.Time
		for executionID, candidate := range workspace.executions {
			if candidate.status == "queued" || candidate.status == "running" {
				continue
			}
			when := candidate.finishedAt
			if oldestID == "" || when.Before(oldest) {
				oldestID, oldest = executionID, when
			}
		}
		if oldestID == "" {
			m.mu.Unlock()
			return ExecutionStatus{}, errors.New("sandbox execution history limit reached")
		}
		delete(workspace.executions, oldestID)
	}
	run := &execution{id: id, status: "queued", logs: []LogLine{}}
	workspace.executions[id] = run
	workspace.active = id
	m.mu.Unlock()
	go m.execute(workspace, run, code, timeout)
	return m.status(scope, sandboxID, id)
}

func (m *Manager) Cancel(scope Scope, sandboxID, executionID string) (ExecutionStatus, error) {
	m.mu.Lock()
	workspace, err := m.ownedLocked(scope, sandboxID)
	if err != nil {
		m.mu.Unlock()
		return ExecutionStatus{}, err
	}
	run, ok := workspace.executions[executionID]
	if !ok {
		m.mu.Unlock()
		return ExecutionStatus{}, errors.New("sandbox execution not found")
	}
	if run.status == "queued" || run.status == "running" {
		run.cancelRequested = true
		if run.cancel != nil {
			run.cancel()
		}
	}
	status := statusOf(workspace, run)
	m.mu.Unlock()
	return status, nil
}

func (m *Manager) Status(scope Scope, sandboxID, executionID string) (ExecutionStatus, error) {
	return m.status(scope, sandboxID, executionID)
}

func (m *Manager) status(scope Scope, sandboxID, executionID string) (ExecutionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workspace, err := m.ownedLocked(scope, sandboxID)
	if err != nil {
		return ExecutionStatus{}, err
	}
	run, ok := workspace.executions[executionID]
	if !ok {
		return ExecutionStatus{}, errors.New("sandbox execution not found")
	}
	return statusOf(workspace, run), nil
}

func statusOf(workspace *workspace, run *execution) ExecutionStatus {
	status := ExecutionStatus{SandboxID: workspace.id, ExecutionID: run.id, Status: run.status, Result: run.result, Error: run.err, Logs: append([]LogLine(nil), run.logs...)}
	if !run.startedAt.IsZero() {
		status.StartedAt = run.startedAt.UnixMilli()
	}
	if !run.finishedAt.IsZero() {
		status.FinishedAt = run.finishedAt.UnixMilli()
	}
	return status
}

func (m *Manager) WriteFile(scope Scope, sandboxID, name string, content []byte) (map[string]any, error) {
	if int64(len(content)) > m.cfg.MaxFileBytes {
		return nil, fmt.Errorf("sandbox file exceeds %d bytes", m.cfg.MaxFileBytes)
	}
	m.mu.Lock()
	workspace, err := m.ownedLocked(scope, sandboxID)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if workspace.active != "" {
		m.mu.Unlock()
		return nil, errors.New("sandbox files cannot change during execution")
	}
	if workspace.ioBusy {
		m.mu.Unlock()
		return nil, errors.New("another sandbox file operation is active")
	}
	workspace.ioBusy = true
	path, err := safeFilePath(filepath.Join(workspace.root, "files"), name)
	root := workspace.root
	m.mu.Unlock()
	defer m.finishIO(sandboxID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	current, err := workspaceSize(root)
	if err != nil {
		return nil, fmt.Errorf("measure sandbox workspace: %w", err)
	}
	old := int64(0)
	if info, statErr := os.Stat(path); statErr == nil {
		old = info.Size()
	}
	if current-old+int64(len(content)) > m.cfg.MaxWorkspaceBytes {
		return nil, errors.New("sandbox workspace byte limit exceeded")
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return nil, err
	}
	if err := m.chownPathFrom(filepath.Join(root, "files"), path); err != nil {
		return nil, err
	}
	return map[string]any{"path": filepath.ToSlash(name), "size": len(content)}, nil
}

func (m *Manager) ReadFile(scope Scope, sandboxID, name string) (map[string]any, error) {
	m.mu.Lock()
	workspace, err := m.ownedLocked(scope, sandboxID)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if workspace.active != "" || workspace.ioBusy {
		m.mu.Unlock()
		return nil, errors.New("sandbox files are busy")
	}
	workspace.ioBusy = true
	path, err := safeFilePath(filepath.Join(workspace.root, "files"), name)
	m.mu.Unlock()
	defer m.finishIO(sandboxID)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sandbox file: %w", err)
	}
	if int64(len(content)) > m.cfg.MaxFileBytes {
		return nil, errors.New("sandbox file exceeds read limit")
	}
	return map[string]any{"contentBase64": base64.StdEncoding.EncodeToString(content), "size": len(content)}, nil
}

func (m *Manager) AttachDuckDB(scope Scope, sandboxID, source, alias string, tables []Table) (Import, error) {
	m.mu.Lock()
	workspace, err := m.ownedLocked(scope, sandboxID)
	if err != nil {
		m.mu.Unlock()
		return Import{}, err
	}
	if !workspace.duckdb {
		m.mu.Unlock()
		return Import{}, errors.New("sandbox did not declare DuckDB")
	}
	if workspace.active != "" {
		m.mu.Unlock()
		return Import{}, errors.New("sandbox imports cannot change during execution")
	}
	if workspace.ioBusy {
		m.mu.Unlock()
		return Import{}, errors.New("another sandbox file operation is active")
	}
	alias = safeIdentifier(alias)
	if alias == "" {
		alias = "data"
	}
	for _, existing := range workspace.imports {
		if existing.Alias == alias {
			m.mu.Unlock()
			return Import{}, fmt.Errorf("sandbox DuckDB alias %q already exists", alias)
		}
	}
	destination := filepath.Join(workspace.root, "imports", alias+".duckdb")
	root := workspace.root
	workspace.ioBusy = true
	m.mu.Unlock()
	defer m.finishIO(sandboxID)
	input, err := os.Open(source)
	if err != nil {
		return Import{}, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return Import{}, err
	}
	if info.Size() > m.cfg.MaxWorkspaceBytes {
		return Import{}, errors.New("DuckDB import exceeds sandbox workspace limit")
	}
	current, err := workspaceSize(root)
	if err != nil {
		return Import{}, fmt.Errorf("measure sandbox workspace: %w", err)
	}
	if current+info.Size() > m.cfg.MaxWorkspaceBytes {
		return Import{}, errors.New("sandbox workspace byte limit exceeded")
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return Import{}, err
	}
	_, copyErr := io.Copy(output, io.LimitReader(input, m.cfg.MaxWorkspaceBytes+1))
	closeErr := output.Close()
	if copyErr != nil {
		_ = os.Remove(destination)
		return Import{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(destination)
		return Import{}, closeErr
	}
	if err := m.chownPath(destination); err != nil {
		_ = os.Remove(destination)
		return Import{}, err
	}
	attached := Import{Alias: alias, Path: filepath.ToSlash(filepath.Join("imports", alias+".duckdb")), Tables: append([]Table(nil), tables...)}
	m.mu.Lock()
	workspace, err = m.ownedLocked(scope, sandboxID)
	if err == nil {
		workspace.imports = append(workspace.imports, attached)
	}
	m.mu.Unlock()
	if err != nil {
		_ = os.Remove(destination)
		return Import{}, err
	}
	return attached, nil
}

func (m *Manager) execute(workspace *workspace, run *execution, code string, timeout time.Duration) {
	select {
	case m.admission <- struct{}{}:
	case <-m.ctx.Done():
		m.finish(workspace.id, run.id, "cancelled", nil, "runtime shut down", nil)
		return
	}
	defer func() { <-m.admission }()
	ctx, cancel := context.WithTimeout(m.ctx, timeout)
	m.mu.Lock()
	if run.cancelRequested {
		m.mu.Unlock()
		cancel()
		m.finish(workspace.id, run.id, "cancelled", nil, "execution cancelled", nil)
		return
	}
	run.status = "running"
	run.startedAt = time.Now().UTC()
	run.cancel = cancel
	request := workerRequest{Version: 1, Root: workspace.root, AllowUnconfined: m.cfg.AllowUnconfined, Code: code, DuckDB: workspace.duckdb, Imports: append([]Import(nil), workspace.imports...), MaxHeapBytes: m.cfg.MaxHeapBytes, MaxFileBytes: m.cfg.MaxFileBytes, MaxWorkspaceBytes: m.cfg.MaxWorkspaceBytes, MaxOutputBytes: m.cfg.MaxOutputBytes, MaxRows: m.cfg.MaxRows, DuckDBMemoryBytes: m.cfg.DuckDBMemoryBytes, TimeoutMS: timeout.Milliseconds(), WorkerUID: m.cfg.WorkerUID, WorkerGID: m.cfg.WorkerGID}
	m.mu.Unlock()
	payload, err := json.Marshal(request)
	if err != nil {
		cancel()
		m.finish(workspace.id, run.id, "failed", nil, err.Error(), nil)
		return
	}
	cmd := exec.Command(m.cfg.WorkerBinary)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Env = []string{}
	if runtime.GOOS != "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	stdout := &limitedBuffer{limit: m.cfg.MaxOutputBytes + 64*1024}
	stderr := &limitedBuffer{limit: 64 * 1024}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		cancel()
		m.finish(workspace.id, run.id, "failed", nil, err.Error(), nil)
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var executionContextError error
	select {
	case err = <-done:
	case <-ctx.Done():
		executionContextError = ctx.Err()
		killProcessTree(cmd)
		err = <-done
	}
	cancel()
	if executionContextError != nil {
		m.mu.Lock()
		cancelled := run.cancelRequested
		m.mu.Unlock()
		if cancelled {
			m.finish(workspace.id, run.id, "cancelled", nil, "execution cancelled", nil)
		} else {
			m.finish(workspace.id, run.id, "timedOut", nil, "execution timed out", nil)
		}
		return
	}
	if size, sizeErr := workspaceSize(workspace.root); sizeErr != nil || size > m.cfg.MaxWorkspaceBytes {
		_ = os.Remove(filepath.Join(workspace.root, "analysis.duckdb"))
		_ = os.Remove(filepath.Join(workspace.root, "analysis.duckdb.wal"))
		if sizeErr != nil {
			m.finish(workspace.id, run.id, "failed", nil, fmt.Sprintf("measure sandbox workspace: %v", sizeErr), nil)
		} else {
			m.finish(workspace.id, run.id, "failed", nil, "sandbox workspace byte limit exceeded", nil)
		}
		return
	}
	var response workerResponse
	if decodeErr := json.Unmarshal(stdout.Bytes(), &response); decodeErr != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = fmt.Sprintf("sandbox worker failed: %v", err)
		}
		m.finish(workspace.id, run.id, "failed", nil, message, nil)
		return
	}
	if !response.OK {
		m.finish(workspace.id, run.id, "failed", nil, response.Error, response.Logs)
		return
	}
	m.finish(workspace.id, run.id, "succeeded", response.Result, "", response.Logs)
}

func (m *Manager) finish(sandboxID, executionID, status string, result any, message string, logs []LogLine) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workspace := m.workspaces[sandboxID]
	if workspace == nil {
		return
	}
	run := workspace.executions[executionID]
	if run == nil {
		return
	}
	run.status, run.result, run.err, run.logs = status, result, message, append([]LogLine(nil), logs...)
	run.finishedAt, run.cancel = time.Now().UTC(), nil
	if workspace.active == executionID {
		workspace.active = ""
	}
}

func (m *Manager) finishIO(sandboxID string) {
	m.mu.Lock()
	if workspace := m.workspaces[sandboxID]; workspace != nil {
		workspace.ioBusy = false
	}
	m.mu.Unlock()
}

func (m *Manager) ownedLocked(scope Scope, sandboxID string) (*workspace, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	workspace := m.workspaces[strings.TrimSpace(sandboxID)]
	if workspace == nil || workspace.scope != scope {
		return nil, errors.New("sandbox not found")
	}
	if !time.Now().UTC().Before(workspace.expiresAt) {
		return nil, errors.New("sandbox expired")
	}
	return workspace, nil
}

func (m *Manager) janitor() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case now := <-ticker.C:
			var roots []string
			m.mu.Lock()
			for id, workspace := range m.workspaces {
				if now.Before(workspace.expiresAt) {
					continue
				}
				if workspace.ioBusy {
					continue
				}
				if workspace.active != "" {
					if run := workspace.executions[workspace.active]; run != nil && run.cancel != nil {
						run.cancel()
					}
				}
				roots = append(roots, workspace.root)
				delete(m.workspaces, id)
			}
			m.mu.Unlock()
			for _, root := range roots {
				_ = os.RemoveAll(root)
			}
		}
	}
}

func safeFilePath(root, name string) (string, error) {
	name = filepath.FromSlash(strings.TrimSpace(name))
	if name == "" || filepath.IsAbs(name) {
		return "", errors.New("sandbox path must be relative")
	}
	clean := filepath.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("sandbox path escapes the workspace")
	}
	path := filepath.Join(root, clean)
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("sandbox path escapes the workspace")
	}
	return path, nil
}

func workspaceSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func safeIdentifier(value string) string {
	var builder strings.Builder
	for _, char := range strings.TrimSpace(value) {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9' && builder.Len() > 0) || char == '_' {
			builder.WriteRune(char)
		} else if builder.Len() > 0 {
			builder.WriteByte('_')
		}
	}
	return strings.Trim(builder.String(), "_")
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(raw[:]), nil
}

func (m *Manager) chownTree(root string) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return nil
	}
	return filepath.Walk(root, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chown(path, m.cfg.WorkerUID, m.cfg.WorkerGID)
	})
}

func (m *Manager) chownPath(path string) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return nil
	}
	if err := os.Chown(filepath.Dir(path), m.cfg.WorkerUID, m.cfg.WorkerGID); err != nil {
		return err
	}
	return os.Chown(path, m.cfg.WorkerUID, m.cfg.WorkerGID)
}

func (m *Manager) chownPathFrom(base, path string) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return nil
	}
	base = filepath.Clean(base)
	directory := filepath.Dir(path)
	relative, err := filepath.Rel(base, directory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("sandbox path escapes the ownership root")
	}
	if err := os.Chown(base, m.cfg.WorkerUID, m.cfg.WorkerGID); err != nil {
		return err
	}
	current := base
	if relative != "." {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			current = filepath.Join(current, component)
			if err := os.Chown(current, m.cfg.WorkerUID, m.cfg.WorkerGID); err != nil {
				return err
			}
		}
	}
	return os.Chown(path, m.cfg.WorkerUID, m.cfg.WorkerGID)
}

func killProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if runtime.GOOS != "windows" {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	_ = cmd.Process.Kill()
}

type limitedBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (buffer *limitedBuffer) Write(value []byte) (int, error) {
	remaining := buffer.limit - buffer.buffer.Len()
	if remaining <= 0 {
		return len(value), nil
	}
	if len(value) > remaining {
		_, _ = buffer.buffer.Write(value[:remaining])
		return len(value), nil
	}
	return buffer.buffer.Write(value)
}
func (buffer *limitedBuffer) Bytes() []byte  { return buffer.buffer.Bytes() }
func (buffer *limitedBuffer) String() string { return buffer.buffer.String() }
