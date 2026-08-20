package moduleengine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultHostBinary is the module host executable looked up on PATH when a
// deployment does not name one. Absent binary and absent endpoint together mean
// "no module host", which is what keeps this opt-in.
const DefaultHostBinary = "gonvex-module-host"

// RemoteRuntime is the Runtime() name reported by engines served by the module
// host. It names the implementation, not the module's language.
const RemoteRuntime = "rust"

// HostOptions configures the one module host a runtime talks to.
type HostOptions struct {
	// Endpoint is a local address: unix:/path/to.sock or tcp:127.0.0.1:7787.
	// Empty with a Binary set makes the runtime choose one.
	Endpoint string
	// Binary is the module host executable. Empty with an Endpoint set means an
	// externally managed host that this runtime only connects to.
	Binary string
	Args   []string
	Dir    string
	Env    []string

	StartTimeout    time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	// DrainTimeout is how long a retired module generation may keep finishing
	// its in-flight calls after a newer one is activated.
	DrainTimeout time.Duration

	MaxFrameBytes      int
	MaxConcurrentCalls int
	IsolatePoolSize    int
	ExecutionTimeout   time.Duration

	Logger *slog.Logger
}

func (o HostOptions) withDefaults() HostOptions {
	if o.StartTimeout <= 0 {
		o.StartTimeout = 30 * time.Second
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 30 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 10 * time.Second
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 30 * time.Second
	}
	if o.MaxFrameBytes <= 0 {
		o.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// RemoteHost owns the runtime's relationship with the module host: at most one
// process and at most one connection, shared by every project.
//
// One host serves every project and every tenant. Engines are per module
// generation, and tenancy travels on the invocation context, so a process per
// tenant would only multiply V8 heaps.
type RemoteHost struct {
	options HostOptions
	managed bool

	mu          sync.Mutex
	session     *session
	process     *hostProcess
	epoch       uint64
	closed      bool
	socketOwned string
}

// HostStatus is a lock-consistent snapshot of the TypeScript module host.
// Ready means a configured host has a live control session and, when Gonvex
// owns the process, that child has not exited.
type HostStatus struct {
	Configured bool
	Managed    bool
	Started    bool
	Running    bool
	Connected  bool
	Ready      bool
	Closed     bool
	Epoch      uint64
	Error      string
}

// NewRemoteHost returns a host handle. It starts nothing: the process is
// launched, or the endpoint dialled, the first time a module needs it, so a
// deployment with no TypeScript modules never pays for one.
func NewRemoteHost(options HostOptions) *RemoteHost {
	options = options.withDefaults()
	options.Endpoint = strings.TrimSpace(options.Endpoint)
	options.Binary = strings.TrimSpace(options.Binary)
	host := &RemoteHost{options: options}
	// An explicit endpoint with no binary is an externally managed host; a
	// binary (named or found on PATH) means this runtime supervises one.
	if host.options.Binary == "" && host.options.Endpoint == "" {
		if resolved, err := exec.LookPath(DefaultHostBinary); err == nil {
			host.options.Binary = resolved
		}
	}
	host.managed = host.options.Binary != ""
	return host
}

// Available reports whether a module host is configured at all. TypeScript
// manifests require this host and fail loudly rather than silently degrading.
func (h *RemoteHost) Available() bool {
	if h == nil {
		return false
	}
	return h.options.Binary != "" || h.options.Endpoint != ""
}

// Describe reports how the host is reached, for logs and errors.
func (h *RemoteHost) Describe() string {
	if h == nil || !h.Available() {
		return "not configured"
	}
	if h.managed {
		return fmt.Sprintf("%s (managed)", h.options.Binary)
	}
	return fmt.Sprintf("%s (external)", h.options.Endpoint)
}

// Status reports module-host liveness without starting a lazy host or changing
// module state. The server combines this with its active-module count so an
// unused, intentionally lazy host does not make an empty runtime unhealthy.
func (h *RemoteHost) Status() HostStatus {
	if h == nil {
		return HostStatus{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	status := HostStatus{
		Configured: h.options.Binary != "" || h.options.Endpoint != "",
		Managed:    h.managed,
		Closed:     h.closed,
		Epoch:      h.epoch,
	}
	if h.process != nil {
		status.Started = true
		status.Running = h.process.running()
		if !status.Running {
			status.Error = fmt.Sprintf("module host process exited: %v", h.process.exitErr())
		}
	} else if !h.managed && h.session != nil {
		status.Started = true
		status.Running = h.session.alive()
	}
	if h.session != nil {
		status.Connected = h.session.alive()
		if !status.Connected && status.Error == "" {
			status.Error = fmt.Sprintf("module host connection lost: %v", h.session.err())
		}
	}
	if status.Closed && status.Error == "" {
		status.Error = "module host is shut down"
	}
	processReady := !status.Managed || status.Running
	status.Ready = status.Configured && !status.Closed && processReady && status.Connected
	return status
}

func (h *RemoteHost) drainTimeout() time.Duration { return h.options.DrainTimeout }

func (h *RemoteHost) requestTimeout() time.Duration { return h.options.RequestTimeout }

// Session returns a live connection, starting or restarting the host process if
// this runtime manages it. Callers re-publish their module when the session
// epoch changes, which is how a host restart heals without a client reconnect.
func (h *RemoteHost) Session(ctx context.Context) (*session, error) {
	if h == nil || !h.Available() {
		return nil, errors.New("moduleengine: no module host is configured; set GONVEX_MODULE_HOST_BINARY or GONVEX_MODULE_HOST_ENDPOINT to run TypeScript modules")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, errors.New("moduleengine: module host is shut down")
	}
	if h.session != nil && h.session.alive() {
		return h.session, nil
	}
	if h.session != nil {
		h.options.Logger.Warn("module host connection lost; reconnecting", "error", h.session.err())
		h.session = nil
	}

	if h.managed {
		if h.process == nil || !h.process.running() {
			if h.process != nil {
				h.options.Logger.Warn("module host process exited; restarting", "error", h.process.exitErr())
				h.releaseProcessLocked()
			}
			process, err := h.startProcessLocked(ctx)
			if err != nil {
				return nil, err
			}
			h.process = process
		}
	}

	endpoint := h.options.Endpoint
	if h.process != nil && h.process.endpoint != "" {
		endpoint = h.process.endpoint
	}
	dialCtx, cancel := context.WithTimeout(ctx, h.options.StartTimeout)
	defer cancel()
	conn, err := waitForEndpoint(dialCtx, endpoint, h.options.StartTimeout)
	if err != nil {
		return nil, err
	}
	h.epoch++
	handshake, cancelHandshake := context.WithTimeout(ctx, h.options.StartTimeout)
	defer cancelHandshake()
	current, err := newSession(handshake, conn, h.epoch, h.options.MaxFrameBytes, h.options.Logger)
	if err != nil {
		return nil, err
	}
	h.options.Logger.Info("connected to the Gonvex module host",
		"endpoint", endpoint, "version", current.version, "protocol", current.protocol, "epoch", current.epoch)
	h.session = current
	return current, nil
}

func (h *RemoteHost) startProcessLocked(ctx context.Context) (*hostProcess, error) {
	endpoint := h.options.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint()
	}
	if network, address, err := parseEndpoint(endpoint); err == nil && network == "unix" {
		// Remembered so a socket file this runtime's host created does not
		// outlive it. Displacing a *live* host is the host's own decision: it
		// connects to a leftover socket before it removes one.
		h.socketOwned = address
	}
	process, err := startHostProcess(ctx, h.options, endpoint)
	if err != nil {
		return nil, err
	}
	return process, nil
}

func (h *RemoteHost) releaseProcessLocked() {
	if h.process == nil {
		return
	}
	h.process.stop(h.options.ShutdownTimeout)
	h.process = nil
	if h.socketOwned != "" {
		_ = os.Remove(h.socketOwned)
	}
}

// Close shuts the host down within a bound: the module host is asked to drain
// its in-flight calls, the connection is closed, and a managed process is given
// the same window to exit before it is killed.
func (h *RemoteHost) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true

	var shutdownErr error
	if h.session != nil && h.session.alive() {
		request, cancel := context.WithTimeout(ctx, h.options.ShutdownTimeout)
		_, shutdownErr = h.session.request(request, shutdownOp{
			Op:      "shutdown",
			GraceMS: uint64(h.options.ShutdownTimeout / time.Millisecond),
		}, nil)
		cancel()
		// A host may close its control socket immediately after accepting the
		// shutdown request. That is a successful terminal state, even if the final
		// acknowledgement loses the race with EOF.
		if errors.Is(shutdownErr, errModuleHostClosed) {
			shutdownErr = nil
		}
		if shutdownErr != nil {
			h.options.Logger.Warn("module host did not confirm shutdown", "error", shutdownErr)
		}
	}
	if h.session != nil {
		h.session.close(errors.New("moduleengine: module host shut down"))
		h.session = nil
	}
	h.releaseProcessLocked()
	return shutdownErr
}

// hostProcess is a supervised module host executable.
type hostProcess struct {
	cmd      *exec.Cmd
	stdin    io.Closer
	endpoint string

	exited  chan struct{}
	waitErr error
}

func startHostProcess(ctx context.Context, options HostOptions, endpoint string) (*hostProcess, error) {
	args := []string{"--listen", endpoint, "--exit-on-stdin-eof"}
	if options.MaxFrameBytes > 0 {
		args = append(args, "--max-frame-bytes", strconv.Itoa(options.MaxFrameBytes))
	}
	if options.MaxConcurrentCalls > 0 {
		args = append(args, "--max-concurrent", strconv.Itoa(options.MaxConcurrentCalls))
	}
	if options.IsolatePoolSize > 0 {
		args = append(args, "--isolate-pool", strconv.Itoa(options.IsolatePoolSize))
	}
	if options.ExecutionTimeout > 0 {
		args = append(args, "--execution-timeout-ms", strconv.FormatInt(options.ExecutionTimeout.Milliseconds(), 10))
	}
	if options.DrainTimeout > 0 {
		args = append(args, "--drain-ms", strconv.FormatInt(options.DrainTimeout.Milliseconds(), 10))
	}
	if options.ShutdownTimeout > 0 {
		args = append(args, "--shutdown-ms", strconv.FormatInt(options.ShutdownTimeout.Milliseconds(), 10))
	}
	args = append(args, options.Args...)

	command := exec.Command(options.Binary, args...)
	command.Dir = options.Dir
	if len(options.Env) > 0 {
		command.Env = options.Env
	}

	// os.Pipe rather than Cmd.StdoutPipe: Wait closes the pipes it created,
	// which would race the readers that stay alive for the process's lifetime.
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("moduleengine: failed to create a module host stdin pipe: %w", err)
	}
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		stdinRead.Close()
		stdinWrite.Close()
		return nil, fmt.Errorf("moduleengine: failed to create a module host stdout pipe: %w", err)
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		stdinRead.Close()
		stdinWrite.Close()
		stdoutRead.Close()
		stdoutWrite.Close()
		return nil, fmt.Errorf("moduleengine: failed to create a module host stderr pipe: %w", err)
	}
	command.Stdin = stdinRead
	command.Stdout = stdoutWrite
	command.Stderr = stderrWrite

	if err := command.Start(); err != nil {
		for _, file := range []*os.File{stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite} {
			file.Close()
		}
		return nil, fmt.Errorf("moduleengine: failed to start the module host %s: %w", options.Binary, err)
	}
	// The child owns its ends now. Keeping stdinWrite open is the orphan guard:
	// closing it, including by this process dying, tells the host to shut down.
	stdinRead.Close()
	stdoutWrite.Close()
	stderrWrite.Close()

	process := &hostProcess{cmd: command, stdin: stdinWrite, endpoint: endpoint, exited: make(chan struct{})}
	ready := make(chan string, 1)
	go readReadyLine(stdoutRead, ready, options.Logger)
	go forwardOutput(stderrRead, options.Logger)
	go func() {
		process.waitErr = command.Wait()
		close(process.exited)
	}()

	select {
	case resolved := <-ready:
		if resolved != "" {
			// A host asked to bind port 0 reports the port it actually got.
			process.endpoint = resolved
		}
	case <-process.exited:
		process.stop(options.ShutdownTimeout)
		return nil, fmt.Errorf("moduleengine: module host exited during startup: %v", process.waitErr)
	case <-time.After(options.StartTimeout):
		process.stop(options.ShutdownTimeout)
		return nil, fmt.Errorf("moduleengine: module host did not report readiness within %s", options.StartTimeout)
	case <-ctx.Done():
		process.stop(options.ShutdownTimeout)
		return nil, ctx.Err()
	}
	options.Logger.Info("started the Gonvex module host",
		"binary", options.Binary, "endpoint", process.endpoint, "pid", command.Process.Pid)
	return process, nil
}

func (p *hostProcess) running() bool {
	if p == nil {
		return false
	}
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (p *hostProcess) exitErr() error {
	if p == nil {
		return nil
	}
	select {
	case <-p.exited:
		return p.waitErr
	default:
		return nil
	}
}

// stop ends the process within timeout: closing stdin asks for a graceful
// shutdown, and a kill is the bound on how long that may take.
func (p *hostProcess) stop(timeout time.Duration) {
	if p == nil {
		return
	}
	if p.stdin != nil {
		_ = p.stdin.Close()
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	select {
	case <-p.exited:
		return
	case <-time.After(timeout):
	}
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.exited:
	case <-time.After(2 * time.Second):
	}
}

// readReadyLine consumes the host's stdout, reporting the endpoint from its
// readiness line and logging anything else it prints.
func readReadyLine(reader io.ReadCloser, ready chan<- string, logger *slog.Logger) {
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	announced := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !announced {
			var announcement struct {
				Ready    bool   `json:"ready"`
				Endpoint string `json:"endpoint"`
			}
			if err := json.Unmarshal([]byte(line), &announcement); err == nil && announcement.Ready {
				announced = true
				ready <- announcement.Endpoint
				continue
			}
		}
		logger.Info("module host", "message", line)
	}
	if !announced {
		close(ready)
	}
}

func forwardOutput(reader io.ReadCloser, logger *slog.Logger) {
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			logger.Info("module host", "message", line)
		}
	}
}

func defaultEndpoint() string {
	if runtime.GOOS == "windows" {
		// Named pipes are not part of this protocol; loopback with a host-chosen
		// port keeps Windows working without a second transport.
		return "tcp:127.0.0.1:0"
	}
	return "unix:" + filepath.Join(os.TempDir(), fmt.Sprintf("gonvex-module-host-%d.sock", os.Getpid()))
}
