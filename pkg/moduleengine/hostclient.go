package moduleengine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

// hostCallDispatcher answers the host calls one invocation makes. It is bound
// to the invocation's own context — and therefore to that call's transaction,
// identity and capabilities — so a host call can never be served with another
// call's authority.
type hostCallDispatcher interface {
	dispatch(ctx context.Context, call hostCallPayload) (json.RawMessage, error)
	// close releases whatever the dispatcher opened for the invocation.
	close()
}

// session is one live connection to a module host. It multiplexes requests by
// id in one direction and host calls by invocation id in the other, so a module
// call and the database reads it makes travel over the same connection while
// the call is still running.
type session struct {
	epoch    uint64
	conn     net.Conn
	limit    int
	logger   *slog.Logger
	protocol int
	version  string

	writeMu sync.Mutex

	mu      sync.Mutex
	nextID  uint64
	pending map[uint64]*pendingRequest

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

type pendingRequest struct {
	ctx        context.Context
	response   chan inboundFrame
	dispatcher hostCallDispatcher
}

func newSession(ctx context.Context, conn net.Conn, epoch uint64, limit int, logger *slog.Logger) (*session, error) {
	if limit <= 0 {
		limit = DefaultMaxFrameBytes
	}
	if logger == nil {
		logger = slog.Default()
	}
	current := &session{
		epoch:   epoch,
		conn:    conn,
		limit:   limit,
		logger:  logger,
		pending: map[uint64]*pendingRequest{},
		closed:  make(chan struct{}),
	}
	go current.readLoop()

	// The handshake is a ping rather than a wait on the host's greeting: it
	// proves the connection round-trips and that both sides speak the same
	// protocol before a module is published to it.
	frame, err := current.request(ctx, pingOp{Op: "ping"}, nil)
	if err != nil {
		current.close(err)
		return nil, err
	}
	var pong pongResponse
	if err := json.Unmarshal(frame.Payload, &pong); err != nil {
		current.close(err)
		return nil, fmt.Errorf("moduleengine: module host sent an unreadable handshake: %w", err)
	}
	if pong.Protocol != ProtocolVersion {
		err := fmt.Errorf("moduleengine: module host speaks protocol %d, this runtime speaks %d", pong.Protocol, ProtocolVersion)
		current.close(err)
		return nil, err
	}
	current.protocol = pong.Protocol
	current.version = pong.Version
	return current, nil
}

func (s *session) readLoop() {
	reader := bufio.NewReaderSize(s.conn, 64<<10)
	for {
		payload, err := readFrame(reader, s.limit)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("moduleengine: module host closed the connection")
			}
			s.close(err)
			return
		}
		var frame inboundFrame
		if err := json.Unmarshal(payload, &frame); err != nil {
			s.logger.Warn("module host sent an unreadable frame", "error", err)
			continue
		}
		switch frame.Type {
		case frameReady:
			s.logger.Debug("module host ready", "protocol", frame.Protocol, "version", frame.Version)
		case frameResponse, frameError:
			s.deliver(frame)
		case frameHostCall:
			go s.serveHostCall(frame)
		default:
			s.logger.Warn("module host sent an unknown frame", "type", frame.Type)
		}
	}
}

func (s *session) deliver(frame inboundFrame) {
	s.mu.Lock()
	pending, ok := s.pending[frame.ID]
	s.mu.Unlock()
	if !ok {
		// A response for a request that was cancelled or timed out. Dropping it
		// is correct; the caller already has its answer.
		return
	}
	select {
	case pending.response <- frame:
	default:
	}
}

// serveHostCall answers one host operation for a running invocation. It runs on
// its own goroutine so a module may have several in flight; dispatchers that
// touch a transaction serialize themselves.
func (s *session) serveHostCall(frame inboundFrame) {
	s.mu.Lock()
	pending, ok := s.pending[frame.Invocation]
	s.mu.Unlock()
	if !ok || pending.dispatcher == nil {
		s.replyHostError(frame.ID, &HostError{
			Code:    codeHostCallFailed,
			Message: fmt.Sprintf("no invocation %d is running on this connection", frame.Invocation),
		})
		return
	}

	var call hostCallPayload
	if err := json.Unmarshal(frame.Payload, &call); err != nil {
		s.replyHostError(frame.ID, &HostError{
			Code:    codeBadRequest,
			Message: fmt.Sprintf("unreadable host call: %v", err),
		})
		return
	}
	value, err := pending.dispatcher.dispatch(pending.ctx, call)
	if err != nil {
		s.replyHostError(frame.ID, &HostError{Code: codeHostCallFailed, Message: err.Error()})
		return
	}
	if len(value) == 0 {
		value = json.RawMessage("null")
	}
	if err := s.write(outboundFrame{Type: frameHostResponse, ID: frame.ID, Value: value}); err != nil {
		s.logger.Warn("failed to answer a module host call", "error", err)
	}
}

func (s *session) replyHostError(id uint64, hostErr *HostError) {
	if err := s.write(outboundFrame{Type: frameHostError, ID: id, Error: hostErr}); err != nil {
		s.logger.Warn("failed to report a module host call failure", "error", err)
	}
}

func (s *session) write(frame outboundFrame) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("moduleengine: failed to encode a module host frame: %w", err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-s.closed:
		return s.err()
	default:
	}
	return writeFrame(s.conn, payload, s.limit)
}

// request sends one operation and waits for its answer. dispatcher is non-nil
// only for invocations, which are the only requests that can call back.
func (s *session) request(ctx context.Context, op any, dispatcher hostCallDispatcher) (inboundFrame, error) {
	select {
	case <-s.closed:
		return inboundFrame{}, s.err()
	default:
	}

	s.mu.Lock()
	s.nextID++
	id := s.nextID
	pending := &pendingRequest{ctx: ctx, response: make(chan inboundFrame, 1), dispatcher: dispatcher}
	s.pending[id] = pending
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}()

	frame := outboundFrame{Type: frameRequest, ID: id, Payload: op}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixMS = deadline.UnixMilli()
	}
	if err := s.write(frame); err != nil {
		return inboundFrame{}, err
	}

	select {
	case response := <-pending.response:
		if response.Type == frameError {
			if response.Error == nil {
				return inboundFrame{}, fmt.Errorf("moduleengine: module host failed without an error")
			}
			return inboundFrame{}, response.Error
		}
		return response, nil
	case <-ctx.Done():
		// Telling the host lets it stop the module and retire the isolate
		// instead of finishing work whose result nobody will read.
		if err := s.write(outboundFrame{Type: frameCancel, ID: id}); err != nil {
			s.logger.Debug("failed to cancel a module host request", "error", err)
		}
		return inboundFrame{}, ctx.Err()
	case <-s.closed:
		return inboundFrame{}, s.err()
	}
}

func (s *session) close(cause error) {
	s.closeOnce.Do(func() {
		s.closeErr = cause
		close(s.closed)
		_ = s.conn.Close()
	})
}

func (s *session) err() error {
	select {
	case <-s.closed:
		if s.closeErr != nil {
			return s.closeErr
		}
		return errors.New("moduleengine: module host connection is closed")
	default:
		return nil
	}
}

func (s *session) alive() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}

// dial opens a local connection to a module host endpoint. Only local
// transports are supported: the protocol carries no authentication because it
// is never meant to leave the machine.
func dial(ctx context.Context, endpoint string) (net.Conn, error) {
	network, address, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("moduleengine: failed to connect to the module host at %s: %w", endpoint, err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return conn, nil
}

// parseEndpoint accepts unix:/path, unix:///path, tcp:host:port, tcp://host:port,
// a bare socket path, or a bare host:port.
func parseEndpoint(endpoint string) (network string, address string, err error) {
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return "", "", errors.New("moduleengine: module host endpoint is empty")
	}
	scheme := ""
	rest := value
	if index := strings.Index(value, "://"); index >= 0 {
		scheme, rest = value[:index], value[index+3:]
	} else if head, tail, ok := strings.Cut(value, ":"); ok && (head == "unix" || head == "tcp") {
		scheme, rest = head, tail
	}
	switch scheme {
	case "unix":
		return "unix", rest, nil
	case "tcp":
		return "tcp", rest, nil
	case "":
		if strings.HasPrefix(rest, "/") || strings.HasPrefix(rest, ".") {
			return "unix", rest, nil
		}
		return "tcp", rest, nil
	default:
		return "", "", fmt.Errorf("moduleengine: unsupported module host endpoint %q", endpoint)
	}
}

// waitForEndpoint dials until the host is accepting or the deadline passes. A
// freshly spawned host binds after it starts, so the first dial usually loses
// the race by a few milliseconds.
func waitForEndpoint(ctx context.Context, endpoint string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		conn, err := dial(attempt, endpoint)
		cancel()
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}
