package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type authMode string

const (
	authModeNone      authMode = "none"
	authModeShared    authMode = "shared"
	authModeSynthetic authMode = "synthetic"
)

type runConfig struct {
	URL                        string
	Project                    string
	Tenant                     string
	Connections                int
	SubscriptionsPerConnection int
	RampDuration               time.Duration
	HoldDuration               time.Duration
	ConnectTimeout             time.Duration
	InitialTimeout             time.Duration
	AuthMode                   authMode
	SharedToken                string
	Compression                bool
	MaximumDialConcurrency     int
	Variables                  map[string]string
	SampleInterval             time.Duration
	TargetPID                  int
	Safety                     safetyLimits
}

type runMetrics struct {
	connectionAttempts atomic.Uint64
	connections        atomic.Uint64
	setupErrors        atomic.Uint64
	unexpectedCloses   atomic.Uint64
	setupFinished      atomic.Uint64
	subscriptionsSent  atomic.Uint64
	initialResults     atomic.Uint64
	subscriptionErrors atomic.Uint64
	logicalBytesRead   atomic.Uint64
	logicalBytesWrite  atomic.Uint64
	wireBytesRead      atomic.Uint64
	wireBytesWritten   atomic.Uint64

	connectLatency *latencyHistogram
	authLatency    *latencyHistogram
	initialLatency *latencyHistogram
	serverLatency  *latencyHistogram

	pathMu       sync.Mutex
	paths        map[string]*pathMetrics
	errorSamples map[string]uint64

	resourceMu  sync.Mutex
	samples     []ResourceSample
	abortReason string
}

type pathMetrics struct {
	initialResults uint64
	errors         uint64
	payloadBytes   uint64
	initialLatency *latencyHistogram
	serverLatency  *latencyHistogram
}

type RunReport struct {
	Profile       string                `json:"profile"`
	Target        string                `json:"target"`
	Project       string                `json:"project"`
	Tenant        string                `json:"tenant"`
	StartedAt     string                `json:"startedAt"`
	CompletedAt   string                `json:"completedAt"`
	DurationMS    int64                 `json:"durationMs"`
	AbortReason   string                `json:"abortReason,omitempty"`
	Connections   ConnectionReport      `json:"connections"`
	Subscriptions SubscriptionReport    `json:"subscriptions"`
	Wire          WireReport            `json:"wire"`
	Latency       LatencyReport         `json:"latency"`
	Paths         map[string]PathReport `json:"paths"`
	Samples       []ResourceSample      `json:"samples,omitempty"`
	ErrorSamples  []ErrorSample         `json:"errorSamples,omitempty"`
}

type ErrorSample struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Count   uint64 `json:"count"`
}

type ConnectionReport struct {
	Target           uint64 `json:"target"`
	Attempted        uint64 `json:"attempted"`
	Established      uint64 `json:"established"`
	UnexpectedCloses uint64 `json:"unexpectedCloses"`
	SetupErrors      uint64 `json:"setupErrors"`
}

type SubscriptionReport struct {
	Target         uint64  `json:"target"`
	Sent           uint64  `json:"sent"`
	InitialResults uint64  `json:"initialResults"`
	Errors         uint64  `json:"errors"`
	ErrorRate      float64 `json:"errorRate"`
}

type WireReport struct {
	BytesRead            uint64  `json:"bytesRead"`
	BytesWritten         uint64  `json:"bytesWritten"`
	LogicalBytesRead     uint64  `json:"logicalBytesRead"`
	LogicalBytesWritten  uint64  `json:"logicalBytesWritten"`
	ReadCompressionRatio float64 `json:"readCompressionRatio"`
}

type LatencyReport struct {
	Connect       HistogramReport `json:"connect"`
	Auth          HistogramReport `json:"auth"`
	InitialResult HistogramReport `json:"initialResult"`
	ServerQuery   HistogramReport `json:"serverQuery"`
}

type HistogramReport struct {
	Count     uint64  `json:"count"`
	AverageMS float64 `json:"averageMs"`
	P50MS     float64 `json:"p50Ms"`
	P95MS     float64 `json:"p95Ms"`
	P99MS     float64 `json:"p99Ms"`
	MaxMS     float64 `json:"maxMs"`
}

type PathReport struct {
	InitialResults uint64          `json:"initialResults"`
	Errors         uint64          `json:"errors"`
	PayloadBytes   uint64          `json:"payloadBytes"`
	InitialLatency HistogramReport `json:"initialLatency"`
	ServerLatency  HistogramReport `json:"serverLatency"`
}

type serverEnvelope struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Path   string          `json:"path"`
	Reason string          `json:"reason"`
	Error  string          `json:"error"`
	Result json.RawMessage `json:"result"`
	Trace  *struct {
		ServerDurationMS float64 `json:"serverDurationMs"`
	} `json:"trace"`
}

type pendingSubscription struct {
	path   string
	sentAt time.Time
	seen   bool
}

func newRunMetrics() *runMetrics {
	return &runMetrics{
		connectLatency: newLatencyHistogram(),
		authLatency:    newLatencyHistogram(),
		initialLatency: newLatencyHistogram(),
		serverLatency:  newLatencyHistogram(),
		paths:          map[string]*pathMetrics{},
		errorSamples:   map[string]uint64{},
	}
}

func (m *runMetrics) path(path string) *pathMetrics {
	m.pathMu.Lock()
	defer m.pathMu.Unlock()
	metrics := m.paths[path]
	if metrics == nil {
		metrics = &pathMetrics{initialLatency: newLatencyHistogram(), serverLatency: newLatencyHistogram()}
		m.paths[path] = metrics
	}
	return metrics
}

func (m *runMetrics) recordInitial(path string, latency time.Duration, serverDuration time.Duration, payloadBytes int) {
	m.initialResults.Add(1)
	m.initialLatency.Observe(latency)
	if serverDuration > 0 {
		m.serverLatency.Observe(serverDuration)
	}
	pathMetrics := m.path(path)
	m.pathMu.Lock()
	pathMetrics.initialResults++
	pathMetrics.payloadBytes += uint64(payloadBytes)
	m.pathMu.Unlock()
	pathMetrics.initialLatency.Observe(latency)
	if serverDuration > 0 {
		pathMetrics.serverLatency.Observe(serverDuration)
	}
}

func (m *runMetrics) recordError(path string, message string) {
	m.subscriptionErrors.Add(1)
	pathMetrics := m.path(path)
	m.pathMu.Lock()
	pathMetrics.errors++
	if len(m.errorSamples) < 20 || m.errorSamples[path+"\x00"+message] > 0 {
		m.errorSamples[path+"\x00"+message]++
	}
	m.pathMu.Unlock()
}

func (m *runMetrics) recordSetupError(path string) {
	m.setupErrors.Add(1)
	pathMetrics := m.path(path)
	m.pathMu.Lock()
	pathMetrics.errors++
	m.pathMu.Unlock()
}

func (m *runMetrics) setAbort(reason string) {
	if strings.TrimSpace(reason) == "" {
		return
	}
	m.resourceMu.Lock()
	if m.abortReason == "" {
		m.abortReason = reason
	}
	m.resourceMu.Unlock()
}

func (m *runMetrics) abort() string {
	m.resourceMu.Lock()
	defer m.resourceMu.Unlock()
	return m.abortReason
}

func runLoad(ctx context.Context, config runConfig, profile Profile) (RunReport, error) {
	if err := validateRunConfig(config, profile); err != nil {
		return RunReport{}, err
	}
	startedAt := time.Now().UTC()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := newRunMetrics()
	dialSemaphore := make(chan struct{}, config.MaximumDialConcurrency)
	var clients sync.WaitGroup
	var sampler sync.WaitGroup
	if config.SampleInterval > 0 {
		sampler.Add(1)
		go func() {
			defer sampler.Done()
			sampleRunResources(runCtx, cancel, config, metrics, startedAt)
		}()
	}

	launched := 0
launchLoop:
	for index := 0; index < config.Connections; index++ {
		if index > 0 && config.RampDuration > 0 {
			interval := config.RampDuration / time.Duration(config.Connections)
			if err := waitContext(runCtx, interval); err != nil {
				break launchLoop
			}
		}
		clients.Add(1)
		launched++
		go func(userIndex int) {
			defer clients.Done()
			runVirtualUser(runCtx, config, profile, userIndex, metrics, dialSemaphore)
		}(index)
	}

	expectedSubscriptions := uint64(config.Connections * config.SubscriptionsPerConnection)
	initialDeadline := time.NewTimer(config.InitialTimeout)
	defer initialDeadline.Stop()
	abortReason := metrics.abort()
	if abortReason == "" && launched != config.Connections {
		abortReason = "connection ramp stopped before all virtual users launched"
	}
	waitTicker := time.NewTicker(10 * time.Millisecond)
	defer waitTicker.Stop()

	waiting := abortReason == ""
	for waiting {
		select {
		case <-runCtx.Done():
			abortReason = metrics.abort()
			if abortReason == "" {
				abortReason = runCtx.Err().Error()
			}
			waiting = false
		case <-initialDeadline.C:
			abortReason = "initial subscription timeout"
			waiting = false
		case <-waitTicker.C:
			settled := metrics.initialResults.Load() + metrics.subscriptionErrors.Load()
			if metrics.setupFinished.Load() == uint64(config.Connections) && settled >= metrics.subscriptionsSent.Load() {
				waiting = false
			}
		}
	}

	if abortReason == "" && expectedSubscriptions != metrics.subscriptionsSent.Load() {
		abortReason = "not all target subscriptions were sent"
	}
	if abortReason == "" && config.HoldDuration > 0 {
		select {
		case <-runCtx.Done():
			abortReason = metrics.abort()
			if abortReason == "" {
				abortReason = runCtx.Err().Error()
			}
		case <-time.After(config.HoldDuration):
		}
	}
	cancel()
	clients.Wait()
	sampler.Wait()
	completedAt := time.Now().UTC()
	return metrics.report(profile, config, startedAt, completedAt, abortReason), nil
}

func validateRunConfig(config runConfig, profile Profile) error {
	if strings.TrimSpace(config.URL) == "" {
		return fmt.Errorf("runtime URL is required")
	}
	if config.Connections < 1 {
		return fmt.Errorf("connections must be positive")
	}
	if config.SubscriptionsPerConnection < 0 || config.SubscriptionsPerConnection > len(profile.Subscriptions) {
		return fmt.Errorf("subscriptions per connection must be between 0 and %d", len(profile.Subscriptions))
	}
	if config.ConnectTimeout <= 0 || config.InitialTimeout <= 0 {
		return fmt.Errorf("connect and initial timeouts must be positive")
	}
	if config.MaximumDialConcurrency < 1 {
		return fmt.Errorf("maximum dial concurrency must be positive")
	}
	if config.SampleInterval < 0 {
		return fmt.Errorf("sample interval cannot be negative")
	}
	if config.AuthMode != authModeNone && config.AuthMode != authModeShared && config.AuthMode != authModeSynthetic {
		return fmt.Errorf("auth mode %q is unsupported", config.AuthMode)
	}
	if config.AuthMode == authModeShared && strings.TrimSpace(config.SharedToken) == "" {
		return fmt.Errorf("shared auth mode requires a token")
	}
	return nil
}

func runVirtualUser(ctx context.Context, config runConfig, profile Profile, userIndex int, metrics *runMetrics, dialSemaphore chan struct{}) {
	metrics.connectionAttempts.Add(1)
	select {
	case dialSemaphore <- struct{}{}:
	case <-ctx.Done():
		metrics.setupFinished.Add(1)
		return
	}
	connectStarted := time.Now()
	connection, _, err := dialRuntime(ctx, config, metrics)
	<-dialSemaphore
	if err != nil {
		metrics.recordSetupError("__connect__")
		metrics.setupFinished.Add(1)
		return
	}
	metrics.connectLatency.Observe(time.Since(connectStarted))
	metrics.connections.Add(1)
	defer connection.Close()
	connection.SetReadLimit(256 << 20)

	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-closed:
		}
	}()

	if err := connection.SetReadDeadline(time.Now().Add(config.ConnectTimeout)); err != nil {
		metrics.recordSetupError("__session__")
		metrics.setupFinished.Add(1)
		return
	}
	message, _, err := readEnvelope(connection, metrics)
	if err != nil || message.Type != "session.ready" {
		metrics.recordSetupError("__session__")
		metrics.setupFinished.Add(1)
		return
	}

	userID := fmt.Sprintf("gonvex-load-%06d", userIndex+1)
	if config.AuthMode != authModeNone && strings.TrimSpace(config.Variables["userId"]) != "" {
		userID = strings.TrimSpace(config.Variables["userId"])
	}
	if config.AuthMode != authModeNone {
		token := config.SharedToken
		if config.AuthMode == authModeSynthetic {
			token = syntheticJWT(userID)
		}
		authID := fmt.Sprintf("auth-%06d", userIndex+1)
		authStarted := time.Now()
		if err := writeEnvelope(connection, metrics, map[string]any{"type": "auth", "id": authID, "token": token, "tenant": config.Tenant, "project": config.Project}); err != nil {
			metrics.recordSetupError("__auth__")
			metrics.setupFinished.Add(1)
			return
		}
		authResult, _, err := readEnvelope(connection, metrics)
		if err != nil || authResult.Type != "auth.result" || authResult.ID != authID {
			metrics.recordSetupError("__auth__")
			metrics.setupFinished.Add(1)
			return
		}
		metrics.authLatency.Observe(time.Since(authStarted))
	}

	pending := make(map[string]*pendingSubscription, config.SubscriptionsPerConnection)
	type receivedEnvelope struct {
		message      serverEnvelope
		payloadBytes int
		err          error
	}
	// Read while subscriptions are being written. A browser's WebSocket event
	// loop does this concurrently; waiting until every write completes can
	// deadlock when initial snapshots fill both peers' socket buffers.
	received := make(chan receivedEnvelope, max(64, config.SubscriptionsPerConnection*2))
	_ = connection.SetReadDeadline(time.Now().Add(config.InitialTimeout))
	go func() {
		for {
			message, payloadBytes, err := readEnvelope(connection, metrics)
			received <- receivedEnvelope{message: message, payloadBytes: payloadBytes, err: err}
			if err != nil {
				return
			}
		}
	}()
	variables := cloneStrings(config.Variables)
	variables["tenant"] = config.Tenant
	variables["userId"] = userID
	for index := 0; index < config.SubscriptionsPerConnection; index++ {
		spec := profile.Subscriptions[index]
		args, err := spec.expandedArgs(variables)
		if err != nil {
			metrics.recordError(spec.Path, err.Error())
			continue
		}
		id := fmt.Sprintf("u%06d-s%03d", userIndex+1, index+1)
		sentAt := time.Now()
		pending[id] = &pendingSubscription{path: spec.Path, sentAt: sentAt}
		if err := writeEnvelope(connection, metrics, map[string]any{"type": "query.subscribe", "id": id, "path": spec.Path, "args": args}); err != nil {
			delete(pending, id)
			metrics.recordError(spec.Path, err.Error())
			continue
		}
		metrics.subscriptionsSent.Add(1)
	}
	metrics.setupFinished.Add(1)
	if len(pending) == 0 {
		<-ctx.Done()
		return
	}
	settled := 0
	for {
		envelope := <-received
		if envelope.err != nil {
			if ctx.Err() == nil && !errors.Is(envelope.err, net.ErrClosed) {
				metrics.unexpectedCloses.Add(1)
				for _, subscription := range pending {
					if !subscription.seen {
						metrics.recordError(subscription.path, "connection closed before initial result: "+envelope.err.Error())
					}
				}
			}
			return
		}
		message := envelope.message
		subscription := pending[message.ID]
		if subscription == nil {
			continue
		}
		switch message.Type {
		case "query.result":
			if message.Reason != "initial" || subscription.seen {
				continue
			}
			subscription.seen = true
			settled++
			serverDuration := time.Duration(0)
			if message.Trace != nil && message.Trace.ServerDurationMS > 0 {
				serverDuration = time.Duration(message.Trace.ServerDurationMS * float64(time.Millisecond))
			}
			metrics.recordInitial(subscription.path, time.Since(subscription.sentAt), serverDuration, envelope.payloadBytes)
		case "query.error":
			if subscription.seen {
				continue
			}
			subscription.seen = true
			settled++
			metrics.recordError(subscription.path, message.Error)
		}
		if settled == len(pending) {
			_ = connection.SetReadDeadline(time.Time{})
		}
	}
}

func dialRuntime(ctx context.Context, config runConfig, metrics *runMetrics) (*websocket.Conn, *http.Response, error) {
	target, err := websocketTarget(config.URL, config.Project, config.Tenant)
	if err != nil {
		return nil, nil, err
	}
	netDialer := &net.Dialer{Timeout: config.ConnectTimeout, KeepAlive: 30 * time.Second}
	dialer := websocket.Dialer{
		HandshakeTimeout:  config.ConnectTimeout,
		EnableCompression: config.Compression,
		NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, err := netDialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return newCountingConnWithTotals(connection, &metrics.wireBytesRead, &metrics.wireBytesWritten), nil
		},
	}
	return dialer.DialContext(ctx, target, nil)
}

func websocketTarget(rawURL, project, tenant string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("runtime URL must use http, https, ws, or wss")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/ws"
	}
	query := parsed.Query()
	if query.Get("project") == "" && strings.TrimSpace(project) != "" {
		query.Set("project", project)
	}
	if query.Get("tenant") == "" && strings.TrimSpace(tenant) != "" {
		query.Set("tenant", tenant)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func writeEnvelope(connection *websocket.Conn, metrics *runMetrics, message any) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	metrics.logicalBytesWrite.Add(uint64(len(payload)))
	return connection.WriteMessage(websocket.TextMessage, payload)
}

func readEnvelope(connection *websocket.Conn, metrics *runMetrics) (serverEnvelope, int, error) {
	_, payload, err := connection.ReadMessage()
	if err != nil {
		return serverEnvelope{}, 0, err
	}
	metrics.logicalBytesRead.Add(uint64(len(payload)))
	var message serverEnvelope
	if err := json.Unmarshal(payload, &message); err != nil {
		return serverEnvelope{}, len(payload), err
	}
	return message, len(payload), nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *runMetrics) report(profile Profile, config runConfig, startedAt, completedAt time.Time, abortReason string) RunReport {
	sent := m.subscriptionsSent.Load()
	errors := m.subscriptionErrors.Load()
	errorRate := float64(0)
	if sent > 0 {
		errorRate = float64(errors) / float64(sent)
	}
	logicalRead := m.logicalBytesRead.Load()
	wireRead := m.wireBytesRead.Load()
	compressionRatio := float64(0)
	if wireRead > 0 {
		compressionRatio = float64(logicalRead) / float64(wireRead)
	}
	paths := map[string]PathReport{}
	errorSamples := []ErrorSample{}
	m.pathMu.Lock()
	pathNames := make([]string, 0, len(m.paths))
	for path := range m.paths {
		pathNames = append(pathNames, path)
	}
	sort.Strings(pathNames)
	for _, path := range pathNames {
		metrics := m.paths[path]
		paths[path] = PathReport{
			InitialResults: metrics.initialResults,
			Errors:         metrics.errors,
			PayloadBytes:   metrics.payloadBytes,
			InitialLatency: histogramReport(metrics.initialLatency),
			ServerLatency:  histogramReport(metrics.serverLatency),
		}
	}
	for key, count := range m.errorSamples {
		path, message, _ := strings.Cut(key, "\x00")
		errorSamples = append(errorSamples, ErrorSample{Path: path, Message: message, Count: count})
	}
	sort.Slice(errorSamples, func(i, j int) bool {
		if errorSamples[i].Count == errorSamples[j].Count {
			return errorSamples[i].Path < errorSamples[j].Path
		}
		return errorSamples[i].Count > errorSamples[j].Count
	})
	m.pathMu.Unlock()
	m.resourceMu.Lock()
	samples := append([]ResourceSample(nil), m.samples...)
	if abortReason == "" {
		abortReason = m.abortReason
	}
	m.resourceMu.Unlock()
	return RunReport{
		Profile:     profile.Name,
		Target:      config.URL,
		Project:     config.Project,
		Tenant:      config.Tenant,
		StartedAt:   startedAt.Format(time.RFC3339Nano),
		CompletedAt: completedAt.Format(time.RFC3339Nano),
		DurationMS:  completedAt.Sub(startedAt).Milliseconds(),
		AbortReason: abortReason,
		Connections: ConnectionReport{
			Target:           uint64(config.Connections),
			Attempted:        m.connectionAttempts.Load(),
			Established:      m.connections.Load(),
			UnexpectedCloses: m.unexpectedCloses.Load(),
			SetupErrors:      m.setupErrors.Load(),
		},
		Subscriptions: SubscriptionReport{
			Target:         uint64(config.Connections * config.SubscriptionsPerConnection),
			Sent:           sent,
			InitialResults: m.initialResults.Load(),
			Errors:         errors,
			ErrorRate:      errorRate,
		},
		Wire: WireReport{
			BytesRead:            wireRead,
			BytesWritten:         m.wireBytesWritten.Load(),
			LogicalBytesRead:     logicalRead,
			LogicalBytesWritten:  m.logicalBytesWrite.Load(),
			ReadCompressionRatio: compressionRatio,
		},
		Latency: LatencyReport{
			Connect:       histogramReport(m.connectLatency),
			Auth:          histogramReport(m.authLatency),
			InitialResult: histogramReport(m.initialLatency),
			ServerQuery:   histogramReport(m.serverLatency),
		},
		Paths:        paths,
		Samples:      samples,
		ErrorSamples: errorSamples,
	}
}

func histogramReport(histogram *latencyHistogram) HistogramReport {
	if histogram == nil {
		return HistogramReport{}
	}
	toMilliseconds := func(duration time.Duration) float64 {
		return float64(duration) / float64(time.Millisecond)
	}
	return HistogramReport{
		Count:     histogram.Count(),
		AverageMS: toMilliseconds(histogram.Average()),
		P50MS:     toMilliseconds(histogram.Percentile(0.50)),
		P95MS:     toMilliseconds(histogram.Percentile(0.95)),
		P99MS:     toMilliseconds(histogram.Percentile(0.99)),
		MaxMS:     toMilliseconds(histogram.Max()),
	}
}
