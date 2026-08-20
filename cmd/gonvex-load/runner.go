package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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

func boolCapability(enabled bool) int {
	if enabled {
		return 1
	}
	return 0
}

type runConfig struct {
	URL                        string
	Project                    string
	Tenant                     string
	Tenants                    []string
	Users                      int
	Connections                int
	SubscriptionsPerConnection int
	RampDuration               time.Duration
	HoldDuration               time.Duration
	ReducerPath                string
	ReducerArgs                map[string]any
	ReducerRate                float64
	ConnectTimeout             time.Duration
	InitialTimeout             time.Duration
	AuthMode                   authMode
	SharedToken                string
	Compression                bool
	QueryResultBatch           bool
	MaximumDialConcurrency     int
	Variables                  map[string]string
	SampleInterval             time.Duration
	TargetPID                  int
	Safety                     safetyLimits
	RunID                      string
}

type runMetrics struct {
	connectionAttempts   atomic.Uint64
	connections          atomic.Uint64
	setupErrors          atomic.Uint64
	unexpectedCloses     atomic.Uint64
	setupFinished        atomic.Uint64
	subscriptionsSent    atomic.Uint64
	initialResults       atomic.Uint64
	subscriptionErrors   atomic.Uint64
	reducersSent         atomic.Uint64
	reducerResults       atomic.Uint64
	reducerErrors        atomic.Uint64
	invalidationResults  atomic.Uint64
	invalidationPatches  atomic.Uint64
	invalidationProgress atomic.Uint64
	invalidationBytes    atomic.Uint64
	logicalBytesRead     atomic.Uint64
	logicalBytesWrite    atomic.Uint64
	wireBytesRead        atomic.Uint64
	wireBytesWritten     atomic.Uint64

	connectLatency            *latencyHistogram
	authLatency               *latencyHistogram
	initialLatency            *latencyHistogram
	serverLatency             *latencyHistogram
	reducerLatency            *latencyHistogram
	reducerServerLatency      *latencyHistogram
	invalidationLatency       *latencyHistogram
	invalidationServerLatency *latencyHistogram
	tenantMu                  sync.Mutex
	tenants                   map[string]*tenantMetrics

	pathMu       sync.Mutex
	paths        map[string]*pathMetrics
	errorSamples map[string]uint64

	resourceMu  sync.Mutex
	samples     []ResourceSample
	abortReason string

	reducerPathMu sync.Mutex
	reducerPaths  map[string]*reducerPathMetrics
	propagation   *propagationAggregator
}

type reducerPathMetrics struct {
	sent      uint64
	succeeded uint64
	errors    uint64
}

type tenantMetrics struct {
	connectionAttempts        atomic.Uint64
	connections               atomic.Uint64
	setupErrors               atomic.Uint64
	unexpectedCloses          atomic.Uint64
	subscriptionsSent         atomic.Uint64
	initialResults            atomic.Uint64
	subscriptionErrors        atomic.Uint64
	reducersSent              atomic.Uint64
	reducerResults            atomic.Uint64
	reducerErrors             atomic.Uint64
	invalidationResults       atomic.Uint64
	invalidationPatches       atomic.Uint64
	invalidationProgress      atomic.Uint64
	invalidationBytes         atomic.Uint64
	connectLatency            *latencyHistogram
	authLatency               *latencyHistogram
	initialLatency            *latencyHistogram
	serverLatency             *latencyHistogram
	reducerLatency            *latencyHistogram
	reducerServerLatency      *latencyHistogram
	invalidationLatency       *latencyHistogram
	invalidationServerLatency *latencyHistogram
}

type pathMetrics struct {
	initialResults            uint64
	errors                    uint64
	payloadBytes              uint64
	invalidations             InvalidationReport
	initialLatency            *latencyHistogram
	serverLatency             *latencyHistogram
	invalidationLatency       *latencyHistogram
	invalidationServerLatency *latencyHistogram
	functionLatency           *latencyHistogram
}

type RunReport struct {
	Profile       string                  `json:"profile"`
	Target        string                  `json:"target"`
	Project       string                  `json:"project"`
	Tenant        string                  `json:"tenant"`
	Tenants       map[string]TenantReport `json:"tenants,omitempty"`
	Configuration RunConfigurationReport  `json:"configuration"`
	StartedAt     string                  `json:"startedAt"`
	CompletedAt   string                  `json:"completedAt"`
	DurationMS    int64                   `json:"durationMs"`
	AbortReason   string                  `json:"abortReason,omitempty"`
	Connections   ConnectionReport        `json:"connections"`
	Subscriptions SubscriptionReport      `json:"subscriptions"`
	Reducers      ReducerReport           `json:"reducers"`
	Invalidations InvalidationReport      `json:"invalidations"`
	TTLU          TTLUReport              `json:"ttlu"`
	Wire          WireReport              `json:"wire"`
	Latency       LatencyReport           `json:"latency"`
	Paths         map[string]PathReport   `json:"paths"`
	Samples       []ResourceSample        `json:"samples,omitempty"`
	ErrorSamples  []ErrorSample           `json:"errorSamples,omitempty"`
}

type RunConfigurationReport struct {
	AuthMode                   authMode `json:"authMode"`
	IdentityMode               string   `json:"identityMode"`
	Compression                bool     `json:"compression"`
	QueryResultBatch           bool     `json:"queryResultBatch"`
	TenantCount                int      `json:"tenantCount"`
	Users                      int      `json:"users"`
	Connections                int      `json:"connections"`
	ConnectionsPerUser         float64  `json:"connectionsPerUser"`
	SubscriptionsPerConnection int      `json:"subscriptionsPerConnection"`
	RampMS                     int64    `json:"rampMs"`
	HoldMS                     int64    `json:"holdMs"`
	ReducerPath                string   `json:"reducerPath,omitempty"`
	ReducerRatePerSec          float64  `json:"reducerRatePerSec,omitempty"`
}

type TenantReport struct {
	Connections   ConnectionReport   `json:"connections"`
	Subscriptions SubscriptionReport `json:"subscriptions"`
	Reducers      ReducerReport      `json:"reducers"`
	Invalidations InvalidationReport `json:"invalidations"`
	Latency       LatencyReport      `json:"latency"`
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

type ReducerReport struct {
	Path                string                       `json:"path,omitempty"`
	RequestedRatePerSec float64                      `json:"requestedRatePerSec"`
	AchievedRatePerSec  float64                      `json:"achievedRatePerSec"`
	Sent                uint64                       `json:"sent"`
	Succeeded           uint64                       `json:"succeeded"`
	Errors              uint64                       `json:"errors"`
	ErrorRate           float64                      `json:"errorRate"`
	ByPath              map[string]ReducerPathReport `json:"byPath,omitempty"`
}

type ReducerPathReport struct {
	RequestedRatePerSec float64 `json:"requestedRatePerSec"`
	AchievedRatePerSec  float64 `json:"achievedRatePerSec"`
	Sent                uint64  `json:"sent"`
	Succeeded           uint64  `json:"succeeded"`
	Errors              uint64  `json:"errors"`
}

type InvalidationReport struct {
	Messages     uint64 `json:"messages"`
	FullResults  uint64 `json:"fullResults"`
	Patches      uint64 `json:"patches"`
	Progress     uint64 `json:"progress"`
	PayloadBytes uint64 `json:"payloadBytes"`
}

type WireReport struct {
	BytesRead            uint64  `json:"bytesRead"`
	BytesWritten         uint64  `json:"bytesWritten"`
	LogicalBytesRead     uint64  `json:"logicalBytesRead"`
	LogicalBytesWritten  uint64  `json:"logicalBytesWritten"`
	ReadCompressionRatio float64 `json:"readCompressionRatio"`
}

type LatencyReport struct {
	Connect                    HistogramReport `json:"connect"`
	Auth                       HistogramReport `json:"auth"`
	InitialResult              HistogramReport `json:"initialResult"`
	ServerQuery                HistogramReport `json:"serverQuery"`
	Reducer                    HistogramReport `json:"reducer"`
	ReducerServer              HistogramReport `json:"reducerServer"`
	InvalidationChangeToClient HistogramReport `json:"invalidationChangeToClient"`
	InvalidationServerQuery    HistogramReport `json:"invalidationServerQuery"`
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
	InitialResults             uint64             `json:"initialResults"`
	Errors                     uint64             `json:"errors"`
	PayloadBytes               uint64             `json:"payloadBytes"`
	InitialLatency             HistogramReport    `json:"initialLatency"`
	ServerLatency              HistogramReport    `json:"serverLatency"`
	Invalidations              InvalidationReport `json:"invalidations"`
	InvalidationChangeToClient HistogramReport    `json:"invalidationChangeToClient"`
	InvalidationServerQuery    HistogramReport    `json:"invalidationServerQuery"`
	FunctionLatency            HistogramReport    `json:"functionLatency"`
}

type serverEnvelope struct {
	Type             string           `json:"type"`
	ID               string           `json:"id"`
	IDs              []string         `json:"ids"`
	QueryType        string           `json:"queryType"`
	Path             string           `json:"path"`
	Reason           string           `json:"reason"`
	Error            string           `json:"error"`
	Result           json.RawMessage  `json:"result"`
	OriginCommandIDs []string         `json:"originCommandIds"`
	Messages         []serverEnvelope `json:"messages"`
	Trace            *struct {
		ServerDurationMS           float64         `json:"serverDurationMs"`
		ServerReducerCommittedAtMS float64         `json:"serverReducerCommittedAtMs"`
		ServerChangeCommittedAtMS  float64         `json:"serverChangeCommittedAtMs"`
		QueryPerf                  json.RawMessage `json:"queryPerf"`
	} `json:"trace"`
}

type pendingSubscription struct {
	path   string
	sentAt time.Time
	seen   bool
}

type pendingReducer struct {
	path   string
	sentAt time.Time
}

func newRunMetrics() *runMetrics {
	return &runMetrics{
		connectLatency:            newLatencyHistogram(),
		authLatency:               newLatencyHistogram(),
		initialLatency:            newLatencyHistogram(),
		serverLatency:             newLatencyHistogram(),
		reducerLatency:            newLatencyHistogram(),
		reducerServerLatency:      newLatencyHistogram(),
		invalidationLatency:       newLatencyHistogram(),
		invalidationServerLatency: newLatencyHistogram(),
		paths:                     map[string]*pathMetrics{},
		errorSamples:              map[string]uint64{},
		tenants:                   map[string]*tenantMetrics{},
		reducerPaths:              map[string]*reducerPathMetrics{},
		propagation:               newPropagationAggregator(),
	}
}

func (m *runMetrics) tenant(tenant string) *tenantMetrics {
	m.tenantMu.Lock()
	defer m.tenantMu.Unlock()
	metrics := m.tenants[tenant]
	if metrics == nil {
		metrics = &tenantMetrics{
			connectLatency: newLatencyHistogram(), authLatency: newLatencyHistogram(),
			initialLatency: newLatencyHistogram(), serverLatency: newLatencyHistogram(),
			reducerLatency: newLatencyHistogram(), reducerServerLatency: newLatencyHistogram(),
			invalidationLatency: newLatencyHistogram(), invalidationServerLatency: newLatencyHistogram(),
		}
		m.tenants[tenant] = metrics
	}
	return metrics
}

func (m *runMetrics) recordReducer(tenant, path string, latency, serverDuration time.Duration) {
	m.reducerResults.Add(1)
	m.reducerPathMu.Lock()
	m.reducerPath(path).succeeded++
	m.reducerPathMu.Unlock()
	m.reducerLatency.Observe(latency)
	metrics := m.tenant(tenant)
	metrics.reducerResults.Add(1)
	metrics.reducerLatency.Observe(latency)
	if serverDuration > 0 {
		m.reducerServerLatency.Observe(serverDuration)
		metrics.reducerServerLatency.Observe(serverDuration)
	}
}

func (m *runMetrics) recordReducerError(tenant, path string) {
	m.reducerErrors.Add(1)
	m.tenant(tenant).reducerErrors.Add(1)
	m.reducerPathMu.Lock()
	m.reducerPath(path).errors++
	m.reducerPathMu.Unlock()
}

func (m *runMetrics) recordReducerSent(tenant, path string) {
	m.reducersSent.Add(1)
	m.tenant(tenant).reducersSent.Add(1)
	m.reducerPathMu.Lock()
	m.reducerPath(path).sent++
	m.reducerPathMu.Unlock()
}

// reducerPath requires reducerPathMu to be held.
func (m *runMetrics) reducerPath(path string) *reducerPathMetrics {
	metrics := m.reducerPaths[path]
	if metrics == nil {
		metrics = &reducerPathMetrics{}
		m.reducerPaths[path] = metrics
	}
	return metrics
}

func (m *runMetrics) recordInvalidation(tenant, path, kind string, latency, serverDuration, functionDuration time.Duration, payloadBytes int) {
	m.recordInvalidationN(tenant, path, kind, latency, serverDuration, functionDuration, payloadBytes, 1)
}

func (m *runMetrics) recordInvalidationN(tenant, path, kind string, latency, serverDuration, functionDuration time.Duration, payloadBytes int, count uint64) {
	tenantMetrics := m.tenant(tenant)
	switch kind {
	case "query.result":
		m.invalidationResults.Add(count)
		tenantMetrics.invalidationResults.Add(count)
	case "query.patch", "query.pagePatch", "query.objectPatch":
		m.invalidationPatches.Add(count)
		tenantMetrics.invalidationPatches.Add(count)
	case "query.progress":
		m.invalidationProgress.Add(count)
		tenantMetrics.invalidationProgress.Add(count)
	}
	m.invalidationBytes.Add(uint64(payloadBytes))
	tenantMetrics.invalidationBytes.Add(uint64(payloadBytes))
	if latency >= 0 {
		m.invalidationLatency.ObserveN(latency, count)
		tenantMetrics.invalidationLatency.ObserveN(latency, count)
	}
	if serverDuration > 0 {
		m.invalidationServerLatency.ObserveN(serverDuration, count)
		tenantMetrics.invalidationServerLatency.ObserveN(serverDuration, count)
	}
	pathMetrics := m.path(path)
	m.pathMu.Lock()
	pathMetrics.invalidations.Messages += count
	pathMetrics.invalidations.PayloadBytes += uint64(payloadBytes)
	switch kind {
	case "query.result":
		pathMetrics.invalidations.FullResults += count
	case "query.patch", "query.pagePatch", "query.objectPatch":
		pathMetrics.invalidations.Patches += count
	case "query.progress":
		pathMetrics.invalidations.Progress += count
	}
	m.pathMu.Unlock()
	if latency >= 0 {
		pathMetrics.invalidationLatency.ObserveN(latency, count)
	}
	if serverDuration > 0 {
		pathMetrics.invalidationServerLatency.ObserveN(serverDuration, count)
	}
	if functionDuration > 0 {
		pathMetrics.functionLatency.ObserveN(functionDuration, count)
	}
}

func (m *runMetrics) path(path string) *pathMetrics {
	m.pathMu.Lock()
	defer m.pathMu.Unlock()
	metrics := m.paths[path]
	if metrics == nil {
		metrics = &pathMetrics{initialLatency: newLatencyHistogram(), serverLatency: newLatencyHistogram(), invalidationLatency: newLatencyHistogram(), invalidationServerLatency: newLatencyHistogram(), functionLatency: newLatencyHistogram()}
		m.paths[path] = metrics
	}
	return metrics
}

func (m *runMetrics) recordInitial(tenant, path string, latency time.Duration, serverDuration time.Duration, payloadBytes int) {
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
	tenantMetrics := m.tenant(tenant)
	tenantMetrics.initialResults.Add(1)
	tenantMetrics.initialLatency.Observe(latency)
	if serverDuration > 0 {
		tenantMetrics.serverLatency.Observe(serverDuration)
	}
}

func (m *runMetrics) recordError(tenant, path string, message string) {
	m.subscriptionErrors.Add(1)
	m.tenant(tenant).subscriptionErrors.Add(1)
	pathMetrics := m.path(path)
	m.pathMu.Lock()
	pathMetrics.errors++
	if len(m.errorSamples) < 20 || m.errorSamples[path+"\x00"+message] > 0 {
		m.errorSamples[path+"\x00"+message]++
	}
	m.pathMu.Unlock()
}

func (m *runMetrics) recordSetupError(tenant, path string) {
	m.setupErrors.Add(1)
	m.tenant(tenant).setupErrors.Add(1)
	pathMetrics := m.path(path)
	m.pathMu.Lock()
	pathMetrics.errors++
	m.pathMu.Unlock()
}

func (m *runMetrics) recordErrorSample(path, message string) {
	m.pathMu.Lock()
	defer m.pathMu.Unlock()
	if len(m.errorSamples) < 20 || m.errorSamples[path+"\x00"+message] > 0 {
		m.errorSamples[path+"\x00"+message]++
	}
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
	if config.Users == 0 {
		// Preserve the original programmatic API: one user per connection.
		config.Users = config.Connections
	}
	if err := validateRunConfig(config, profile); err != nil {
		return RunReport{}, err
	}
	plans := makeSessionPlans(config, profile)
	startedAt := time.Now().UTC()
	if config.RunID == "" {
		config.RunID = "r" + strconv.FormatInt(startedAt.UnixNano(), 36)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := newRunMetrics()
	reducerStart := make(chan struct{})
	reducerStop := make(chan struct{})
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
	for index, plan := range plans {
		if index > 0 && config.RampDuration > 0 {
			interval := config.RampDuration / time.Duration(len(plans))
			if err := waitContext(runCtx, interval); err != nil {
				break launchLoop
			}
		}
		clients.Add(1)
		launched++
		go func(plan sessionPlan) {
			defer clients.Done()
			runVirtualUser(runCtx, config, profile, plan, metrics, dialSemaphore, reducerStart, reducerStop)
		}(plan)
	}

	expectedSubscriptions := uint64(0)
	for _, plan := range plans {
		expectedSubscriptions += uint64(plan.subscriptionCount)
	}
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
		close(reducerStart)
		select {
		case <-runCtx.Done():
			abortReason = metrics.abort()
			if abortReason == "" {
				abortReason = runCtx.Err().Error()
			}
		case <-time.After(config.HoldDuration):
		}
		close(reducerStop)
		if abortReason == "" && requestedReducerRate(config, profile) > 0 {
			settleDeadline := time.NewTimer(config.ConnectTimeout)
			settleTicker := time.NewTicker(time.Millisecond)
		settleLoop:
			for metrics.reducerResults.Load()+metrics.reducerErrors.Load() < metrics.reducersSent.Load() {
				select {
				case <-runCtx.Done():
					break settleLoop
				case <-settleDeadline.C:
					abortReason = "reducer result timeout"
					break settleLoop
				case <-settleTicker.C:
				}
			}
			settleTicker.Stop()
			if !settleDeadline.Stop() {
				select {
				case <-settleDeadline.C:
				default:
				}
			}
			// Reducer acknowledgements can precede the last reactive delivery.
			// The protocol does not expose an expected recipient count, so retain
			// every socket for the full operation timeout as a bounded drain window.
			_ = waitContext(runCtx, config.ConnectTimeout)
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
	if config.Users < 1 {
		return fmt.Errorf("users must be positive")
	}
	if config.Connections < config.Users {
		return fmt.Errorf("connections (%d) must be at least users (%d)", config.Connections, config.Users)
	}
	if config.Connections < 1 {
		return fmt.Errorf("connections must be positive")
	}
	if len(config.tenantList()) == 0 {
		return fmt.Errorf("at least one tenant is required")
	}
	availableSubscriptions := len(profile.expandedSubscriptions())
	if config.SubscriptionsPerConnection < -1 || config.SubscriptionsPerConnection > availableSubscriptions {
		return fmt.Errorf("subscriptions per connection must be -1 or between 0 and %d", availableSubscriptions)
	}
	validationVariables := profile.sessionVariables(0, config.Variables)
	for name, value := range map[string]any{
		"tenant": config.tenantList()[0], "userId": "gonvex-load-000001",
		"sequence": "1", "commandId": "u000001-c01-r000001",
	} {
		validationVariables[name] = value
	}
	for _, spec := range profile.expandedSubscriptions() {
		if _, err := spec.expandedArgs(validationVariables); err != nil {
			return fmt.Errorf("subscription %s args: %w", spec.Path, err)
		}
	}
	for _, spec := range profile.Reducers {
		if _, err := spec.expandedArgs(validationVariables); err != nil {
			return fmt.Errorf("reducer %s args: %w", spec.Path, err)
		}
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
	if config.ReducerRate < 0 {
		return fmt.Errorf("reducer rate cannot be negative")
	}
	if config.ReducerRate > 0 {
		if !functionPathPattern.MatchString(strings.TrimSpace(config.ReducerPath)) {
			return fmt.Errorf("reducer rate requires a valid reducer path")
		}
		if config.ReducerArgs == nil {
			return fmt.Errorf("reducer rate requires reducer args")
		}
		if config.HoldDuration <= 0 {
			return fmt.Errorf("reducer rate requires a positive hold duration")
		}
	}
	if config.AuthMode != authModeNone && config.AuthMode != authModeShared && config.AuthMode != authModeSynthetic {
		return fmt.Errorf("auth mode %q is unsupported", config.AuthMode)
	}
	if config.AuthMode == authModeShared && strings.TrimSpace(config.SharedToken) == "" {
		return fmt.Errorf("shared auth mode requires a token")
	}
	return nil
}

type sessionPlan struct {
	userIndex         int
	connectionIndex   int
	subscriptionCount int
}

func makeSessionPlans(config runConfig, profile Profile) []sessionPlan {
	plans := make([]sessionPlan, 0, config.Connections)
	connectionsByUser := make([]int, config.Users)
	for index := 0; index < config.Connections; index++ {
		userIndex := index % config.Users
		plan := sessionPlan{
			userIndex: userIndex, connectionIndex: connectionsByUser[userIndex],
			subscriptionCount: profile.subscriptionCount(index, config.SubscriptionsPerConnection),
		}
		connectionsByUser[userIndex]++
		plans = append(plans, plan)
	}
	return plans
}

func (config runConfig) tenantList() []string {
	if len(config.Tenants) > 0 {
		return config.Tenants
	}
	if tenant := strings.TrimSpace(config.Tenant); tenant != "" {
		return []string{tenant}
	}
	return nil
}

func (config runConfig) tenantForUser(userIndex int) string {
	tenants := config.tenantList()
	return tenants[userIndex%len(tenants)]
}

func runVirtualUser(ctx context.Context, config runConfig, profile Profile, plan sessionPlan, metrics *runMetrics, dialSemaphore chan struct{}, reducerStart, reducerStop <-chan struct{}) {
	userIndex := plan.userIndex
	tenant := config.tenantForUser(userIndex)
	tenantMetrics := metrics.tenant(tenant)
	metrics.connectionAttempts.Add(1)
	tenantMetrics.connectionAttempts.Add(1)
	select {
	case dialSemaphore <- struct{}{}:
	case <-ctx.Done():
		metrics.setupFinished.Add(1)
		return
	}
	connectStarted := time.Now()
	connection, _, err := dialRuntime(ctx, config, tenant, metrics)
	<-dialSemaphore
	if err != nil {
		metrics.recordSetupError(tenant, "__connect__")
		metrics.setupFinished.Add(1)
		return
	}
	metrics.connectLatency.Observe(time.Since(connectStarted))
	tenantMetrics.connectLatency.Observe(time.Since(connectStarted))
	metrics.connections.Add(1)
	tenantMetrics.connections.Add(1)
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
		metrics.recordSetupError(tenant, "__session__")
		metrics.setupFinished.Add(1)
		return
	}
	message, _, err := readEnvelope(connection, metrics)
	if err != nil || message.Type != "session.ready" {
		metrics.recordSetupError(tenant, "__session__")
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
		authID := fmt.Sprintf("auth-%06d-%02d", userIndex+1, plan.connectionIndex+1)
		authStarted := time.Now()
		if err := writeEnvelope(connection, metrics, map[string]any{
			"type": "auth", "id": authID, "token": token, "tenant": tenant, "project": config.Project,
			"capabilities": map[string]any{"queryPagePatch": 1, "queryObjectPatch": 1, "queryOrderDelta": 1, "queryFanout": 1, "queryResultBatch": boolCapability(config.QueryResultBatch)},
		}); err != nil {
			metrics.recordSetupError(tenant, "__auth__")
			metrics.setupFinished.Add(1)
			return
		}
		authResult, _, err := readEnvelope(connection, metrics)
		if err != nil || authResult.Type != "auth.result" || authResult.ID != authID {
			metrics.recordSetupError(tenant, "__auth__")
			metrics.setupFinished.Add(1)
			return
		}
		metrics.authLatency.Observe(time.Since(authStarted))
		tenantMetrics.authLatency.Observe(time.Since(authStarted))
	}

	pending := make(map[string]*pendingSubscription, plan.subscriptionCount)
	pendingReducers := map[string]pendingReducer{}
	var pendingReducerMu sync.Mutex
	type receivedEnvelope struct {
		message      serverEnvelope
		payloadBytes int
		receivedAt   time.Time
		err          error
	}
	// Read while subscriptions are being written. A browser's WebSocket event
	// loop does this concurrently; waiting until every write completes can
	// deadlock when initial snapshots fill both peers' socket buffers.
	received := make(chan receivedEnvelope, max(64, plan.subscriptionCount*2))
	var setupSettled atomic.Bool
	_ = connection.SetReadDeadline(time.Now().Add(config.InitialTimeout))
	go func() {
		for {
			message, payloadBytes, err := readEnvelope(connection, metrics)
			receivedAt := time.Now()
			if err != nil {
				received <- receivedEnvelope{message: message, payloadBytes: payloadBytes, receivedAt: receivedAt, err: err}
				return
			}
			queue := []serverEnvelope{message}
			if message.Type == "query.batch" {
				queue = message.Messages
			}
			first := true
			for _, nested := range queue {
				if nested.Type == "query.fanout" && len(nested.IDs) > 0 {
					if setupSettled.Load() && nested.Reason == "invalidate" {
						copyBytes := 0
						if first {
							copyBytes, first = payloadBytes, false
						}
						received <- receivedEnvelope{message: nested, payloadBytes: copyBytes, receivedAt: receivedAt}
						continue
					}
					for _, id := range nested.IDs {
						copy := nested
						copy.Type, copy.ID, copy.IDs, copy.QueryType = nested.QueryType, id, nil, ""
						copyBytes := 0
						if first {
							copyBytes, first = payloadBytes, false
						}
						received <- receivedEnvelope{message: copy, payloadBytes: copyBytes, receivedAt: receivedAt}
					}
					continue
				}
				copyBytes := 0
				if first {
					copyBytes, first = payloadBytes, false
				}
				received <- receivedEnvelope{message: nested, payloadBytes: copyBytes, receivedAt: receivedAt}
			}
		}
	}()
	variables := profile.sessionVariables(userIndex, config.Variables)
	variables["tenant"] = tenant
	variables["userId"] = userID
	subscriptions := profile.expandedSubscriptions()
	for index := 0; index < plan.subscriptionCount; index++ {
		spec := subscriptions[index]
		args, err := spec.expandedArgs(variables)
		if err != nil {
			metrics.recordError(tenant, spec.Path, err.Error())
			continue
		}
		id := fmt.Sprintf("u%06d-s%03d", userIndex+1, index+1)
		if plan.connectionIndex > 0 {
			id = fmt.Sprintf("u%06d-c%02d-s%03d", userIndex+1, plan.connectionIndex+1, index+1)
		}
		sentAt := time.Now()
		pending[id] = &pendingSubscription{path: spec.Path, sentAt: sentAt}
		if err := writeEnvelope(connection, metrics, map[string]any{"type": "query.subscribe", "id": id, "path": spec.Path, "args": args}); err != nil {
			delete(pending, id)
			metrics.recordError(tenant, spec.Path, err.Error())
			continue
		}
		metrics.subscriptionsSent.Add(1)
		tenantMetrics.subscriptionsSent.Add(1)
	}
	metrics.setupFinished.Add(1)
	settled := 0
	reducerWriterStarted := false
	for {
		if settled == len(pending) && !reducerWriterStarted && connectionHasReducers(config, profile, plan) {
			reducerWriterStarted = true
			go runReducerWriter(ctx, reducerStart, reducerStop, connection, config, profile, tenant, userID, plan, metrics, &pendingReducerMu, pendingReducers)
		}
		envelope := <-received
		if envelope.err != nil {
			if ctx.Err() == nil && !errors.Is(envelope.err, net.ErrClosed) {
				metrics.unexpectedCloses.Add(1)
				tenantMetrics.unexpectedCloses.Add(1)
				for _, subscription := range pending {
					if !subscription.seen {
						metrics.recordError(tenant, subscription.path, "connection closed before initial result: "+envelope.err.Error())
					}
				}
			}
			return
		}
		message := envelope.message
		if message.Type == "query.fanout" && message.Reason == "invalidate" && len(message.IDs) > 0 {
			receivedAt := envelope.receivedAt
			if receivedAt.IsZero() {
				receivedAt = time.Now()
			}
			delay := changeToClientDuration(message, receivedAt)
			metrics.recordInvalidationN(tenant, message.Path, message.QueryType, delay, traceDuration(message), traceFunctionDuration(message), envelope.payloadBytes, uint64(len(message.IDs)))
			if message.Trace != nil {
				clientID := fmt.Sprintf("u%06d-c%02d", userIndex+1, plan.connectionIndex+1)
				if len(message.OriginCommandIDs) > 0 {
					metrics.propagation.RecordOriginCommandDeliveries(message.OriginCommandIDs, clientID, float64(receivedAt.UnixNano())/float64(time.Millisecond))
				} else {
					metrics.propagation.RecordDelivery(message.Trace.ServerChangeCommittedAtMS, clientID, delay)
				}
			}
			continue
		}
		pendingReducerMu.Lock()
		reducer, isReducer := pendingReducers[message.ID]
		if isReducer && (message.Type == "reducer.result" || message.Type == "reducer.error") {
			delete(pendingReducers, message.ID)
		}
		pendingReducerMu.Unlock()
		if isReducer {
			if message.Type == "reducer.result" {
				serverDuration := traceDuration(message)
				metrics.recordReducer(tenant, reducer.path, time.Since(reducer.sentAt), serverDuration)
				if message.Trace != nil {
					metrics.propagation.RecordCommandCommit(message.ID, message.Trace.ServerReducerCommittedAtMS, reducer.path)
				}
			} else if message.Type == "reducer.error" {
				metrics.recordReducerError(tenant, reducer.path)
				metrics.recordErrorSample(reducer.path, message.Error)
			}
			continue
		}
		subscription := pending[message.ID]
		if subscription == nil {
			continue
		}
		if message.Reason == "invalidate" && (message.Type == "query.result" || message.Type == "query.patch" || message.Type == "query.pagePatch" || message.Type == "query.objectPatch" || message.Type == "query.progress") {
			receivedAt := envelope.receivedAt
			if receivedAt.IsZero() {
				receivedAt = time.Now()
			}
			delay := changeToClientDuration(message, receivedAt)
			metrics.recordInvalidation(tenant, subscription.path, message.Type, delay, traceDuration(message), traceFunctionDuration(message), envelope.payloadBytes)
			if message.Trace != nil {
				clientID := fmt.Sprintf("u%06d-c%02d", userIndex+1, plan.connectionIndex+1)
				if len(message.OriginCommandIDs) > 0 {
					metrics.propagation.RecordOriginCommandDeliveries(message.OriginCommandIDs, clientID, float64(receivedAt.UnixNano())/float64(time.Millisecond))
				} else {
					metrics.propagation.RecordDelivery(message.Trace.ServerChangeCommittedAtMS, clientID, delay)
				}
			}
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
			metrics.recordInitial(tenant, subscription.path, time.Since(subscription.sentAt), serverDuration, envelope.payloadBytes)
		case "query.error":
			if subscription.seen {
				continue
			}
			subscription.seen = true
			settled++
			metrics.recordError(tenant, subscription.path, message.Error)
		}
		if settled == len(pending) {
			setupSettled.Store(true)
			_ = connection.SetReadDeadline(time.Time{})
		}
	}
}

type reducerSchedule struct {
	spec     ReducerSpec
	interval time.Duration
	next     time.Time
}

func connectionHasReducers(config runConfig, profile Profile, plan sessionPlan) bool {
	return config.ReducerRate > 0 || (plan.connectionIndex == 0 && len(profile.Reducers) > 0)
}

func requestedReducerRate(config runConfig, profile Profile) float64 {
	rate := config.ReducerRate
	for _, spec := range profile.Reducers {
		rate += spec.RatePerMinute / 60
		rate += float64(activeUserCount(config.Users, spec.ActiveUsers)) * spec.RatePerUserPerMinute / 60
	}
	return rate
}

func requestedReducerRates(config runConfig, profile Profile) map[string]float64 {
	rates := map[string]float64{}
	if config.ReducerRate > 0 {
		rates[config.ReducerPath] += config.ReducerRate
	}
	for _, spec := range profile.Reducers {
		rates[spec.Path] += spec.RatePerMinute / 60
		rates[spec.Path] += float64(activeUserCount(config.Users, spec.ActiveUsers)) * spec.RatePerUserPerMinute / 60
	}
	return rates
}

func activeUserCount(users int, fraction float64) int {
	return int(math.Round(float64(users) * fraction))
}

func reducerSchedules(config runConfig, profile Profile, plan sessionPlan, now time.Time) []reducerSchedule {
	schedules := []reducerSchedule{}
	if config.ReducerRate > 0 {
		rate := config.ReducerRate / float64(config.Connections)
		schedules = append(schedules, reducerSchedule{
			spec:     ReducerSpec{Path: config.ReducerPath, Args: config.ReducerArgs},
			interval: time.Duration(float64(time.Second) / rate), next: now,
		})
	}
	if plan.connectionIndex != 0 {
		return schedules
	}
	for reducerIndex, spec := range profile.Reducers {
		rate := spec.RatePerMinute / 60 / float64(config.Users)
		if plan.userIndex < activeUserCount(config.Users, spec.ActiveUsers) {
			rate += spec.RatePerUserPerMinute / 60
		}
		if rate <= 0 {
			continue
		}
		interval := time.Duration(float64(time.Second) / rate)
		phaseParts := 10000
		phase := deterministicIndex(fmt.Sprintf("%s:phase:%d", spec.Path, reducerIndex), plan.userIndex, phaseParts)
		next := now.Add(time.Duration(float64(interval) * float64(phase) / float64(phaseParts)))
		schedules = append(schedules, reducerSchedule{spec: spec, interval: interval, next: next})
	}
	return schedules
}

func runReducerWriter(ctx context.Context, start, stop <-chan struct{}, connection *websocket.Conn, config runConfig, profile Profile, tenant, userID string, plan sessionPlan, metrics *runMetrics, pendingMu *sync.Mutex, pending map[string]pendingReducer) {
	select {
	case <-ctx.Done():
		return
	case <-start:
	}
	schedules := reducerSchedules(config, profile, plan, time.Now())
	if len(schedules) == 0 {
		return
	}
	sequence := 0
	for {
		nextIndex := 0
		for index := range schedules {
			if schedules[index].next.Before(schedules[nextIndex].next) {
				nextIndex = index
			}
		}
		timer := time.NewTimer(time.Until(schedules[nextIndex].next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		sequence++
		variables := profile.sessionVariables(plan.userIndex, config.Variables)
		variables["tenant"] = tenant
		variables["userId"] = userID
		variables["sequence"] = fmt.Sprintf("%d", sequence)
		variables["commandId"] = fmt.Sprintf("%s-u%06d-c%02d-r%06d", config.RunID, plan.userIndex+1, plan.connectionIndex+1, sequence)
		schedule := &schedules[nextIndex]
		args, err := schedule.spec.expandedArgs(variables)
		if err != nil {
			metrics.recordReducerError(tenant, schedule.spec.Path)
			metrics.recordErrorSample(schedule.spec.Path, err.Error())
			schedule.next = schedule.next.Add(schedule.interval)
			continue
		}
		id := variables["commandId"].(string)
		sentAt := time.Now()
		pendingMu.Lock()
		pending[id] = pendingReducer{path: schedule.spec.Path, sentAt: sentAt}
		pendingMu.Unlock()
		message := map[string]any{
			"type": "reducer.call", "id": id, "path": schedule.spec.Path, "args": args,
			"trace": map[string]any{"clientSentAtMs": float64(sentAt.UnixNano()) / float64(time.Millisecond)},
		}
		if err := writeEnvelope(connection, metrics, message); err != nil {
			pendingMu.Lock()
			delete(pending, id)
			pendingMu.Unlock()
			if ctx.Err() == nil {
				metrics.recordReducerError(tenant, schedule.spec.Path)
			}
			return
		}
		metrics.recordReducerSent(tenant, schedule.spec.Path)
		schedule.next = schedule.next.Add(schedule.interval)
		if schedule.next.Before(time.Now()) {
			schedule.next = time.Now().Add(schedule.interval)
		}
	}
}

func traceDuration(message serverEnvelope) time.Duration {
	if message.Trace == nil || message.Trace.ServerDurationMS <= 0 {
		return 0
	}
	return time.Duration(message.Trace.ServerDurationMS * float64(time.Millisecond))
}

func traceFunctionDuration(message serverEnvelope) time.Duration {
	if message.Trace == nil || len(message.Trace.QueryPerf) == 0 {
		return 0
	}
	var perf struct {
		ServerFunctionDurationMS float64 `json:"serverFunctionDurationMs"`
	}
	if json.Unmarshal(message.Trace.QueryPerf, &perf) != nil || perf.ServerFunctionDurationMS <= 0 {
		return 0
	}
	return time.Duration(perf.ServerFunctionDurationMS * float64(time.Millisecond))
}

func changeToClientDuration(message serverEnvelope, receivedAt time.Time) time.Duration {
	if message.Trace == nil || message.Trace.ServerChangeCommittedAtMS <= 0 {
		return -1
	}
	committedAt := time.Unix(0, int64(message.Trace.ServerChangeCommittedAtMS*float64(time.Millisecond)))
	return receivedAt.Sub(committedAt)
}

func dialRuntime(ctx context.Context, config runConfig, tenant string, metrics *runMetrics) (*websocket.Conn, *http.Response, error) {
	target, err := websocketTarget(config.URL, config.Project, tenant)
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
	plans := makeSessionPlans(config, profile)
	totalSubscriptionTarget := 0
	for _, plan := range plans {
		totalSubscriptionTarget += plan.subscriptionCount
	}
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
	reducerSent := m.reducersSent.Load()
	reducerErrors := m.reducerErrors.Load()
	reducerErrorRate := float64(0)
	if reducerSent > 0 {
		reducerErrorRate = float64(reducerErrors) / float64(reducerSent)
	}
	holdSeconds := config.HoldDuration.Seconds()
	requestedRates := requestedReducerRates(config, profile)
	reducerByPath := map[string]ReducerPathReport{}
	m.reducerPathMu.Lock()
	for path, metrics := range m.reducerPaths {
		achieved := float64(0)
		if holdSeconds > 0 {
			achieved = float64(metrics.succeeded) / holdSeconds
		}
		reducerByPath[path] = ReducerPathReport{
			RequestedRatePerSec: requestedRates[path], AchievedRatePerSec: achieved,
			Sent: metrics.sent, Succeeded: metrics.succeeded, Errors: metrics.errors,
		}
	}
	for path, rate := range requestedRates {
		if _, ok := reducerByPath[path]; !ok {
			reducerByPath[path] = ReducerPathReport{RequestedRatePerSec: rate}
		}
	}
	m.reducerPathMu.Unlock()
	achievedReducerRate := float64(0)
	if holdSeconds > 0 {
		achievedReducerRate = float64(m.reducerResults.Load()) / holdSeconds
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
			InitialResults:             metrics.initialResults,
			Errors:                     metrics.errors,
			PayloadBytes:               metrics.payloadBytes,
			InitialLatency:             histogramReport(metrics.initialLatency),
			ServerLatency:              histogramReport(metrics.serverLatency),
			Invalidations:              metrics.invalidations,
			InvalidationChangeToClient: histogramReport(metrics.invalidationLatency),
			InvalidationServerQuery:    histogramReport(metrics.invalidationServerLatency),
			FunctionLatency:            histogramReport(metrics.functionLatency),
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
	tenantReports := map[string]TenantReport{}
	tenantNames := config.tenantList()
	for _, tenant := range tenantNames {
		metrics := m.tenant(tenant)
		targetConnections := 0
		targetSubscriptions := 0
		for _, plan := range plans {
			if config.tenantForUser(plan.userIndex) == tenant {
				targetConnections++
				targetSubscriptions += plan.subscriptionCount
			}
		}
		tenantSent := metrics.subscriptionsSent.Load()
		tenantErrors := metrics.subscriptionErrors.Load()
		tenantErrorRate := float64(0)
		if tenantSent > 0 {
			tenantErrorRate = float64(tenantErrors) / float64(tenantSent)
		}
		tenantReducerSent := metrics.reducersSent.Load()
		tenantReducerErrors := metrics.reducerErrors.Load()
		tenantReducerErrorRate := float64(0)
		if tenantReducerSent > 0 {
			tenantReducerErrorRate = float64(tenantReducerErrors) / float64(tenantReducerSent)
		}
		tenantReports[tenant] = TenantReport{
			Connections: ConnectionReport{
				Target: uint64(targetConnections), Attempted: metrics.connectionAttempts.Load(),
				Established: metrics.connections.Load(), UnexpectedCloses: metrics.unexpectedCloses.Load(),
				SetupErrors: metrics.setupErrors.Load(),
			},
			Subscriptions: SubscriptionReport{
				Target: uint64(targetSubscriptions), Sent: tenantSent,
				InitialResults: metrics.initialResults.Load(), Errors: tenantErrors, ErrorRate: tenantErrorRate,
			},
			Reducers: ReducerReport{
				Path:                config.ReducerPath,
				RequestedRatePerSec: requestedReducerRate(config, profile) / float64(len(tenantNames)),
				Sent:                tenantReducerSent, Succeeded: metrics.reducerResults.Load(), Errors: tenantReducerErrors,
				ErrorRate: tenantReducerErrorRate,
			},
			Invalidations: InvalidationReport{
				Messages:    metrics.invalidationResults.Load() + metrics.invalidationPatches.Load() + metrics.invalidationProgress.Load(),
				FullResults: metrics.invalidationResults.Load(), Patches: metrics.invalidationPatches.Load(),
				Progress: metrics.invalidationProgress.Load(), PayloadBytes: metrics.invalidationBytes.Load(),
			},
			Latency: LatencyReport{
				Connect: histogramReport(metrics.connectLatency), Auth: histogramReport(metrics.authLatency),
				InitialResult: histogramReport(metrics.initialLatency), ServerQuery: histogramReport(metrics.serverLatency),
				Reducer: histogramReport(metrics.reducerLatency), ReducerServer: histogramReport(metrics.reducerServerLatency),
				InvalidationChangeToClient: histogramReport(metrics.invalidationLatency),
				InvalidationServerQuery:    histogramReport(metrics.invalidationServerLatency),
			},
		}
	}
	primaryTenant := ""
	if len(tenantNames) > 0 {
		primaryTenant = tenantNames[0]
	}
	identityMode := "anonymous"
	if config.AuthMode == authModeShared || strings.TrimSpace(config.Variables["userId"]) != "" {
		identityMode = "shared"
	} else if config.AuthMode == authModeSynthetic {
		identityMode = "distinct"
	}
	return RunReport{
		Profile: profile.Name,
		Target:  config.URL,
		Project: config.Project,
		Tenant:  primaryTenant,
		Tenants: tenantReports,
		Configuration: RunConfigurationReport{
			AuthMode: config.AuthMode, IdentityMode: identityMode, Compression: config.Compression, QueryResultBatch: config.QueryResultBatch,
			TenantCount: len(tenantNames), Users: config.Users, Connections: config.Connections,
			ConnectionsPerUser:         float64(config.Connections) / float64(config.Users),
			SubscriptionsPerConnection: config.SubscriptionsPerConnection,
			RampMS:                     config.RampDuration.Milliseconds(), HoldMS: config.HoldDuration.Milliseconds(),
			ReducerPath: config.ReducerPath, ReducerRatePerSec: config.ReducerRate,
		},
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
			Target:         uint64(totalSubscriptionTarget),
			Sent:           sent,
			InitialResults: m.initialResults.Load(),
			Errors:         errors,
			ErrorRate:      errorRate,
		},
		Reducers: ReducerReport{
			Path:                config.ReducerPath,
			RequestedRatePerSec: requestedReducerRate(config, profile), AchievedRatePerSec: achievedReducerRate,
			Sent: reducerSent, Succeeded: m.reducerResults.Load(), Errors: reducerErrors,
			ErrorRate: reducerErrorRate, ByPath: reducerByPath,
		},
		Invalidations: InvalidationReport{
			Messages:    m.invalidationResults.Load() + m.invalidationPatches.Load() + m.invalidationProgress.Load(),
			FullResults: m.invalidationResults.Load(), Patches: m.invalidationPatches.Load(),
			Progress: m.invalidationProgress.Load(), PayloadBytes: m.invalidationBytes.Load(),
		},
		TTLU: m.propagation.Report(),
		Wire: WireReport{
			BytesRead:            wireRead,
			BytesWritten:         m.wireBytesWritten.Load(),
			LogicalBytesRead:     logicalRead,
			LogicalBytesWritten:  m.logicalBytesWrite.Load(),
			ReadCompressionRatio: compressionRatio,
		},
		Latency: LatencyReport{
			Connect:                    histogramReport(m.connectLatency),
			Auth:                       histogramReport(m.authLatency),
			InitialResult:              histogramReport(m.initialLatency),
			ServerQuery:                histogramReport(m.serverLatency),
			Reducer:                    histogramReport(m.reducerLatency),
			ReducerServer:              histogramReport(m.reducerServerLatency),
			InvalidationChangeToClient: histogramReport(m.invalidationLatency),
			InvalidationServerQuery:    histogramReport(m.invalidationServerLatency),
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
