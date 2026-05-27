package server

import (
	"sort"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
)

const (
	metricsBucketWidth = 30 * time.Second
	metricsBucketCount = 24
	metricsLogLimit    = 100
)

type runtimeMetrics struct {
	mu        sync.Mutex
	functions map[string]*functionMetrics
	cache     cacheMetrics
	logs      []runtimeLogEntry
}

type functionMetrics struct {
	Kind            string
	Calls           int64
	Errors          int64
	TotalDurationMS float64
	LastDurationMS  float64
	LastCalledAt    time.Time
	Buckets         map[int64]*functionMetricsBucket
}

type functionMetricsBucket struct {
	Calls           int64
	Errors          int64
	TotalDurationMS float64
}

type cacheMetrics struct {
	Hits     int64
	Misses   int64
	Bypasses int64
	Buckets  map[int64]*cacheMetricsBucket
}

type cacheMetricsBucket struct {
	Hits     int64
	Misses   int64
	Bypasses int64
}

type runtimeLogEntry struct {
	Time       string  `json:"time"`
	Path       string  `json:"path"`
	Kind       string  `json:"kind"`
	Outcome    string  `json:"outcome"`
	DurationMS float64 `json:"durationMs"`
	Error      string  `json:"error,omitempty"`
	Cache      string  `json:"cache,omitempty"`
}

type runtimeMetricsSnapshot struct {
	GeneratedAt string                            `json:"generatedAt"`
	Functions   map[string]functionMetricSnapshot `json:"functions"`
	Cache       cacheMetricSnapshot               `json:"cache"`
	WebSocket   websocketMetricSnapshot           `json:"websocket"`
	Logs        []runtimeLogEntry                 `json:"logs"`
}

type functionMetricSnapshot struct {
	Kind              string                `json:"kind"`
	Calls             int64                 `json:"calls"`
	Errors            int64                 `json:"errors"`
	TotalDurationMS   float64               `json:"totalDurationMs"`
	AverageDurationMS float64               `json:"averageDurationMs"`
	LastDurationMS    float64               `json:"lastDurationMs"`
	LastCalledAt      string                `json:"lastCalledAt,omitempty"`
	Series            []functionMetricPoint `json:"series"`
}

type functionMetricPoint struct {
	Time              string  `json:"time"`
	Calls             int64   `json:"calls"`
	Errors            int64   `json:"errors"`
	AverageDurationMS float64 `json:"averageDurationMs"`
}

type cacheMetricSnapshot struct {
	Hits     int64              `json:"hits"`
	Misses   int64              `json:"misses"`
	Bypasses int64              `json:"bypasses"`
	Requests int64              `json:"requests"`
	HitRate  float64            `json:"hitRate"`
	Series   []cacheMetricPoint `json:"series"`
}

type cacheMetricPoint struct {
	Time     string  `json:"time"`
	Hits     int64   `json:"hits"`
	Misses   int64   `json:"misses"`
	Bypasses int64   `json:"bypasses"`
	HitRate  float64 `json:"hitRate"`
}

type websocketMetricSnapshot struct {
	Connections   int `json:"connections"`
	Subscriptions int `json:"subscriptions"`
}

func newRuntimeMetrics() *runtimeMetrics {
	return &runtimeMetrics{
		functions: map[string]*functionMetrics{},
		cache:     cacheMetrics{Buckets: map[int64]*cacheMetricsBucket{}},
	}
}

func (m *runtimeMetrics) recordFunction(path string, kind string, duration time.Duration, err error) {
	if m == nil || path == "" {
		return
	}
	now := time.Now().UTC()
	durationMS := float64(duration.Microseconds()) / 1000
	outcome := "ok"
	errorMessage := ""
	if err != nil {
		outcome = "error"
		errorMessage = err.Error()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.functions[path]
	if entry == nil {
		entry = &functionMetrics{Kind: kind, Buckets: map[int64]*functionMetricsBucket{}}
		m.functions[path] = entry
	}
	if kind != "" {
		entry.Kind = kind
	}
	entry.Calls++
	entry.TotalDurationMS += durationMS
	entry.LastDurationMS = durationMS
	entry.LastCalledAt = now
	if err != nil {
		entry.Errors++
	}

	bucket := entry.bucket(now)
	bucket.Calls++
	bucket.TotalDurationMS += durationMS
	if err != nil {
		bucket.Errors++
	}
	entry.trimBuckets(now)

	m.appendLog(runtimeLogEntry{
		Time:       now.Format(time.RFC3339Nano),
		Path:       path,
		Kind:       entry.Kind,
		Outcome:    outcome,
		DurationMS: durationMS,
		Error:      errorMessage,
	})
}

func (m *runtimeMetrics) recordCache(outcome string) {
	if m == nil {
		return
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	bucket := m.cache.bucket(now)
	switch outcome {
	case "hit":
		m.cache.Hits++
		bucket.Hits++
	case "miss":
		m.cache.Misses++
		bucket.Misses++
	default:
		m.cache.Bypasses++
		bucket.Bypasses++
		outcome = "bypass"
	}
	m.cache.trimBuckets(now)
	m.appendLog(runtimeLogEntry{
		Time:    now.Format(time.RFC3339Nano),
		Path:    "dev.data.rows",
		Kind:    "cache",
		Outcome: "ok",
		Cache:   outcome,
	})
}

func (m *runtimeMetrics) snapshot(current manifest.Manifest, connections int, subscriptions int) runtimeMetricsSnapshot {
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	functions := map[string]functionMetricSnapshot{}
	for path, entry := range current.Functions {
		metrics := m.functions[path]
		if metrics == nil {
			metrics = &functionMetrics{Kind: string(entry.Kind), Buckets: map[int64]*functionMetricsBucket{}}
		}
		functions[path] = metrics.snapshot(now)
	}
	for path, metrics := range m.functions {
		if _, ok := functions[path]; !ok {
			functions[path] = metrics.snapshot(now)
		}
	}

	logs := make([]runtimeLogEntry, len(m.logs))
	copy(logs, m.logs)
	sort.Slice(logs, func(left, right int) bool {
		return logs[left].Time > logs[right].Time
	})

	return runtimeMetricsSnapshot{
		GeneratedAt: now.Format(time.RFC3339Nano),
		Functions:   functions,
		Cache:       m.cache.snapshot(now),
		WebSocket: websocketMetricSnapshot{
			Connections:   connections,
			Subscriptions: subscriptions,
		},
		Logs: logs,
	}
}

func (m *runtimeMetrics) appendLog(entry runtimeLogEntry) {
	m.logs = append(m.logs, entry)
	if len(m.logs) > metricsLogLimit {
		m.logs = m.logs[len(m.logs)-metricsLogLimit:]
	}
}

func (m *functionMetrics) bucket(now time.Time) *functionMetricsBucket {
	key := bucketKey(now)
	bucket := m.Buckets[key]
	if bucket == nil {
		bucket = &functionMetricsBucket{}
		m.Buckets[key] = bucket
	}
	return bucket
}

func (m *functionMetrics) trimBuckets(now time.Time) {
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for key := range m.Buckets {
		if key < oldest {
			delete(m.Buckets, key)
		}
	}
}

func (m *functionMetrics) snapshot(now time.Time) functionMetricSnapshot {
	averageDurationMS := 0.0
	if m.Calls > 0 {
		averageDurationMS = m.TotalDurationMS / float64(m.Calls)
	}
	lastCalledAt := ""
	if !m.LastCalledAt.IsZero() {
		lastCalledAt = m.LastCalledAt.Format(time.RFC3339Nano)
	}
	return functionMetricSnapshot{
		Kind:              m.Kind,
		Calls:             m.Calls,
		Errors:            m.Errors,
		TotalDurationMS:   m.TotalDurationMS,
		AverageDurationMS: averageDurationMS,
		LastDurationMS:    m.LastDurationMS,
		LastCalledAt:      lastCalledAt,
		Series:            m.series(now),
	}
}

func (m *functionMetrics) series(now time.Time) []functionMetricPoint {
	points := make([]functionMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := m.Buckets[bucketStart.UnixMilli()]
		point := functionMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.Calls = bucket.Calls
			point.Errors = bucket.Errors
			if bucket.Calls > 0 {
				point.AverageDurationMS = bucket.TotalDurationMS / float64(bucket.Calls)
			}
		}
		points = append(points, point)
	}
	return points
}

func (m *cacheMetrics) bucket(now time.Time) *cacheMetricsBucket {
	key := bucketKey(now)
	bucket := m.Buckets[key]
	if bucket == nil {
		bucket = &cacheMetricsBucket{}
		m.Buckets[key] = bucket
	}
	return bucket
}

func (m *cacheMetrics) trimBuckets(now time.Time) {
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for key := range m.Buckets {
		if key < oldest {
			delete(m.Buckets, key)
		}
	}
}

func (m *cacheMetrics) snapshot(now time.Time) cacheMetricSnapshot {
	requests := m.Hits + m.Misses
	return cacheMetricSnapshot{
		Hits:     m.Hits,
		Misses:   m.Misses,
		Bypasses: m.Bypasses,
		Requests: requests,
		HitRate:  cacheHitRate(m.Hits, m.Misses),
		Series:   m.series(now),
	}
}

func (m *cacheMetrics) series(now time.Time) []cacheMetricPoint {
	points := make([]cacheMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := m.Buckets[bucketStart.UnixMilli()]
		point := cacheMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.Hits = bucket.Hits
			point.Misses = bucket.Misses
			point.Bypasses = bucket.Bypasses
			point.HitRate = cacheHitRate(bucket.Hits, bucket.Misses)
		}
		points = append(points, point)
	}
	return points
}

func cacheHitRate(hits int64, misses int64) float64 {
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

func bucketKey(now time.Time) int64 {
	return now.UTC().Truncate(metricsBucketWidth).UnixMilli()
}
