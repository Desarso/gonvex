package main

import (
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var defaultLatencyBounds = []time.Duration{
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	60 * time.Second,
	120 * time.Second,
}

type latencyHistogram struct {
	mu     sync.Mutex
	bounds []time.Duration
	counts []uint64
	count  uint64
	total  time.Duration
	max    time.Duration
}

func newLatencyHistogram() *latencyHistogram {
	bounds := append([]time.Duration(nil), defaultLatencyBounds...)
	return &latencyHistogram{bounds: bounds, counts: make([]uint64, len(bounds)+1)}
}

func (h *latencyHistogram) Observe(value time.Duration) {
	if value < 0 {
		value = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	index := len(h.bounds)
	for candidate, bound := range h.bounds {
		if value <= bound {
			index = candidate
			break
		}
	}
	h.counts[index]++
	h.count++
	h.total += value
	if value > h.max {
		h.max = value
	}
}

func (h *latencyHistogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func (h *latencyHistogram) Max() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.max
}

func (h *latencyHistogram) Percentile(ratio float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	target := uint64(math.Ceil(float64(h.count) * ratio))
	if target == 0 {
		target = 1
	}
	var seen uint64
	for index, count := range h.counts {
		seen += count
		if seen < target {
			continue
		}
		if index < len(h.bounds) {
			return h.bounds[index]
		}
		return h.max
	}
	return h.max
}

func (h *latencyHistogram) Average() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	return time.Duration(int64(h.total) / int64(h.count))
}

type countingConn struct {
	net.Conn
	read         atomic.Uint64
	written      atomic.Uint64
	readTotal    *atomic.Uint64
	writtenTotal *atomic.Uint64
}

func newCountingConn(connection net.Conn) *countingConn {
	return &countingConn{Conn: connection}
}

func newCountingConnWithTotals(connection net.Conn, readTotal, writtenTotal *atomic.Uint64) *countingConn {
	return &countingConn{Conn: connection, readTotal: readTotal, writtenTotal: writtenTotal}
}

func (c *countingConn) Read(buffer []byte) (int, error) {
	count, err := c.Conn.Read(buffer)
	c.read.Add(uint64(count))
	if c.readTotal != nil {
		c.readTotal.Add(uint64(count))
	}
	return count, err
}

func (c *countingConn) Write(buffer []byte) (int, error) {
	count, err := c.Conn.Write(buffer)
	c.written.Add(uint64(count))
	if c.writtenTotal != nil {
		c.writtenTotal.Add(uint64(count))
	}
	return count, err
}

func (c *countingConn) BytesRead() uint64 {
	return c.read.Load()
}

func (c *countingConn) BytesWritten() uint64 {
	return c.written.Load()
}

type safetyLimits struct {
	MinimumHostAvailableBytes uint64
	MaximumTargetRSSBytes     uint64
	MaximumErrorRate          float64
	MinimumOperations         uint64
}

type safetySnapshot struct {
	HostAvailableBytes uint64
	TargetRSSBytes     uint64
	StartedOperations  uint64
	FailedOperations   uint64
}

func evaluateSafety(limits safetyLimits, snapshot safetySnapshot) string {
	if limits.MinimumHostAvailableBytes > 0 && snapshot.HostAvailableBytes < limits.MinimumHostAvailableBytes {
		return "host available memory fell below the safety reserve"
	}
	if limits.MaximumTargetRSSBytes > 0 && snapshot.TargetRSSBytes > limits.MaximumTargetRSSBytes {
		return "target runtime RSS exceeded the safety limit"
	}
	minimumOperations := limits.MinimumOperations
	if minimumOperations == 0 {
		minimumOperations = 100
	}
	if limits.MaximumErrorRate > 0 && snapshot.StartedOperations >= minimumOperations {
		errorRate := float64(snapshot.FailedOperations) / float64(snapshot.StartedOperations)
		if errorRate > limits.MaximumErrorRate {
			return "operation error rate exceeded the safety limit"
		}
	}
	return ""
}
