package main

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func TestLatencyHistogramProducesBoundedPercentiles(t *testing.T) {
	histogram := newLatencyHistogram()
	for _, value := range []time.Duration{
		500 * time.Microsecond,
		2 * time.Millisecond,
		8 * time.Millisecond,
		25 * time.Millisecond,
		900 * time.Millisecond,
	} {
		histogram.Observe(value)
	}

	if histogram.Count() != 5 {
		t.Fatalf("count = %d, want 5", histogram.Count())
	}
	if got := histogram.Percentile(0.50); got < 5*time.Millisecond || got > 10*time.Millisecond {
		t.Fatalf("p50 = %s, want histogram bound between 5ms and 10ms", got)
	}
	if got := histogram.Percentile(0.95); got < 900*time.Millisecond || got > time.Second {
		t.Fatalf("p95 = %s, want histogram bound between 900ms and 1s", got)
	}
	if histogram.Max() != 900*time.Millisecond {
		t.Fatalf("max = %s", histogram.Max())
	}
}

func TestCountingConnCountsActualTransportBytes(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	counted := newCountingConn(left)

	written := make(chan error, 1)
	go func() {
		_, err := counted.Write([]byte("wire-out"))
		written <- err
	}()
	buffer := make([]byte, len("wire-out"))
	if _, err := io.ReadFull(right, buffer); err != nil {
		t.Fatalf("read written bytes: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("write through counted connection: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := right.Write([]byte("wire-in"))
		readDone <- err
	}()
	readBuffer := make([]byte, len("wire-in"))
	if _, err := io.ReadFull(counted, readBuffer); err != nil {
		t.Fatalf("read through counted connection: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("peer write: %v", err)
	}

	if !bytes.Equal(buffer, []byte("wire-out")) || !bytes.Equal(readBuffer, []byte("wire-in")) {
		t.Fatal("counted connection changed payload")
	}
	if got := counted.BytesWritten(); got != uint64(len("wire-out")) {
		t.Fatalf("bytes written = %d", got)
	}
	if got := counted.BytesRead(); got != uint64(len("wire-in")) {
		t.Fatalf("bytes read = %d", got)
	}
}

func TestEvaluateSafetyStopsBeforeHostExhaustion(t *testing.T) {
	limits := safetyLimits{
		MinimumHostAvailableBytes: 4 << 30,
		MaximumTargetRSSBytes:     12 << 30,
		MaximumErrorRate:          0.05,
	}

	if reason := evaluateSafety(limits, safetySnapshot{HostAvailableBytes: 3 << 30}); reason == "" {
		t.Fatal("expected low-memory abort")
	}
	if reason := evaluateSafety(limits, safetySnapshot{HostAvailableBytes: 8 << 30, TargetRSSBytes: 13 << 30}); reason == "" {
		t.Fatal("expected target RSS abort")
	}
	if reason := evaluateSafety(limits, safetySnapshot{HostAvailableBytes: 8 << 30, StartedOperations: 100, FailedOperations: 6}); reason == "" {
		t.Fatal("expected error-rate abort")
	}
	if reason := evaluateSafety(limits, safetySnapshot{HostAvailableBytes: 8 << 30, TargetRSSBytes: 2 << 30, StartedOperations: 100, FailedOperations: 4}); reason != "" {
		t.Fatalf("unexpected abort: %s", reason)
	}
}
