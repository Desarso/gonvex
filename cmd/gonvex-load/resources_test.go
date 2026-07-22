package main

import (
	"os"
	"runtime"
	"testing"
	"time"
)

func TestReadHostAvailableMemory(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("procfs sampler is Linux-specific")
	}
	available, err := readHostAvailableMemory()
	if err != nil {
		t.Fatalf("readHostAvailableMemory: %v", err)
	}
	if available == 0 {
		t.Fatal("available memory must be positive")
	}
}

func TestReadProcessSnapshotForCurrentProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("procfs sampler is Linux-specific")
	}
	snapshot, err := readProcessSnapshot(os.Getpid())
	if err != nil {
		t.Fatalf("readProcessSnapshot: %v", err)
	}
	if snapshot.RSSBytes == 0 || snapshot.VirtualBytes == 0 {
		t.Fatalf("process memory is missing: %#v", snapshot)
	}
	if snapshot.Threads < 1 || snapshot.FileDescriptors < 1 {
		t.Fatalf("process counts are missing: %#v", snapshot)
	}
	if _, err := parseProcessJiffies("123 (command with spaces) S 1 2 3 4 5 6 7 8 9 10 40 2 0 0"); err != nil {
		t.Fatalf("parseProcessJiffies: %v", err)
	}
}

func TestParseProcessJiffiesHandlesSpacesInCommand(t *testing.T) {
	jiffies, err := parseProcessJiffies("123 (command with spaces) S 1 2 3 4 5 6 7 8 9 10 40 2 0 0")
	if err != nil {
		t.Fatalf("parseProcessJiffies: %v", err)
	}
	if jiffies != 42 {
		t.Fatalf("jiffies = %d, want 42", jiffies)
	}
}

func TestCPUTrackerReportsCoreUtilization(t *testing.T) {
	tracker := cpuTracker{}
	if got := tracker.observe(100, 10_000, 8); got != 0 {
		t.Fatalf("first observation = %f, want 0", got)
	}
	// 100 process jiffies out of 800 system jiffies across 8 CPUs equals one core.
	if got := tracker.observe(200, 10_800, 8); got < 0.99 || got > 1.01 {
		t.Fatalf("core utilization = %f, want 1", got)
	}
}

func TestRateTrackerDoesNotInflateFirstSample(t *testing.T) {
	tracker := rateTracker{}
	start := time.Unix(100, 0)
	if got := tracker.observe(4096, start); got != 0 {
		t.Fatalf("first rate = %f, want 0", got)
	}
	if got := tracker.observe(6144, start.Add(2*time.Second)); got != 1024 {
		t.Fatalf("second rate = %f, want 1024", got)
	}
}
