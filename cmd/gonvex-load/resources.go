package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type ResourceSample struct {
	Time                   string           `json:"time"`
	ElapsedMS              int64            `json:"elapsedMs"`
	ConnectionsEstablished uint64           `json:"connectionsEstablished"`
	SubscriptionsSent      uint64           `json:"subscriptionsSent"`
	InitialResults         uint64           `json:"initialResults"`
	InitialResultsPerSec   float64          `json:"initialResultsPerSec"`
	MutationsSent          uint64           `json:"mutationsSent"`
	MutationsPerSec        float64          `json:"mutationsPerSec"`
	InvalidationMessages   uint64           `json:"invalidationMessages"`
	InvalidationsPerSec    float64          `json:"invalidationsPerSec"`
	WireReadBytesPerSec    float64          `json:"wireReadBytesPerSec"`
	WireWriteBytesPerSec   float64          `json:"wireWriteBytesPerSec"`
	HostAvailableBytes     uint64           `json:"hostAvailableBytes"`
	Generator              processSnapshot  `json:"generator"`
	Target                 *processSnapshot `json:"target,omitempty"`
	GeneratorGoRoutines    int              `json:"generatorGoRoutines"`
}

type processSnapshot struct {
	PID             int     `json:"pid"`
	RSSBytes        uint64  `json:"rssBytes"`
	VirtualBytes    uint64  `json:"virtualBytes"`
	Threads         int     `json:"threads"`
	FileDescriptors int     `json:"fileDescriptors"`
	ReadBytes       uint64  `json:"readBytes"`
	WriteBytes      uint64  `json:"writeBytes"`
	Jiffies         uint64  `json:"-"`
	CPUCores        float64 `json:"cpuCores"`
}

type cpuTracker struct {
	processJiffies uint64
	systemJiffies  uint64
	initialized    bool
}

type rateTracker struct {
	value       uint64
	at          time.Time
	initialized bool
}

func (t *rateTracker) observe(value uint64, at time.Time) float64 {
	if !t.initialized {
		t.value = value
		t.at = at
		t.initialized = true
		return 0
	}
	seconds := at.Sub(t.at).Seconds()
	delta := value - t.value
	t.value = value
	t.at = at
	if seconds <= 0 {
		return 0
	}
	return float64(delta) / seconds
}

func (t *cpuTracker) observe(processJiffies, systemJiffies uint64, cpuCount int) float64 {
	if !t.initialized {
		t.processJiffies = processJiffies
		t.systemJiffies = systemJiffies
		t.initialized = true
		return 0
	}
	processDelta := processJiffies - t.processJiffies
	systemDelta := systemJiffies - t.systemJiffies
	t.processJiffies = processJiffies
	t.systemJiffies = systemJiffies
	if systemDelta == 0 || cpuCount < 1 {
		return 0
	}
	return (float64(processDelta) / float64(systemDelta)) * float64(cpuCount)
}

func readHostAvailableMemory() (uint64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kilobytes * 1024, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("MemAvailable was not found in /proc/meminfo")
}

func readSystemJiffies() (uint64, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return 0, fmt.Errorf("/proc/stat is empty")
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 2 || fields[0] != "cpu" {
		return 0, fmt.Errorf("/proc/stat has no aggregate CPU row")
	}
	var total uint64
	for _, raw := range fields[1:] {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, err
		}
		total += value
	}
	return total, nil
}

func readProcessSnapshot(pid int) (processSnapshot, error) {
	if pid < 1 {
		return processSnapshot{}, fmt.Errorf("process id must be positive")
	}
	snapshot := processSnapshot{PID: pid}
	status, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return processSnapshot{}, err
	}
	scanner := bufio.NewScanner(status)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "VmRSS:":
			snapshot.RSSBytes, _ = parseProcKilobytes(fields[1])
		case "VmSize:":
			snapshot.VirtualBytes, _ = parseProcKilobytes(fields[1])
		case "Threads:":
			snapshot.Threads, _ = strconv.Atoi(fields[1])
		}
	}
	status.Close()
	if err := scanner.Err(); err != nil {
		return processSnapshot{}, err
	}

	statPayload, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return processSnapshot{}, err
	}
	processJiffies, err := parseProcessJiffies(string(statPayload))
	if err != nil {
		return processSnapshot{}, err
	}
	snapshot.Jiffies = processJiffies

	if entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid)); err == nil {
		snapshot.FileDescriptors = len(entries)
	}
	if ioFile, err := os.Open(fmt.Sprintf("/proc/%d/io", pid)); err == nil {
		ioScanner := bufio.NewScanner(ioFile)
		for ioScanner.Scan() {
			fields := strings.Fields(ioScanner.Text())
			if len(fields) != 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "read_bytes:":
				snapshot.ReadBytes = value
			case "write_bytes:":
				snapshot.WriteBytes = value
			}
		}
		ioFile.Close()
	}
	return snapshot, nil
}

func parseProcessJiffies(statPayload string) (uint64, error) {
	closingParen := strings.LastIndexByte(statPayload, ')')
	if closingParen < 0 || closingParen+2 >= len(statPayload) {
		return 0, fmt.Errorf("process stat has an invalid command field")
	}
	statFields := strings.Fields(statPayload[closingParen+2:])
	// The suffix starts at field 3 (state), so indexes 11 and 12 are utime/stime.
	if len(statFields) < 13 {
		return 0, fmt.Errorf("process stat has %d fields after command", len(statFields))
	}
	userJiffies, err := strconv.ParseUint(statFields[11], 10, 64)
	if err != nil {
		return 0, err
	}
	systemJiffies, err := strconv.ParseUint(statFields[12], 10, 64)
	if err != nil {
		return 0, err
	}
	return userJiffies + systemJiffies, nil
}

func parseProcKilobytes(raw string) (uint64, error) {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return value * 1024, nil
}

func sampleProcess(pid int, tracker *cpuTracker, systemJiffies uint64) (processSnapshot, error) {
	snapshot, err := readProcessSnapshot(pid)
	if err != nil {
		return processSnapshot{}, err
	}
	snapshot.CPUCores = tracker.observe(snapshot.Jiffies, systemJiffies, runtime.NumCPU())
	return snapshot, nil
}

func sampleRunResources(ctx context.Context, cancel context.CancelFunc, config runConfig, metrics *runMetrics, startedAt time.Time) {
	interval := config.SampleInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	selfTracker := cpuTracker{}
	targetTracker := cpuTracker{}
	resultsRate := rateTracker{}
	mutationRate := rateTracker{}
	invalidationRate := rateTracker{}
	wireReadRate := rateTracker{}
	wireWriteRate := rateTracker{}

	collect := func() {
		now := time.Now().UTC()
		systemJiffies, _ := readSystemJiffies()
		hostAvailable, _ := readHostAvailableMemory()
		generator, _ := sampleProcess(os.Getpid(), &selfTracker, systemJiffies)
		var target *processSnapshot
		if config.TargetPID > 0 {
			if snapshot, err := sampleProcess(config.TargetPID, &targetTracker, systemJiffies); err == nil {
				target = &snapshot
			}
		}
		results := metrics.initialResults.Load()
		mutations := metrics.mutationsSent.Load()
		invalidations := metrics.invalidationResults.Load() + metrics.invalidationPatches.Load() + metrics.invalidationProgress.Load()
		wireRead := metrics.wireBytesRead.Load()
		wireWrite := metrics.wireBytesWritten.Load()
		sample := ResourceSample{
			Time:                   now.Format(time.RFC3339Nano),
			ElapsedMS:              now.Sub(startedAt).Milliseconds(),
			ConnectionsEstablished: metrics.connections.Load(),
			SubscriptionsSent:      metrics.subscriptionsSent.Load(),
			InitialResults:         results,
			InitialResultsPerSec:   resultsRate.observe(results, now),
			MutationsSent:          mutations,
			MutationsPerSec:        mutationRate.observe(mutations, now),
			InvalidationMessages:   invalidations,
			InvalidationsPerSec:    invalidationRate.observe(invalidations, now),
			WireReadBytesPerSec:    wireReadRate.observe(wireRead, now),
			WireWriteBytesPerSec:   wireWriteRate.observe(wireWrite, now),
			HostAvailableBytes:     hostAvailable,
			Generator:              generator,
			Target:                 target,
			GeneratorGoRoutines:    runtime.NumGoroutine(),
		}
		metrics.resourceMu.Lock()
		metrics.samples = append(metrics.samples, sample)
		metrics.resourceMu.Unlock()
		targetRSS := uint64(0)
		if target != nil {
			targetRSS = target.RSSBytes
		}
		if reason := evaluateSafety(config.Safety, safetySnapshot{
			HostAvailableBytes: hostAvailable,
			TargetRSSBytes:     targetRSS,
			StartedOperations:  metrics.connectionAttempts.Load() + metrics.subscriptionsSent.Load() + metrics.mutationsSent.Load(),
			FailedOperations:   metrics.setupErrors.Load() + metrics.subscriptionErrors.Load() + metrics.mutationErrors.Load(),
		}); reason != "" {
			metrics.setAbort(reason)
			cancel()
		}
	}

	collect()
	for {
		select {
		case <-ctx.Done():
			collect()
			return
		case <-ticker.C:
			collect()
		}
	}
}
