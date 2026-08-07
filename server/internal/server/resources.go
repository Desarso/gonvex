package server

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// resourceSnapshot reports runtime-process resources and project-scoped socket
// traffic. Memory and CPU are capacity averages, not client attribution.
func (s *Server) resourceSnapshot(websocket websocketMetricSnapshot) runtimeResourceSnapshot {
	memoryBytes := processResidentBytes()
	cpuSeconds := processCPUSeconds()
	now := time.Now()
	s.resourceMu.Lock()
	if !s.resourceSampleAt.IsZero() {
		elapsed := now.Sub(s.resourceSampleAt).Seconds()
		if elapsed > 0 && cpuSeconds >= s.resourceCPUSeconds {
			s.resourceCPUPercent = (cpuSeconds - s.resourceCPUSeconds) / elapsed * 100
		}
	}
	s.resourceSampleAt, s.resourceCPUSeconds = now, cpuSeconds
	cpuPercent := s.resourceCPUPercent
	s.resourceMu.Unlock()

	result := runtimeResourceSnapshot{MemoryBytes: memoryBytes, CPUPercent: cpuPercent, BytesReceived: websocket.BytesReceived, BytesSent: websocket.BytesSent}
	if websocket.Connections > 0 {
		clients := uint64(websocket.Connections)
		result.MemoryPerClientBytes = memoryBytes / clients
		result.CPUPerClientPercent = cpuPercent / float64(websocket.Connections)
		result.BytesPerClient = (websocket.BytesReceived + websocket.BytesSent) / clients
	}
	return result
}

func processResidentBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 1 {
			pages, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr == nil {
				return pages * uint64(os.Getpagesize())
			}
		}
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Sys
}

func processCPUSeconds() float64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	text := string(data)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		return 0
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) <= 12 {
		return 0
	}
	userTicks, errUser := strconv.ParseFloat(fields[11], 64)
	systemTicks, errSystem := strconv.ParseFloat(fields[12], 64)
	if errUser != nil || errSystem != nil {
		return 0
	}
	return (userTicks + systemTicks) / 100
}
