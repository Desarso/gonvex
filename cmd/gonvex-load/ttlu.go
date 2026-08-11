package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

// TTLUReport separates individual client deliveries from time-to-last-user,
// which is the slowest delivery observed for one committed mutation.
type TTLUReport struct {
	CommittedMutations        uint64                    `json:"committedMutations"`
	CommitsWithPropagation    uint64                    `json:"commitsWithPropagation"`
	CommitsWithoutPropagation uint64                    `json:"commitsWithoutPropagation"`
	PropagationSamples        uint64                    `json:"propagationSamples"`
	PerClient                 HistogramReport           `json:"perClient"`
	AcrossCommits             HistogramReport           `json:"acrossCommits"`
	ByMutationPath            map[string]TTLUPathReport `json:"byMutationPath"`
}

type TTLUPathReport struct {
	Commits            uint64          `json:"commits"`
	PropagationSamples uint64          `json:"propagationSamples"`
	TTLU               HistogramReport `json:"ttlu"`
}

type propagationCommit struct {
	path    string
	clients map[string]time.Duration
}

type propagationAggregator struct {
	mu      sync.Mutex
	commits map[int64]*propagationCommit
}

func newPropagationAggregator() *propagationAggregator {
	return &propagationAggregator{commits: map[int64]*propagationCommit{}}
}

func propagationCommitKey(epochMilliseconds float64) int64 {
	return int64(math.Round(epochMilliseconds * 1000))
}

func (a *propagationAggregator) RecordCommit(epochMilliseconds float64, mutationPath string) {
	if epochMilliseconds <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	commit := a.commit(epochMilliseconds)
	commit.path = mutationPath
}

func (a *propagationAggregator) RecordDelivery(epochMilliseconds float64, clientID string, delay time.Duration) {
	if epochMilliseconds <= 0 || clientID == "" || delay < 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	commit := a.commit(epochMilliseconds)
	// One commit may invalidate several subscriptions on a client. Treat the
	// client's propagation as complete when its last affected subscription is
	// delivered, so the commit maximum is truly time-to-last-user.
	if previous, ok := commit.clients[clientID]; !ok || delay > previous {
		commit.clients[clientID] = delay
	}
}

func (a *propagationAggregator) commit(epochMilliseconds float64) *propagationCommit {
	key := propagationCommitKey(epochMilliseconds)
	commit := a.commits[key]
	if commit == nil {
		commit = &propagationCommit{clients: map[string]time.Duration{}}
		a.commits[key] = commit
	}
	return commit
}

func (a *propagationAggregator) Report() TTLUReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	report := TTLUReport{ByMutationPath: map[string]TTLUPathReport{}}
	perClient := []time.Duration{}
	allCommitMaxima := []time.Duration{}
	type pathValues struct {
		maxima  []time.Duration
		samples uint64
	}
	byPath := map[string]*pathValues{}
	for _, commit := range a.commits {
		if commit.path == "" {
			// An invalidation from a mutation outside this load run can appear on
			// the same local runtime. It belongs in the legacy invalidation metrics,
			// but not in TTLU for commits made by this harness.
			continue
		}
		report.CommittedMutations++
		if len(commit.clients) == 0 {
			report.CommitsWithoutPropagation++
			continue
		}
		report.CommitsWithPropagation++
		maximum := time.Duration(0)
		values := byPath[commit.path]
		if values == nil {
			values = &pathValues{}
			byPath[commit.path] = values
		}
		for _, delay := range commit.clients {
			perClient = append(perClient, delay)
			values.samples++
			if delay > maximum {
				maximum = delay
			}
		}
		values.maxima = append(values.maxima, maximum)
		allCommitMaxima = append(allCommitMaxima, maximum)
	}
	report.PropagationSamples = uint64(len(perClient))
	report.PerClient = exactHistogramReport(perClient)
	report.AcrossCommits = exactHistogramReport(allCommitMaxima)
	for path, values := range byPath {
		report.ByMutationPath[path] = TTLUPathReport{
			Commits: uint64(len(values.maxima)), PropagationSamples: values.samples,
			TTLU: exactHistogramReport(values.maxima),
		}
	}
	return report
}

func exactHistogramReport(values []time.Duration) HistogramReport {
	if len(values) == 0 {
		return HistogramReport{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, value := range sorted {
		total += value
	}
	percentile := func(ratio float64) time.Duration {
		index := int(math.Ceil(float64(len(sorted))*ratio)) - 1
		if index < 0 {
			index = 0
		}
		return sorted[index]
	}
	toMS := func(value time.Duration) float64 { return float64(value) / float64(time.Millisecond) }
	return HistogramReport{
		Count: uint64(len(sorted)), AverageMS: toMS(total / time.Duration(len(sorted))),
		P50MS: toMS(percentile(.50)), P95MS: toMS(percentile(.95)),
		P99MS: toMS(percentile(.99)), MaxMS: toMS(sorted[len(sorted)-1]),
	}
}
