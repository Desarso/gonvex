package main

import (
	"fmt"
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
	path          string
	committedAtMS float64
	clients       map[string]time.Duration
	receivedAtMS  map[string]float64
}

type propagationAggregator struct {
	mu       sync.Mutex
	commits  map[string]*propagationCommit
	receipts [64]struct {
		sync.Mutex
		byCommit map[string]map[string]float64
	}
}

func newPropagationAggregator() *propagationAggregator {
	a := &propagationAggregator{commits: map[string]*propagationCommit{}}
	for index := range a.receipts {
		a.receipts[index].byCommit = map[string]map[string]float64{}
	}
	return a
}

func propagationReceiptShard(clientID string) uint64 {
	// FNV-1a is sufficient here: this is only a stable distribution function for
	// runner-local mutexes, not an authorization or integrity boundary.
	const offset64 = uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	hash := offset64
	for index := 0; index < len(clientID); index++ {
		hash ^= uint64(clientID[index])
		hash *= prime64
	}
	return hash % 64
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
	commit := a.commit(timestampCommitKey(epochMilliseconds))
	commit.path = mutationPath
	commit.committedAtMS = epochMilliseconds
}

func (a *propagationAggregator) RecordDelivery(epochMilliseconds float64, clientID string, delay time.Duration) {
	if epochMilliseconds <= 0 || clientID == "" || delay < 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := timestampCommitKey(epochMilliseconds)
	commit := a.commits[key]
	if commit == nil {
		for _, candidate := range a.commits {
			if propagationCommitKey(candidate.committedAtMS) == propagationCommitKey(epochMilliseconds) {
				commit = candidate
				break
			}
		}
	}
	if commit == nil {
		commit = a.commit(key)
	}
	// One commit may invalidate several subscriptions on a client. Treat the
	// client's propagation as complete when its last affected subscription is
	// delivered, so the commit maximum is truly time-to-last-user.
	if previous, ok := commit.clients[clientID]; !ok || delay > previous {
		commit.clients[clientID] = delay
	}
}

func timestampCommitKey(epochMilliseconds float64) string {
	return fmt.Sprintf("time:%d", propagationCommitKey(epochMilliseconds))
}

// RecordCommitID correlates a mutation result using its protocol request ID.
// Unlike independently sampled timestamps, that identifier survives LISTEN
// delivery and commit coalescing without precision or scheduling ambiguity.
func (a *propagationAggregator) RecordCommitID(id string, epochMilliseconds float64, mutationPath string) {
	if id == "" || epochMilliseconds <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	commit := a.commit("id:" + id)
	commit.path = mutationPath
	commit.committedAtMS = epochMilliseconds
	for clientID, receivedAtMS := range commit.receivedAtMS {
		a.recordReceiptLocked(commit, clientID, receivedAtMS)
	}
	commit.receivedAtMS = map[string]float64{}
}

func (a *propagationAggregator) RecordDeliveryIDs(ids []string, clientID string, receivedAtMS float64) {
	if clientID == "" || receivedAtMS <= 0 {
		return
	}
	shard := &a.receipts[propagationReceiptShard(clientID)]
	shard.Lock()
	defer shard.Unlock()
	for _, id := range ids {
		if id == "" {
			continue
		}
		key := "id:" + id
		clients := shard.byCommit[key]
		if clients == nil {
			clients = map[string]float64{}
			shard.byCommit[key] = clients
		}
		if previous := clients[clientID]; receivedAtMS > previous {
			clients[clientID] = receivedAtMS
		}
	}
}

func (a *propagationAggregator) recordReceiptLocked(commit *propagationCommit, clientID string, receivedAtMS float64) {
	delayMS := receivedAtMS - commit.committedAtMS
	if delayMS < 0 {
		return
	}
	delay := time.Duration(delayMS * float64(time.Millisecond))
	if previous, ok := commit.clients[clientID]; !ok || delay > previous {
		commit.clients[clientID] = delay
	}
}

func (a *propagationAggregator) commit(key string) *propagationCommit {
	commit := a.commits[key]
	if commit == nil {
		commit = &propagationCommit{clients: map[string]time.Duration{}, receivedAtMS: map[string]float64{}}
		a.commits[key] = commit
	}
	return commit
}

func (a *propagationAggregator) Report() TTLUReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Fold sharded receipt maxima into their commit records only when producing
	// the report. Each connection writes to exactly one shard, eliminating the
	// all-client mutex convoy from the measured delivery path.
	for index := range a.receipts {
		shard := &a.receipts[index]
		shard.Lock()
		for key, clients := range shard.byCommit {
			commit := a.commits[key]
			if commit == nil || commit.committedAtMS <= 0 {
				continue
			}
			for clientID, receivedAtMS := range clients {
				a.recordReceiptLocked(commit, clientID, receivedAtMS)
			}
		}
		shard.Unlock()
	}
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
