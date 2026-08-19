package server

import (
	"context"
	"sync"
	"time"
)

// admissionClass partitions database-backed query executions by why they run.
// Reactive work (invalidation and recovery reruns, authoritative sync
// recomputation) keeps TTLU low for already-connected clients; foreground work
// answers an interactive request; bootstrap work hydrates newly attached
// subscriptions and sync snapshots. Bootstrap bursts are the only class whose
// arrival rate is proportional to reconnect stampedes, so it is the only class
// with its own concurrency share.
type admissionClass int

const (
	admissionReactive admissionClass = iota
	admissionForeground
	admissionBootstrap
	admissionClassCount
)

// admissionClassForReason maps a subscription execution reason onto its work
// class. "initial" executions are bootstrap; everything triggered by committed
// writes or listener recovery is reactive.
func admissionClassForReason(reason string) admissionClass {
	if reason == "invalidate" || reason == "recover" {
		return admissionReactive
	}
	return admissionBootstrap
}

// foregroundAgingThreshold bounds how long sustained reactive load may delay a
// waiting foreground query before it wins the next free slot anyway.
const foregroundAgingThreshold = 250 * time.Millisecond

type admissionWaiter struct {
	class      admissionClass
	tenant     string
	ready      chan struct{}
	enqueuedAt time.Time
	granted    bool
	removed    bool
}

// admissionQueue holds FIFO waiter queues per tenant plus a round-robin cursor
// so one tenant's burst cannot starve another tenant's queued work of the same
// class.
type admissionQueue struct {
	byTenant map[string][]*admissionWaiter
	order    []string
	cursor   int
	waiting  int
}

func newAdmissionQueue() *admissionQueue {
	return &admissionQueue{byTenant: map[string][]*admissionWaiter{}}
}

func (q *admissionQueue) push(waiter *admissionWaiter) {
	if _, exists := q.byTenant[waiter.tenant]; !exists {
		q.order = append(q.order, waiter.tenant)
	}
	q.byTenant[waiter.tenant] = append(q.byTenant[waiter.tenant], waiter)
	q.waiting++
}

// pop returns the next waiter in tenant round-robin order, skipping tenants
// whose queues drained and compacting cancelled entries as it goes.
func (q *admissionQueue) pop() *admissionWaiter {
	for len(q.order) > 0 {
		if q.cursor >= len(q.order) {
			q.cursor = 0
		}
		tenant := q.order[q.cursor]
		queue := q.byTenant[tenant]
		for len(queue) > 0 && queue[0].removed {
			queue = queue[1:]
		}
		if len(queue) == 0 {
			delete(q.byTenant, tenant)
			q.order = append(q.order[:q.cursor], q.order[q.cursor+1:]...)
			continue
		}
		waiter := queue[0]
		q.byTenant[tenant] = queue[1:]
		q.cursor++
		q.waiting--
		return waiter
	}
	return nil
}

// oldestWait reports how long the oldest live waiter has been queued.
func (q *admissionQueue) oldestWait(now time.Time) time.Duration {
	oldest := time.Duration(0)
	for _, queue := range q.byTenant {
		for _, waiter := range queue {
			if waiter.removed {
				continue
			}
			if wait := now.Sub(waiter.enqueuedAt); wait > oldest {
				oldest = wait
			}
			break
		}
	}
	return oldest
}

func (q *admissionQueue) discard(waiter *admissionWaiter) {
	waiter.removed = true
	q.waiting--
}

func (q *admissionQueue) tenantStats() (tenants int, largest int) {
	for _, queue := range q.byTenant {
		live := 0
		for _, waiter := range queue {
			if !waiter.removed {
				live++
			}
		}
		if live > 0 {
			tenants++
			if live > largest {
				largest = live
			}
		}
	}
	return tenants, largest
}

type admissionClassMetrics struct {
	Admitted        uint64
	Waited          uint64
	WaitMS          float64
	MaxWaitMS       float64
	Cancelled       uint64
	QueueDepth      int
	Active          int
	DelayedByBurst  uint64
	TenantsQueued   int
	LargestTenantQ  int
	longestObserved time.Duration
}

type queryAdmissionSnapshot struct {
	TotalPermits     int
	BootstrapPermits int
	Active           int
	BootstrapActive  int
	Classes          [admissionClassCount]admissionClassMetrics
}

// queryAdmission is the unified admission controller for every database-backed
// query execution. It enforces one global concurrency cap, reserves a bounded
// share for bootstrap hydration, keeps tenant round-robin fairness inside each
// class, and lets bootstrap borrow idle capacity when nothing else is waiting.
// A nil *queryAdmission admits everything immediately, preserving the historic
// unlimited behavior for tests and embedded callers.
type queryAdmission struct {
	mu               sync.Mutex
	totalPermits     int
	bootstrapPermits int
	active           int
	bootstrapActive  int
	queues           [admissionClassCount]*admissionQueue
	metrics          [admissionClassCount]admissionClassMetrics
}

func newQueryAdmission(totalPermits, bootstrapPermits int) *queryAdmission {
	if totalPermits <= 0 {
		return nil
	}
	if bootstrapPermits <= 0 {
		bootstrapPermits = max(1, totalPermits/4)
	}
	if bootstrapPermits > totalPermits {
		bootstrapPermits = totalPermits
	}
	admission := &queryAdmission{totalPermits: totalPermits, bootstrapPermits: bootstrapPermits}
	for class := range admission.queues {
		admission.queues[class] = newAdmissionQueue()
	}
	return admission
}

// acquire blocks until a permit is available or ctx is cancelled. It returns a
// release function and whether a permit was granted; on cancellation the
// waiter is removed from its queue immediately.
func (a *queryAdmission) acquire(ctx context.Context, class admissionClass, tenant string) (func(), bool) {
	if a == nil {
		return func() {}, true
	}
	if class < 0 || class >= admissionClassCount {
		class = admissionForeground
	}
	a.mu.Lock()
	if a.admissibleLocked(class) {
		a.grantLocked(class)
		a.metrics[class].Admitted++
		a.mu.Unlock()
		return func() { a.release(class) }, true
	}
	waiter := &admissionWaiter{class: class, tenant: tenant, ready: make(chan struct{}), enqueuedAt: time.Now()}
	a.queues[class].push(waiter)
	a.metrics[class].Waited++
	if class == admissionReactive && a.bootstrapActive > 0 {
		a.metrics[admissionReactive].DelayedByBurst++
	}
	a.mu.Unlock()

	select {
	case <-waiter.ready:
		a.mu.Lock()
		a.recordWaitLocked(class, waiter)
		a.mu.Unlock()
		return func() { a.release(class) }, true
	case <-ctx.Done():
		a.mu.Lock()
		if waiter.granted {
			// The dispatcher granted concurrently with cancellation; hand the
			// permit straight back so it is not leaked.
			a.recordWaitLocked(class, waiter)
			a.releaseLocked(class)
		} else {
			a.queues[class].discard(waiter)
			a.metrics[class].Cancelled++
			// A cancelled reactive/foreground waiter may have been the only
			// contention blocking bootstrap borrowing; re-dispatch immediately.
			a.dispatchLocked()
		}
		a.mu.Unlock()
		return nil, false
	}
}

func (a *queryAdmission) recordWaitLocked(class admissionClass, waiter *admissionWaiter) {
	wait := time.Since(waiter.enqueuedAt)
	metric := &a.metrics[class]
	metric.WaitMS += float64(wait.Microseconds()) / 1000
	if wait > metric.longestObserved {
		metric.longestObserved = wait
		metric.MaxWaitMS = float64(wait.Microseconds()) / 1000
	}
}

// admissibleLocked reports whether class may start one more execution right
// now, without considering queued waiters of the same class (callers dispatch
// queues in order).
func (a *queryAdmission) admissibleLocked(class admissionClass) bool {
	if a.active >= a.totalPermits {
		return false
	}
	if class != admissionBootstrap {
		return true
	}
	if a.bootstrapActive < a.bootstrapPermits {
		return true
	}
	// Borrow idle capacity: hydration may exceed its share only while no
	// reactive or foreground work is waiting for a slot.
	return a.queues[admissionReactive].waiting == 0 && a.queues[admissionForeground].waiting == 0
}

func (a *queryAdmission) grantLocked(class admissionClass) {
	a.active++
	a.metrics[class].Active++
	if class == admissionBootstrap {
		a.bootstrapActive++
	}
}

func (a *queryAdmission) release(class admissionClass) {
	a.mu.Lock()
	a.releaseLocked(class)
	a.mu.Unlock()
}

func (a *queryAdmission) releaseLocked(class admissionClass) {
	a.active--
	a.metrics[class].Active--
	if class == admissionBootstrap {
		a.bootstrapActive--
	}
	a.dispatchLocked()
}

// dispatchLocked hands free permits to queued waiters. Bootstrap always drains
// up to its reserved share first, so a reactive flood cannot starve hydration;
// beyond that share, reactive work wins unless a foreground waiter has aged
// past its threshold.
func (a *queryAdmission) dispatchLocked() {
	now := time.Now()
	for a.active < a.totalPermits {
		var waiter *admissionWaiter
		if a.bootstrapActive < a.bootstrapPermits {
			waiter = a.queues[admissionBootstrap].pop()
		}
		if waiter == nil {
			if a.queues[admissionForeground].oldestWait(now) > foregroundAgingThreshold {
				waiter = a.queues[admissionForeground].pop()
			}
		}
		if waiter == nil {
			waiter = a.queues[admissionReactive].pop()
		}
		if waiter == nil {
			waiter = a.queues[admissionForeground].pop()
		}
		if waiter == nil && a.queues[admissionReactive].waiting == 0 && a.queues[admissionForeground].waiting == 0 {
			waiter = a.queues[admissionBootstrap].pop()
		}
		if waiter == nil {
			return
		}
		waiter.granted = true
		a.grantLocked(waiter.class)
		a.metrics[waiter.class].Admitted++
		close(waiter.ready)
	}
}

// acquireQueryAdmission gates one database-backed query execution. The
// returned release function must be called as soon as the execution finishes;
// admission is always leaf-scoped, so no caller may hold a permit while
// acquiring another.
func (s *Server) acquireQueryAdmission(ctx context.Context, class admissionClass, project, tenant string) (func(), bool) {
	return s.admission.acquire(ctx, class, project+"/"+tenant)
}

func (a *queryAdmission) snapshot() queryAdmissionSnapshot {
	if a == nil {
		return queryAdmissionSnapshot{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	snapshot := queryAdmissionSnapshot{
		TotalPermits:     a.totalPermits,
		BootstrapPermits: a.bootstrapPermits,
		Active:           a.active,
		BootstrapActive:  a.bootstrapActive,
	}
	for class := range a.queues {
		metric := a.metrics[class]
		metric.QueueDepth = a.queues[class].waiting
		metric.TenantsQueued, metric.LargestTenantQ = a.queues[class].tenantStats()
		snapshot.Classes[class] = metric
	}
	return snapshot
}
