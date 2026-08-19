package server

import (
	"context"
	"testing"
	"time"
)

func mustAcquire(t *testing.T, a *queryAdmission, class admissionClass, tenant string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, ok := a.acquire(ctx, class, tenant)
	if !ok {
		t.Fatalf("acquire(%v, %s) was not admitted", class, tenant)
	}
	return release
}

// acquireAsync starts a blocking acquire and reports the release function on
// the returned channel once the permit is granted.
func acquireAsync(ctx context.Context, a *queryAdmission, class admissionClass, tenant string) chan func() {
	granted := make(chan func(), 1)
	go func() {
		release, ok := a.acquire(ctx, class, tenant)
		if !ok {
			close(granted)
			return
		}
		granted <- release
	}()
	return granted
}

func waitGranted(t *testing.T, granted chan func()) func() {
	t.Helper()
	select {
	case release, ok := <-granted:
		if !ok {
			t.Fatal("acquire failed instead of being granted")
		}
		return release
	case <-time.After(time.Second):
		t.Fatal("queued acquire was never granted")
	}
	return nil
}

func assertPending(t *testing.T, granted chan func()) {
	t.Helper()
	select {
	case release, ok := <-granted:
		if !ok {
			t.Fatal("acquire failed while it should still be queued")
		}
		release()
		t.Fatal("acquire was granted while it should still be queued")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestQueryAdmissionDisabledAdmitsEverything(t *testing.T) {
	var admission *queryAdmission
	if admission != newQueryAdmission(0, 0) {
		t.Fatal("zero total permits must disable admission")
	}
	release, ok := admission.acquire(context.Background(), admissionBootstrap, "t")
	if !ok {
		t.Fatal("nil admission controller must admit immediately")
	}
	release()
}

func TestQueryAdmissionEnforcesGlobalCap(t *testing.T) {
	admission := newQueryAdmission(2, 1)
	first := mustAcquire(t, admission, admissionReactive, "a")
	second := mustAcquire(t, admission, admissionReactive, "a")
	queued := acquireAsync(context.Background(), admission, admissionReactive, "a")
	assertPending(t, queued)
	first()
	release := waitGranted(t, queued)
	release()
	second()
	snapshot := admission.snapshot()
	if snapshot.Active != 0 {
		t.Fatalf("active permits after release = %d, want 0", snapshot.Active)
	}
	if got := snapshot.Classes[admissionReactive].Admitted; got != 3 {
		t.Fatalf("reactive admissions = %d, want 3", got)
	}
}

func TestQueryAdmissionBootstrapBorrowsIdleCapacity(t *testing.T) {
	admission := newQueryAdmission(4, 1)
	releases := make([]func(), 0, 4)
	for range 4 {
		releases = append(releases, mustAcquire(t, admission, admissionBootstrap, "a"))
	}
	if got := admission.snapshot().BootstrapActive; got != 4 {
		t.Fatalf("idle-borrowed bootstrap permits = %d, want 4", got)
	}
	// With a reactive waiter queued, freed capacity must go reactive first and
	// queued bootstrap work stays within its reserved share.
	reactive := acquireAsync(context.Background(), admission, admissionReactive, "a")
	bootstrap := acquireAsync(context.Background(), admission, admissionBootstrap, "a")
	assertPending(t, reactive)
	releases[0]()
	releaseReactive := waitGranted(t, reactive)
	assertPending(t, bootstrap)
	releases[1]()
	releases[2]()
	releaseBootstrap := waitGranted(t, bootstrap)
	releaseBootstrap()
	releaseReactive()
	releases[3]()
}

func TestQueryAdmissionBootstrapReservedShareSurvivesReactiveFlood(t *testing.T) {
	admission := newQueryAdmission(2, 1)
	first := mustAcquire(t, admission, admissionReactive, "a")
	second := mustAcquire(t, admission, admissionReactive, "a")
	bootstrap := acquireAsync(context.Background(), admission, admissionBootstrap, "a")
	reactive := acquireAsync(context.Background(), admission, admissionReactive, "a")
	assertPending(t, bootstrap)
	first()
	releaseBootstrap := waitGranted(t, bootstrap)
	assertPending(t, reactive)
	second()
	releaseReactive := waitGranted(t, reactive)
	releaseReactive()
	releaseBootstrap()
}

func TestQueryAdmissionCancellationRemovesWaiterAndUnblocksBorrow(t *testing.T) {
	admission := newQueryAdmission(2, 1)
	bootstrapHeld := mustAcquire(t, admission, admissionBootstrap, "a")
	reactiveHeld := mustAcquire(t, admission, admissionReactive, "a")
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	reactiveQueued := acquireAsync(waiterCtx, admission, admissionReactive, "a")
	assertPending(t, reactiveQueued)
	// A queued reactive waiter blocks bootstrap borrowing beyond its share.
	bootstrapQueued := acquireAsync(context.Background(), admission, admissionBootstrap, "a")
	assertPending(t, bootstrapQueued)
	reactiveHeld()
	releaseReactive := waitGranted(t, reactiveQueued)
	releaseReactive()

	reactiveQueued = acquireAsync(waiterCtx, admission, admissionReactive, "a")
	assertPending(t, reactiveQueued)
	cancelWaiter()
	select {
	case _, ok := <-reactiveQueued:
		if ok {
			t.Fatal("cancelled waiter must not be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	// Cancelling the only competing waiter lets bootstrap borrow the free slot.
	releaseBootstrapQueued := waitGranted(t, bootstrapQueued)
	releaseBootstrapQueued()
	bootstrapHeld()
	snapshot := admission.snapshot()
	if got := snapshot.Classes[admissionReactive].Cancelled; got != 1 {
		t.Fatalf("reactive cancellations = %d, want 1", got)
	}
	if depth := snapshot.Classes[admissionReactive].QueueDepth; depth != 0 {
		t.Fatalf("reactive queue depth after cancel = %d, want 0", depth)
	}
}

func TestQueryAdmissionTenantRoundRobin(t *testing.T) {
	admission := newQueryAdmission(1, 1)
	held := mustAcquire(t, admission, admissionReactive, "seed")
	order := make(chan string, 4)
	grant := func(tenant string) chan func() {
		granted := make(chan func(), 1)
		go func() {
			release, ok := admission.acquire(context.Background(), admissionReactive, tenant)
			if ok {
				granted <- release
				order <- tenant
			}
		}()
		return granted
	}
	firstA := grant("tenant-a")
	// Queue deterministically: tenant-a twice, then tenant-b.
	waitForDepth := func(want int) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if admission.snapshot().Classes[admissionReactive].QueueDepth == want {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("reactive queue depth never reached %d", want)
	}
	waitForDepth(1)
	secondA := grant("tenant-a")
	waitForDepth(2)
	firstB := grant("tenant-b")
	waitForDepth(3)

	held()
	got := make([]string, 0, 3)
	for range 3 {
		select {
		case tenant := <-order:
			got = append(got, tenant)
		case <-time.After(time.Second):
			t.Fatalf("grants stalled after %v", got)
		}
		for _, granted := range []chan func(){firstA, secondA, firstB} {
			select {
			case release := <-granted:
				release()
			default:
			}
		}
	}
	want := []string{"tenant-a", "tenant-b", "tenant-a"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("grant order = %v, want %v", got, want)
		}
	}
}
