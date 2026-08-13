package main

import (
	"testing"
	"time"
)

func TestPropagationAggregatorComputesTimeToLastUserByMutationPath(t *testing.T) {
	aggregator := newPropagationAggregator()
	aggregator.RecordDelivery(1000.125, "client-a", 10*time.Millisecond) // delivery may precede ack
	aggregator.RecordDelivery(1000.125, "client-a", 20*time.Millisecond)
	aggregator.RecordDelivery(1000.125, "client-b", 30*time.Millisecond)
	aggregator.RecordCommit(1000.125, "tasks.create")
	aggregator.RecordCommit(2000.250, "tasks.create")
	aggregator.RecordDelivery(2000.250, "client-a", 40*time.Millisecond)
	aggregator.RecordCommit(3000.375, "tasks.update")
	aggregator.RecordDelivery(3000.375, "client-a", 5*time.Millisecond)
	aggregator.RecordDelivery(3000.375, "client-b", 15*time.Millisecond)
	aggregator.RecordCommit(4000.500, "tasks.update") // no subscribed query changed

	report := aggregator.Report()
	if report.CommittedMutations != 4 || report.CommitsWithPropagation != 3 || report.CommitsWithoutPropagation != 1 {
		t.Fatalf("unexpected commit counts: %#v", report)
	}
	if report.PropagationSamples != 5 || report.PerClient.P50MS != 20 || report.PerClient.P95MS != 40 {
		t.Fatalf("unexpected per-client distribution: %#v", report.PerClient)
	}
	if report.AcrossCommits.Count != 3 || report.AcrossCommits.P50MS != 30 || report.AcrossCommits.P95MS != 40 || report.AcrossCommits.MaxMS != 40 {
		t.Fatalf("unexpected TTLU distribution: %#v", report.AcrossCommits)
	}
	create := report.ByMutationPath["tasks.create"]
	if create.Commits != 2 || create.PropagationSamples != 3 || create.TTLU.P50MS != 30 || create.TTLU.P95MS != 40 {
		t.Fatalf("unexpected tasks.create TTLU: %#v", create)
	}
	update := report.ByMutationPath["tasks.update"]
	if update.Commits != 1 || update.PropagationSamples != 2 || update.TTLU.MaxMS != 15 {
		t.Fatalf("unexpected tasks.update TTLU: %#v", update)
	}
}

func TestPropagationAggregatorCorrelatesMutationIDsAcrossTimestampSkewAndCoalescing(t *testing.T) {
	aggregator := newPropagationAggregator()
	// A delivery may race ahead of the mutation response on another socket.
	aggregator.RecordDeliveryIDs([]string{"mutation-a", "mutation-b"}, "client-a", 1050)
	aggregator.RecordCommitID("mutation-a", 1000, "tasks.create")
	aggregator.RecordCommitID("mutation-b", 1020, "tasks.update")
	aggregator.RecordDeliveryIDs([]string{"mutation-a", "mutation-b"}, "client-b", 1080)

	report := aggregator.Report()
	if report.CommittedMutations != 2 || report.CommitsWithPropagation != 2 || report.PropagationSamples != 4 {
		t.Fatalf("unexpected ID-correlated commit counts: %#v", report)
	}
	if got := report.ByMutationPath["tasks.create"].TTLU.MaxMS; got != 80 {
		t.Fatalf("tasks.create TTLU = %v, want 80", got)
	}
	if got := report.ByMutationPath["tasks.update"].TTLU.MaxMS; got != 60 {
		t.Fatalf("tasks.update TTLU = %v, want 60", got)
	}
}
