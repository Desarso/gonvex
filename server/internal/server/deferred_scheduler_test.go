package server

import (
	"encoding/json"
	"testing"
	"time"
)

type recordingScheduler struct {
	calls []deferredScheduledJob
}

func (scheduler *recordingScheduler) RunAfter(delay time.Duration, functionPath string, args any) (string, error) {
	return scheduler.RunAt(time.Now().Add(delay), functionPath, args)
}

func (scheduler *recordingScheduler) RunAt(at time.Time, functionPath string, args any) (string, error) {
	raw, err := encodeSchedulerArgs(args)
	if err != nil {
		return "", err
	}
	scheduler.calls = append(scheduler.calls, deferredScheduledJob{at: at, functionPath: functionPath, args: raw})
	return "scheduled", nil
}

func TestDeferredSchedulerPublishesOnlyWhenFlushed(t *testing.T) {
	base := &recordingScheduler{}
	scheduler := newDeferredScheduler(base)
	fixedNow := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	scheduler.now = func() time.Time { return fixedNow }

	id, err := scheduler.RunAfter(0, "chat.process", map[string]any{"messageId": "message-1"})
	if err != nil {
		t.Fatal(err)
	}
	if id != "deferred_job_1" || len(base.calls) != 0 {
		t.Fatalf("job escaped before commit: id=%q calls=%d", id, len(base.calls))
	}
	if err := scheduler.flush(); err != nil {
		t.Fatal(err)
	}
	if len(base.calls) != 1 || base.calls[0].functionPath != "chat.process" || !base.calls[0].at.Equal(fixedNow) {
		t.Fatalf("flushed calls = %#v", base.calls)
	}
	var args map[string]any
	if err := json.Unmarshal(base.calls[0].args, &args); err != nil || args["messageId"] != "message-1" {
		t.Fatalf("flushed args = %s, err=%v", base.calls[0].args, err)
	}
}

func TestDeferredSchedulerRejectsUnencodableArgsBeforeCommit(t *testing.T) {
	scheduler := newDeferredScheduler(&recordingScheduler{})
	if _, err := scheduler.RunAfter(0, "chat.process", make(chan int)); err == nil {
		t.Fatal("expected JSON encoding error")
	}
	if len(scheduler.jobs) != 0 {
		t.Fatal("invalid job should not be buffered")
	}
}
