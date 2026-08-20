package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

const scheduledJobLease = 5 * time.Minute

type scheduledJobStore interface {
	enqueue(context.Context, scheduledJob) error
	claimDue(context.Context, time.Time, int, string) ([]scheduledJob, error)
	renew(context.Context, string, string) (bool, error)
	complete(context.Context, string, string) error
	release(context.Context, string, string) error
}

func newScheduledJobID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return prefix + hex.EncodeToString(bytes[:])
	}
	return prefix + strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
}
