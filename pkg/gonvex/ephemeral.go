package gonvex

import (
	"encoding/json"
	"errors"
	"time"
)

// ErrEphemeralNotConfigured is returned only by contexts constructed outside
// a Gonvex runtime without an Ephemeral implementation. A running server
// always provides either the shared Valkey backend or the single-process
// in-memory fallback.
var ErrEphemeralNotConfigured = errors.New("gonvex: ephemeral store is not configured")

// EphemeralEntry is one live item returned by EphemeralAPI.List. Key is the
// application-supplied logical key; project and tenant namespace components
// are never exposed to app code.
type EphemeralEntry struct {
	Key       string
	Value     json.RawMessage
	ExpiresAt time.Time
}

// Decode unmarshals the entry's JSON value into target.
func (entry EphemeralEntry) Decode(target any) error {
	return json.Unmarshal(entry.Value, target)
}

// EphemeralAPI is a tenant-scoped, JSON-valued store for short-lived state.
// Every Set requires a positive TTL. List returns only live entries whose
// logical key starts with prefix; it is backed by a per-project/tenant expiry
// index rather than a Redis keyspace scan.
//
// The runtime automatically scopes every operation to the executing project
// and tenant. App functions cannot select or override that namespace.
// RuntimeContext.ProjectEphemeral exposes a separate project-wide namespace
// for explicitly cross-tenant transient state.
type EphemeralAPI interface {
	Set(key string, value any, ttl time.Duration) error
	Get(key string, target any) (found bool, err error)
	Delete(key string) error
	List(prefix string) ([]EphemeralEntry, error)
}

type ephemeralUnavailable struct{}

func (ephemeralUnavailable) Set(string, any, time.Duration) error {
	return ErrEphemeralNotConfigured
}

func (ephemeralUnavailable) Get(string, any) (bool, error) {
	return false, ErrEphemeralNotConfigured
}

func (ephemeralUnavailable) Delete(string) error {
	return ErrEphemeralNotConfigured
}

func (ephemeralUnavailable) List(string) ([]EphemeralEntry, error) {
	return nil, ErrEphemeralNotConfigured
}
