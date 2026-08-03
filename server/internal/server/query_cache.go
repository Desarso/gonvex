package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	queryCacheProtocolVersion = 1
	queryCacheMaxAge          = 24 * time.Hour
)

type queryCacheDirective struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Scope           string `json:"scope"`
	// SyncScope identifies (project, tenant, user, permissions) without the
	// code epoch. Sync collections are persisted and resumed under this scope
	// so a deploy does not invalidate them; correctness across code changes is
	// guaranteed by the authoritative reconcile that every resume performs.
	SyncScope string `json:"syncScope"`
	Epoch     string `json:"epoch"`
	MaxAgeMS  int64  `json:"maxAgeMs"`
}

func (s *Server) queryCacheDirective(projectID string, tenantID string, caller callerContext) *queryCacheDirective {
	if !s.config.QueryCacheEnabled {
		return nil
	}

	current := s.runtime.ManifestForProject(projectID)
	bundleHash := ""
	if current.Bundle != nil {
		bundleHash = current.Bundle.Hash
	}
	epoch := hashQueryCacheValue(struct {
		ProtocolVersion int    `json:"protocolVersion"`
		Project         string `json:"project"`
		Database        string `json:"database"`
		Functions       any    `json:"functions"`
		Schema          any    `json:"schema"`
		BundleHash      string `json:"bundleHash"`
	}{
		ProtocolVersion: queryCacheProtocolVersion,
		Project:         current.Project,
		Database:        s.databaseURLForTenant(projectID, tenantID),
		Functions:       current.Functions,
		Schema:          current.Schema.Normalize(),
		BundleHash:      bundleHash,
	})

	userID := "anonymous"
	if caller.user != nil && caller.user.ID != "" {
		userID = caller.user.ID
	}
	permissionsHash := hashQueryCacheValue(caller.permissions)
	scope := hashQueryCacheValue(struct {
		ProtocolVersion int    `json:"protocolVersion"`
		ProjectID       string `json:"projectId"`
		TenantID        string `json:"tenantId"`
		UserID          string `json:"userId"`
		PermissionsHash string `json:"permissionsHash"`
		Epoch           string `json:"epoch"`
	}{
		ProtocolVersion: queryCacheProtocolVersion,
		ProjectID:       projectID,
		TenantID:        tenantID,
		UserID:          userID,
		PermissionsHash: permissionsHash,
		Epoch:           epoch,
	})

	return &queryCacheDirective{
		ProtocolVersion: queryCacheProtocolVersion,
		Scope:           scope,
		SyncScope:       syncVisibilityScope(projectID, tenantID, caller),
		Epoch:           epoch,
		MaxAgeMS:        queryCacheMaxAge.Milliseconds(),
	}
}

// syncVisibilityScope keys sync-collection persistence and cursors by who can
// see the rows — not by which code bundle produced them. Query results must be
// invalidated when code changes (the same query can compute a different
// answer), but sync collections are row projections whose staleness is always
// repaired by the authoritative reconcile on resume, so tying them to the
// bundle epoch only forces needless full re-hydrations after every deploy.
func syncVisibilityScope(projectID string, tenantID string, caller callerContext) string {
	userID := "anonymous"
	if caller.user != nil && caller.user.ID != "" {
		userID = caller.user.ID
	}
	return hashQueryCacheValue(struct {
		ProtocolVersion int    `json:"protocolVersion"`
		Kind            string `json:"kind"`
		ProjectID       string `json:"projectId"`
		TenantID        string `json:"tenantId"`
		UserID          string `json:"userId"`
		PermissionsHash string `json:"permissionsHash"`
	}{
		ProtocolVersion: queryCacheProtocolVersion,
		Kind:            "sync-visibility",
		ProjectID:       projectID,
		TenantID:        tenantID,
		UserID:          userID,
		PermissionsHash: hashQueryCacheValue(caller.permissions),
	})
}

func hashQueryCacheValue(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (s *Server) nextQueryCacheRevision(contentHash ...[sha256.Size]byte) string {
	sequence := s.queryCacheSequence.Add(1)
	revision := fmt.Sprintf("%013d:%020d", s.queryCacheStartedAtMS, sequence)
	if len(contentHash) > 0 {
		revision += ":" + hex.EncodeToString(contentHash[0][:])
	}
	return revision
}

func queryCacheRevisionMatchesHash(revision string, contentHash [sha256.Size]byte) bool {
	lastColon := strings.LastIndexByte(revision, ':')
	if lastColon < 0 || lastColon == len(revision)-1 {
		return false
	}
	return revision[lastColon+1:] == hex.EncodeToString(contentHash[:])
}
