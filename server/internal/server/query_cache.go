package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const (
	queryCacheProtocolVersion = 1
	queryCacheMaxAge          = 24 * time.Hour
)

type queryCacheDirective struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Scope           string `json:"scope"`
	Epoch           string `json:"epoch"`
	MaxAgeMS        int64  `json:"maxAgeMs"`
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
		Functions       any    `json:"functions"`
		Schema          any    `json:"schema"`
		BundleHash      string `json:"bundleHash"`
	}{
		ProtocolVersion: queryCacheProtocolVersion,
		Project:         current.Project,
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
		Epoch:           epoch,
		MaxAgeMS:        queryCacheMaxAge.Milliseconds(),
	}
}

func hashQueryCacheValue(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (s *Server) nextQueryCacheRevision() string {
	sequence := s.queryCacheSequence.Add(1)
	return fmt.Sprintf("%013d:%020d", s.queryCacheStartedAtMS, sequence)
}
