package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const replicaProtocolVersion = 1

type replicaDirective struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Scope           string `json:"scope"`
	// VisibilityScope identifies (project, tenant, account, permissions) without
	// the code epoch. Replica Collections persist across deploys under this scope
	// and are authoritatively reconciled before becoming current.
	VisibilityScope string `json:"visibilityScope"`
	Epoch           string `json:"epoch"`
}

func (s *Server) replicaDirective(projectID string, tenantID string, caller callerContext) *replicaDirective {
	current := s.runtime.ManifestForProject(projectID)
	moduleHash := ""
	if current.Module != nil {
		moduleHash = current.Module.Identity()
	}
	epoch := hashReplicaValue(struct {
		ProtocolVersion int    `json:"protocolVersion"`
		Project         string `json:"project"`
		Database        string `json:"database"`
		Functions       any    `json:"functions"`
		Schema          any    `json:"schema"`
		ModuleHash      string `json:"moduleHash"`
	}{
		ProtocolVersion: replicaProtocolVersion,
		Project:         current.Project,
		Database:        s.databaseURLForTenant(projectID, tenantID),
		Functions:       current.Functions,
		Schema:          current.Schema.Normalize(),
		ModuleHash:      moduleHash,
	})

	accountID := "anonymous"
	if caller.user != nil && caller.user.ID != "" {
		accountID = caller.user.ID
	}
	permissionsHash := hashReplicaValue(caller.permissions)
	scope := hashReplicaValue(struct {
		ProtocolVersion int    `json:"protocolVersion"`
		ProjectID       string `json:"projectId"`
		TenantID        string `json:"tenantId"`
		AccountID       string `json:"accountId"`
		PermissionsHash string `json:"permissionsHash"`
		Epoch           string `json:"epoch"`
	}{
		ProtocolVersion: replicaProtocolVersion,
		ProjectID:       projectID,
		TenantID:        tenantID,
		AccountID:       accountID,
		PermissionsHash: permissionsHash,
		Epoch:           epoch,
	})

	return &replicaDirective{
		ProtocolVersion: replicaProtocolVersion,
		Scope:           scope,
		VisibilityScope: replicaVisibilityScope(projectID, tenantID, caller),
		Epoch:           epoch,
	}
}

// replicaVisibilityScope keys Replica Collection state by who can see rows,
// independently of the module generation that produced a Live Query window.
func replicaVisibilityScope(projectID string, tenantID string, caller callerContext) string {
	accountID := "anonymous"
	if caller.user != nil && caller.user.ID != "" {
		accountID = caller.user.ID
	}
	return hashReplicaValue(struct {
		ProtocolVersion int    `json:"protocolVersion"`
		Kind            string `json:"kind"`
		ProjectID       string `json:"projectId"`
		TenantID        string `json:"tenantId"`
		AccountID       string `json:"accountId"`
		PermissionsHash string `json:"permissionsHash"`
	}{
		ProtocolVersion: replicaProtocolVersion,
		Kind:            "replica-visibility",
		ProjectID:       projectID,
		TenantID:        tenantID,
		AccountID:       accountID,
		PermissionsHash: hashReplicaValue(caller.permissions),
	})
}

func hashReplicaValue(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (s *Server) nextWindowRevision(contentHash ...[sha256.Size]byte) string {
	sequence := s.replicaSequence.Add(1)
	revision := fmt.Sprintf("%013d:%020d", s.replicaStartedAtMS, sequence)
	if len(contentHash) > 0 {
		revision += ":" + hex.EncodeToString(contentHash[0][:])
	}
	return revision
}

func replicaRevisionMatchesHash(revision string, contentHash [sha256.Size]byte) bool {
	lastColon := strings.LastIndexByte(revision, ':')
	if lastColon < 0 || lastColon == len(revision)-1 {
		return false
	}
	return revision[lastColon+1:] == hex.EncodeToString(contentHash[:])
}
