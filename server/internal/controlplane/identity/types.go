package identity

import "time"

// Account is the global human/service identity. Tenant-specific authorization
// must never be inferred from this record or from the directory index.
type Account struct {
	ID          string     `json:"id"`
	AuthRealmID string     `json:"authRealmId,omitempty"`
	Email       string     `json:"email,omitempty"`
	Name        string     `json:"name,omitempty"`
	AvatarURL   string     `json:"avatarUrl,omitempty"`
	DisabledAt  *time.Time `json:"disabledAt,omitempty"`
}

type AccountIdentity struct {
	AccountID     string `json:"accountId"`
	Provider      string `json:"provider"`
	Issuer        string `json:"issuer,omitempty"`
	Subject       string `json:"subject"`
	Email         string `json:"email,omitempty"`
	VerifiedEmail bool   `json:"verifiedEmail"`
}

// AccountTenantIndex is a directory projection used for tenant discovery and
// database routing. It is not an authorization decision; ValidateTenantMember
// must read the selected tenant database before admitting a request.
type AccountTenantIndex struct {
	AccountID                string `json:"accountId"`
	TenantID                 string `json:"tenantId"`
	MemberID                 string `json:"memberId"`
	Status                   string `json:"status"`
	TenantMembershipRevision int64  `json:"tenantMembershipRevision"`
}

type LegacyIdentity struct {
	Source        string `json:"source"`
	LegacyUserID  string `json:"legacyUserId"`
	Provider      string `json:"provider,omitempty"`
	Issuer        string `json:"issuer,omitempty"`
	Subject       string `json:"subject,omitempty"`
	Email         string `json:"email,omitempty"`
	EmailVerified bool   `json:"emailVerified"`
	Name          string `json:"name,omitempty"`
	AvatarURL     string `json:"avatarUrl,omitempty"`
}

type ExistingAccount struct {
	Account    Account           `json:"account"`
	Identities []AccountIdentity `json:"identities"`
}

type ResolutionKind string

const (
	ResolutionProviderSubject ResolutionKind = "provider_subject"
	ResolutionVerifiedEmail   ResolutionKind = "verified_email"
	ResolutionNewAccount      ResolutionKind = "new_account"
	ResolutionCollision       ResolutionKind = "collision"
)

type LegacyAccountResolution struct {
	Legacy      LegacyIdentity  `json:"legacy"`
	Account     Account         `json:"account"`
	Identity    AccountIdentity `json:"identity"`
	Kind        ResolutionKind  `json:"kind"`
	Candidates  []string        `json:"candidates,omitempty"`
	NeedsReview bool            `json:"needsReview"`
}

type IdentityResolutionResult struct {
	Items      []LegacyAccountResolution `json:"items"`
	Collisions []LegacyAccountResolution `json:"collisions"`
}

type MigrationPlan struct {
	RunID               string                    `json:"runId"`
	Source              string                    `json:"source"`
	Checksum            string                    `json:"checksum"`
	Items               []LegacyAccountResolution `json:"items"`
	Collisions          []LegacyAccountResolution `json:"collisions"`
	LegacyRows          int                       `json:"legacyRows"`
	UniqueAccounts      int                       `json:"uniqueAccounts"`
	ProviderMatches     int                       `json:"providerMatches"`
	EmailMatches        int                       `json:"emailMatches"`
	NewAccounts         int                       `json:"newAccounts"`
	AmbiguousCollisions int                       `json:"ambiguousCollisions"`
}

type MigrationRun struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	PlanChecksum string `json:"planChecksum"`
	Status       string `json:"status"`
}

type MigrationCheckpoint struct {
	RunID            string `json:"runId"`
	Scope            string `json:"scope"`
	CompletedIndex   int    `json:"completedIndex"`
	LastLegacyUserID string `json:"lastLegacyUserId"`
	RowsProcessed    int    `json:"rowsProcessed"`
	Checksum         string `json:"checksum"`
	Status           string `json:"status"`
}

type VerificationFinding struct {
	Code      string `json:"code"`
	Scope     string `json:"scope"`
	LegacyID  string `json:"legacyId,omitempty"`
	AccountID string `json:"accountId,omitempty"`
	Detail    string `json:"detail"`
}

type VerificationResult struct {
	PlanChecksum string                `json:"planChecksum"`
	Findings     []VerificationFinding `json:"findings"`
}
