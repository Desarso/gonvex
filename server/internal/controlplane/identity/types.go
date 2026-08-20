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
	Source        string
	LegacyUserID  string
	Provider      string
	Issuer        string
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	AvatarURL     string
}

type ExistingAccount struct {
	Account    Account
	Identities []AccountIdentity
}

type ResolutionKind string

const (
	ResolutionProviderSubject ResolutionKind = "provider_subject"
	ResolutionVerifiedEmail   ResolutionKind = "verified_email"
	ResolutionNewAccount      ResolutionKind = "new_account"
	ResolutionCollision       ResolutionKind = "collision"
)

type LegacyAccountResolution struct {
	Legacy      LegacyIdentity
	Account     Account
	Identity    AccountIdentity
	Kind        ResolutionKind
	Candidates  []string
	NeedsReview bool
}

type IdentityResolutionResult struct {
	Items      []LegacyAccountResolution
	Collisions []LegacyAccountResolution
}

type MigrationPlan struct {
	RunID               string
	Source              string
	Checksum            string
	Items               []LegacyAccountResolution
	Collisions          []LegacyAccountResolution
	LegacyRows          int
	UniqueAccounts      int
	ProviderMatches     int
	EmailMatches        int
	NewAccounts         int
	AmbiguousCollisions int
}

type MigrationRun struct {
	ID           string
	Source       string
	PlanChecksum string
	Status       string
}

type MigrationCheckpoint struct {
	RunID            string
	Scope            string
	CompletedIndex   int
	LastLegacyUserID string
	RowsProcessed    int
	Checksum         string
	Status           string
}

type VerificationFinding struct {
	Code      string
	Scope     string
	LegacyID  string
	AccountID string
	Detail    string
}

type VerificationResult struct {
	PlanChecksum string
	Findings     []VerificationFinding
}
