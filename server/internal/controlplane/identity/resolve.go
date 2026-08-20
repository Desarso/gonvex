package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// ResolveLegacyAccounts applies the only automatic merge rules permitted by
// identity-v2: an exact provider/issuer/subject match first, then one unique
// verified-email match. Unverified email never merges accounts. Ambiguous
// matches are returned for human review and are never silently merged.
func ResolveLegacyAccounts(records []LegacyIdentity, existing []ExistingAccount) IdentityResolutionResult {
	ordered := append([]LegacyIdentity(nil), records...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Source != ordered[j].Source {
			return ordered[i].Source < ordered[j].Source
		}
		return ordered[i].LegacyUserID < ordered[j].LegacyUserID
	})
	provider := map[string]map[string]bool{}
	email := map[string]map[string]bool{}
	accounts := map[string]Account{}
	for _, item := range existing {
		accounts[item.Account.ID] = item.Account
		for _, identity := range item.Identities {
			if strings.TrimSpace(identity.Provider) != "" && strings.TrimSpace(identity.Subject) != "" {
				addIndex(provider, identityKey(identity.Provider, identity.Issuer, identity.Subject), item.Account.ID)
			}
			if identity.VerifiedEmail {
				verifiedEmail := identity.Email
				if verifiedEmail == "" {
					verifiedEmail = item.Account.Email
				}
				addIndex(email, normalizeEmail(verifiedEmail), item.Account.ID)
			}
		}
	}
	result := IdentityResolutionResult{}
	for _, legacy := range ordered {
		kind := ResolutionNewAccount
		candidates := []string{}
		accountID := ""
		providerKey := identityKey(legacy.Provider, legacy.Issuer, legacy.Subject)
		if strings.TrimSpace(legacy.Provider) != "" && strings.TrimSpace(legacy.Subject) != "" {
			candidates = sortedIDs(provider[providerKey])
			if len(candidates) == 1 {
				accountID, kind = candidates[0], ResolutionProviderSubject
			} else if len(candidates) > 1 {
				kind = ResolutionCollision
			}
		}
		if kind == ResolutionNewAccount && legacy.EmailVerified {
			candidates = sortedIDs(email[normalizeEmail(legacy.Email)])
			if len(candidates) == 1 {
				accountID, kind = candidates[0], ResolutionVerifiedEmail
			} else if len(candidates) > 1 {
				kind = ResolutionCollision
			}
		}
		if kind == ResolutionNewAccount {
			accountID = deterministicAccountID(legacy.Source, legacy.LegacyUserID)
			for accounts[accountID].ID != "" {
				accountID = deterministicAccountID(accountID, legacy.LegacyUserID)
			}
			accounts[accountID] = Account{ID: accountID, Email: normalizeEmail(legacy.Email), Name: legacy.Name, AvatarURL: legacy.AvatarURL}
		}
		item := LegacyAccountResolution{
			Legacy:   legacy,
			Account:  accounts[accountID],
			Identity: AccountIdentity{AccountID: accountID, Provider: strings.TrimSpace(legacy.Provider), Issuer: strings.TrimSpace(legacy.Issuer), Subject: strings.TrimSpace(legacy.Subject), Email: normalizeEmail(legacy.Email), VerifiedEmail: legacy.EmailVerified},
			Kind:     kind, Candidates: candidates, NeedsReview: kind == ResolutionCollision,
		}
		if kind == ResolutionCollision {
			result.Collisions = append(result.Collisions, item)
		} else {
			result.Items = append(result.Items, item)
			if item.Identity.VerifiedEmail {
				addIndex(email, normalizeEmail(legacy.Email), accountID)
			}
			if item.Identity.Provider != "" && item.Identity.Subject != "" {
				addIndex(provider, providerKey, accountID)
			}
		}
	}
	return result
}

func addIndex(index map[string]map[string]bool, key, accountID string) {
	if key == "" || accountID == "" {
		return
	}
	if index[key] == nil {
		index[key] = map[string]bool{}
	}
	index[key][accountID] = true
}

func sortedIDs(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func identityKey(provider, issuer, subject string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	issuer = strings.TrimSpace(issuer)
	subject = strings.TrimSpace(subject)
	if provider == "" || subject == "" {
		return ""
	}
	return provider + "\x00" + issuer + "\x00" + subject
}

func normalizeEmail(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func deterministicAccountID(source, legacyID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(source) + "\x00" + strings.TrimSpace(legacyID)))
	return fmt.Sprintf("acct_%s", hex.EncodeToString(sum[:])[:32])
}
