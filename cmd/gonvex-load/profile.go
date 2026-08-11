package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"regexp"
	"strings"
	"time"
)

const profileVersion = 2

var functionPathPattern = regexp.MustCompile(`^[A-Za-z0-9_/-]+\.[A-Za-z0-9_]+$`)
var exactPlaceholderPattern = regexp.MustCompile(`^(?:\$\{([A-Za-z][A-Za-z0-9_]*)\}|\$([A-Za-z][A-Za-z0-9_]*))$`)

// Profile describes users, their open app tabs, and steady-state activity.
// Version 1 subscription-only profiles remain readable for previous load runs.
type Profile struct {
	Version                    int                `json:"version"`
	Name                       string             `json:"name"`
	Users                      int                `json:"users,omitempty"`
	ConnectionsPerUser         float64            `json:"connectionsPerUser,omitempty"`
	Pools                      map[string][]any   `json:"pools,omitempty"`
	Variables                  map[string]string  `json:"variables,omitempty"` // v1 compatibility
	SubscriptionsPerConnection []int              `json:"subscriptionsPerConnection,omitempty"`
	Subscriptions              []SubscriptionSpec `json:"subscriptions"`
	Mutations                  []MutationSpec     `json:"mutations,omitempty"`
}

type SubscriptionSpec struct {
	Path  string `json:"path"`
	Args  any    `json:"args"`
	Count int    `json:"count,omitempty"`

	pools map[string][]any
}

type MutationSpec struct {
	Path                 string  `json:"path"`
	Args                 any     `json:"args"`
	RatePerUserPerMinute float64 `json:"ratePerUserPerMinute,omitempty"`
	ActiveUsers          float64 `json:"activeUsers,omitempty"`
	RatePerMinute        float64 `json:"ratePerMinute,omitempty"`

	pools map[string][]any
}

func loadProfileReader(reader io.Reader) (Profile, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	var profile Profile
	if err := decoder.Decode(&profile); err != nil {
		return Profile{}, fmt.Errorf("decode load profile: %w", err)
	}
	if profile.Version != 1 && profile.Version != profileVersion {
		return Profile{}, fmt.Errorf("profile version %d is unsupported; want 1 or %d", profile.Version, profileVersion)
	}
	if strings.TrimSpace(profile.Name) == "" {
		profile.Name = "unnamed"
	}
	if profile.Version == 1 {
		profile.Users = 1
		profile.ConnectionsPerUser = 1
		profile.Pools = make(map[string][]any, len(profile.Variables))
		for name, value := range profile.Variables {
			profile.Pools[name] = []any{value}
		}
	} else {
		if profile.Users < 1 {
			return Profile{}, fmt.Errorf("users must be positive")
		}
		if profile.ConnectionsPerUser < 1 {
			return Profile{}, fmt.Errorf("connectionsPerUser must be at least 1")
		}
	}
	for name, values := range profile.Pools {
		if !validVariableName(name) {
			return Profile{}, fmt.Errorf("pool %q has an invalid name", name)
		}
		if len(values) == 0 {
			return Profile{}, fmt.Errorf("pool %q must contain at least one value", name)
		}
	}
	if len(profile.Subscriptions) == 0 {
		return Profile{}, fmt.Errorf("profile must contain at least one subscription")
	}
	for index := range profile.Subscriptions {
		spec := &profile.Subscriptions[index]
		if spec.Args == nil {
			spec.Args = map[string]any{}
		}
		if err := validateOperation(spec.Path, spec.Args, "subscription", index); err != nil {
			return Profile{}, err
		}
		if spec.Count == 0 {
			spec.Count = 1
		}
		if spec.Count < 1 {
			return Profile{}, fmt.Errorf("subscription %d (%s) count must be positive", index, spec.Path)
		}
		spec.pools = clonePools(profile.Pools)
	}
	availableSubscriptions := len(profile.expandedSubscriptions())
	for index, count := range profile.SubscriptionsPerConnection {
		if count < 0 || count > availableSubscriptions {
			return Profile{}, fmt.Errorf("subscriptionsPerConnection[%d] must be between 0 and %d", index, availableSubscriptions)
		}
	}
	for index := range profile.Mutations {
		spec := &profile.Mutations[index]
		if spec.Args == nil {
			spec.Args = map[string]any{}
		}
		if err := validateOperation(spec.Path, spec.Args, "mutation", index); err != nil {
			return Profile{}, err
		}
		if spec.RatePerUserPerMinute < 0 || spec.RatePerMinute < 0 {
			return Profile{}, fmt.Errorf("mutation %d (%s) rates cannot be negative", index, spec.Path)
		}
		if spec.RatePerUserPerMinute == 0 && spec.RatePerMinute == 0 {
			return Profile{}, fmt.Errorf("mutation %d (%s) must define a positive rate", index, spec.Path)
		}
		if spec.ActiveUsers == 0 {
			spec.ActiveUsers = 1
		}
		if spec.ActiveUsers < 0 || spec.ActiveUsers > 1 {
			return Profile{}, fmt.Errorf("mutation %d (%s) activeUsers must be between 0 and 1", index, spec.Path)
		}
		spec.pools = clonePools(profile.Pools)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return Profile{}, fmt.Errorf("profile must contain exactly one JSON document")
	}
	return profile, nil
}

func validateOperation(path string, args any, kind string, index int) error {
	path = strings.TrimSpace(path)
	if !functionPathPattern.MatchString(path) {
		return fmt.Errorf("%s %d has invalid function path %q", kind, index, path)
	}
	// Empty args are common in the production subscription inventory. Treat an
	// omitted args member as {}, while profiles that need seeded values use
	// explicit templates.
	if args == nil {
		args = map[string]any{}
	}
	if _, ok := args.(map[string]any); !ok {
		return fmt.Errorf("%s %d (%s) args must be a JSON object", kind, index, path)
	}
	return nil
}

func validVariableName(name string) bool {
	return regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`).MatchString(name)
}

func (p Profile) connectionCount(users int) int {
	if users < 1 {
		users = p.Users
	}
	return int(math.Round(float64(users) * p.ConnectionsPerUser))
}

func (p Profile) expandedSubscriptions() []SubscriptionSpec {
	result := make([]SubscriptionSpec, 0, len(p.Subscriptions))
	for _, spec := range p.Subscriptions {
		for copyIndex := 0; copyIndex < spec.Count; copyIndex++ {
			result = append(result, spec)
		}
	}
	return result
}

func (p Profile) subscriptionCount(connectionIndex, override int) int {
	if override >= 0 {
		return override
	}
	if len(p.SubscriptionsPerConnection) > 0 {
		return p.SubscriptionsPerConnection[deterministicIndex("subscriptions", connectionIndex, len(p.SubscriptionsPerConnection))]
	}
	return len(p.expandedSubscriptions())
}

func (p Profile) sessionVariables(userIndex int, overrides map[string]string) map[string]any {
	variables := make(map[string]any, len(p.Pools)+len(overrides))
	for name, values := range p.Pools {
		variables[name] = values[deterministicIndex(name, userIndex, len(values))]
	}
	for name, value := range overrides {
		variables[name] = value
	}
	return variables
}

func deterministicIndex(namespace string, index, size int) int {
	if size <= 1 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = fmt.Fprintf(hash, "%s:%d", namespace, index)
	return int(hash.Sum64() % uint64(size))
}

func (s SubscriptionSpec) expandedArgs(runtimeVariables map[string]any) (any, error) {
	return expandProfileValue(s.Args, runtimeVariables)
}

func (s MutationSpec) expandedArgs(runtimeVariables map[string]any) (map[string]any, error) {
	value, err := expandProfileValue(s.Args, runtimeVariables)
	if err != nil {
		return nil, err
	}
	args, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expanded mutation args are not an object")
	}
	return args, nil
}

func expandProfileValue(value any, variables map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		match := exactPlaceholderPattern.FindStringSubmatch(typed)
		if len(match) == 0 {
			return typed, nil
		}
		name := match[1]
		if name == "" {
			name = match[2]
		}
		replacement, ok := variables[name]
		if !ok {
			return nil, fmt.Errorf("placeholder %q has no pool, override, or built-in value", typed)
		}
		return replacement, nil
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			expanded, err := expandProfileValue(item, variables)
			if err != nil {
				return nil, err
			}
			result[index] = expanded
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			expanded, err := expandProfileValue(item, variables)
			if err != nil {
				return nil, err
			}
			result[key] = expanded
		}
		return result, nil
	default:
		return typed, nil
	}
}

func clonePools(values map[string][]any) map[string][]any {
	result := make(map[string][]any, len(values))
	for key, value := range values {
		result[key] = append([]any(nil), value...)
	}
	return result
}

func syntheticJWT(userID string) string {
	header, _ := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"sub":   userID,
		"email": userID + "@gonvex-load.invalid",
		"iat":   time.Now().Unix(),
	})
	encode := base64.RawURLEncoding.EncodeToString
	return encode(header) + "." + encode(payload) + ".synthetic"
}
