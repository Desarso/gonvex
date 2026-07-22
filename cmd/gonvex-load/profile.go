package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const profileVersion = 1

var functionPathPattern = regexp.MustCompile(`^[A-Za-z0-9_/-]+\.[A-Za-z0-9_]+$`)
var exactPlaceholderPattern = regexp.MustCompile(`^\$\{([A-Za-z][A-Za-z0-9_]*)\}$`)

type Profile struct {
	Version       int                `json:"version"`
	Name          string             `json:"name"`
	Variables     map[string]string  `json:"variables,omitempty"`
	Subscriptions []SubscriptionSpec `json:"subscriptions"`
}

type SubscriptionSpec struct {
	Path string `json:"path"`
	Args any    `json:"args"`

	variables map[string]string
}

func loadProfileReader(reader io.Reader) (Profile, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	var profile Profile
	if err := decoder.Decode(&profile); err != nil {
		return Profile{}, fmt.Errorf("decode load profile: %w", err)
	}
	if profile.Version != profileVersion {
		return Profile{}, fmt.Errorf("profile version %d is unsupported; want %d", profile.Version, profileVersion)
	}
	if strings.TrimSpace(profile.Name) == "" {
		profile.Name = "unnamed"
	}
	if len(profile.Subscriptions) == 0 {
		return Profile{}, fmt.Errorf("profile must contain at least one subscription")
	}
	for index := range profile.Subscriptions {
		spec := &profile.Subscriptions[index]
		spec.Path = strings.TrimSpace(spec.Path)
		if !functionPathPattern.MatchString(spec.Path) {
			return Profile{}, fmt.Errorf("subscription %d has invalid function path %q", index, spec.Path)
		}
		if spec.Args == nil {
			return Profile{}, fmt.Errorf("subscription %d (%s) must define args", index, spec.Path)
		}
		if _, ok := spec.Args.(map[string]any); !ok {
			return Profile{}, fmt.Errorf("subscription %d (%s) args must be a JSON object", index, spec.Path)
		}
		spec.variables = cloneStrings(profile.Variables)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return Profile{}, fmt.Errorf("profile must contain exactly one JSON document")
	}
	return profile, nil
}

func (s SubscriptionSpec) expandedArgs(runtimeVariables map[string]string) (any, error) {
	variables := cloneStrings(s.variables)
	for key, value := range runtimeVariables {
		variables[key] = value
	}
	return expandProfileValue(s.Args, variables), nil
}

func expandProfileValue(value any, variables map[string]string) any {
	switch typed := value.(type) {
	case string:
		match := exactPlaceholderPattern.FindStringSubmatch(typed)
		if len(match) != 2 {
			return typed
		}
		if replacement, ok := variables[match[1]]; ok {
			return replacement
		}
		return typed
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = expandProfileValue(item, variables)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = expandProfileValue(item, variables)
		}
		return result
	default:
		return typed
	}
}

func cloneStrings(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
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
