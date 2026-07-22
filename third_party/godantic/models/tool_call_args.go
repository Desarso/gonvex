package models

import (
	"encoding/json"
	"strings"
)

// UnmarshalToolCallArguments parses a tool-call arguments JSON string into a
// map. Empty input becomes {}.
//
// Some providers (notably via OpenRouter streaming) can leave trailing junk —
// commonly a second concatenated object `{...}{...}` when multiple parallel
// tool calls were incorrectly merged. Prefer the first valid JSON value so the
// call still has usable args instead of silently becoming {}.
func UnmarshalToolCallArguments(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, nil
	}

	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err == nil {
		if args == nil {
			args = map[string]any{}
		}
		return args, nil
	}

	dec := json.NewDecoder(strings.NewReader(raw))
	if err := dec.Decode(&args); err != nil {
		return map[string]any{}, err
	}
	if args == nil {
		args = map[string]any{}
	}
	return args, nil
}
