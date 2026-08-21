package manifest

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
)

// ValidateTypeScriptContract proves that the delivery manifest and executable
// module artifact describe exactly the same TypeScript application. The
// runtime uses the top-level map for routing/replication and the embedded map
// for V8 dispatch, so accepting drift between them would create two sources of
// truth for one function.
func (m Manifest) ValidateTypeScriptContract() error {
	if m.Module == nil {
		return fmt.Errorf("Gonvex v2 requires a TypeScript module artifact")
	}
	if err := m.Module.Validate(); err != nil {
		return err
	}
	if len(m.Functions) != len(m.Module.Functions) {
		return fmt.Errorf("manifest function map does not match the TypeScript module artifact")
	}
	for path, moduleFunction := range m.Module.Functions {
		entry, exists := m.Functions[path]
		if !exists {
			return fmt.Errorf("manifest function %q exists only in the TypeScript module artifact", path)
		}
		expected := FunctionEntry{
			Kind:               moduleFunction.Kind,
			Handler:            moduleFunction.Handler,
			File:               moduleFunction.File,
			Args:               moduleFunction.Args,
			Result:             moduleFunction.Result,
			Internal:           moduleFunction.Internal,
			Delivery:           moduleFunction.Delivery,
			Dependencies:       moduleFunction.Dependencies,
			Replica:            moduleFunction.Replica,
			Offline:            moduleFunction.Offline,
			Optimistic:         moduleFunction.Optimistic,
			ActionProfile:      moduleFunction.ActionProfile,
			ActionCapabilities: moduleFunction.ActionCapabilities,
		}
		if !reflect.DeepEqual(entry, expected) {
			return fmt.Errorf("manifest function %q does not match the TypeScript module artifact", path)
		}
	}
	if !jsonEquivalent(m.Visibility, m.Module.Visibility) {
		return fmt.Errorf("manifest visibility does not match the TypeScript module artifact")
	}
	return nil
}

func jsonEquivalent(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	var leftValue, rightValue any
	if json.Unmarshal(leftJSON, &leftValue) != nil || json.Unmarshal(rightJSON, &rightValue) != nil {
		return false
	}
	if isEmptyJSONMap(leftValue) && isEmptyJSONMap(rightValue) {
		return true
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func isEmptyJSONMap(value any) bool {
	if value == nil {
		return true
	}
	mapping, ok := value.(map[string]any)
	return ok && len(mapping) == 0
}

func validateModuleSchema(schema ModuleSchema, path string, allowOptional bool) error {
	if schema == nil {
		return fmt.Errorf("%s is required", path)
	}
	kind, ok := schema["kind"].(string)
	if !ok || strings.TrimSpace(kind) == "" {
		return fmt.Errorf("%s.kind is required", path)
	}
	allowed := map[string]bool{"kind": true}
	switch kind {
	case "string":
		allowed["format"], allowed["minLength"], allowed["maxLength"] = true, true, true
		if format, exists := schema["format"]; exists {
			value, valid := format.(string)
			if !valid || (value != "email" && value != "uri" && value != "uuid" && value != "datetime") {
				return fmt.Errorf("%s.format is unsupported", path)
			}
		}
		if err := validateNonNegativeInteger(schema, path, "minLength"); err != nil {
			return err
		}
		if err := validateNonNegativeInteger(schema, path, "maxLength"); err != nil {
			return err
		}
	case "number":
		allowed["integer"], allowed["minimum"], allowed["maximum"] = true, true, true
		if integer, exists := schema["integer"]; exists {
			if _, ok := integer.(bool); !ok {
				return fmt.Errorf("%s.integer must be boolean", path)
			}
		}
		for _, field := range []string{"minimum", "maximum"} {
			if value, exists := schema[field]; exists {
				if _, ok := numericValue(value); !ok {
					return fmt.Errorf("%s.%s must be a number", path, field)
				}
			}
		}
	case "boolean", "null", "any":
	case "id":
		allowed["entity"] = true
		if value, ok := schema["entity"].(string); !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s.entity is required", path)
		}
	case "literal":
		allowed["value"] = true
		if _, exists := schema["value"]; !exists {
			return fmt.Errorf("%s.value is required", path)
		}
	case "array":
		allowed["items"] = true
		child, ok := asModuleSchema(schema["items"])
		if !ok {
			return fmt.Errorf("%s.items is required", path)
		}
		if err := validateModuleSchema(child, path+".items", false); err != nil {
			return err
		}
	case "record":
		allowed["values"] = true
		child, ok := asModuleSchema(schema["values"])
		if !ok {
			return fmt.Errorf("%s.values is required", path)
		}
		if err := validateModuleSchema(child, path+".values", false); err != nil {
			return err
		}
	case "optional":
		allowed["value"] = true
		if !allowOptional {
			return fmt.Errorf("%s optional schemas are allowed only on object fields", path)
		}
		child, ok := asModuleSchema(schema["value"])
		if !ok {
			return fmt.Errorf("%s.value is required", path)
		}
		if err := validateModuleSchema(child, path+".value", false); err != nil {
			return err
		}
	case "object":
		allowed["fields"], allowed["allowUnknown"] = true, true
		if allowUnknown, exists := schema["allowUnknown"]; exists {
			if _, ok := allowUnknown.(bool); !ok {
				return fmt.Errorf("%s.allowUnknown must be boolean", path)
			}
		}
		fields, ok := moduleSchemaFields(schema["fields"])
		if !ok {
			return fmt.Errorf("%s.fields is required", path)
		}
		for name, raw := range fields {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("%s.fields contains an empty name", path)
			}
			child, ok := asModuleSchema(raw)
			if !ok {
				return fmt.Errorf("%s.fields.%s is invalid", path, name)
			}
			if err := validateModuleSchema(child, path+".fields."+name, true); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s.kind %q is unsupported", path, kind)
	}
	for field := range schema {
		if !allowed[field] {
			return fmt.Errorf("%s.%s is unsupported", path, field)
		}
	}
	return nil
}

func asModuleSchema(value any) (ModuleSchema, bool) {
	switch typed := value.(type) {
	case ModuleSchema:
		return typed, true
	case map[string]any:
		return ModuleSchema(typed), true
	default:
		return nil, false
	}
}

func moduleSchemaFields(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[string]ModuleSchema:
		fields := make(map[string]any, len(typed))
		for name, schema := range typed {
			fields[name] = schema
		}
		return fields, true
	default:
		return nil, false
	}
}

func numericValue(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, !math.IsNaN(number) && !math.IsInf(number, 0)
	case float32:
		converted := float64(number)
		return converted, !math.IsNaN(converted) && !math.IsInf(converted, 0)
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case json.Number:
		converted, err := number.Float64()
		return converted, err == nil && !math.IsNaN(converted) && !math.IsInf(converted, 0)
	default:
		return 0, false
	}
}

func validateNonNegativeInteger(schema ModuleSchema, path, field string) error {
	value, exists := schema[field]
	if !exists {
		return nil
	}
	number, ok := numericValue(value)
	if !ok || number < 0 || number != math.Trunc(number) {
		return fmt.Errorf("%s.%s must be a non-negative integer", path, field)
	}
	return nil
}

func validateOfflinePolicy(value any, path string) error {
	policy, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("reducer %q must declare a valid offline policy", path)
	}
	mode, _ := policy["mode"].(string)
	switch mode {
	case "forbidden":
	case "onlineOnly":
		if reason, ok := policy["reason"].(string); !ok || strings.TrimSpace(reason) == "" {
			return fmt.Errorf("reducer %q onlineOnly policy requires a reason", path)
		}
	case "allowed":
		if conflict, exists := policy["conflict"]; exists {
			value, ok := conflict.(string)
			if !ok || (value != "reject" && value != "expectedVersion" && value != "merge") {
				return fmt.Errorf("reducer %q has an invalid offline conflict policy", path)
			}
		}
	default:
		return fmt.Errorf("reducer %q must declare a valid offline policy", path)
	}
	return nil
}

func validateOptimisticTransaction(value any, path string) error {
	transaction, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("reducer %q optimistic metadata is invalid", path)
	}
	effects, ok := transaction["effects"].([]any)
	if !ok || len(effects) == 0 {
		return fmt.Errorf("reducer %q optimistic metadata requires effects", path)
	}
	for index, raw := range effects {
		effect, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("reducer %q optimistic effect %d is invalid", path, index)
		}
		operation, _ := effect["operation"].(string)
		if operation != "patch" && operation != "upsert" && operation != "delete" {
			return fmt.Errorf("reducer %q optimistic effect %d has an invalid operation", path, index)
		}
		if entity, ok := effect["entity"].(string); !ok || strings.TrimSpace(entity) == "" {
			return fmt.Errorf("reducer %q optimistic effect %d requires an entity", path, index)
		}
		if !validOptimisticID(effect["id"]) {
			return fmt.Errorf("reducer %q optimistic effect %d requires an id", path, index)
		}
		if operation == "patch" {
			if _, ok := effect["fields"].(map[string]any); !ok {
				return fmt.Errorf("reducer %q optimistic patch effect %d requires fields", path, index)
			}
		}
		if operation == "upsert" {
			if _, ok := effect["value"].(map[string]any); !ok {
				return fmt.Errorf("reducer %q optimistic upsert effect %d requires a value", path, index)
			}
		}
	}
	return nil
}

func validOptimisticID(value any) bool {
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) != ""
	}
	parts, ok := value.([]any)
	if !ok || len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		text, ok := part.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return false
		}
	}
	return true
}
