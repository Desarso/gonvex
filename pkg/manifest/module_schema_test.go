package manifest

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestModuleSchemaRoundTripsPortableShape(t *testing.T) {
	want := ModuleSchema{
		"kind": "object",
		"fields": map[string]any{
			"taskId": ModuleSchema{"kind": "id", "entity": "tasks"},
			"title":  ModuleSchema{"kind": "optional", "value": ModuleSchema{"kind": "string", "minLength": float64(1)}},
		},
		"allowUnknown": false,
	}
	payload, err := json.Marshal(ModuleFunction{Kind: FunctionKindReducer, Args: want, Result: ModuleSchema{"kind": "null"}})
	if err != nil {
		t.Fatal(err)
	}
	var got ModuleFunction
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var normalizedWant ModuleSchema
	if err := json.Unmarshal(wantJSON, &normalizedWant); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Args, normalizedWant) {
		t.Fatalf("args schema changed across JSON round-trip:\n got %#v\nwant %#v", got.Args, want)
	}
}
