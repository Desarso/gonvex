package models

import (
	"testing"
)

func TestUnmarshalToolCallArgumentsEmpty(t *testing.T) {
	args, err := UnmarshalToolCallArguments("")
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 0 {
		t.Fatalf("got %#v", args)
	}
}

func TestUnmarshalToolCallArgumentsValid(t *testing.T) {
	args, err := UnmarshalToolCallArguments(`{"fileKey":"data_csv_abc","operation":"overview"}`)
	if err != nil {
		t.Fatal(err)
	}
	if args["fileKey"] != "data_csv_abc" || args["operation"] != "overview" {
		t.Fatalf("got %#v", args)
	}
}

func TestUnmarshalToolCallArgumentsConcatenatedObjects(t *testing.T) {
	// Classic symptom of accumulating parallel tool_calls under choice.Index.
	raw := `{"fileKey":"data_csv_abc","operation":"overview"}{"name":"tasks","limit":5}`
	args, err := UnmarshalToolCallArguments(raw)
	if err != nil {
		t.Fatal(err)
	}
	if args["fileKey"] != "data_csv_abc" || args["operation"] != "overview" {
		t.Fatalf("expected first object, got %#v", args)
	}
	if _, ok := args["name"]; ok {
		t.Fatalf("should not merge second object: %#v", args)
	}
}
