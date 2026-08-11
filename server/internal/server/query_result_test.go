package server

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestQueryResultSemanticsCopiesPerformanceMetadataToTrace(t *testing.T) {
	payload := json.RawMessage(`{"page":[{"id":"task-1"}],"total":1,"perf":{"source":"tasksSQL","durationMs":4.25}}`)
	_, queryPerf := queryResultSemantics(payload)
	if string(queryPerf) != `{"source":"tasksSQL","durationMs":4.25}` {
		t.Fatalf("query perf = %s", queryPerf)
	}
	if !bytes.Contains(payload, []byte(`"perf"`)) {
		t.Fatal("semantic hashing mutated the backward-compatible result payload")
	}
	encodedTrace, err := json.Marshal(messageTrace{QueryPerf: queryPerf})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encodedTrace, []byte(`"queryPerf":{"source":"tasksSQL","durationMs":4.25}`)) {
		t.Fatalf("trace did not expose query perf: %s", encodedTrace)
	}
}

func TestQueryResultSemanticsKeepsNestedAndScalarPerfSemantic(t *testing.T) {
	nestedA, _ := queryResultSemantics(json.RawMessage(`{"row":{"perf":{"score":1}}}`))
	nestedB, _ := queryResultSemantics(json.RawMessage(`{"row":{"perf":{"score":2}}}`))
	if nestedA == nestedB {
		t.Fatal("nested perf data was incorrectly treated as top-level instrumentation")
	}
	scalarA, _ := queryResultSemantics(json.RawMessage(`{"perf":1}`))
	scalarB, _ := queryResultSemantics(json.RawMessage(`{"perf":2}`))
	if scalarA == scalarB {
		t.Fatal("scalar perf data was incorrectly treated as instrumentation")
	}
}
