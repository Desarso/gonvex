package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
)

// queryResultSemantics separates top-level query performance instrumentation
// from the result content used for cache revisions and unchanged-result
// suppression. The original payload still goes over the wire on full results
// for clients that read result.perf, while the current instrumentation is also
// available out of band on trace.queryPerf.
//
// Only an object-valued top-level "perf" field has these semantics. Nested
// fields and scalar fields named "perf" remain ordinary result data.
func queryResultSemantics(payload json.RawMessage) ([sha256.Size]byte, json.RawMessage) {
	semanticPayload := payload
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope == nil {
		return sha256.Sum256(semanticPayload), nil
	}
	perf, ok := envelope["perf"]
	if !ok || !bytes.HasPrefix(bytes.TrimSpace(perf), []byte("{")) {
		return sha256.Sum256(semanticPayload), nil
	}
	delete(envelope, "perf")
	if normalized, err := json.Marshal(envelope); err == nil {
		semanticPayload = normalized
	}
	return sha256.Sum256(semanticPayload), append(json.RawMessage(nil), perf...)
}
