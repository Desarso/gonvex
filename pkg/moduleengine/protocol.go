package moduleengine

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ProtocolVersion must match the module host's. A host that speaks a different
// version is refused at connect time rather than at the first invocation.
const ProtocolVersion = 2

// DefaultMaxFrameBytes bounds one frame in either direction. It is large enough
// for a sizeable module bundle and small enough that a corrupt length prefix
// cannot make either side allocate its way out of memory.
const DefaultMaxFrameBytes = 64 << 20

const frameHeaderBytes = 4

// Wire error codes. They are the host's vocabulary, mapped onto Go dispatch
// errors by the engine so callers keep the error surface they already handle.
const (
	codeBadRequest           = "bad_request"
	codeUnsupported          = "unsupported"
	codeFrameTooLarge        = "frame_too_large"
	codeInvalidArtifact      = "invalid_artifact"
	codeArtifactHashMismatch = "artifact_hash_mismatch"
	codeModuleLoadFailed     = "module_load_failed"
	codeGenerationConflict   = "generation_conflict"
	codeUnknownGeneration    = "unknown_generation"
	codeModuleNotLoaded      = "module_not_loaded"
	codeFunctionNotFound     = "function_not_found"
	codeWrongFunctionKind    = "wrong_function_kind"
	codeInvalidArgs          = "invalid_args"
	codeInvalidResult        = "invalid_result"
	codeDeadlineExceeded     = "deadline_exceeded"
	codeBudgetExceeded       = "budget_exceeded"
	codeExecutionFailed      = "execution_failed"
	codeCancelled            = "cancelled"
	codeOverloaded           = "overloaded"
	codeShuttingDown         = "shutting_down"
	codeHostCallFailed       = "host_call_failed"
)

// ErrFrameTooLarge is returned when either side would exceed the frame budget.
var ErrFrameTooLarge = errors.New("moduleengine: frame exceeds the maximum frame size")

// HostError is a precise failure reported by the module host.
type HostError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (e *HostError) Error() string {
	if e == nil {
		return "moduleengine: unknown module host error"
	}
	if e.Code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// outboundFrame is every frame the runtime sends. One struct carries all four
// shapes because only the tagged fields are ever populated, and omitempty keeps
// the wire free of fields a frame does not use.
type outboundFrame struct {
	Type           string          `json:"type"`
	ID             uint64          `json:"id"`
	DeadlineUnixMS int64           `json:"deadlineUnixMs,omitempty"`
	Payload        any             `json:"payload,omitempty"`
	Value          json.RawMessage `json:"value,omitempty"`
	Error          *HostError      `json:"error,omitempty"`
}

// inboundFrame is every frame the host sends.
type inboundFrame struct {
	Type       string          `json:"type"`
	ID         uint64          `json:"id"`
	Invocation uint64          `json:"invocation"`
	Protocol   int             `json:"protocol"`
	Version    string          `json:"version"`
	Payload    json.RawMessage `json:"payload"`
	Error      *HostError      `json:"error"`
}

const (
	frameRequest      = "request"
	frameHostResponse = "hostResponse"
	frameHostError    = "hostError"
	frameCancel       = "cancel"

	frameReady    = "ready"
	frameResponse = "response"
	frameError    = "error"
	frameHostCall = "hostCall"
)

// Request operations. The `op` tag names the operation; the host rejects
// anything it does not know rather than guessing.
type pingOp struct {
	Op string `json:"op"`
}

type loadOp struct {
	Op         string          `json:"op"`
	ModuleID   string          `json:"moduleId"`
	Generation *uint64         `json:"generation,omitempty"`
	Artifact   artifactPayload `json:"artifact"`
}

type activateOp struct {
	Op         string `json:"op"`
	ModuleID   string `json:"moduleId"`
	Generation uint64 `json:"generation"`
	DrainMS    uint64 `json:"drainMs,omitempty"`
}

type describeOp struct {
	Op       string `json:"op"`
	ModuleID string `json:"moduleId"`
}

type unloadOp struct {
	Op       string `json:"op"`
	ModuleID string `json:"moduleId"`
	DrainMS  uint64 `json:"drainMs,omitempty"`
}

type shutdownOp struct {
	Op      string `json:"op"`
	GraceMS uint64 `json:"graceMs,omitempty"`
}

type invokeOp struct {
	Op         string            `json:"op"`
	ModuleID   string            `json:"moduleId"`
	Generation uint64            `json:"generation,omitempty"`
	Function   string            `json:"function"`
	Kind       string            `json:"kind"`
	Args       string            `json:"args"`
	Context    invocationContext `json:"context"`
}

// invocationContext is everything the host may know about a call. It carries
// identity and scope, never a database URL or a credential: the module reaches
// data only by asking the runtime, which already holds the transaction.
type invocationContext struct {
	ProjectID      string            `json:"projectId"`
	TenantID       string            `json:"tenantId"`
	OperationID    string            `json:"operationId,omitempty"`
	Tenant         *tenantIdentity   `json:"tenant,omitempty"`
	Account        *accountIdentity  `json:"account,omitempty"`
	Member         *memberIdentity   `json:"member,omitempty"`
	Permissions    any               `json:"permissions,omitempty"`
	Environment    map[string]string `json:"environment,omitempty"`
	ActionTools    []string          `json:"actionTools,omitempty"`
	Capabilities   capabilities      `json:"capabilities"`
	NowUnixMS      int64             `json:"nowUnixMs"`
	DeadlineUnixMS int64             `json:"deadlineUnixMs,omitempty"`
}

type tenantIdentity struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId,omitempty"`
	Name      string `json:"name,omitempty"`
}

type accountIdentity struct {
	ID        string `json:"id"`
	Email     string `json:"email,omitempty"`
	Name      string `json:"name,omitempty"`
	AvatarURL string `json:"avatarUrl,omitempty"`
}

type memberIdentity struct {
	ID          string `json:"id"`
	AccountID   string `json:"accountId,omitempty"`
	Status      string `json:"status,omitempty"`
	Role        string `json:"role,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Permissions any    `json:"permissions,omitempty"`
}

// capabilities is the host's grant. The module host intersects it with what the
// function kind may structurally reach, so a grant can only ever narrow.
type capabilities struct {
	DBRead       bool `json:"dbRead"`
	DBWrite      bool `json:"dbWrite"`
	ActionOutbox bool `json:"actionOutbox"`
	ActionTools  bool `json:"actionTools"`
	Scheduler    bool `json:"scheduler"`
	Network      bool `json:"network"`
	Storage      bool `json:"storage"`
	Secrets      bool `json:"secrets"`
}

// artifactPayload is the module as the runtime holds it. The host re-verifies
// the JavaScript hash before it evaluates anything.
type artifactPayload struct {
	Language   string            `json:"language"`
	Entrypoint string            `json:"entrypoint,omitempty"`
	Hash       string            `json:"hash"`
	JavaScript javaScriptPayload `json:"javascript"`
	Functions  []functionPayload `json:"functions"`
	Metadata   map[string]any    `json:"metadata,omitempty"`
}

type javaScriptPayload struct {
	Path string `json:"path,omitempty"`
	Hash string `json:"hash"`
	Code string `json:"code"`
}

type functionPayload struct {
	Path     string         `json:"path"`
	Kind     string         `json:"kind"`
	Internal bool           `json:"internal,omitempty"`
	Delivery string         `json:"delivery,omitempty"`
	Handler  string         `json:"handler,omitempty"`
	Export   string         `json:"export,omitempty"`
	File     string         `json:"file,omitempty"`
	Args     any            `json:"args,omitempty"`
	Result   any            `json:"result,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Response payloads, discriminated by `result`.
type responseEnvelope struct {
	Result string `json:"result"`
}

type loadedResponse struct {
	ModuleID   string            `json:"moduleId"`
	Generation uint64            `json:"generation"`
	Functions  []functionSummary `json:"functions"`
}

type activatedResponse struct {
	ModuleID   string  `json:"moduleId"`
	Generation uint64  `json:"generation"`
	Retired    *uint64 `json:"retired"`
}

type describedResponse struct {
	ModuleID   string            `json:"moduleId"`
	Generation *uint64           `json:"generation"`
	Functions  []functionSummary `json:"functions"`
}

type invokedResponse struct {
	Value string `json:"value"`
}

type shuttingDownResponse struct {
	Drained bool `json:"drained"`
}

type pongResponse struct {
	Protocol int    `json:"protocol"`
	Version  string `json:"version"`
}

type functionSummary struct {
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Internal bool   `json:"internal"`
	Delivery string `json:"delivery,omitempty"`
}

// hostCallPayload is a host operation the module asked for while an invocation
// is running. Writes name a table, a key and an object; the statement text for
// a write is built here in Go, with quoted identifiers and bound parameters.
type hostCallPayload struct {
	Kind       string          `json:"kind"`
	Statement  string          `json:"statement"`
	Parameters json.RawMessage `json:"parameters"`
	Table      string          `json:"table"`
	Row        json.RawMessage `json:"row"`
	Key        string          `json:"key"`
	ID         json.RawMessage `json:"id"`
	Patch      json.RawMessage `json:"patch"`
	Function   string          `json:"function"`
	Tool       string          `json:"tool"`
	Args       json.RawMessage `json:"args"`
	DelayMS    int64           `json:"delayMs"`
	AtUnixMS   int64           `json:"atUnixMs"`
	Request    json.RawMessage `json:"request"`
	Operation  string          `json:"operation"`
	Payload    json.RawMessage `json:"payload"`
}

const (
	hostCallDBQuery       = "dbQuery"
	hostCallDBInsert      = "dbInsert"
	hostCallDBUpdate      = "dbUpdate"
	hostCallDBDelete      = "dbDelete"
	hostCallActionEnqueue = "actionEnqueue"
	hostCallToolInvoke    = "toolInvoke"
	hostCallScheduleAfter = "scheduleAfter"
	hostCallScheduleAt    = "scheduleAt"
	hostCallFetch         = "fetch"
	hostCallStorage       = "storage"
)

// readFrame reads one length-prefixed JSON frame. The prefix is what bounds the
// protocol: the size is known before anything is allocated for it.
func readFrame(reader io.Reader, limit int) ([]byte, error) {
	var header [frameHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(header[:]))
	if size == 0 {
		return nil, fmt.Errorf("moduleengine: module host sent an empty frame")
	}
	if limit > 0 && size > limit {
		return nil, fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrFrameTooLarge, size, limit)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	return payload, nil
}

func writeFrame(writer io.Writer, payload []byte, limit int) error {
	if limit > 0 && len(payload) > limit {
		return fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrFrameTooLarge, len(payload), limit)
	}
	// Header and payload go out as one write so a concurrent writer can never
	// interleave itself between a length prefix and the bytes it describes.
	buffer := make([]byte, frameHeaderBytes+len(payload))
	binary.BigEndian.PutUint32(buffer[:frameHeaderBytes], uint32(len(payload)))
	copy(buffer[frameHeaderBytes:], payload)
	_, err := writer.Write(buffer)
	return err
}

// normalizeKind canonicalizes the public Query/Reducer/Action vocabulary the
// host speaks.
func normalizeKind(kind Kind) string {
	switch kind {
	case KindQuery, KindReducer, KindAction:
		return string(kind)
	default:
		return strings.ToLower(strings.TrimSpace(string(kind)))
	}
}
