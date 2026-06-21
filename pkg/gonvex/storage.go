package gonvex

import (
	"errors"
	"time"
)

// Storage errors surfaced to handlers. They mirror the small, explicit error
// set used elsewhere in the runtime so callers can branch on them with
// errors.Is.
var (
	// ErrStorageNotConfigured is returned by every StorageAPI method when the
	// runtime was started without S3-compatible storage configured.
	ErrStorageNotConfigured = errors.New("gonvex: storage is not configured")
	// ErrFileNotFound is returned when a file id has no metadata record.
	ErrFileNotFound = errors.New("gonvex: file not found")
	// ErrForbidden is returned when a caller is not allowed to access a file.
	ErrForbidden = errors.New("gonvex: forbidden")
)

// FileVisibility controls who may read a stored file. Permission enforcement
// happens in handler code (see ctx.User / ctx.Permissions); the visibility is
// recorded on the metadata row so handlers and download flows can reason about
// it consistently.
type FileVisibility string

const (
	// FileVisibilityPrivate restricts the file to its owner.
	FileVisibilityPrivate FileVisibility = "private"
	// FileVisibilityTenant allows any member of the tenant to read the file.
	FileVisibilityTenant FileVisibility = "tenant"
	// FileVisibilityPublic allows anyone with a URL to read the file.
	FileVisibilityPublic FileVisibility = "public"
)

// File lifecycle states recorded on the metadata row.
const (
	// FileStatusPending marks a file whose upload URL was issued but whose
	// bytes have not been confirmed in object storage yet.
	FileStatusPending = "pending"
	// FileStatusUploaded marks a file whose bytes are present in object storage.
	FileStatusUploaded = "uploaded"
)

// FileMetadata is the Postgres-backed record describing a stored object. The
// bytes live in S3-compatible object storage; everything needed for
// permissions, ownership, tenant isolation and lifecycle lives here.
type FileMetadata struct {
	ID          string         `json:"id"`
	TenantID    string         `json:"tenantId"`
	OwnerID     string         `json:"ownerId,omitempty"`
	Bucket      string         `json:"bucket"`
	ObjectKey   string         `json:"objectKey"`
	ContentType string         `json:"contentType,omitempty"`
	Size        int64          `json:"size"`
	Checksum    string         `json:"checksum,omitempty"`
	Visibility  FileVisibility `json:"visibility"`
	Status      string         `json:"status"`
	CreatedAt   time.Time      `json:"createdAt"`
	UploadedAt  *time.Time     `json:"uploadedAt,omitempty"`
}

// UploadOptions describes the file a client intends to upload (or that an
// action wants to store directly).
type UploadOptions struct {
	// ContentType is the MIME type the object will be stored with. Optional.
	ContentType string
	// Size is the expected byte length, recorded for billing/usage. Optional.
	Size int64
	// Visibility defaults to FileVisibilityPrivate when empty.
	Visibility FileVisibility
	// OwnerID associates the file with a user. Defaults to ctx.User.ID.
	OwnerID string
	// Expires overrides the presigned upload URL lifetime. Optional.
	Expires time.Duration
}

// UploadTarget is what GenerateUploadURL hands back to the frontend: a
// short-lived URL the client uploads to directly, plus the file id used to
// reference the object afterwards.
type UploadTarget struct {
	FileID  string            `json:"fileId"`
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
}

// StorageAPI is the Convex-equivalent storage surface exposed on every runtime
// context as ctx.Storage. The concrete implementation lives in pkg/storage and
// is bound per request to the active tenant database and bucket.
type StorageAPI interface {
	// GenerateUploadURL records a pending file and returns a presigned URL the
	// client PUTs the bytes to directly.
	GenerateUploadURL(opts UploadOptions) (UploadTarget, error)
	// GetURL returns a URL the caller can use to read the file. Public files
	// get a stable URL; private/tenant files get a short-lived signed URL.
	GetURL(fileID string) (string, error)
	// GenerateDownloadURL returns a signed read URL valid for ttl.
	GenerateDownloadURL(fileID string, ttl time.Duration) (string, error)
	// GetMetadata returns the metadata record, confirming the object in storage
	// if the upload had not yet been finalized.
	GetMetadata(fileID string) (FileMetadata, error)
	// Delete removes both the object and its metadata record.
	Delete(fileID string) error
	// Store uploads bytes from server code (e.g. an action) and returns the
	// finalized metadata record.
	Store(content []byte, opts UploadOptions) (FileMetadata, error)
}

// storageUnavailable is the StorageAPI used when no storage backend is
// configured. It fails every call with ErrStorageNotConfigured so handlers get
// a clean, branchable error instead of a nil-pointer panic.
type storageUnavailable struct{}

func (storageUnavailable) GenerateUploadURL(UploadOptions) (UploadTarget, error) {
	return UploadTarget{}, ErrStorageNotConfigured
}

func (storageUnavailable) GetURL(string) (string, error) {
	return "", ErrStorageNotConfigured
}

func (storageUnavailable) GenerateDownloadURL(string, time.Duration) (string, error) {
	return "", ErrStorageNotConfigured
}

func (storageUnavailable) GetMetadata(string) (FileMetadata, error) {
	return FileMetadata{}, ErrStorageNotConfigured
}

func (storageUnavailable) Delete(string) error {
	return ErrStorageNotConfigured
}

func (storageUnavailable) Store([]byte, UploadOptions) (FileMetadata, error) {
	return FileMetadata{}, ErrStorageNotConfigured
}
