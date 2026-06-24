package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// Default presigned URL lifetimes.
const (
	defaultUploadTTL   = 15 * time.Minute
	defaultDownloadTTL = 10 * time.Minute
)

// filesTableDDL creates the per-tenant metadata table. Storage metadata is a
// system concern, so it is managed here directly rather than through the
// user-facing manifest schema. The shape mirrors the architecture doc's files
// table.
const filesTableDDL = `
CREATE TABLE IF NOT EXISTS _gonvex_files (
	id TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL,
	owner_id TEXT NOT NULL DEFAULT '',
	bucket TEXT NOT NULL,
	object_key TEXT NOT NULL,
	content_type TEXT NOT NULL DEFAULT '',
	size_bytes BIGINT NOT NULL DEFAULT 0,
	checksum TEXT NOT NULL DEFAULT '',
	visibility TEXT NOT NULL DEFAULT 'private',
	status TEXT NOT NULL DEFAULT 'pending',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	uploaded_at TIMESTAMPTZ,
	deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS _gonvex_files_tenant_idx ON _gonvex_files (tenant_id);
CREATE INDEX IF NOT EXISTS _gonvex_files_owner_idx ON _gonvex_files (owner_id);
`

const fileColumns = "id, tenant_id, owner_id, bucket, object_key, content_type, size_bytes, checksum, visibility, status, created_at, uploaded_at"

// Factory builds per-request Tenant storage handles, all sharing one S3 client.
// It is created once at server start and remembers which databases already have
// the metadata table so the DDL only runs once per database.
type Factory struct {
	client      *Client
	uploadTTL   time.Duration
	downloadTTL time.Duration

	ensuredMu sync.Mutex
	ensured   map[*sql.DB]struct{}
}

// NewFactory returns a Factory, or nil when storage is not configured. A nil
// Factory means the runtime falls back to the not-configured storage path.
func NewFactory(cfg Config) *Factory {
	if !cfg.Configured() {
		return nil
	}
	return &Factory{
		client:      NewClient(cfg),
		uploadTTL:   defaultUploadTTL,
		downloadTTL: defaultDownloadTTL,
		ensured:     map[*sql.DB]struct{}{},
	}
}

// Tenant binds the shared S3 client to a specific tenant database, project and
// caller. It implements gonvex.StorageAPI and is created fresh per request.
func (f *Factory) Tenant(ctx context.Context, db *sql.DB, projectID, tenantID, ownerID string) (*Tenant, error) {
	if f == nil {
		return nil, gonvex.ErrStorageNotConfigured
	}
	if db == nil {
		return nil, fmt.Errorf("storage: tenant database is required")
	}
	if err := f.ensureTable(ctx, db); err != nil {
		return nil, err
	}
	return &Tenant{
		ctx:         ctx,
		client:      f.client,
		db:          db,
		bucket:      f.client.Bucket(),
		projectID:   projectID,
		tenantID:    tenantID,
		ownerID:     ownerID,
		uploadTTL:   f.uploadTTL,
		downloadTTL: f.downloadTTL,
		now:         f.client.now,
	}, nil
}

func (f *Factory) ensureTable(ctx context.Context, db *sql.DB) error {
	f.ensuredMu.Lock()
	_, ok := f.ensured[db]
	f.ensuredMu.Unlock()
	if ok {
		return nil
	}
	if _, err := db.ExecContext(ctx, filesTableDDL); err != nil {
		return fmt.Errorf("storage: ensure files table: %w", err)
	}
	f.ensuredMu.Lock()
	f.ensured[db] = struct{}{}
	f.ensuredMu.Unlock()
	return nil
}

// FetchObject streams a stored object from the (internal) S3 endpoint, for the
// runtime's storage proxy handler. The caller must close the response body.
func (f *Factory) FetchObject(ctx context.Context, objectKey string) (*http.Response, error) {
	if f == nil {
		return nil, gonvex.ErrStorageNotConfigured
	}
	return f.client.GetObject(ctx, objectKey)
}

// VerifyProxyGet validates a storage-proxy URL signature + expiry for objectKey.
func (f *Factory) VerifyProxyGet(objectKey string, exp int64, sig string) bool {
	return f != nil && f.client.VerifyProxyGet(objectKey, exp, sig)
}

// Tenant is the per-request StorageAPI implementation.
type Tenant struct {
	ctx         context.Context
	client      *Client
	db          *sql.DB
	bucket      string
	projectID   string
	tenantID    string
	ownerID     string
	uploadTTL   time.Duration
	downloadTTL time.Duration
	now         func() time.Time
}

var _ gonvex.StorageAPI = (*Tenant)(nil)

// GenerateUploadURL records a pending file and returns a presigned PUT URL.
func (t *Tenant) GenerateUploadURL(opts gonvex.UploadOptions) (gonvex.UploadTarget, error) {
	fileID, err := newFileID()
	if err != nil {
		return gonvex.UploadTarget{}, err
	}
	visibility := normalizeVisibility(opts.Visibility)
	owner := firstNonEmpty(opts.OwnerID, t.ownerID)
	objectKey := t.objectKey(fileID)

	_, err = t.db.ExecContext(t.ctx, `
		INSERT INTO _gonvex_files (id, tenant_id, owner_id, bucket, object_key, content_type, size_bytes, visibility, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		fileID, t.tenantID, owner, t.bucket, objectKey, opts.ContentType, opts.Size, string(visibility), gonvex.FileStatusPending, t.now().UTC(),
	)
	if err != nil {
		return gonvex.UploadTarget{}, fmt.Errorf("storage: record upload: %w", err)
	}

	ttl := opts.Expires
	if ttl <= 0 {
		ttl = t.uploadTTL
	}
	url, err := t.client.PresignPut(objectKey, ttl)
	if err != nil {
		return gonvex.UploadTarget{}, err
	}

	target := gonvex.UploadTarget{FileID: fileID, URL: url, Method: "PUT"}
	if opts.ContentType != "" {
		target.Headers = map[string]string{"Content-Type": opts.ContentType}
	}
	return target, nil
}

// GetURL returns a public URL for public files and a short-lived signed URL
// otherwise. It confirms the upload first so a freshly uploaded file resolves.
func (t *Tenant) GetURL(fileID string) (string, error) {
	meta, err := t.GetMetadata(fileID)
	if err != nil {
		return "", err
	}
	if t.client.cfg.PublicBaseURL != "" {
		return t.client.ProxyGetURL(meta.ObjectKey, t.downloadTTL), nil
	}
	if meta.Visibility == gonvex.FileVisibilityPublic {
		return t.client.PublicURL(meta.ObjectKey), nil
	}
	return t.client.PresignGet(meta.ObjectKey, t.downloadTTL)
}

// GenerateDownloadURL returns a signed read URL valid for ttl.
func (t *Tenant) GenerateDownloadURL(fileID string, ttl time.Duration) (string, error) {
	meta, err := t.load(fileID)
	if err != nil {
		return "", err
	}
	if ttl <= 0 {
		ttl = t.downloadTTL
	}
	if t.client.cfg.PublicBaseURL != "" {
		return t.client.ProxyGetURL(meta.ObjectKey, ttl), nil
	}
	return t.client.PresignGet(meta.ObjectKey, ttl)
}

// GetMetadata returns the metadata record. When the file is still pending it
// HEADs object storage and, if the bytes are present, finalizes the record.
func (t *Tenant) GetMetadata(fileID string) (gonvex.FileMetadata, error) {
	meta, err := t.load(fileID)
	if err != nil {
		return gonvex.FileMetadata{}, err
	}
	if meta.Status == gonvex.FileStatusUploaded {
		return meta, nil
	}
	head, found, err := t.client.HeadObject(t.ctx, meta.ObjectKey)
	if err != nil {
		return gonvex.FileMetadata{}, err
	}
	if !found {
		return meta, nil
	}
	return t.finalize(meta, head)
}

// Delete removes the object and its metadata record.
func (t *Tenant) Delete(fileID string) error {
	meta, err := t.load(fileID)
	if err != nil {
		return err
	}
	if err := t.client.DeleteObject(t.ctx, meta.ObjectKey); err != nil {
		return err
	}
	if _, err := t.db.ExecContext(t.ctx, `DELETE FROM _gonvex_files WHERE id = $1`, fileID); err != nil {
		return fmt.Errorf("storage: delete metadata: %w", err)
	}
	return nil
}

// Store uploads bytes directly (e.g. from an action) and returns the finalized
// metadata.
func (t *Tenant) Store(content []byte, opts gonvex.UploadOptions) (gonvex.FileMetadata, error) {
	fileID, err := newFileID()
	if err != nil {
		return gonvex.FileMetadata{}, err
	}
	visibility := normalizeVisibility(opts.Visibility)
	owner := firstNonEmpty(opts.OwnerID, t.ownerID)
	objectKey := t.objectKey(fileID)
	contentType := opts.ContentType

	if err := t.client.PutObject(t.ctx, objectKey, content, contentType); err != nil {
		return gonvex.FileMetadata{}, err
	}

	now := t.now().UTC()
	checksum := sha256Hex(content)
	_, err = t.db.ExecContext(t.ctx, `
		INSERT INTO _gonvex_files (id, tenant_id, owner_id, bucket, object_key, content_type, size_bytes, checksum, visibility, status, created_at, uploaded_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
		fileID, t.tenantID, owner, t.bucket, objectKey, contentType, int64(len(content)), checksum, string(visibility), gonvex.FileStatusUploaded, now,
	)
	if err != nil {
		return gonvex.FileMetadata{}, fmt.Errorf("storage: record store: %w", err)
	}

	return gonvex.FileMetadata{
		ID:          fileID,
		TenantID:    t.tenantID,
		OwnerID:     owner,
		Bucket:      t.bucket,
		ObjectKey:   objectKey,
		ContentType: contentType,
		Size:        int64(len(content)),
		Checksum:    checksum,
		Visibility:  visibility,
		Status:      gonvex.FileStatusUploaded,
		CreatedAt:   now,
		UploadedAt:  &now,
	}, nil
}

// finalize marks a pending row uploaded using freshly-HEADed object metadata.
func (t *Tenant) finalize(meta gonvex.FileMetadata, head HeadResult) (gonvex.FileMetadata, error) {
	now := t.now().UTC()
	contentType := firstNonEmpty(head.ContentType, meta.ContentType)
	size := meta.Size
	if head.Size > 0 {
		size = head.Size
	}
	_, err := t.db.ExecContext(t.ctx, `
		UPDATE _gonvex_files
		SET status = $2, size_bytes = $3, content_type = $4, checksum = $5, uploaded_at = $6
		WHERE id = $1`,
		meta.ID, gonvex.FileStatusUploaded, size, contentType, head.ETag, now,
	)
	if err != nil {
		return gonvex.FileMetadata{}, fmt.Errorf("storage: finalize file: %w", err)
	}
	meta.Status = gonvex.FileStatusUploaded
	meta.Size = size
	meta.ContentType = contentType
	meta.Checksum = head.ETag
	meta.UploadedAt = &now
	return meta, nil
}

// load reads one metadata row.
func (t *Tenant) load(fileID string) (gonvex.FileMetadata, error) {
	row := t.db.QueryRowContext(t.ctx,
		`SELECT `+fileColumns+` FROM _gonvex_files WHERE id = $1`, fileID)
	meta, err := scanFile(row)
	if err == sql.ErrNoRows {
		return gonvex.FileMetadata{}, gonvex.ErrFileNotFound
	}
	if err != nil {
		return gonvex.FileMetadata{}, err
	}
	return meta, nil
}

// objectKey namespaces objects by project and tenant for isolation within a
// shared bucket.
func (t *Tenant) objectKey(fileID string) string {
	parts := make([]string, 0, 3)
	if p := sanitizeSegment(t.projectID); p != "" {
		parts = append(parts, p)
	}
	if tn := sanitizeSegment(t.tenantID); tn != "" {
		parts = append(parts, tn)
	}
	parts = append(parts, fileID)
	return path.Join(parts...)
}

// rowScanner is satisfied by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanFile(row rowScanner) (gonvex.FileMetadata, error) {
	var (
		meta       gonvex.FileMetadata
		visibility string
		uploadedAt sql.NullTime
	)
	if err := row.Scan(
		&meta.ID, &meta.TenantID, &meta.OwnerID, &meta.Bucket, &meta.ObjectKey,
		&meta.ContentType, &meta.Size, &meta.Checksum, &visibility, &meta.Status,
		&meta.CreatedAt, &uploadedAt,
	); err != nil {
		return gonvex.FileMetadata{}, err
	}
	meta.Visibility = gonvex.FileVisibility(visibility)
	if uploadedAt.Valid {
		ts := uploadedAt.Time
		meta.UploadedAt = &ts
	}
	return meta, nil
}

func normalizeVisibility(v gonvex.FileVisibility) gonvex.FileVisibility {
	switch v {
	case gonvex.FileVisibilityTenant, gonvex.FileVisibilityPublic, gonvex.FileVisibilityPrivate:
		return v
	default:
		return gonvex.FileVisibilityPrivate
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// sanitizeSegment keeps an object-key path segment to safe characters.
func sanitizeSegment(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9',
			ch == '-', ch == '_', ch == '.':
			builder.WriteRune(ch)
		default:
			builder.WriteByte('_')
		}
	}
	cleaned := strings.Trim(builder.String(), "_")
	// A segment made entirely of dots ("." / "..") would be collapsed by
	// path.Clean inside objectKey and could escape the prefix. Drop it.
	if strings.Trim(cleaned, ".") == "" {
		return ""
	}
	return cleaned
}

// newFileID returns a random, URL-safe file identifier.
func newFileID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("storage: generate file id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
