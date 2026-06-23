// Command clone-storage migrates Convex file-storage blobs into the Gonvex
// S3/MinIO bucket and records matching _gonvex_files metadata rows.
//
// Background: cloning a tenant from Convex (see the client's
// scripts/clone-tenant-from-convex.mjs) copies table rows only. Chat
// attachments are stored in messages as "[name](convex-file:<storageId>)", and
// the bytes live in Convex's file storage — which the row clone never touches.
// On Gonvex, files.getFileUrl resolves a storage id through ctx.Storage.GetURL,
// which reads a _gonvex_files row and presigns its object in the bucket. Without
// the bytes in MinIO and a metadata row, cloned attachments never load.
//
// This tool closes that gap. It reads a `convex export --include-file-storage`
// ZIP (or an already-extracted directory), uploads each blob to the bucket, and
// upserts a _gonvex_files row whose id IS the original Convex storage id — so
// existing "convex-file:<id>" references resolve with no message rewriting.
//
// READ-ONLY against the export; idempotent against the target (re-running skips
// rows that already exist unless --overwrite is set).
//
// Usage:
//
//	go run ./cmd/clone-storage \
//	  --export ../whagons/whagons5-client/tmp/calaluna-export.zip \
//	  --env .env --project whagons-5 --tenant calaluna
//
// S3/MinIO credentials are read from the environment (S3_ENDPOINT, S3_REGION,
// S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_FORCE_PATH_STYLE),
// matching the server config. --env loads them from a KEY=VALUE file first.
package main

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gonvex/gonvex/pkg/storage"
)

// _gonvex_files DDL mirrors pkg/storage so the tool works even if the runtime
// has not created the table in this tenant database yet.
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

// storageDoc is one row of _storage/documents.jsonl from a Convex export.
type storageDoc struct {
	ID          string `json:"_id"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
}

type options struct {
	exportPath string
	envFile    string
	pgURL      string
	project    string
	tenant     string
	visibility string
	only       map[string]bool
	overwrite  bool
	dryRun     bool
	verify     bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		os.Exit(1)
	}
}

func run() error {
	opts, err := parseFlags()
	if err != nil {
		return err
	}
	if opts.envFile != "" {
		if err := loadEnvFile(opts.envFile); err != nil {
			return err
		}
	}

	pgURL := opts.pgURL
	if pgURL == "" {
		pgURL = resolveTenantURL(opts.project, opts.tenant)
	}
	if pgURL == "" {
		return fmt.Errorf("could not resolve target Postgres URL; pass --pg-url or set GONVEX_TENANT_DATABASE_URLS for %q:%q", opts.project, opts.tenant)
	}

	cfg := storage.Config{
		Endpoint:        os.Getenv("S3_ENDPOINT"),
		Region:          firstNonEmpty(os.Getenv("S3_REGION"), "us-east-1"),
		Bucket:          os.Getenv("S3_BUCKET"),
		AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:  envBool("S3_FORCE_PATH_STYLE", true),
	}
	if !cfg.Configured() {
		return fmt.Errorf("S3 storage not configured (need S3_ENDPOINT/S3_BUCKET/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY); pass --env or export them")
	}

	client := storage.NewClient(cfg)

	if opts.verify {
		ctx := context.Background()
		db, err := sql.Open("pgx", pgURL)
		if err != nil {
			return fmt.Errorf("open postgres: %w", err)
		}
		defer db.Close()
		return verifyObjects(ctx, db, client, opts)
	}

	src, err := openSource(opts.exportPath)
	if err != nil {
		return err
	}
	defer src.Close()

	docs, err := src.readMetadata()
	if err != nil {
		return err
	}

	fmt.Printf("Export:    %s\n", opts.exportPath)
	fmt.Printf("Target DB: %s\n", maskURL(pgURL))
	fmt.Printf("Bucket:    %s @ %s\n", cfg.Bucket, cfg.Endpoint)
	fmt.Printf("Project:   %s   Tenant: %s   Visibility: %s\n", opts.project, opts.tenant, opts.visibility)
	fmt.Printf("Storage objects in export: %d%s\n", len(docs), dryRunSuffix(opts.dryRun))

	ctx := context.Background()
	db, err := sql.Open("pgx", pgURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	if !opts.dryRun {
		if err := ensureFilesTable(ctx, db); err != nil {
			return err
		}
	}

	var uploaded, skippedExisting, skippedFilter, missingBlob int
	for _, doc := range docs {
		if opts.only != nil && !opts.only[doc.ID] {
			skippedFilter++
			continue
		}

		if !opts.overwrite && !opts.dryRun {
			exists, err := rowExists(ctx, db, doc.ID)
			if err != nil {
				return err
			}
			if exists {
				skippedExisting++
				continue
			}
		}

		body, ok, err := src.readBlob(doc.ID)
		if err != nil {
			return fmt.Errorf("read blob %s: %w", doc.ID, err)
		}
		if !ok {
			fmt.Printf("  [warn] no blob in export for %s; skipping\n", doc.ID)
			missingBlob++
			continue
		}

		objectKey := path.Join(sanitizeSegment(opts.project), sanitizeSegment(opts.tenant), doc.ID)
		contentType := doc.ContentType
		checksum := sha256Hex(body)

		if opts.dryRun {
			fmt.Printf("  [dry] %s -> %s (%d bytes, %s)\n", doc.ID, objectKey, len(body), contentType)
			uploaded++
			continue
		}

		if err := client.PutObject(ctx, objectKey, body, contentType); err != nil {
			return fmt.Errorf("upload %s: %w", doc.ID, err)
		}
		if err := upsertRow(ctx, db, doc.ID, opts, cfg.Bucket, objectKey, contentType, int64(len(body)), checksum); err != nil {
			return fmt.Errorf("record %s: %w", doc.ID, err)
		}
		uploaded++
		if uploaded%25 == 0 {
			fmt.Printf("  ... %d uploaded\n", uploaded)
		}
	}

	fmt.Println("\n================ SUMMARY ================")
	fmt.Printf("uploaded:          %d\n", uploaded)
	fmt.Printf("skipped (existing): %d\n", skippedExisting)
	if opts.only != nil {
		fmt.Printf("skipped (filter):   %d\n", skippedFilter)
	}
	if missingBlob > 0 {
		fmt.Printf("missing blob:       %d\n", missingBlob)
	}
	fmt.Println("========================================")
	return nil
}

func parseFlags() (options, error) {
	var (
		exportPath = flag.String("export", "", "path to a `convex export --include-file-storage` ZIP or extracted directory (required)")
		envFile    = flag.String("env", "", "optional KEY=VALUE env file to load (e.g. gonvex .env) for S3_* and GONVEX_TENANT_DATABASE_URLS")
		pgURL      = flag.String("pg-url", "", "target tenant Postgres URL (default resolved from GONVEX_TENANT_DATABASE_URLS)")
		project    = flag.String("project", "whagons-5", "project id used for object-key namespacing")
		tenant     = flag.String("tenant", "", "tenant slug used for object-key namespacing and URL resolution (required)")
		visibility = flag.String("visibility", "tenant", "file visibility: private | tenant | public")
		only       = flag.String("only", "", "comma-separated storage ids to migrate (default: all in export)")
		overwrite  = flag.Bool("overwrite", false, "re-upload and update rows that already exist")
		dryRun     = flag.Bool("dry-run", false, "plan only; do not upload or write")
		verify     = flag.Bool("verify", false, "verify mode: HEAD every _gonvex_files object in the bucket and report; no upload")
	)
	flag.Parse()

	if *verify {
		// In verify mode the export is not needed; satisfy the shared validation.
		if *exportPath == "" {
			*exportPath = "(verify)"
		}
	}
	if *exportPath == "" {
		return options{}, fmt.Errorf("--export is required")
	}
	if *tenant == "" {
		return options{}, fmt.Errorf("--tenant is required")
	}
	switch *visibility {
	case "private", "tenant", "public":
	default:
		return options{}, fmt.Errorf("invalid --visibility %q (expected private|tenant|public)", *visibility)
	}

	var onlySet map[string]bool
	if strings.TrimSpace(*only) != "" {
		onlySet = map[string]bool{}
		for _, id := range strings.Split(*only, ",") {
			if id = strings.TrimSpace(id); id != "" {
				onlySet[id] = true
			}
		}
	}

	return options{
		exportPath: *exportPath,
		envFile:    *envFile,
		pgURL:      *pgURL,
		project:    *project,
		tenant:     *tenant,
		visibility: *visibility,
		only:       onlySet,
		overwrite:  *overwrite,
		dryRun:     *dryRun,
		verify:     *verify,
	}, nil
}

// ensureFilesTable creates _gonvex_files when missing and repairs a malformed
// one. Cloning a tenant can leave behind a stale/polluted _gonvex_files lacking
// the `id` primary key the storage layer's load()/GetURL() rely on (observed in
// calaluna: convex-style _id/tenantId/createdAt/deletedAt columns and no `id`).
// That breaks storage for the whole tenant, not just this migration. When the
// table is empty we recreate it cleanly; when it has data we refuse and ask for
// a manual repair rather than risk dropping real rows.
func ensureFilesTable(ctx context.Context, db *sql.DB) error {
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public._gonvex_files') IS NOT NULL`).Scan(&present); err != nil {
		return fmt.Errorf("probe _gonvex_files: %w", err)
	}
	if !present {
		if _, err := db.ExecContext(ctx, filesTableDDL); err != nil {
			return fmt.Errorf("create _gonvex_files: %w", err)
		}
		return nil
	}

	var hasID bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='_gonvex_files' AND column_name='id')`).Scan(&hasID); err != nil {
		return fmt.Errorf("inspect _gonvex_files: %w", err)
	}
	if hasID {
		return nil
	}

	var rows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM _gonvex_files`).Scan(&rows); err != nil {
		return fmt.Errorf("count _gonvex_files: %w", err)
	}
	if rows > 0 {
		return fmt.Errorf("_gonvex_files exists without an 'id' column and has %d row(s); refusing to recreate. Repair it manually, then re-run", rows)
	}
	fmt.Println("  [repair] _gonvex_files is malformed (no 'id' column) and empty — recreating with the correct schema")
	if _, err := db.ExecContext(ctx, `DROP TABLE _gonvex_files`); err != nil {
		return fmt.Errorf("drop malformed _gonvex_files: %w", err)
	}
	if _, err := db.ExecContext(ctx, filesTableDDL); err != nil {
		return fmt.Errorf("recreate _gonvex_files: %w", err)
	}
	return nil
}

// verifyObjects HEADs every _gonvex_files object (optionally filtered by --only)
// against the bucket and reports presence and size agreement. This is the proof
// that GetURL will resolve: it reads the same row load() reads and confirms the
// bytes are actually in object storage.
func verifyObjects(ctx context.Context, db *sql.DB, client *storage.Client, opts options) error {
	rows, err := db.QueryContext(ctx, `SELECT id, object_key, size_bytes, status FROM _gonvex_files ORDER BY id`)
	if err != nil {
		return fmt.Errorf("read _gonvex_files: %w", err)
	}
	defer rows.Close()

	var total, found, missing, sizeMismatch int
	for rows.Next() {
		var id, objectKey, status string
		var size int64
		if err := rows.Scan(&id, &objectKey, &size, &status); err != nil {
			return err
		}
		if opts.only != nil && !opts.only[id] {
			continue
		}
		total++
		head, ok, err := client.HeadObject(ctx, objectKey)
		if err != nil {
			return fmt.Errorf("head %s: %w", objectKey, err)
		}
		if !ok {
			missing++
			fmt.Printf("  [MISSING] %s -> %s\n", id, objectKey)
			continue
		}
		found++
		if head.Size != size {
			sizeMismatch++
			fmt.Printf("  [size?]  %s row=%d bucket=%d\n", id, size, head.Size)
		}
		if opts.only != nil {
			fmt.Printf("  [ok] %s -> %s (%d bytes, status=%s)\n", id, objectKey, head.Size, status)
			// Browser-equivalent check: fetch the presigned GET URL (what
			// GetURL hands the client) with no auth header.
			signed, err := client.PresignGet(objectKey, 5*time.Minute)
			if err != nil {
				return fmt.Errorf("presign %s: %w", id, err)
			}
			n, code, err := httpGetSize(signed)
			if err != nil {
				return fmt.Errorf("fetch presigned %s: %w", id, err)
			}
			fmt.Printf("       presigned GET -> HTTP %d, %d bytes downloaded\n", code, n)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	fmt.Println("\n================ VERIFY ================")
	fmt.Printf("rows checked:   %d\n", total)
	fmt.Printf("present in bucket: %d\n", found)
	fmt.Printf("missing:        %d\n", missing)
	fmt.Printf("size mismatch:  %d\n", sizeMismatch)
	fmt.Println("=======================================")
	if missing > 0 {
		return fmt.Errorf("%d object(s) missing from the bucket", missing)
	}
	return nil
}

// httpGetSize fetches url (no auth) and returns the downloaded byte count and
// status code. Used to prove a presigned GET URL is usable by a browser.
func httpGetSize(url string) (int64, int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	return n, resp.StatusCode, err
}

func rowExists(ctx context.Context, db *sql.DB, id string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM _gonvex_files WHERE id = $1`, id).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check existing %s: %w", id, err)
	}
	return true, nil
}

func upsertRow(ctx context.Context, db *sql.DB, id string, opts options, bucket, objectKey, contentType string, size int64, checksum string) error {
	now := time.Now().UTC()
	_, err := db.ExecContext(ctx, `
		INSERT INTO _gonvex_files
			(id, tenant_id, owner_id, bucket, object_key, content_type, size_bytes, checksum, visibility, status, created_at, uploaded_at)
		VALUES ($1, $2, '', $3, $4, $5, $6, $7, $8, 'uploaded', $9, $9)
		ON CONFLICT (id) DO UPDATE SET
			tenant_id = EXCLUDED.tenant_id,
			bucket = EXCLUDED.bucket,
			object_key = EXCLUDED.object_key,
			content_type = EXCLUDED.content_type,
			size_bytes = EXCLUDED.size_bytes,
			checksum = EXCLUDED.checksum,
			visibility = EXCLUDED.visibility,
			status = 'uploaded',
			uploaded_at = EXCLUDED.uploaded_at,
			deleted_at = NULL`,
		id, opts.tenant, bucket, objectKey, contentType, size, checksum, opts.visibility, now,
	)
	return err
}

// ---------------------------------------------------------------------------
// Export source (zip or directory)
// ---------------------------------------------------------------------------

type source struct {
	zr    *zip.ReadCloser
	dir   string
	byID  map[string]*zip.File // storage id -> blob entry (zip mode)
	meta  *zip.File            // _storage/documents.jsonl (zip mode)
	dirID map[string]string    // storage id -> blob path (dir mode)
}

func openSource(p string) (*source, error) {
	info, err := os.Stat(p)
	if err != nil {
		return nil, fmt.Errorf("open export: %w", err)
	}
	if info.IsDir() {
		return openDirSource(p)
	}
	return openZipSource(p)
}

func openZipSource(p string) (*source, error) {
	zr, err := zip.OpenReader(p)
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	s := &source{zr: zr, byID: map[string]*zip.File{}}
	for _, f := range zr.File {
		name := strings.ReplaceAll(f.Name, "\\", "/")
		if !strings.HasPrefix(name, "_storage/") {
			continue
		}
		base := path.Base(name)
		if base == "documents.jsonl" {
			s.meta = f
			continue
		}
		s.byID[storageIDFromName(base)] = f
	}
	if s.meta == nil {
		zr.Close()
		return nil, fmt.Errorf("export zip has no _storage/documents.jsonl (was it exported with --include-file-storage?)")
	}
	return s, nil
}

func openDirSource(dir string) (*source, error) {
	storageDir := dir
	if filepath.Base(dir) != "_storage" {
		storageDir = filepath.Join(dir, "_storage")
	}
	entries, err := os.ReadDir(storageDir)
	if err != nil {
		return nil, fmt.Errorf("read _storage dir: %w (expected an extracted export containing _storage/)", err)
	}
	s := &source{dir: storageDir, dirID: map[string]string{}}
	hasMeta := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if e.Name() == "documents.jsonl" {
			hasMeta = true
			continue
		}
		s.dirID[storageIDFromName(e.Name())] = filepath.Join(storageDir, e.Name())
	}
	if !hasMeta {
		return nil, fmt.Errorf("no documents.jsonl in %s", storageDir)
	}
	return s, nil
}

func (s *source) Close() error {
	if s.zr != nil {
		return s.zr.Close()
	}
	return nil
}

func (s *source) readMetadata() ([]storageDoc, error) {
	var r io.ReadCloser
	var err error
	if s.zr != nil {
		r, err = s.meta.Open()
	} else {
		r, err = os.Open(filepath.Join(s.dir, "documents.jsonl"))
	}
	if err != nil {
		return nil, fmt.Errorf("open metadata: %w", err)
	}
	defer r.Close()

	var docs []storageDoc
	dec := json.NewDecoder(r)
	for dec.More() {
		var d storageDoc
		if err := dec.Decode(&d); err != nil {
			return nil, fmt.Errorf("parse metadata: %w", err)
		}
		if d.ID != "" {
			docs = append(docs, d)
		}
	}
	return docs, nil
}

func (s *source) readBlob(id string) ([]byte, bool, error) {
	if s.zr != nil {
		f, ok := s.byID[id]
		if !ok {
			return nil, false, nil
		}
		rc, err := f.Open()
		if err != nil {
			return nil, false, err
		}
		defer rc.Close()
		body, err := io.ReadAll(rc)
		return body, err == nil, err
	}
	p, ok := s.dirID[id]
	if !ok {
		return nil, false, nil
	}
	body, err := os.ReadFile(p)
	return body, err == nil, err
}

// storageIDFromName strips the content-type extension a Convex export appends to
// the storage id (e.g. "kg24...jpeg" -> "kg24..."). Storage ids contain no dots.
func storageIDFromName(base string) string {
	if i := strings.IndexByte(base, '.'); i >= 0 {
		return base[:i]
	}
	return base
}

// ---------------------------------------------------------------------------
// Env + helpers
// ---------------------------------------------------------------------------

func loadEnvFile(file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read env file: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if len(val) >= 2 && (val[0] == '"' && val[len(val)-1] == '"' || val[0] == '\'' && val[len(val)-1] == '\'') {
			val = val[1 : len(val)-1]
		}
		// Do not clobber values already set in the real environment.
		if _, present := os.LookupEnv(key); !present {
			os.Setenv(key, val)
		}
	}
	return nil
}

// resolveTenantURL reads GONVEX_TENANT_DATABASE_URLS (a JSON map) and returns
// the URL for "<project>:<tenant>".
func resolveTenantURL(project, tenant string) string {
	raw := os.Getenv("GONVEX_TENANT_DATABASE_URLS")
	if raw == "" {
		return ""
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return ""
	}
	return m[project+":"+tenant]
}

func envBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "":
		return def
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// sanitizeSegment matches pkg/storage's object-key segment cleaning so migrated
// keys line up with what the runtime would generate for new uploads.
func sanitizeSegment(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9',
			ch == '-', ch == '_', ch == '.':
			b.WriteRune(ch)
		default:
			b.WriteByte('_')
		}
	}
	cleaned := strings.Trim(b.String(), "_")
	if strings.Trim(cleaned, ".") == "" {
		return ""
	}
	return cleaned
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// maskURL hides the password in a postgres URL (between the user ':' and '@').
func maskURL(u string) string {
	i := strings.Index(u, "://")
	if i < 0 {
		return u
	}
	rest := u[i+3:]
	c := strings.IndexByte(rest, ':')
	a := strings.IndexByte(rest, '@')
	if c >= 0 && a > c {
		return u[:i+3] + rest[:c+1] + "***" + rest[a:]
	}
	return u
}

func dryRunSuffix(dry bool) string {
	if dry {
		return "  (DRY RUN)"
	}
	return ""
}
