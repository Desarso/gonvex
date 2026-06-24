package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
	"github.com/gonvex/gonvex/pkg/storage"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/data"
	"github.com/gonvex/gonvex/server/internal/runtime"
	"github.com/gonvex/gonvex/server/internal/schema"
)

type Server struct {
	config            config.Config
	runtime           *runtime.Runtime
	app               *gonvex.App
	storage           *storage.Factory
	tenantStores      *tenantStoreResolver
	cache             *rowsCache
	metrics           *runtimeMetrics
	scheduler         *scheduler
	telemetryWrites   chan struct{}
	projectMu         sync.RWMutex
	projects          map[string]projectTarget
	tenants           map[string]tenantTarget
	tenantDiscoveryMu sync.Mutex
	tenantDiscoveryAt map[string]time.Time
	wsMu              sync.RWMutex
	wsConns           map[*wsConn]bool
	tableChangeMu     sync.Mutex
	tableChangeWait   map[string]*time.Timer
	tableChanges      map[string]tableChange
}

func New(cfg config.Config) *Server {
	return NewWithApp(cfg, nil)
}

func NewWithApp(cfg config.Config, app *gonvex.App) *Server {
	if app == nil {
		app = gonvex.NewApp()
	}
	cache, _ := newRowsCache(cfg.ValkeyURL, cfg.RowsCacheTTL)
	server := &Server{
		config:  cfg,
		runtime: runtime.NewWithLoader(projectbundle.NewLoader(cfg.PluginCacheDir, cfg.GonvexModuleRoot)),
		app:     app,
		storage: storage.NewFactory(storage.Config{
			Endpoint:        cfg.S3Endpoint,
			Region:          cfg.S3Region,
			Bucket:          cfg.S3Bucket,
			AccessKeyID:     cfg.S3AccessKeyID,
			SecretAccessKey: cfg.S3SecretAccessKey,
			ForcePathStyle:  cfg.S3ForcePathStyle,
			PublicBaseURL:   cfg.StoragePublicURL,
			URLSigningKey:   cfg.S3SecretAccessKey,
		}),
		cache:             cache,
		metrics:           newRuntimeMetrics(cfg.TelemetryLogPath),
		telemetryWrites:   make(chan struct{}, 4),
		projects:          map[string]projectTarget{},
		tenants:           map[string]tenantTarget{},
		tenantDiscoveryAt: map[string]time.Time{},
		wsConns:           map[*wsConn]bool{},
		tableChangeWait:   map[string]*time.Timer{},
		tableChanges:      map[string]tableChange{},
	}
	server.scheduler = newScheduler(server.runScheduledJob)
	server.tenantStores = newTenantStoreResolver(&server.config)
	server.loadConfiguredTenantDatabases()
	server.tenantStores.StartIdleReaper(context.Background())
	server.startLandlordMigrations()
	server.scheduler.start(context.Background())
	go server.hydrateRuntimeState(context.Background())
	server.startPostgresNotifications()
	return server
}

// runScheduledJob is the scheduler's executor: it dispatches a due job through
// the same mutation/action execution path as client-triggered calls, so
// scheduled work shows up in the function and concurrency metrics too.
func (s *Server) runScheduledJob(ctx context.Context, job scheduledJob) error {
	app := s.appForProject(ctx, job.ProjectID)
	function, ok := app.Lookup(job.FunctionPath)
	if !ok {
		return fmt.Errorf("scheduled function %q is not registered", job.FunctionPath)
	}
	switch function.Kind {
	case gonvex.FunctionKindAction:
		_, err := s.executeTenantAction(ctx, job.ProjectID, job.TenantID, job.FunctionPath, job.Args)
		return err
	case gonvex.FunctionKindMutation:
		_, err := s.executeTenantMutation(ctx, job.ProjectID, job.TenantID, job.FunctionPath, job.Args)
		return err
	case gonvex.FunctionKindInternalMutation:
		return s.executeScheduledInternalMutation(ctx, job)
	default:
		return fmt.Errorf("scheduled function %q must be a mutation or action, got %s", job.FunctionPath, function.Kind)
	}
}

// executeScheduledInternalMutation runs an internal mutation from the scheduler.
// Internal mutations aren't reachable from clients, so they're dispatched here
// rather than through executeTenantMutation, but still get metrics and a
// surrounding transaction.
func (s *Server) executeScheduledInternalMutation(ctx context.Context, job scheduledJob) (err error) {
	const kind = "internalMutation"
	s.metrics.recordFunctionStart(kind)
	started := time.Now()
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunction(job.FunctionPath, kind, time.Since(started), err)
	}()

	app := s.appForProject(ctx, job.ProjectID)
	mutationCtx, ctxErr := s.mutationContext(ctx, job.ProjectID, job.TenantID, callerContext{})
	if ctxErr != nil {
		return ctxErr
	}
	_, err = s.runMutationInTx(mutationCtx, job.FunctionPath, job.Args, app.ExecuteInternalMutation)
	return err
}

// registerProjectCrons mirrors a project's declared crons into the scheduler.
// Safe to call repeatedly; unchanged crons keep their run history.
func (s *Server) registerProjectCrons(projectID string) {
	if s.scheduler == nil {
		return
	}
	app := s.runtime.AppForProject(projectID)
	if app == nil {
		return
	}
	s.scheduler.syncCrons(projectID, app.Crons())
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /storage/{key...}", s.handleStorageProxy)
	mux.HandleFunc("GET /dev/manifest", s.handleManifest)
	mux.HandleFunc("GET /dev/metrics", s.handleMetrics)
	mux.HandleFunc("GET /dev/projects", s.handleProjects)
	mux.HandleFunc("POST /dev/projects", s.handleCreateProject)
	mux.HandleFunc("POST /dev/projects/{project}/key", s.handleProjectKey)
	mux.HandleFunc("DELETE /dev/projects/{project}", s.handleDeleteProject)
	mux.HandleFunc("GET /dev/tenants", s.handleTenants)
	mux.HandleFunc("POST /dev/tenants", s.handleCreateTenant)
	mux.HandleFunc("DELETE /dev/tenants/{tenant}", s.handleDeleteTenant)
	mux.HandleFunc("GET /dev/storage/files", s.handleStorageFiles)
	mux.HandleFunc("GET /dev/data/tables", s.handleDataTables)
	mux.HandleFunc("GET /dev/data/tables/{table}/rows", s.handleDataRows)
	mux.HandleFunc("POST /dev/data/tables/{table}/rows", s.handleInsertDataRow)
	mux.HandleFunc("POST /dev/sync", s.handleDevSync)
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	return withGzip(withJSON(mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"time":        time.Now().UTC().Format(time.RFC3339Nano),
		"postgresSet": s.config.PostgresURL != "",
		"valkeySet":   s.config.ValkeyURL != "",
		"rowsCache":   s.cache.enabled(),
		"s3Set":       s.storage != nil,
	})
}

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	s.hydrateRuntimeStateForProject(r.Context(), projectID(r))
	writeJSON(w, http.StatusOK, s.runtime.ManifestForProject(projectID(r)))
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	connections, subscriptions := s.websocketStats()
	snapshot := s.metrics.snapshot(s.runtime.Manifest(), connections, subscriptions)
	if s.scheduler != nil {
		schedulerSnapshot := s.scheduler.snapshot()
		snapshot.Scheduler = &schedulerSnapshot
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleDataTables(w http.ResponseWriter, r *http.Request) {
	s.hydrateProjectTenantDatabases(r.Context(), projectID(r))
	tables, err := data.ListTables(r.Context(), s.databaseURL(r))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": tables})
}

func (s *Server) handleDataRows(w http.ResponseWriter, r *http.Request) {
	s.hydrateProjectTenantDatabases(r.Context(), projectID(r))
	table := r.PathValue("table")
	project := projectID(r)
	tenant := tenantIDFromRequest(project, tenantID(r))
	if s.cache.enabled() {
		key := s.cache.rowsKey(project, tenant, table, r.URL.Query())
		if payload, ok := s.cache.get(r.Context(), key); ok {
			s.metrics.recordCache("hit")
			w.Header().Set("content-type", "application/json")
			w.Header().Set("x-gonvex-cache", "hit")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		s.metrics.recordCache("miss")
		w.Header().Set("x-gonvex-cache", "miss")
	} else {
		s.metrics.recordCache("bypass")
		w.Header().Set("x-gonvex-cache", "bypass")
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	filters, err := parseRowsFilters(r.URL.Query().Get("filters"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	countMode := r.URL.Query().Get("count")
	result, err := data.ReadRows(r.Context(), s.databaseURL(r), table, data.RowsOptions{
		Limit:           limit,
		Offset:          offset,
		Search:          r.URL.Query().Get("search"),
		SortColumn:      r.URL.Query().Get("sort"),
		SortDirection:   r.URL.Query().Get("direction"),
		Filters:         filters,
		Columns:         parseColumns(r.URL.Query().Get("columns")),
		ExactTotal:      countMode != "false" && countMode != "estimate",
		EstimateTotal:   countMode == "estimate",
		CursorCreatedAt: r.URL.Query().Get("cursorCreatedAt"),
		CursorID:        r.URL.Query().Get("cursorId"),
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cache.enabled() {
		s.cache.set(r.Context(), s.cache.rowsKey(project, tenant, table, r.URL.Query()), payload)
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func parseColumns(raw string) []string {
	if raw == "" {
		return nil
	}
	columns := strings.Split(raw, ",")
	for index, column := range columns {
		columns[index] = strings.TrimSpace(column)
	}
	return columns
}

func parseRowsFilters(raw string) ([]data.RowsFilter, error) {
	if raw == "" {
		return nil, nil
	}
	var filters []data.RowsFilter
	if err := json.Unmarshal([]byte(raw), &filters); err != nil {
		return nil, err
	}
	return filters, nil
}

func (s *Server) handleInsertDataRow(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := data.InsertRow(r.Context(), s.databaseURL(r), r.PathValue("table"), payload)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.broadcastTenantTableChange(projectID(r), tenantIDFromRequest(projectID(r), tenantID(r)), r.PathValue("table"))
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) handleDevSync(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// Per-project auth: the sync uploads source the runtime compiles and runs,
	// so it must present the target project's own key. Hydrate the project first
	// so its key is loaded, then require it. Falls back to the global
	// GONVEX_DEV_SYNC_KEY only for projects that have no key yet.
	syncProjectID := strings.TrimSpace(r.Header.Get("x-gonvex-project-id"))
	if syncProjectID != "" {
		s.hydrateRuntimeStateForProject(r.Context(), syncProjectID)
	}
	if !s.acceptsSyncKey(syncProjectID, syncKey(r)) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Gonvex sync key"})
		return
	}

	var next manifest.Manifest
	if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if next.Functions == nil {
		next.Functions = map[string]manifest.FunctionEntry{}
	}
	if next.Project == "" {
		next.Project = r.Header.Get("x-gonvex-project-id")
	}
	if headerProject := r.Header.Get("x-gonvex-project-id"); headerProject != "" && next.Project != "" && headerProject != next.Project {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "manifest project does not match x-gonvex-project-id"})
		return
	}
	if next.Schema.Tables == nil {
		next.Schema = manifest.EmptySchema()
	}

	migrationResult, err := schema.Apply(r.Context(), s.databaseURLForProject(next.Project), next.Schema.LandlordSchema())
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	tenantMigrationResult, err := s.applyTenantSchemasForProject(r.Context(), next.Project, next.Schema)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}

	if err := s.runtime.SyncManifest(next); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	s.registerProjectCrons(next.Project)
	if err := s.saveRuntimeManifest(r.Context(), next); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.cache.invalidateRows(r.Context(), next.Project, tenantIDFromRequest(next.Project, ""), "")
	s.broadcastTableChange(next.Project, "tasks")
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"project":         next.Project,
		"functionCount":   len(next.Functions),
		"schema":          migrationResult,
		"tenantSchema":    tenantMigrationResult,
		"runtimeReloaded": true,
	})
}

func syncKey(r *http.Request) string {
	if value := r.Header.Get("x-gonvex-key"); value != "" {
		return value
	}
	value := r.Header.Get("authorization")
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[len("Bearer "):])
	}
	return ""
}

func (s *Server) acceptsAdminKey(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	if s.config.AdminKey != "" && key == s.config.AdminKey {
		return true
	}
	return s.config.AdminKey == "" && s.config.DevSyncKey != "" && key == s.config.DevSyncKey
}

// acceptsSyncKey gates POST /dev/sync. If the target project has a registered
// key, exactly that key is required (per-project). Otherwise it falls back to
// the global GONVEX_DEV_SYNC_KEY, and if neither is configured the endpoint is
// open (local dev only).
func (s *Server) acceptsSyncKey(projectID, provided string) bool {
	provided = strings.TrimSpace(provided)
	if projectID != "" {
		if key := strings.TrimSpace(s.config.ProjectKeys[projectID]); key != "" {
			return provided != "" && provided == key
		}
	}
	if s.config.DevSyncKey != "" {
		return provided != "" && provided == s.config.DevSyncKey
	}
	return true
}

func projectID(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("x-gonvex-project-id")); value != "" {
		return value
	}
	if value := strings.TrimSpace(r.URL.Query().Get("project")); value != "" {
		return value
	}
	return ""
}

func tenantID(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("x-gonvex-tenant-id")); value != "" {
		return value
	}
	if value := strings.TrimSpace(r.URL.Query().Get("tenant")); value != "" {
		return value
	}
	return ""
}

func (s *Server) databaseURL(r *http.Request) string {
	return s.databaseURLForTenant(projectID(r), tenantID(r))
}

func (s *Server) databaseURLForProject(projectID string) string {
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	return s.config.DatabaseURL(projectID)
}

func (s *Server) databaseURLForTenant(projectID string, tenantID string) string {
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || tenantID == projectID {
		return s.config.DatabaseURL(projectID)
	}
	if value := s.configuredTenantDatabaseURLLocked(projectID, tenantTarget{ID: tenantID}); value != "" {
		return value
	}
	if tenant, ok := s.tenants[tenantStoreKey(projectID, tenantID)]; ok {
		if value := s.configuredTenantDatabaseURLLocked(projectID, tenant); value != "" {
			return value
		}
		if tenant.databaseURL != "" {
			return tenant.databaseURL
		}
	}
	return s.config.DatabaseURL(projectID)
}

func (s *Server) hydrateRuntimeState(ctx context.Context) {
	// Resolve every project's database + key from the control plane so
	// databaseURLForProject works right after a restart, without waiting for
	// something to list projects first.
	s.hydrateProjects()
	manifests, err := s.loadRuntimeManifests(ctx)
	if err != nil {
		slog.Debug("load persisted Gonvex runtime manifests", "error", err)
		return
	}
	for _, next := range manifests {
		if err := s.runtime.SyncManifest(next); err != nil {
			slog.Warn("load persisted Gonvex runtime manifest", "project", next.Project, "error", err)
			continue
		}
		s.registerProjectCrons(next.Project)
	}
}

func (s *Server) hydrateRuntimeStateForProject(ctx context.Context, projectID string) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return
	}
	// Projects are created dynamically, so resolve this project's database from
	// the control plane (gonvex_runtime_projects) on demand if we haven't yet.
	// Without this, databaseURLForProject falls back to POSTGRES_URL and the
	// runtime reads landlord tables from the wrong (control) database. This must
	// run even when the app/manifest is already loaded, since the DB mapping is
	// independent of the compiled bundle.
	s.projectMu.RLock()
	_, haveDB := s.config.ProjectDatabases[projectID]
	s.projectMu.RUnlock()
	if !haveDB {
		s.hydrateProjects()
	}
	if s.runtime.AppForProject(projectID) != nil {
		return
	}
	next, ok, err := s.loadRuntimeManifest(ctx, projectID)
	if err != nil {
		slog.Debug("load persisted Gonvex project runtime manifest", "project", projectID, "error", err)
		return
	}
	if !ok {
		return
	}
	if err := s.runtime.SyncManifest(next); err != nil {
		slog.Warn("load persisted Gonvex project runtime manifest", "project", projectID, "error", err)
		return
	}
	s.registerProjectCrons(projectID)
}

func (s *Server) appForProject(ctx context.Context, projectID string) *gonvex.App {
	s.hydrateRuntimeStateForProject(ctx, projectID)
	if app := s.runtime.AppForProject(projectID); app != nil {
		return app
	}
	return s.app
}

func (s *Server) configuredTenantDatabaseURLLocked(projectID string, tenant tenantTarget) string {
	if s.config.TenantDatabases == nil {
		return ""
	}
	for _, candidate := range tenantLookupCandidates(tenant) {
		if candidate == "" {
			continue
		}
		if value := s.config.TenantDatabases[tenantStoreKey(projectID, candidate)]; value != "" {
			return value
		}
		if value := s.config.TenantDatabases[candidate]; value != "" {
			return value
		}
	}
	needles := tenantDatabaseNeedles(tenant)
	for _, value := range s.config.TenantDatabases {
		databaseName := databaseNameFromURL(value, "")
		normalizedDatabase := normalizeDatabaseAlias(databaseName)
		for _, needle := range needles {
			if needle != "" && strings.Contains(normalizedDatabase, needle) {
				return value
			}
		}
	}
	return ""
}

func tenantLookupCandidates(tenant tenantTarget) []string {
	return uniqueStrings([]string{
		tenant.ID,
		tenant.Database,
		tenant.domain,
		slug(tenant.Name),
		strings.ReplaceAll(slug(tenant.Name), "_", "-"),
	})
}

func tenantDatabaseNeedles(tenant tenantTarget) []string {
	values := []string{tenant.ID, tenant.Database, tenant.domain, tenant.Name}
	needles := make([]string, 0, len(values))
	for _, value := range values {
		needle := normalizeDatabaseAlias(value)
		if len(needle) >= 4 {
			needles = append(needles, needle)
		}
	}
	return uniqueStrings(needles)
}

func normalizeDatabaseAlias(value string) string {
	return strings.ToLower(strings.NewReplacer("-", "", "_", "", " ", "").Replace(strings.TrimSpace(value)))
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func withJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		w.Header().Set("access-control-allow-headers", "content-type, authorization, x-gonvex-project-id, x-gonvex-tenant-id")
		w.Header().Set("access-control-allow-methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
