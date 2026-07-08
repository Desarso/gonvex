package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/schema"
)

const projectTenantDiscoveryTTL = 5 * time.Second

type tenantTarget struct {
	ID             string `json:"id"`
	ProjectID      string `json:"projectId"`
	Name           string `json:"name"`
	Database       string `json:"database"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Provisioned    bool   `json:"provisioned"`
	RuntimeCreated bool   `json:"runtimeCreated"`
	databaseURL    string
	databaseName   string
	domain         string
	// standaloneDiscovered marks databases claimed by pg_database scanning with
	// no project suffix or registration tying them to this project. Any project
	// that syncs will "discover" them, so schema conflicts there are expected
	// (another project's tenant) and must not fail the owning project's sync.
	standaloneDiscovered bool
}

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	s.hydrateLandlordTenants(r.Context(), project)
	s.hydrateProjectTenantDatabases(r.Context(), project)

	s.projectMu.RLock()
	tenants := make([]tenantTarget, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		if project == "" || tenant.ProjectID == "" || tenant.ProjectID == project {
			tenants = append(tenants, tenant)
		}
	}
	s.projectMu.RUnlock()
	tenants = dedupeTenantTargets(tenants)

	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].ProjectID == tenants[j].ProjectID {
			return strings.ToLower(tenants[i].Name) < strings.ToLower(tenants[j].Name)
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func dedupeTenantTargets(tenants []tenantTarget) []tenantTarget {
	byScopeAndDatabase := map[string]int{}
	result := make([]tenantTarget, 0, len(tenants))
	for _, tenant := range tenants {
		databaseKey := normalizeDatabaseAlias(tenant.Database)
		if databaseKey == "" {
			databaseKey = normalizeDatabaseAlias(tenant.databaseName)
		}
		if databaseKey == "" {
			databaseKey = normalizeDatabaseAlias(tenant.ID)
		}
		key := tenant.ProjectID + ":" + databaseKey
		if index, ok := byScopeAndDatabase[key]; ok {
			if tenantTargetPriority(tenant) > tenantTargetPriority(result[index]) {
				result[index] = tenant
			}
			continue
		}
		byScopeAndDatabase[key] = len(result)
		result = append(result, tenant)
	}
	return result
}

func tenantTargetPriority(tenant tenantTarget) int {
	if tenant.Description == "Persisted tenant from landlord database." {
		return 3
	}
	if tenant.Description == "Discovered local tenant database." {
		return 2
	}
	if tenant.ProjectID != "" {
		return 1
	}
	return 0
}

func (s *Server) loadConfiguredTenantDatabases() {
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	for key, databaseURL := range s.config.TenantDatabases {
		project, tenantID := splitTenantDatabaseKey(key)
		if tenantID == "" || databaseURL == "" {
			continue
		}
		storeKey := tenantStoreKey(project, tenantID)
		s.tenants[storeKey] = tenantTarget{
			ID:             tenantID,
			ProjectID:      project,
			Name:           tenantID,
			Database:       databaseNameFromURL(databaseURL, tenantID),
			Status:         "local",
			Description:    "Configured tenant database.",
			Provisioned:    true,
			databaseURL:    databaseURL,
			databaseName:   databaseNameFromURL(databaseURL, tenantID),
			RuntimeCreated: false,
		}
	}
}

func splitTenantDatabaseKey(key string) (string, string) {
	project, tenantID, ok := strings.Cut(strings.TrimSpace(key), ":")
	if !ok {
		return "", strings.TrimSpace(key)
	}
	return strings.TrimSpace(project), strings.TrimSpace(tenantID)
}

func (s *Server) hydrateLandlordTenants(ctx context.Context, project string) {
	project = strings.TrimSpace(project)
	if project == "" {
		return
	}
	projectDatabaseURL := s.databaseURLForProject(project)
	if strings.TrimSpace(projectDatabaseURL) == "" {
		return
	}
	db, err := sql.Open("pgx", projectDatabaseURL)
	if err != nil {
		return
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT id, COALESCE(name, ''), COALESCE(database, ''), COALESCE(domain, '') FROM tenants ORDER BY name, id`)
	if err != nil {
		return
	}
	defer rows.Close()

	existingDatabases := s.existingLocalDatabaseNames(ctx)
	imported := map[string]tenantTarget{}
	for rows.Next() {
		var tenantID string
		var name string
		var databaseAlias string
		var domain string
		if err := rows.Scan(&tenantID, &name, &databaseAlias, &domain); err != nil {
			return
		}
		tenantID = strings.TrimSpace(tenantID)
		if tenantID == "" {
			tenantID = strings.TrimSpace(domain)
		}
		if tenantID == "" {
			continue
		}
		if strings.TrimSpace(name) == "" {
			name = tenantID
		}
		tenant := tenantTarget{
			ID:           tenantID,
			ProjectID:    project,
			Name:         name,
			Database:     databaseAlias,
			Status:       "local",
			Description:  "Persisted tenant from landlord database.",
			Provisioned:  false,
			databaseName: tenantDatabaseNameForPersistedTenant(project, tenantID, databaseAlias, domain, existingDatabases),
			domain:       domain,
		}
		imported[tenantStoreKey(project, tenantID)] = tenant
	}
	if len(imported) == 0 {
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	for key, tenant := range imported {
		existing := s.tenants[key]
		tenant = s.resolveTenantDatabaseURLLocked(project, tenant)
		previousURL := existing.databaseURL
		if existing.Provisioned && existing.databaseURL != "" && existing.databaseURL == tenant.databaseURL {
			tenant.Provisioned = true
		}
		s.tenants[key] = tenant
		if tenant.databaseURL != "" {
			s.config.TenantDatabases[key] = tenant.databaseURL
		}
		if previousURL != "" && previousURL != tenant.databaseURL {
			go s.cache.invalidateRows(context.Background(), project, tenant.ID, "")
		}
	}
}

func (s *Server) resolveTenantDatabaseURLLocked(project string, tenant tenantTarget) tenantTarget {
	if configuredURL := s.configuredTenantDatabaseURLLocked(project, tenant); configuredURL != "" {
		tenant.databaseURL = configuredURL
		tenant.databaseName = databaseNameFromURL(configuredURL, tenant.databaseName)
		return tenant
	}
	if strings.TrimSpace(tenant.databaseName) != "" && strings.TrimSpace(s.config.PostgresURL) != "" {
		if tenantURL, err := databaseURL(s.config.PostgresURL, tenant.databaseName); err == nil {
			tenant.databaseURL = tenantURL
		}
	}
	return tenant
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		ProjectID string `json:"projectId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	project := strings.TrimSpace(payload.ProjectID)
	if project == "" {
		project = projectID(r)
	}
	if project == "" {
		project = "default"
	}
	name := strings.TrimSpace(payload.Name)
	requestedTenantID := slug(payload.ID)
	if name == "" {
		name = requestedTenantID
	}
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant name or id is required"})
		return
	}
	if s.config.PostgresURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DATABASE_URL is not configured"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()

	tenantID := requestedTenantID
	if tenantID == "" {
		tenantID = s.uniqueTenantIDLocked(project, name)
	}
	key := tenantStoreKey(project, tenantID)
	if existing, ok := s.tenants[key]; ok {
		if existing.databaseURL != "" {
			if err := provisionTenantDatabase(r.Context(), existing.databaseURL, s.runtime.ManifestForProject(project).Schema.TenantSchema()); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"tenant": existing})
		return
	}
	databaseAlias := slug(name)
	if databaseAlias == "" {
		databaseAlias = tenantID
	}
	if s.tenantDatabaseAliasTakenLocked(project, databaseAlias, key) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "tenant database name already exists for this project"})
		return
	}
	databaseName := tenantDatabaseNameWithAlias(project, tenantID, databaseAlias)
	createdDatabase := true
	tenantDatabaseURL, err := createProjectDatabase(r.Context(), s.config.PostgresURL, databaseName)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		createdDatabase = false
		tenantDatabaseURL, err = databaseURL(s.config.PostgresURL, databaseName)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	if err := provisionTenantDatabase(r.Context(), tenantDatabaseURL, s.runtime.ManifestForProject(project).Schema.TenantSchema()); err != nil {
		if createdDatabase {
			_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	s.config.TenantDatabases[key] = tenantDatabaseURL
	tenant := tenantTarget{
		ID:             tenantID,
		ProjectID:      project,
		Name:           name,
		Database:       databaseAlias,
		Status:         "local",
		Description:    "Runtime-created tenant database.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    tenantDatabaseURL,
		databaseName:   databaseName,
	}
	s.tenants[key] = tenant
	s.invalidateProjectTenantDiscovery(project)

	writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		project = "default"
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	if tenantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant id is required"})
		return
	}

	s.hydrateLandlordTenants(r.Context(), project)
	s.hydrateProjectTenantDatabases(r.Context(), project)

	s.projectMu.Lock()
	key := tenantStoreKey(project, tenantID)
	tenant, ok := s.tenants[key]
	if !ok {
		s.projectMu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}
	databaseName := tenant.databaseName
	if databaseName == "" {
		databaseName = tenant.Database
	}
	s.projectMu.Unlock()

	if err := s.cleanupProjectLandlordTenantReferences(r.Context(), project, tenant); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if databaseName != "" {
		if err := dropProjectDatabase(r.Context(), s.config.PostgresURL, databaseName); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}

	s.projectMu.Lock()
	delete(s.tenants, key)
	if s.config.TenantDatabases != nil {
		delete(s.config.TenantDatabases, key)
	}
	s.projectMu.Unlock()
	s.invalidateProjectTenantDiscovery(project)
	s.tenantStores.Close()
	s.cache.invalidateRows(r.Context(), project, tenantID, "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) cleanupProjectLandlordTenantReferences(ctx context.Context, project string, tenant tenantTarget) error {
	projectDatabaseURL := s.databaseURLForProject(project)
	if strings.TrimSpace(projectDatabaseURL) == "" {
		return nil
	}
	db, err := sql.Open("pgx", projectDatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	aliases := tenantReferenceAliases(tenant)
	if len(aliases) == 0 {
		return nil
	}
	if err := deleteRowsMatchingAnyColumn(ctx, db, "userTenantMap", []string{"tenantId", "tenant_id"}, aliases); err != nil {
		return err
	}
	if err := deleteRowsMatchingAnyColumn(ctx, db, "users", []string{"tenantId", "tenant_id"}, aliases); err != nil {
		return err
	}
	return deleteRowsMatchingAnyColumn(ctx, db, "tenants", []string{"id", "domain", "database"}, aliases)
}

func tenantReferenceAliases(tenant tenantTarget) []string {
	seen := map[string]bool{}
	aliases := []string{}
	for _, value := range []string{tenant.ID, tenant.domain, tenant.Database, tenant.databaseName, tenant.Name} {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		aliases = append(aliases, value)
	}
	return aliases
}

func deleteRowsMatchingAnyColumn(ctx context.Context, db *sql.DB, table string, candidateColumns []string, aliases []string) error {
	if len(aliases) == 0 || !serverTableExists(ctx, db, table) {
		return nil
	}
	columns, err := serverTableColumns(ctx, db, table)
	if err != nil {
		return err
	}
	columnSet := map[string]bool{}
	for _, column := range columns {
		columnSet[column] = true
	}
	matchedColumns := []string{}
	for _, column := range candidateColumns {
		if columnSet[column] {
			matchedColumns = append(matchedColumns, column)
		}
	}
	if len(matchedColumns) == 0 {
		return nil
	}

	for _, alias := range aliases {
		predicates := make([]string, 0, len(matchedColumns))
		args := make([]any, 0, len(matchedColumns))
		for _, column := range matchedColumns {
			args = append(args, alias)
			predicates = append(predicates, fmt.Sprintf("%s::text = $%d", quoteIdent(column), len(args)))
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(table), strings.Join(predicates, " OR "))
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func serverTableExists(ctx context.Context, db *sql.DB, table string) bool {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`, table).Scan(&exists)
	return err == nil && exists
}

func serverTableColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []string{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

func (s *Server) uniqueTenantIDLocked(projectID string, name string) string {
	base := slug(name)
	if base == "" {
		base = "tenant"
	}
	return uniqueName(base, func(value string) bool {
		if _, ok := s.tenants[tenantStoreKey(projectID, value)]; ok {
			return true
		}
		if s.config.TenantDatabases != nil && s.config.TenantDatabases[tenantStoreKey(projectID, value)] != "" {
			return true
		}
		return false
	})
}

func (s *Server) uniqueTenantDatabaseNameLocked(projectID string, tenantID string) string {
	base := tenantDatabaseName(projectID, tenantID)
	return uniqueName(base, func(value string) bool {
		for _, tenant := range s.tenants {
			if tenant.databaseName == value {
				return true
			}
		}
		return false
	})
}

func (s *Server) tenantDatabaseAliasTakenLocked(projectID string, alias string, exceptKey string) bool {
	alias = normalizeDatabaseAlias(alias)
	if alias == "" {
		return false
	}
	for key, tenant := range s.tenants {
		if key == exceptKey || tenant.ProjectID != projectID {
			continue
		}
		if normalizeDatabaseAlias(tenant.Database) == alias {
			return true
		}
	}
	return false
}

func provisionTenantDatabase(ctx context.Context, databaseURL string, desiredSchema manifest.Schema) error {
	if databaseURL == "" {
		return fmt.Errorf("tenant database URL is not configured")
	}
	if _, err := schema.Apply(ctx, databaseURL, desiredSchema); err != nil {
		return err
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if err := ensureTenantLocalTables(ctx, db); err != nil {
		return err
	}
	return nil
}

func (s *Server) ensureRuntimeTenantDatabase(ctx context.Context, project string, tenantID string, tenantDatabaseURL string) (string, error) {
	project = strings.TrimSpace(project)
	tenantID = strings.TrimSpace(tenantID)
	tenantDatabaseURL = strings.TrimSpace(tenantDatabaseURL)
	if project == "" || tenantID == "" || tenantID == project || tenantDatabaseURL == "" {
		return tenantDatabaseURL, nil
	}

	key := tenantStoreKey(project, tenantID)
	s.projectMu.RLock()
	tenant, ok := s.tenants[key]
	projectDatabaseURL := s.config.DatabaseURL(project)
	postgresURL := strings.TrimSpace(s.config.PostgresURL)
	s.projectMu.RUnlock()
	if !ok || tenant.databaseURL == "" || tenant.databaseURL != tenantDatabaseURL || tenantDatabaseURL == projectDatabaseURL {
		return tenantDatabaseURL, nil
	}
	if tenant.Provisioned {
		return tenantDatabaseURL, nil
	}

	desiredSchema := s.runtime.ManifestForProject(project).Schema.TenantSchema()
	if err := provisionTenantDatabase(ctx, tenantDatabaseURL, desiredSchema); err == nil {
		s.markTenantDatabaseProvisioned(project, tenantID, tenantDatabaseURL)
		return tenantDatabaseURL, nil
	} else if !isMissingTenantDatabaseError(err) {
		return "", err
	}

	if postgresURL == "" {
		return "", fmt.Errorf("tenant database %q does not exist and DATABASE_URL is not configured", databaseNameFromURL(tenantDatabaseURL, tenant.databaseName))
	}
	databaseName := strings.TrimSpace(tenant.databaseName)
	if databaseName == "" {
		databaseName = databaseNameFromURL(tenantDatabaseURL, tenantDatabaseName(project, tenantID))
	}
	createdURL, err := createProjectDatabase(ctx, postgresURL, databaseName)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return "", err
		}
		createdURL, err = databaseURL(postgresURL, databaseName)
		if err != nil {
			return "", err
		}
	}
	if err := provisionTenantDatabase(ctx, createdURL, desiredSchema); err != nil {
		return "", err
	}
	s.markTenantDatabaseProvisioned(project, tenantID, createdURL)
	return createdURL, nil
}

func (s *Server) markTenantDatabaseProvisioned(project string, tenantID string, databaseURL string) {
	key := tenantStoreKey(project, tenantID)
	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	tenant := s.tenants[key]
	tenant.ID = tenantID
	tenant.ProjectID = project
	tenant.databaseURL = databaseURL
	tenant.databaseName = databaseNameFromURL(databaseURL, tenant.databaseName)
	if tenant.Database == "" {
		tenant.Database = tenant.databaseName
	}
	tenant.Provisioned = true
	s.tenants[key] = tenant
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	s.config.TenantDatabases[key] = databaseURL
}

func (s *Server) provisionCreatedTenant(ctx context.Context, project string, result any) error {
	tenantID := tenantIDFromMutationResult(result)
	if tenantID == "" {
		return nil
	}
	s.hydrateLandlordTenants(ctx, project)
	databaseURL := s.databaseURLForTenant(project, tenantID)
	_, err := s.ensureRuntimeTenantDatabase(ctx, project, tenantID, databaseURL)
	return err
}

func tenantIDFromMutationResult(result any) string {
	switch value := result.(type) {
	case string:
		return strings.TrimSpace(value)
	case map[string]any:
		for _, key := range []string{"_id", "id", "domain", "database"} {
			if id := strings.TrimSpace(fmt.Sprint(value[key])); id != "" && id != "<nil>" {
				return id
			}
		}
	case map[string]string:
		for _, key := range []string{"_id", "id", "domain", "database"} {
			if id := strings.TrimSpace(value[key]); id != "" {
				return id
			}
		}
	}
	return ""
}

func (s *Server) applyTenantSchemasForProject(ctx context.Context, project string, desiredSchema manifest.Schema) (schema.Result, error) {
	s.hydrateLandlordTenants(ctx, project)
	s.hydrateProjectTenantDatabases(ctx, project)
	desiredSchema = desiredSchema.TenantSchema()

	s.projectMu.RLock()
	tenants := make([]tenantTarget, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		if tenant.ProjectID == project && tenant.databaseURL != "" {
			tenants = append(tenants, tenant)
		}
	}
	s.projectMu.RUnlock()

	result := schema.Result{}
	seen := map[string]bool{}
	for _, tenant := range dedupeTenantTargets(tenants) {
		if tenant.databaseURL == "" || seen[tenant.databaseURL] {
			continue
		}
		seen[tenant.databaseURL] = true
		applied, err := schema.Apply(ctx, tenant.databaseURL, desiredSchema)
		if err != nil {
			if isMissingTenantDatabaseError(err) {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: skipped missing tenant database", tenant.ID))
				continue
			}
			// A standalone-discovered database that rejects this schema almost
			// certainly belongs to a different project (e.g. the dashboard app
			// syncing while whagons tenant databases exist in the same cluster).
			// Skip it instead of failing the whole sync.
			if tenant.standaloneDiscovered && errors.Is(err, schema.ErrUnsafeChange) {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s: skipped standalone-discovered database with incompatible schema: %v", tenant.ID, err))
				continue
			}
			return result, fmt.Errorf("tenant %s schema sync failed: %w", tenant.ID, err)
		}
		for _, statement := range applied.Applied {
			result.Applied = append(result.Applied, fmt.Sprintf("%s: %s", tenant.ID, statement))
		}
		for _, warning := range applied.Warnings {
			result.Warnings = append(result.Warnings, fmt.Sprintf("%s: %s", tenant.ID, warning))
		}
	}
	return result, nil
}

func (s *Server) hydrateProjectTenantDatabases(ctx context.Context, project string) {
	project = strings.TrimSpace(project)
	if project == "" || strings.TrimSpace(s.config.PostgresURL) == "" {
		return
	}
	if !s.shouldDiscoverProjectTenantDatabases(project) {
		return
	}
	tenants, err := s.discoverProjectTenantDatabases(ctx, project)
	if err != nil || len(tenants) == 0 {
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	for _, tenant := range tenants {
		key := tenantStoreKey(project, tenant.ID)
		if existing, ok := s.tenants[key]; ok && tenantTargetPriority(existing) > tenantTargetPriority(tenant) {
			continue
		}
		s.tenants[key] = tenant
		s.config.TenantDatabases[key] = tenant.databaseURL
	}
}

func (s *Server) shouldDiscoverProjectTenantDatabases(project string) bool {
	now := time.Now()
	s.tenantDiscoveryMu.Lock()
	defer s.tenantDiscoveryMu.Unlock()
	if last, ok := s.tenantDiscoveryAt[project]; ok && now.Sub(last) < projectTenantDiscoveryTTL {
		return false
	}
	s.tenantDiscoveryAt[project] = now
	return true
}

func (s *Server) invalidateProjectTenantDiscovery(project string) {
	project = strings.TrimSpace(project)
	if project == "" {
		return
	}
	s.tenantDiscoveryMu.Lock()
	delete(s.tenantDiscoveryAt, project)
	s.tenantDiscoveryMu.Unlock()
}

func (s *Server) discoverProjectTenantDatabases(ctx context.Context, project string) ([]tenantTarget, error) {
	db, err := openMaintenanceDB(s.config.PostgresURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	projectSuffix := tenantDatabaseProjectSuffix(project)
	projectSuffixPattern := strings.ReplaceAll(projectSuffix, "_", `\_`)
	projectDatabase := databaseNameFromURL(s.databaseURLForProject(project), "")
	rows, err := db.QueryContext(ctx, `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false AND datname LIKE $1 ESCAPE '\'
		ORDER BY datname
	`, `%\_`+projectSuffixPattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tenants []tenantTarget
	for rows.Next() {
		var databaseName string
		if err := rows.Scan(&databaseName); err != nil {
			return nil, err
		}
		if databaseName == projectDatabase {
			continue
		}
		alias, ok := strings.CutSuffix(databaseName, "_"+projectSuffix)
		if !ok || alias == "" {
			continue
		}
		tenantID := strings.ReplaceAll(alias, "_", "-")
		databaseURL, err := databaseURL(s.config.PostgresURL, databaseName)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, tenantTarget{
			ID:             tenantID,
			ProjectID:      project,
			Name:           tenantID,
			Database:       alias,
			Status:         "local",
			Description:    "Discovered local tenant database.",
			Provisioned:    true,
			RuntimeCreated: true,
			databaseURL:    databaseURL,
			databaseName:   databaseName,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	standalone, err := s.discoverStandaloneTenantDatabases(ctx, db, project, projectDatabase)
	if err != nil {
		return nil, err
	}
	tenants = append(tenants, standalone...)
	return tenants, nil
}

func (s *Server) discoverStandaloneTenantDatabases(ctx context.Context, maintenanceDB *sql.DB, project string, projectDatabase string) ([]tenantTarget, error) {
	rows, err := maintenanceDB.QueryContext(ctx, `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false
		ORDER BY datname
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tenants []tenantTarget
	for rows.Next() {
		var databaseName string
		if err := rows.Scan(&databaseName); err != nil {
			return nil, err
		}
		if !s.isStandaloneTenantDatabaseCandidate(project, projectDatabase, databaseName) {
			continue
		}
		databaseURL, err := databaseURL(s.config.PostgresURL, databaseName)
		if err != nil {
			return nil, err
		}
		if !tenantDatabaseHasAppTables(ctx, databaseURL) {
			continue
		}
		tenantID := strings.ReplaceAll(databaseName, "_", "-")
		tenants = append(tenants, tenantTarget{
			ID:                   tenantID,
			ProjectID:            project,
			Name:                 tenantID,
			Database:             databaseName,
			Status:               "local",
			Description:          "Discovered standalone local tenant database.",
			Provisioned:          true,
			RuntimeCreated:       true,
			databaseURL:          databaseURL,
			databaseName:         databaseName,
			standaloneDiscovered: true,
		})
	}
	return tenants, rows.Err()
}

func (s *Server) isStandaloneTenantDatabaseCandidate(project string, projectDatabase string, databaseName string) bool {
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" || databaseName == projectDatabase {
		return false
	}
	switch databaseName {
	case "postgres", "gonvex_app_telemetry", "gonvex_test", "gonvex_test_telemetry":
		return false
	}
	if strings.HasPrefix(databaseName, "gonvex_") {
		return false
	}
	projectSuffix := tenantDatabaseProjectSuffix(project)
	if projectSuffix != "" && strings.HasSuffix(databaseName, "_"+projectSuffix) {
		return false
	}
	if strings.HasSuffix(databaseName, "_dashboard") {
		return false
	}
	if strings.HasPrefix(databaseName, "e2e_") || strings.HasPrefix(databaseName, "testing_") {
		return false
	}
	return true
}

func tenantDatabaseHasAppTables(ctx context.Context, databaseURL string) bool {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return false
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_name = ANY($1)
	`, []string{"tasks", "workspaces", "spots", "teams"})
	if err != nil {
		return false
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return false
		}
		found[table] = true
	}
	return found["tasks"] && found["workspaces"]
}

func (s *Server) existingLocalDatabaseNames(ctx context.Context) map[string]bool {
	if strings.TrimSpace(s.config.PostgresURL) == "" {
		return nil
	}
	db, err := openMaintenanceDB(s.config.PostgresURL)
	if err != nil {
		return nil
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil
		}
		names[name] = true
	}
	return names
}

func tenantDatabaseNameForPersistedTenant(project string, tenantID string, databaseAlias string, domain string, existingDatabases map[string]bool) string {
	for _, candidate := range uniqueStrings([]string{databaseAlias, tenantID, domain}) {
		if existingDatabases[candidate] {
			return candidate
		}
	}
	return tenantDatabaseNameWithAlias(project, tenantID, databaseAlias)
}

func isMissingTenantDatabaseError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database") && strings.Contains(message, "does not exist")
}

func ensureTenantLocalTables(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS members (
			user_id TEXT PRIMARY KEY,
			role TEXT NOT NULL DEFAULT 'member',
			permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS members_by_role ON members (role)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
