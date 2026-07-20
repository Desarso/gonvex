package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

const (
	defaultTenantStoreIdleTTL = 5 * time.Minute
)

type tenantStoreResolver struct {
	cfg          *config.Config
	now          func() time.Time
	idleTTL      time.Duration
	openDatabase func(context.Context, string) (*sql.DB, error)

	mu           sync.Mutex
	stores       map[string]*tenantStore
	initializing map[string]*tenantStoreInitialization
}

type tenantStoreInitialization struct {
	done  chan struct{}
	store *tenantStore
	err   error
}

type tenantStore struct {
	TenantID    string
	DatabaseURL string
	DB          *sql.DB
	lastUsed    time.Time
}

type databasePoolStats struct {
	Pools              int
	OpenConnections    int
	InUse              int
	Idle               int
	MaxOpenConnections int
	WaitCount          int64
	WaitDuration       time.Duration
}

func newTenantStoreResolver(cfg *config.Config) *tenantStoreResolver {
	return &tenantStoreResolver{
		cfg:          cfg,
		now:          time.Now,
		idleTTL:      defaultTenantStoreIdleTTL,
		openDatabase: openTenantDatabase,
		stores:       map[string]*tenantStore{},
		initializing: map[string]*tenantStoreInitialization{},
	}
}

func openTenantDatabase(ctx context.Context, databaseURL string) (*sql.DB, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (r *tenantStoreResolver) StartIdleReaper(ctx context.Context) {
	if r.idleTTL <= 0 {
		return
	}
	ticker := time.NewTicker(r.idleTTL)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				r.Close()
				return
			case <-ticker.C:
				r.ReapIdle()
			}
		}
	}()
}

func (r *tenantStoreResolver) TenantStore(ctx context.Context, tenantID string) (*tenantStore, error) {
	tenantID = normalizeTenantID(tenantID)
	return r.Store(ctx, tenantID, r.cfg.TenantDatabaseURL(tenantID))
}

func (r *tenantStoreResolver) Store(ctx context.Context, tenantID string, databaseURL string) (*tenantStore, error) {
	tenantID = normalizeTenantID(tenantID)
	if databaseURL == "" {
		return &tenantStore{TenantID: tenantID, lastUsed: r.now()}, nil
	}

	for {
		r.mu.Lock()
		if store := r.stores[tenantID]; store != nil && store.DatabaseURL == databaseURL {
			store.lastUsed = r.now()
			r.mu.Unlock()
			return store, nil
		}
		if pending := r.initializing[tenantID]; pending != nil {
			done := pending.done
			r.mu.Unlock()
			select {
			case <-done:
				if pending.err != nil {
					return nil, pending.err
				}
				if pending.store != nil && pending.store.DatabaseURL == databaseURL {
					return pending.store, nil
				}
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		pending := &tenantStoreInitialization{done: make(chan struct{})}
		r.initializing[tenantID] = pending
		r.mu.Unlock()

		store, err := r.initializeStore(ctx, tenantID, databaseURL)
		r.mu.Lock()
		if err == nil {
			if previous := r.stores[tenantID]; previous != nil && previous.DatabaseURL != databaseURL && previous.DB != nil {
				_ = previous.DB.Close()
			}
			r.stores[tenantID] = store
		}
		pending.store = store
		pending.err = err
		delete(r.initializing, tenantID)
		close(pending.done)
		r.mu.Unlock()
		return store, err
	}
}

func (r *tenantStoreResolver) initializeStore(ctx context.Context, tenantID string, databaseURL string) (*tenantStore, error) {
	db, err := r.openDatabase(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	return &tenantStore{
		TenantID:    tenantID,
		DatabaseURL: databaseURL,
		DB:          db,
		lastUsed:    r.now(),
	}, nil
}

func (r *tenantStoreResolver) ReapIdle() int {
	// database/sql retires idle physical connections using the configured
	// timeout. Retain the logical pool so future requests do not repeat tenant
	// resolution and pool initialization.
	return 0
}

func (r *tenantStoreResolver) Close() {
	r.mu.Lock()
	stores := make([]*tenantStore, 0, len(r.stores))
	for tenantID, store := range r.stores {
		delete(r.stores, tenantID)
		stores = append(stores, store)
	}
	r.mu.Unlock()
	for _, store := range stores {
		if store.DB != nil {
			_ = store.DB.Close()
		}
	}
}

func (r *tenantStoreResolver) DatabaseStats(projectID string) databasePoolStats {
	if r == nil {
		return databasePoolStats{}
	}
	prefix := strings.TrimSpace(projectID) + ":"
	r.mu.Lock()
	databases := make([]*sql.DB, 0, len(r.stores))
	for key, store := range r.stores {
		if store == nil || store.DB == nil || (projectID != "" && !strings.HasPrefix(key, prefix)) {
			continue
		}
		databases = append(databases, store.DB)
	}
	r.mu.Unlock()

	result := databasePoolStats{Pools: len(databases)}
	unlimited := false
	for _, database := range databases {
		stats := database.Stats()
		result.OpenConnections += stats.OpenConnections
		result.InUse += stats.InUse
		result.Idle += stats.Idle
		result.WaitCount += stats.WaitCount
		result.WaitDuration += stats.WaitDuration
		if stats.MaxOpenConnections == 0 {
			unlimited = true
		} else {
			result.MaxOpenConnections += stats.MaxOpenConnections
		}
	}
	if unlimited {
		result.MaxOpenConnections = 0
	}
	return result
}

func normalizeTenantID(tenantID string) string {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return "default"
	}
	return tenantID
}

func tenantIDFromRequest(projectID string, rawTenantID string) string {
	rawTenantID = strings.TrimSpace(rawTenantID)
	if rawTenantID != "" {
		return rawTenantID
	}
	if strings.TrimSpace(projectID) != "" {
		return projectID
	}
	return "default"
}

func tenantDatabaseName(projectID string, tenantID string) string {
	return tenantDatabaseNameWithAlias(projectID, tenantID, tenantID)
}

// tenantDatabaseNameWithAlias returns the physical Postgres database name for a
// tenant. New databases use a pure UUIDv6 so infinite projects × tenants never
// collide on org slugs. Human-readable names live in the control-plane registry
// (gonvex_runtime_tenants.name / database_alias), not in the DB identifier.
func tenantDatabaseNameWithAlias(projectID string, tenantID string, alias string) string {
	if name, err := generateTenantPhysicalDatabaseName(); err == nil {
		return name
	}
	return legacyTenantDatabaseNameWithAlias(projectID, tenantID, alias)
}

// legacyTenantDatabaseNameWithAlias is the historical
// <alias>_<project_id> convention. Kept for discovering already-provisioned
// databases and as a fallback if UUID generation fails.
func legacyTenantDatabaseNameWithAlias(projectID string, tenantID string, alias string) string {
	base := strings.ReplaceAll(slug(alias), "-", "_")
	if base == "" {
		base = strings.ReplaceAll(slug(tenantID), "-", "_")
	}
	if base == "" {
		base = "tenant"
	}
	suffix := tenantDatabaseProjectSuffix(projectID)
	maxBaseLength := 63 - len(suffix) - 1
	if maxBaseLength < 1 {
		maxBaseLength = 1
	}
	if len(base) > maxBaseLength {
		base = strings.Trim(base[:maxBaseLength], "_")
	}
	if base == "" {
		base = "tenant"
	}
	return base + "_" + suffix
}

func tenantDatabaseProjectSuffix(projectID string) string {
	suffix := strings.ReplaceAll(slug(projectID), "-", "_")
	if suffix == "" {
		return "default"
	}
	return suffix
}

func tenantStoreKey(projectID string, tenantID string) string {
	if projectID == "" {
		return normalizeTenantID(tenantID)
	}
	return fmt.Sprintf("%s:%s", strings.TrimSpace(projectID), normalizeTenantID(tenantID))
}
