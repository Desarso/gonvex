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
	cfg     *config.Config
	now     func() time.Time
	idleTTL time.Duration

	mu     sync.Mutex
	stores map[string]*tenantStore
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
		cfg:     cfg,
		now:     time.Now,
		idleTTL: defaultTenantStoreIdleTTL,
		stores:  map[string]*tenantStore{},
	}
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

	r.mu.Lock()
	if store := r.stores[tenantID]; store != nil && store.DatabaseURL == databaseURL {
		store.lastUsed = r.now()
		r.mu.Unlock()
		return store, nil
	}
	r.mu.Unlock()

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	dbpool.Configure(db)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	store := &tenantStore{
		TenantID:    tenantID,
		DatabaseURL: databaseURL,
		DB:          db,
		lastUsed:    r.now(),
	}

	r.mu.Lock()
	if previous := r.stores[tenantID]; previous != nil && previous.DatabaseURL == databaseURL {
		previous.lastUsed = r.now()
		r.mu.Unlock()
		_ = db.Close()
		return previous, nil
	} else if previous != nil {
		_ = previous.DB.Close()
	}
	r.stores[tenantID] = store
	r.mu.Unlock()
	return store, nil
}

func (r *tenantStoreResolver) ReapIdle() int {
	cutoff := r.now().Add(-r.idleTTL)
	var closing []*sql.DB

	r.mu.Lock()
	for tenantID, store := range r.stores {
		if store.lastUsed.Before(cutoff) {
			delete(r.stores, tenantID)
			closing = append(closing, store.DB)
		}
	}
	r.mu.Unlock()

	for _, db := range closing {
		if db != nil {
			_ = db.Close()
		}
	}
	return len(closing)
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

func tenantDatabaseNameWithAlias(projectID string, tenantID string, alias string) string {
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
