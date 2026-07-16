package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type rowsCache struct {
	client *redis.Client
	ttl    time.Duration
}

func newRowsCache(rawURL string, ttl time.Duration) (*rowsCache, error) {
	if rawURL == "" || ttl <= 0 {
		return nil, nil
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	return &rowsCache{client: redis.NewClient(options), ttl: ttl}, nil
}

func (c *rowsCache) enabled() bool {
	return c != nil && c.client != nil && c.ttl > 0
}

func (c *rowsCache) close() error {
	if !c.enabled() {
		return nil
	}
	return c.client.Close()
}

func (c *rowsCache) rowsKey(projectID string, tenantID string, table string, query url.Values) string {
	hash := sha256.Sum256([]byte(query.Encode()))
	projectID, tenantID = cacheScope(projectID, tenantID)
	return "gonvex:rows:v2:" + projectID + ":" + tenantID + ":" + table + ":" + hex.EncodeToString(hash[:])
}

func (c *rowsCache) get(ctx context.Context, key string) ([]byte, bool) {
	value, outcome := c.read(ctx, key)
	return value, outcome == "hit"
}

func (c *rowsCache) read(ctx context.Context, key string) ([]byte, string) {
	if !c.enabled() {
		return nil, "bypass"
	}
	value, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, "miss"
	}
	if err != nil {
		return nil, "error"
	}
	return value, "hit"
}

func (c *rowsCache) set(ctx context.Context, key string, value []byte) {
	if !c.enabled() {
		return
	}
	_ = c.client.Set(ctx, key, value, c.ttl).Err()
}

func (c *rowsCache) queryKey(projectID string, tenantID string, generation string, scope string, path string, args []byte) string {
	projectID, tenantID = cacheScope(projectID, tenantID)
	prefix := "gonvex:queries:v2:" + cacheKeyPart(projectID) + ":" + cacheKeyPart(tenantID) + ":"
	hash := sha256.Sum256([]byte(strings.Join([]string{scope, path, string(args)}, "\x00")))
	generationHash := sha256.Sum256([]byte(generation))
	return prefix + hex.EncodeToString(generationHash[:12]) + ":" + hex.EncodeToString(hash[:])
}

func (c *rowsCache) queryGeneration(ctx context.Context, projectID string, tenantID string, tables []string) (string, bool) {
	if !c.enabled() {
		return "", false
	}
	tables = queryCacheTables(tables)
	keys := make([]string, 0, len(tables)+1)
	keys = append(keys, c.queryGenerationKey(projectID, tenantID, "*"))
	for _, table := range tables {
		keys = append(keys, c.queryGenerationKey(projectID, tenantID, table))
	}
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return "", false
	}
	parts := make([]string, len(keys))
	for index, value := range values {
		parts[index] = keys[index] + "=" + strings.TrimSpace(fmt.Sprint(value))
	}
	return strings.Join(parts, "\x00"), true
}

func (c *rowsCache) invalidateQueries(ctx context.Context, projectID string, tenantID string, tables []string) {
	if !c.enabled() {
		return
	}
	tables = queryCacheTables(tables)
	if len(tables) == 0 {
		tables = []string{"*"}
	}
	pipeline := c.client.Pipeline()
	for _, table := range tables {
		pipeline.Incr(ctx, c.queryGenerationKey(projectID, tenantID, table))
	}
	_, _ = pipeline.Exec(ctx)
}

func (c *rowsCache) queryGenerationKey(projectID string, tenantID string, table string) string {
	projectID, tenantID = cacheScope(projectID, tenantID)
	return "gonvex:queries:v2:generation:" + cacheKeyPart(projectID) + ":" + cacheKeyPart(tenantID) + ":" + cacheKeyPart(table)
}

func queryCacheTables(tables []string) []string {
	unique := map[string]bool{}
	for _, table := range tables {
		table = strings.TrimSpace(table)
		if table == "" || table == "*" {
			continue
		}
		unique[table] = true
	}
	result := make([]string, 0, len(unique))
	for table := range unique {
		result = append(result, table)
	}
	sort.Strings(result)
	return result
}

func (c *rowsCache) invalidateRows(ctx context.Context, projectID string, tenantID string, table string) {
	if !c.enabled() {
		return
	}
	projectID, tenantID = cacheScope(projectID, tenantID)
	pattern := "gonvex:rows:v2:" + projectID + ":" + tenantID + ":*"
	if table != "" {
		pattern = "gonvex:rows:v2:" + projectID + ":" + tenantID + ":" + table + ":*"
	}
	c.deletePattern(ctx, pattern)
}

func (c *rowsCache) deletePattern(ctx context.Context, pattern string) {
	var cursor uint64
	for {
		keys, nextCursor, err := c.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			_ = c.client.Del(ctx, keys...).Err()
		}
		if nextCursor == 0 {
			return
		}
		cursor = nextCursor
	}
}

func cacheKeyPart(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:12])
}

func cacheScope(projectID string, tenantID string) (string, string) {
	if projectID == "" {
		projectID = "default"
	}
	if tenantID == "" {
		tenantID = projectID
	}
	return projectID, tenantID
}
