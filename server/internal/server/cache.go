package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strconv"
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

func (c *rowsCache) queryKey(projectID string, tenantID string, generation int64, scope string, path string, args []byte) string {
	projectID, tenantID = cacheScope(projectID, tenantID)
	prefix := "gonvex:queries:v1:" + cacheKeyPart(projectID) + ":" + cacheKeyPart(tenantID) + ":" + strconv.FormatInt(generation, 10) + ":"
	hash := sha256.Sum256([]byte(strings.Join([]string{scope, path, string(args)}, "\x00")))
	return prefix + hex.EncodeToString(hash[:])
}

func (c *rowsCache) queryGeneration(ctx context.Context, projectID string, tenantID string) (int64, bool) {
	if !c.enabled() {
		return 0, false
	}
	value, err := c.client.Get(ctx, c.queryGenerationKey(projectID, tenantID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, true
	}
	return value, err == nil
}

func (c *rowsCache) invalidateQueries(ctx context.Context, projectID string, tenantID string) {
	if !c.enabled() {
		return
	}
	_ = c.client.Incr(ctx, c.queryGenerationKey(projectID, tenantID)).Err()
}

func (c *rowsCache) queryGenerationKey(projectID string, tenantID string) string {
	projectID, tenantID = cacheScope(projectID, tenantID)
	return "gonvex:queries:v1:generation:" + cacheKeyPart(projectID) + ":" + cacheKeyPart(tenantID)
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
