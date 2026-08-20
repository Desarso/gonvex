package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type rowsCache struct {
	client *redis.Client
	ttl    time.Duration
	// bootEpoch is unique per process lifetime. It is mixed into every query
	// generation so a runtime restart never serves pre-restart Valkey entries
	// (even when Redis generation counters were not incremented).
	bootEpoch string
}

func newRowsCache(rawURL string, ttl time.Duration) (*rowsCache, error) {
	if rawURL == "" || ttl <= 0 {
		return nil, nil
	}
	client, err := newValkeyClient(rawURL)
	if err != nil {
		return nil, err
	}
	return newRowsCacheWithClient(client, ttl), nil
}

func newValkeyClient(rawURL string) (*redis.Client, error) {
	options, err := redis.ParseURL(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, err
	}
	return redis.NewClient(options), nil
}

func newRowsCacheWithClient(client *redis.Client, ttl time.Duration) *rowsCache {
	if client == nil || ttl <= 0 {
		return nil
	}
	return &rowsCache{
		client:    client,
		ttl:       ttl,
		bootEpoch: fmt.Sprintf("%d-%d", time.Now().UTC().UnixNano(), os.Getpid()),
	}
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

func (c *rowsCache) rowsKey(ctx context.Context, projectID string, tenantID string, table string, query url.Values) string {
	hash := sha256.Sum256([]byte(query.Encode()))
	generationHash := sha256.Sum256([]byte(c.rowsGeneration(ctx, projectID, tenantID, table)))
	projectID, tenantID = replicaScope(projectID, tenantID)
	return "gonvex:rows:v2:" + projectID + ":" + tenantID + ":" + table + ":" +
		hex.EncodeToString(generationHash[:12]) + ":" + hex.EncodeToString(hash[:])
}

func (c *rowsCache) rowsGeneration(ctx context.Context, projectID string, tenantID string, table string) string {
	if !c.enabled() {
		return c.bootEpoch
	}
	keys := []string{
		c.rowsGenerationKey(projectID, tenantID, "*"),
		c.rowsGenerationKey(projectID, tenantID, table),
	}
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return c.bootEpoch
	}
	return strings.Join([]string{
		"boot=" + c.bootEpoch,
		keys[0] + "=" + strings.TrimSpace(fmt.Sprint(values[0])),
		keys[1] + "=" + strings.TrimSpace(fmt.Sprint(values[1])),
	}, "\x00")
}

func (c *rowsCache) rowsGenerationKey(projectID string, tenantID string, table string) string {
	projectID, tenantID = replicaScope(projectID, tenantID)
	return "gonvex:rows:v2:generation:" + cacheKeyPart(projectID) + ":" + cacheKeyPart(tenantID) + ":" + cacheKeyPart(table)
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
	c.setWithTTL(ctx, key, value, c.ttl)
}

func (c *rowsCache) setWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if !c.enabled() {
		return
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	_ = c.client.Set(ctx, key, value, ttl).Err()
}

func (c *rowsCache) invalidateRows(ctx context.Context, projectID string, tenantID string, table string) {
	if !c.enabled() {
		return
	}
	if strings.TrimSpace(table) == "" {
		table = "*"
	}
	_ = c.client.Incr(ctx, c.rowsGenerationKey(projectID, tenantID, table)).Err()
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

func (c *rowsCache) clearProject(ctx context.Context, projectID string) int64 {
	if !c.enabled() {
		return 0
	}
	projectID, _ = replicaScope(projectID, "")
	patterns := []string{
		"gonvex:rows:v2:" + projectID + ":*",
		"gonvex:rows:v2:generation:" + cacheKeyPart(projectID) + ":*",
	}
	var cleared int64
	for _, pattern := range patterns {
		var cursor uint64
		for {
			keys, nextCursor, err := c.client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				if count, err := c.client.Del(ctx, keys...).Result(); err == nil {
					cleared += count
				}
			}
			if nextCursor == 0 {
				break
			}
			cursor = nextCursor
		}
	}
	return cleared
}

func cacheKeyPart(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:12])
}

func replicaScope(projectID string, tenantID string) (string, string) {
	if projectID == "" {
		projectID = "default"
	}
	if tenantID == "" {
		tenantID = projectID
	}
	return projectID, tenantID
}
