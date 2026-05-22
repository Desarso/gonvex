package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
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

func (c *rowsCache) rowsKey(table string, query url.Values) string {
	hash := sha256.Sum256([]byte(query.Encode()))
	return "gonvex:rows:v1:" + table + ":" + hex.EncodeToString(hash[:])
}

func (c *rowsCache) get(ctx context.Context, key string) ([]byte, bool) {
	if !c.enabled() {
		return nil, false
	}
	value, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) || err != nil {
		return nil, false
	}
	return value, true
}

func (c *rowsCache) set(ctx context.Context, key string, value []byte) {
	if !c.enabled() {
		return
	}
	_ = c.client.Set(ctx, key, value, c.ttl).Err()
}

func (c *rowsCache) invalidateRows(ctx context.Context, table string) {
	if !c.enabled() {
		return
	}
	pattern := "gonvex:rows:v1:*"
	if table != "" {
		pattern = "gonvex:rows:v1:" + table + ":*"
	}
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
