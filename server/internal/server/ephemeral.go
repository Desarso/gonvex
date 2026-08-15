package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/redis/go-redis/v9"
)

const maxEphemeralKeyBytes = 512

type ephemeralBackend interface {
	ForTenant(context.Context, string, string) gonvex.EphemeralAPI
	ForProject(context.Context, string) gonvex.EphemeralAPI
}

type valkeyEphemeralBackend struct{ client *redis.Client }

func (backend *valkeyEphemeralBackend) ForTenant(ctx context.Context, projectID, tenantID string) gonvex.EphemeralAPI {
	return newTenantEphemeralAPI(ctx, backend.client, projectID, tenantID)
}

func (backend *valkeyEphemeralBackend) ForProject(ctx context.Context, projectID string) gonvex.EphemeralAPI {
	return newProjectEphemeralAPI(ctx, backend.client, projectID)
}

func newScopedEphemeralAPI(ctx context.Context, backend ephemeralBackend, projectID, tenantID string) gonvex.EphemeralAPI {
	if backend == nil {
		return nil
	}
	return backend.ForTenant(ctx, projectID, tenantID)
}

func newProjectScopedEphemeralAPI(ctx context.Context, backend ephemeralBackend, projectID string) gonvex.EphemeralAPI {
	if backend == nil {
		return nil
	}
	return backend.ForProject(ctx, projectID)
}

// tenantEphemeralAPI keeps one expiry-sorted index per project/tenant. Listing
// walks only that tenant's live members (never the global Redis keyspace), then
// fetches their independently expiring value keys with MGET.
type tenantEphemeralAPI struct {
	ctx         context.Context
	client      *redis.Client
	indexKey    string
	valuePrefix string
	now         func() time.Time
}

var _ gonvex.EphemeralAPI = (*tenantEphemeralAPI)(nil)

func newTenantEphemeralAPI(ctx context.Context, client *redis.Client, projectID, tenantID string) gonvex.EphemeralAPI {
	if client == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	projectID, tenantID = cacheScope(projectID, tenantID)
	scope := sha256.Sum256([]byte(projectID + "\x00" + tenantID))
	return newEphemeralAPI(ctx, client, scope)
}

func newProjectEphemeralAPI(ctx context.Context, client *redis.Client, projectID string) gonvex.EphemeralAPI {
	if client == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	projectID, _ = cacheScope(projectID, projectID)
	// Keep this distinct from the existing project+tenant hash input so no
	// tenant identifier can collide with the project-wide namespace.
	scope := sha256.Sum256([]byte("gonvex:project-ephemeral:v1\x00" + projectID))
	return newEphemeralAPI(ctx, client, scope)
}

func newEphemeralAPI(ctx context.Context, client *redis.Client, scope [sha256.Size]byte) gonvex.EphemeralAPI {
	// A Redis hash tag keeps this tenant's index and values in one cluster slot.
	prefix := "gonvex:ephemeral:v1:{" + hex.EncodeToString(scope[:16]) + "}"
	return &tenantEphemeralAPI{
		ctx:         ctx,
		client:      client,
		indexKey:    prefix + ":expires",
		valuePrefix: prefix + ":value:",
		now:         time.Now,
	}
}

func (store *tenantEphemeralAPI) Set(key string, value any, ttl time.Duration) error {
	if err := validateEphemeralFragment("key", key, false); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("gonvex: ephemeral Set requires a positive TTL")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("gonvex: encode ephemeral value: %w", err)
	}
	expiresAt := store.now().UTC().Add(ttl)
	_, err = store.client.TxPipelined(store.ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(store.ctx, store.valueKey(key), payload, ttl)
		pipe.ZAdd(store.ctx, store.indexKey, redis.Z{Score: float64(expiresAt.UnixMilli()), Member: key})
		return nil
	})
	if err != nil {
		return fmt.Errorf("gonvex: write ephemeral value to Valkey: %w", err)
	}
	return nil
}

func (store *tenantEphemeralAPI) Get(key string, target any) (bool, error) {
	if err := validateEphemeralFragment("key", key, false); err != nil {
		return false, err
	}
	if target == nil {
		return false, fmt.Errorf("gonvex: ephemeral Get target is required")
	}
	payload, err := store.client.Get(store.ctx, store.valueKey(key)).Bytes()
	if err == redis.Nil {
		_ = store.client.ZRem(store.ctx, store.indexKey, key).Err()
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("gonvex: read ephemeral value from Valkey: %w", err)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return false, fmt.Errorf("gonvex: decode ephemeral value: %w", err)
	}
	return true, nil
}

func (store *tenantEphemeralAPI) Delete(key string) error {
	if err := validateEphemeralFragment("key", key, false); err != nil {
		return err
	}
	_, err := store.client.TxPipelined(store.ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(store.ctx, store.valueKey(key))
		pipe.ZRem(store.ctx, store.indexKey, key)
		return nil
	})
	if err != nil {
		return fmt.Errorf("gonvex: delete ephemeral value from Valkey: %w", err)
	}
	return nil
}

func (store *tenantEphemeralAPI) List(prefix string) ([]gonvex.EphemeralEntry, error) {
	if err := validateEphemeralFragment("prefix", prefix, true); err != nil {
		return nil, err
	}
	nowMS := store.now().UTC().UnixMilli()
	// Pruning and listing are bounded by this tenant's index. No SCAN occurs.
	_ = store.client.ZRemRangeByScore(store.ctx, store.indexKey, "-inf", strconv.FormatInt(nowMS-1, 10)).Err()
	members, err := store.client.ZRangeByScoreWithScores(store.ctx, store.indexKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(nowMS, 10), Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("gonvex: list ephemeral index from Valkey: %w", err)
	}
	filtered := make([]redis.Z, 0, len(members))
	valueKeys := make([]string, 0, len(members))
	for _, member := range members {
		key, ok := member.Member.(string)
		if !ok || !strings.HasPrefix(key, prefix) {
			continue
		}
		filtered = append(filtered, member)
		valueKeys = append(valueKeys, store.valueKey(key))
	}
	if len(filtered) == 0 {
		return []gonvex.EphemeralEntry{}, nil
	}
	values, err := store.client.MGet(store.ctx, valueKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("gonvex: read ephemeral values from Valkey: %w", err)
	}
	entries := make([]gonvex.EphemeralEntry, 0, len(values))
	stale := make([]any, 0)
	for index, value := range values {
		key, _ := filtered[index].Member.(string)
		if value == nil {
			stale = append(stale, key)
			continue
		}
		var payload []byte
		switch typed := value.(type) {
		case string:
			payload = []byte(typed)
		case []byte:
			payload = append([]byte(nil), typed...)
		default:
			payload = []byte(fmt.Sprint(typed))
		}
		entries = append(entries, gonvex.EphemeralEntry{
			Key:       key,
			Value:     json.RawMessage(payload),
			ExpiresAt: time.UnixMilli(int64(filtered[index].Score)).UTC(),
		})
	}
	if len(stale) > 0 {
		_ = store.client.ZRem(store.ctx, store.indexKey, stale...).Err()
	}
	return entries, nil
}

func (store *tenantEphemeralAPI) valueKey(key string) string {
	return store.valuePrefix + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func validateEphemeralFragment(name, value string, allowEmpty bool) error {
	if value == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("gonvex: ephemeral %s is required", name)
	}
	if len(value) > maxEphemeralKeyBytes {
		return fmt.Errorf("gonvex: ephemeral %s exceeds %d bytes", name, maxEphemeralKeyBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("gonvex: ephemeral %s must be valid UTF-8", name)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("gonvex: ephemeral %s must not contain control characters", name)
		}
	}
	return nil
}
