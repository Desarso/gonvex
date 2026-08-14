package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// scheduledJobsKey is the mixed queue used by runtimes before priority
	// queues were introduced. Keep it during rolling upgrades and migrate its
	// entries lazily in claimDue.
	scheduledJobsKey          = "gonvex:scheduler:{global}:jobs"
	scheduledOneShotJobsKey   = "gonvex:scheduler:{global}:jobs:oneshot"
	scheduledCronJobsKey      = "gonvex:scheduler:{global}:jobs:cron"
	scheduledJobPayloadsKey   = "gonvex:scheduler:{global}:payloads"
	scheduledJobFenceKey      = "gonvex:scheduler:{global}:fence"
	scheduledJobClaimPrefix   = "gonvex:scheduler:{global}:claim:"
	scheduledJobDedupePrefix  = "gonvex:scheduler:{global}:seen:"
	scheduledJobLease         = 5 * time.Minute
	scheduledJobDedupeTTL     = 30 * 24 * time.Hour
	scheduledLegacyBatchLimit = 4096
	scheduledClaimPageLimit   = 16
)

type scheduledJobStore interface {
	enqueue(context.Context, scheduledJob) error
	claimDue(context.Context, time.Time, int, string) ([]scheduledJob, error)
	renew(context.Context, string, string) (bool, error)
	complete(context.Context, string, string) error
	release(context.Context, string, string) error
}

type valkeyScheduledJobStore struct {
	client *redis.Client
}

func newValkeyScheduledJobStore(client *redis.Client) *valkeyScheduledJobStore {
	if client == nil {
		return nil
	}
	return &valkeyScheduledJobStore{client: client}
}

type scheduledJobPayload struct {
	ID           string          `json:"id"`
	ProjectID    string          `json:"projectId"`
	TenantID     string          `json:"tenantId,omitempty"`
	FunctionPath string          `json:"functionPath"`
	Args         json.RawMessage `json:"args,omitempty"`
	RunAt        time.Time       `json:"runAt"`
	ScheduledFor time.Time       `json:"scheduledFor"`
	CronName     string          `json:"cronName,omitempty"`
}

func payloadForScheduledJob(job scheduledJob) scheduledJobPayload {
	return scheduledJobPayload{
		ID: job.ID, ProjectID: job.ProjectID, TenantID: job.TenantID,
		FunctionPath: job.FunctionPath, Args: job.Args, RunAt: job.RunAt,
		ScheduledFor: job.ScheduledFor, CronName: job.CronName,
	}
}

func (payload scheduledJobPayload) job() scheduledJob {
	return scheduledJob{
		ID: payload.ID, ProjectID: payload.ProjectID, TenantID: payload.TenantID,
		FunctionPath: payload.FunctionPath, Args: payload.Args, RunAt: payload.RunAt,
		ScheduledFor: payload.ScheduledFor, CronName: payload.CronName,
	}
}

var enqueueScheduledJobScript = redis.NewScript(`
if not redis.call('SET', KEYS[3], '1', 'NX', 'PX', ARGV[4]) then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], 'NX', ARGV[3], ARGV[1])
return 1
`)

func (store *valkeyScheduledJobStore) enqueue(ctx context.Context, job scheduledJob) error {
	payload, err := json.Marshal(payloadForScheduledJob(job))
	if err != nil {
		return err
	}
	jobsKey := scheduledOneShotJobsKey
	if job.CronName != "" {
		jobsKey = scheduledCronJobsKey
	}
	_, err = enqueueScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{jobsKey, scheduledJobPayloadsKey, scheduledJobDedupePrefix + job.ID},
		job.ID,
		payload,
		job.RunAt.UnixMilli(),
		scheduledJobDedupeTTL.Milliseconds(),
	).Result()
	return err
}

var claimScheduledJobsScript = redis.NewScript(`
local limit = tonumber(ARGV[2])
local claimed = {}

local function remove_job(id)
  redis.call('ZREM', KEYS[1], id)
  redis.call('ZREM', KEYS[2], id)
  redis.call('ZREM', KEYS[3], id)
end

-- Each priority has its own durable index, so overdue cron volume cannot
-- affect discovery of a user-triggered job. Scan past leased entries; the
-- worker pool bounds their count in normal operation.
local function claim_from(jobs_key)
  local offset = 0
  local page_size = math.max(limit * 8, 64)
  local pages = 0
  while #claimed < limit * 3 and pages < tonumber(ARGV[7]) do
    pages = pages + 1
    local ids = redis.call('ZRANGEBYSCORE', jobs_key, '-inf', ARGV[1], 'LIMIT', offset, page_size)
    if #ids == 0 then
      return
    end
    local removed = 0
    for _, id in ipairs(ids) do
      if #claimed >= limit * 3 then
        return
      end
      local payload = redis.call('HGET', KEYS[4], id)
      if payload then
        local claim_key = ARGV[3] .. id
        if redis.call('EXISTS', claim_key) == 0 then
          local fence = redis.call('INCR', KEYS[5])
          local claim_token = ARGV[4] .. ':' .. tostring(fence)
          redis.call('SET', claim_key, claim_token, 'PX', ARGV[5])
          table.insert(claimed, id)
          table.insert(claimed, payload)
          table.insert(claimed, claim_token)
        end
      else
        remove_job(id)
        removed = removed + 1
      end
    end
    if #ids < page_size then
      return
    end
    offset = offset + #ids - removed
  end
end

claim_from(KEYS[1])

-- Older runtimes used one mixed queue. Partition one bounded batch per poll.
-- Never claim a cron while a due legacy entry remains: a later legacy page
-- could still conceal a one-shot, so the next poll continues migration first.
if #claimed < limit * 3 then
  local legacy_ids = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, ARGV[6])
  for _, id in ipairs(legacy_ids) do
    local payload = redis.call('HGET', KEYS[4], id)
    if payload then
      local decoded_ok, decoded = pcall(cjson.decode, payload)
      local is_cron = decoded_ok and type(decoded) == 'table' and decoded['cronName'] ~= nil and decoded['cronName'] ~= ''
      local destination = KEYS[1]
      if is_cron then
        destination = KEYS[2]
      end
      local score = redis.call('ZSCORE', KEYS[3], id)
      redis.call('ZADD', destination, 'NX', score, id)
    else
      redis.call('HDEL', KEYS[4], id)
    end
    redis.call('ZREM', KEYS[3], id)
  end
  claim_from(KEYS[1])
end

if #claimed < limit * 3 and redis.call('ZCOUNT', KEYS[3], '-inf', ARGV[1]) > 0 then
  return claimed
end

claim_from(KEYS[2])
return claimed
`)

func (store *valkeyScheduledJobStore) claimDue(ctx context.Context, now time.Time, limit int, owner string) ([]scheduledJob, error) {
	if limit <= 0 {
		return nil, nil
	}
	result, err := claimScheduledJobsScript.Run(
		ctx,
		store.client,
		[]string{
			scheduledOneShotJobsKey,
			scheduledCronJobsKey,
			scheduledJobsKey,
			scheduledJobPayloadsKey,
			scheduledJobFenceKey,
		},
		now.UnixMilli(),
		limit,
		scheduledJobClaimPrefix,
		owner,
		scheduledJobLease.Milliseconds(),
		scheduledLegacyBatchLimit,
		scheduledClaimPageLimit,
	).Result()
	if err != nil {
		return nil, err
	}
	values, ok := result.([]any)
	if !ok || len(values)%3 != 0 {
		return nil, fmt.Errorf("scheduler claim returned malformed payload %T", result)
	}
	jobs := make([]scheduledJob, 0, len(values)/3)
	for index := 0; index < len(values); index += 3 {
		id := fmt.Sprint(values[index])
		raw := []byte(fmt.Sprint(values[index+1]))
		claimToken := fmt.Sprint(values[index+2])
		var payload scheduledJobPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			for _, claimed := range jobs {
				_ = store.release(ctx, claimed.ID, claimed.ClaimToken)
			}
			_ = store.release(ctx, id, claimToken)
			return nil, fmt.Errorf("decode scheduled job %s: %w", id, err)
		}
		job := payload.job()
		job.ID = id
		job.ClaimToken = claimToken
		jobs = append(jobs, job)
	}
	return jobs, nil
}

var renewScheduledJobScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

func (store *valkeyScheduledJobStore) renew(ctx context.Context, id string, owner string) (bool, error) {
	result, err := renewScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{scheduledJobClaimPrefix + id},
		owner,
		scheduledJobLease.Milliseconds(),
	).Int64()
	return result == 1, err
}

var completeScheduledJobScript = redis.NewScript(`
if redis.call('GET', KEYS[5]) ~= ARGV[1] then
  if redis.call('GET', KEYS[5]) == false and redis.call('HEXISTS', KEYS[4], ARGV[2]) == 0 and redis.call('ZSCORE', KEYS[1], ARGV[2]) == false and redis.call('ZSCORE', KEYS[2], ARGV[2]) == false and redis.call('ZSCORE', KEYS[3], ARGV[2]) == false then
    return 1
  end
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
redis.call('DEL', KEYS[5])
redis.call('SET', KEYS[6], '1', 'PX', ARGV[3])
return 1
`)

func (store *valkeyScheduledJobStore) complete(ctx context.Context, id string, owner string) error {
	completed, err := completeScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{
			scheduledOneShotJobsKey,
			scheduledCronJobsKey,
			scheduledJobsKey,
			scheduledJobPayloadsKey,
			scheduledJobClaimPrefix + id,
			scheduledJobDedupePrefix + id,
		},
		owner,
		id,
		scheduledJobDedupeTTL.Milliseconds(),
	).Int64()
	if err != nil {
		return err
	}
	if completed != 1 {
		return fmt.Errorf("scheduled job %s completion rejected: claim is missing or owned by another runtime", id)
	}
	return nil
}

var releaseScheduledJobScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

func (store *valkeyScheduledJobStore) release(ctx context.Context, id string, owner string) error {
	_, err := releaseScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{scheduledJobClaimPrefix + id},
		owner,
	).Result()
	return err
}

var guardScheduledJobScript = redis.NewScript(`
local payload_exists = redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1
local seen = redis.call('EXISTS', KEYS[2]) == 1
if payload_exists then
  return 1
end
if seen then
  return 2
end
local existing = redis.call('GET', KEYS[3])
if existing then
  if existing == ARGV[2] then
    redis.call('PEXPIRE', KEYS[3], ARGV[3])
    return 0
  end
  return 1
end
if redis.call('SET', KEYS[3], ARGV[2], 'NX', 'PX', ARGV[3]) then
  return 0
end
return 1
`)

// guard shares the legacy claim key with Valkey-only runtimes. A Postgres job
// therefore cannot overlap the same deterministic cron occurrence while old
// and new containers coexist during a rolling update.
func (store *valkeyScheduledJobStore) guard(ctx context.Context, id string, owner string) (legacyScheduledJobGuard, error) {
	result, err := guardScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{scheduledJobPayloadsKey, scheduledJobDedupePrefix + id, scheduledJobClaimPrefix + id},
		id,
		owner,
		scheduledJobLease.Milliseconds(),
	).Int64()
	if err != nil {
		return legacyScheduledJobPending, err
	}
	switch result {
	case 0:
		return legacyScheduledJobGuarded, nil
	case 2:
		return legacyScheduledJobCompleted, nil
	default:
		return legacyScheduledJobPending, nil
	}
}

// refreshCompletionMarkers closes the only ambiguity left by the Valkey-only
// scheduler during a rolling upgrade. Its enqueue marker historically expired
// after 30 days even when the scheduled job was farther in the future. Refresh
// every still-queued payload before the Postgres runtime starts claiming work,
// so completion by an overlapping old runtime remains distinguishable from a
// brand-new Postgres-only job.
func (store *valkeyScheduledJobStore) refreshCompletionMarkers(ctx context.Context) error {
	var cursor uint64
	for {
		entries, next, err := store.client.HScan(ctx, scheduledJobPayloadsKey, cursor, "", 500).Result()
		if err != nil {
			return err
		}
		pipe := store.client.Pipeline()
		for index := 0; index+1 < len(entries); index += 2 {
			pipe.Set(ctx, scheduledJobDedupePrefix+entries[index], "1", scheduledJobDedupeTTL)
		}
		if len(entries) > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func newScheduledJobID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return prefix + hex.EncodeToString(bytes[:])
	}
	return prefix + strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
}
