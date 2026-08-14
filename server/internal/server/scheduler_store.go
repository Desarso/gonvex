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
	scheduledJobsKey         = "gonvex:scheduler:{global}:jobs"
	scheduledJobPayloadsKey  = "gonvex:scheduler:{global}:payloads"
	scheduledJobFenceKey     = "gonvex:scheduler:{global}:fence"
	scheduledJobClaimPrefix  = "gonvex:scheduler:{global}:claim:"
	scheduledJobDedupePrefix = "gonvex:scheduler:{global}:seen:"
	scheduledJobLease        = 5 * time.Minute
	scheduledJobDedupeTTL    = 30 * 24 * time.Hour
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

func newValkeyScheduledJobStore(client *redis.Client) scheduledJobStore {
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
	_, err = enqueueScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{scheduledJobsKey, scheduledJobPayloadsKey, scheduledJobDedupePrefix + job.ID},
		job.ID,
		payload,
		job.RunAt.UnixMilli(),
		scheduledJobDedupeTTL.Milliseconds(),
	).Result()
	return err
}

var claimScheduledJobsScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local claimed = {}
for _, id in ipairs(ids) do
  if #claimed >= tonumber(ARGV[2]) * 3 then
    break
  end
  local claim_key = ARGV[3] .. id
  if redis.call('EXISTS', claim_key) == 0 then
    local fence = redis.call('INCR', KEYS[3])
    local claim_token = ARGV[4] .. ':' .. tostring(fence)
    redis.call('SET', claim_key, claim_token, 'PX', ARGV[5])
    local payload = redis.call('HGET', KEYS[2], id)
    if payload then
      table.insert(claimed, id)
      table.insert(claimed, payload)
      table.insert(claimed, claim_token)
    else
      redis.call('ZREM', KEYS[1], id)
      redis.call('DEL', claim_key)
    end
  end
end
return claimed
`)

func (store *valkeyScheduledJobStore) claimDue(ctx context.Context, now time.Time, limit int, owner string) ([]scheduledJob, error) {
	if limit <= 0 {
		return nil, nil
	}
	result, err := claimScheduledJobsScript.Run(
		ctx,
		store.client,
		[]string{scheduledJobsKey, scheduledJobPayloadsKey, scheduledJobFenceKey},
		now.UnixMilli(),
		limit,
		scheduledJobClaimPrefix,
		owner,
		scheduledJobLease.Milliseconds(),
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
if redis.call('GET', KEYS[3]) ~= ARGV[1] then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[2])
redis.call('DEL', KEYS[3])
return 1
`)

func (store *valkeyScheduledJobStore) complete(ctx context.Context, id string, owner string) error {
	completed, err := completeScheduledJobScript.Run(
		ctx,
		store.client,
		[]string{scheduledJobsKey, scheduledJobPayloadsKey, scheduledJobClaimPrefix + id},
		owner,
		id,
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

func newScheduledJobID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return prefix + hex.EncodeToString(bytes[:])
	}
	return prefix + strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
}
