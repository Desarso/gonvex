# Gonvex Whagons-profile load runner

`gonvex-load` creates protocol-level Whagons users without opening browsers.
Each user authenticates, owns one or more persistent WebSocket connections,
opens an app-shaped subscription set, and generates profile-defined mutations.
The runner is intended for an isolated local runtime and disposable, seeded
tenant database.

It reports connection and subscription health, requested versus achieved
mutation throughput, invalidation traffic, wire bytes, and resource samples for
both the generator and an optional runtime PID. It also measures update
propagation from the server commit timestamp:

- **per-client propagation** is commit to receipt on one client;
- **TTLU (time to last user)** is the maximum per-client delay for one commit;
- the report includes exact TTLU p50/p95/max overall and per mutation path.

An invalidation may arrive before its mutation acknowledgement. The runner
joins both messages by their server commit timestamp and keeps sockets open for
a short drain after the final acknowledgement. Commits that do not invalidate
any subscribed query are reported separately as `commitsWithoutPropagation`.

## Profile schema

Version 2 profiles contain users, an average connection count, named value
pools, an empirical subscription-count distribution, subscription templates,
and a mutation mix:

```json
{
  "version": 2,
  "name": "example",
  "users": 1000,
  "connectionsPerUser": 1.7,
  "pools": {
    "workspace": ["workspace-a", "workspace-b"],
    "task": ["seed-task-a"],
    "status": ["seed-status-a"]
  },
  "subscriptionsPerConnection": [80, 89, 99],
  "subscriptions": [
    {"path": "bulk.tasksByWorkspace", "args": {"workspaceId": "$workspace"}, "count": 2},
    {"path": "users.me", "args": {}}
  ],
  "mutations": [
    {
      "path": "tasks.create",
      "args": {"workspaceId": "$workspace", "statusId": "$status"},
      "ratePerUserPerMinute": 0.2,
      "activeUsers": 0.2
    },
    {
      "path": "workplans.generateDueWorkplans",
      "args": {},
      "ratePerMinute": 34.2
    }
  ]
}
```

Pool values are selected uniformly and deterministically per user session, so
all of a user's connections use the same seeded workspace/task/status. Exact
`$name` and `${name}` strings are replaced while other strings stay literal.
Built-ins are `$tenant`, `$userId`, `$sequence`, and `$mutationId`. Use repeated
`--var name=value` flags to override a named pool for a run. Version 1
subscription-only profiles and the legacy single-mutation flags remain
supported.

`ratePerUserPerMinute` is applied only to the `activeUsers` fraction. A fixed
`ratePerMinute` models tenant-wide scheduler/heartbeat traffic. The bundled
[`whagons-prod-2026-08-11.json`](profiles/whagons-prod-2026-08-11.json) preserves
the production observation, while
[`whagons-1000-users.json`](profiles/whagons-1000-users.json) scales it to 1,000
users with 20% interactively active.

## Local 1,000-user run

1. Start a locally configured Gonvex runtime with the Whagons project and a
   disposable seeded tenant. The repository reference stack uses
   `make stack` and normally exposes the runtime at `http://127.0.0.1:8080`;
   use the actual URL of your Whagons runtime if it differs.

2. Seed at least one workspace, task, and status. Replace the placeholder pool
   values using flags (or edit a copy of the profile). Build the runner:

   ```bash
   GOCACHE=/tmp/gonvex-go-cache go build -o ./tmp/gonvex-load ./cmd/gonvex-load
   ```

3. Validate the complete plan without opening a socket:

   ```bash
   ./tmp/gonvex-load \
     --profile ./cmd/gonvex-load/profiles/whagons-1000-users.json \
     --url http://127.0.0.1:8080 \
     --project whagons-5 \
     --tenant loadtest \
     --var workspace=WORKSPACE_ID \
     --var task=TASK_ID \
     --var status=STATUS_ID \
     --dry-run
   ```

4. Run the simulation. Synthetic auth is the default and is suitable only when
   the local runtime accepts synthetic test JWTs. For a runtime requiring a
   real token, set `GONVEX_LOAD_TOKEN` and add `--auth-mode shared` (all users
   then share that identity).

   ```bash
   RUNTIME_PID=12345
   ./tmp/gonvex-load \
     --profile ./cmd/gonvex-load/profiles/whagons-1000-users.json \
     --url http://127.0.0.1:8080 \
     --project whagons-5 \
     --tenant loadtest \
     --var workspace=WORKSPACE_ID \
     --var task=TASK_ID \
     --var status=STATUS_ID \
     --ramp 2m \
     --hold 10m \
     --target-pid "$RUNTIME_PID" \
     --min-host-available-mib 4096 \
     --max-target-rss-mib 20480 \
     --report ./tmp/load-reports/whagons-1000.json
   ```

The CLI rejects non-loopback targets unless `--allow-non-loopback` is supplied.
Do not use that escape hatch for production stress tests without separate
approval and traffic controls.

## Reading the result

The terminal summary shows opened subscriptions, errors, achieved/requested
mutation throughput, and TTLU. The JSON `RunReport` contains the same totals,
per-path mutation and TTLU reports, the per-client propagation distribution,
all resource samples, and sampled errors.

For a healthy same-host local run, the working target is **TTLU p95 below
200 ms**, with no setup/unexpected-close errors, a low operation error rate,
and achieved mutation throughput close to requested. Interpret p95 only when
there are enough propagated commits; a growing gap between requested and
achieved throughput or rising TTLU generally indicates queueing. CPU, RSS,
connections, and database capacity must still retain headroom at the end of the
hold.
