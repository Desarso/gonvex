# Gonvex persistent-load runner

`gonvex-load` creates virtual users without opening browsers. Each virtual user
holds one WebSocket connection, keeps a configured set of query subscriptions
alive, and can issue mutations during the steady-state hold.

The runner records:

- connection, authentication, initial-result, server-query, mutation, and
  invalidation latency;
- successful and failed connections/subscriptions, by query path;
- successful and failed mutations, plus full-result, patch, and unchanged
  progress messages caused by invalidations;
- compressed wire bytes and logical WebSocket payload bytes;
- per-second throughput, process RSS/CPU/threads/file descriptors, and host
  available memory;
- automatic abort reasons when error or memory limits are crossed.

Build it from the Gonvex repository:

```bash
go build -o ./tmp/gonvex-load-runner ./cmd/gonvex-load
```

Validate a plan without connecting:

```bash
./tmp/gonvex-load-runner \
  --profile /path/to/whagons-workspace-50.json \
  --tenant e2e-loadtest \
  --connections 1000 \
  --dry-run
```

Run only against an isolated local runtime and disposable tenant database:

```bash
./tmp/gonvex-load-runner \
  --profile /path/to/whagons-workspace-50.json \
  --url http://127.0.0.1:18080 \
  --tenant e2e-loadtest \
  --connections 1000 \
  --ramp 90s \
  --hold 60s \
  --target-pid "$RUNTIME_PID" \
  --min-host-available-mib 8192 \
  --max-target-rss-mib 8192 \
  --report ./tmp/load-reports/stage-1000.json
```

Remote targets are rejected unless `--allow-non-loopback` is explicitly set.
Do not use that flag for production stress tests without separate approval and
production-safe traffic controls.

Synthetic authentication uses a distinct user identity per connection by
default. Add `--var userId=shared-load-user` to model many tabs/devices for one
identity and exercise shared-subscription fan-out. Keep the default when the
goal is to model distinct users and user-specific visibility scopes.

Pass a comma-separated tenant list to distribute connections round-robin and
include a per-tenant breakdown in the report:

```bash
./tmp/gonvex-load-runner \
  --profile /path/to/whagons-workspace-50.json \
  --tenant loadtest-a,loadtest-b,loadtest-c \
  --connections 10000
```

Add an aggregate mutation rate to measure write acknowledgements and reactive
change delivery. Mutations begin only after every initial subscription has
settled, are spread evenly across the virtual users, and stop at the end of the
hold. The runner then waits for every mutation result before closing sockets:

```bash
./tmp/gonvex-load-runner \
  --profile /path/to/whagons-workspace-50.json \
  --tenant loadtest-a,loadtest-b \
  --connections 100 \
  --hold 30s \
  --mutation-path analytics.createSessionLog \
  --mutation-args '{"tenantId":"${tenant}","userId":"${userId}","description":"load ${sequence}"}' \
  --mutation-rate 100
```

Mutation arguments support exact `${tenant}`, `${userId}`, and `${sequence}`
placeholders. Use a mutation that writes a table read by at least one profile
subscription; otherwise mutation latency is measured but no reactive update is
expected.
