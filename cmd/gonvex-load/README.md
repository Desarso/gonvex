# Gonvex persistent-load runner

`gonvex-load` creates virtual users without opening browsers. Each virtual user
holds one WebSocket connection and keeps a configured set of query
subscriptions alive.

The runner records:

- connection, authentication, initial-result, and server-query latency;
- successful and failed connections/subscriptions, by query path;
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
