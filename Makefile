.PHONY: dev services runtime dashboard packages docs

dev:
	@bash -lc 'set -euo pipefail; \
		runtime_pid=""; dashboard_pid=""; packages_pid=""; \
		cleanup() { \
			[ -n "$$runtime_pid" ] && kill "$$runtime_pid" 2>/dev/null || true; \
			[ -n "$$dashboard_pid" ] && kill "$$dashboard_pid" 2>/dev/null || true; \
			[ -n "$$packages_pid" ] && kill "$$packages_pid" 2>/dev/null || true; \
		}; \
		trap cleanup EXIT INT TERM; \
		pnpm build:packages; \
		pnpm dev:packages & packages_pid=$$!; \
		pnpm dev:runtime & runtime_pid=$$!; \
		pnpm dev:dashboard & dashboard_pid=$$!; \
		wait -n "$$runtime_pid" "$$dashboard_pid" "$$packages_pid"'

services:
	docker compose -f infra/docker-compose.dev.yml up -d --wait minio valkey

runtime:
	pnpm dev:runtime

dashboard:
	pnpm dev:dashboard

packages:
	pnpm dev:packages

docs:
	pnpm dev:docs
