.PHONY: dev stack stack-storage services storage files runtime dashboard packages docs version release-notes-preview release-dry-run release-prod

OPENROUTER_MODEL ?= moonshotai/kimi-k2.5

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
		while kill -0 "$$packages_pid" 2>/dev/null \
			&& kill -0 "$$runtime_pid" 2>/dev/null \
			&& kill -0 "$$dashboard_pid" 2>/dev/null; do \
			sleep 1; \
		done'

services:
	docker compose -f infra/docker-compose.dev.yml up -d --wait postgres valkey

stack:
	docker compose up -d --build --wait postgres valkey minio runtime dashboard
	docker compose run --rm minio-init

stack-storage: stack

storage:
	docker compose -f infra/docker-compose.dev.yml --profile storage up -d --wait minio
	docker compose -f infra/docker-compose.dev.yml --profile storage run --rm minio-init

files: storage

runtime:
	pnpm dev:runtime

dashboard:
	pnpm dev:dashboard

packages:
	pnpm dev:packages

docs:
	pnpm dev:docs

version:
	node scripts/release-cli.mjs --version-info

release-notes-preview:
	OPENROUTER_MODEL="$(OPENROUTER_MODEL)" VERSION="$(VERSION)" node scripts/release-cli.mjs --notes-preview

release-dry-run:
	OPENROUTER_MODEL="$(OPENROUTER_MODEL)" VERSION="$(VERSION)" node scripts/release-cli.mjs --dry-run

release-prod:
	OPENROUTER_MODEL="$(OPENROUTER_MODEL)" VERSION="$(VERSION)" node scripts/release-cli.mjs
