# SDK SQLite build

The SDK packages these official SQLite WASM/oo1/SAH-pool assets directly. Client builds verify every asset and the C extension against source.json. They do not download or rebuild SQLite. The official npm dependency supplies compatible TypeScript definitions.

The generic extension supplies exact SHA-256 and fixed-size snapshot receipts inside SQLite, plus conservative ingress casing. Unsupported casing takes the exact ECMAScript host callback. The bindings remain official oo1. WAL, EXCLUSIVE locking, FULL sync, 4096-byte pages and the owner epoch fence are enforced when opening the host.

To reproduce the assets, use the Emscripten SDK commit/version in source.json and an absolute, disposable build root with adequate disk and memory. On Gabriel's workstation run build-headroom first and use the capped heavy-workloads.slice scope required by AGENTS.md.

```sh
SQLITE_BUILD_ROOT=/mnt/data/work/sqlite-rebuild EMSDK=/path/to/pinned/emsdk pnpm --filter @gonvex/client sqlite:rebuild --verify
```

The rebuild verifies both source archives and the active toolchain, builds in a new subdirectory, applies the debug-argument and literal asset URL transforms, and compares byte-for-byte hashes. `--write` deliberately replaces the packaged artifacts and updates their manifest after a C change. Review the source and artifact diff together and rerun native SQL, OPFS crash/atomicity and multi-tab checks before releasing it. Build output and caches must stay outside this source directory.

The pinned official VFS contains the reserved-lock recovery fix. No experimental write buffer, browser memory flag, rollback-journal mode or application-specific code is included.
