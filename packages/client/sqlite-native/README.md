# SDK SQLite build

The SDK packages these official SQLite WASM/oo1/SAH-pool assets directly. Client builds verify every asset and the C extension against source.json. They do not download or rebuild SQLite. The official npm dependency supplies compatible TypeScript definitions.

The generic extension supplies exact SHA-256 and fixed-size snapshot receipts inside SQLite, plus conservative ingress casing. Unsupported casing takes the exact ECMAScript host callback. The bindings remain official oo1. WAL, EXCLUSIVE locking, FULL sync, 4096-byte pages and the owner epoch fence are enforced when opening the host.

To reproduce the assets, use the Emscripten SDK commit/version in source.json and an absolute, disposable build root with adequate disk and memory. On Gabriel's workstation run build-headroom first and use the capped heavy-workloads.slice scope required by AGENTS.md.

```sh
SQLITE_BUILD_ROOT=/mnt/data/work/sqlite-rebuild EMSDK=/path/to/pinned/emsdk pnpm --filter @gonvex/client sqlite:rebuild --verify
```

The rebuild verifies both source archives and the active toolchain, builds in a new subdirectory, applies the debug-argument, literal asset URL and checked SAH offset/view transforms, and compares byte-for-byte hashes. `--write` deliberately replaces the packaged artifacts and updates their manifest after a C change. Review the source and artifact diff together and rerun native SQL, OPFS crash/atomicity and multi-tab checks before releasing it. Build output and caches must stay outside this source directory.

The pinned official VFS contains the reserved-lock recovery fix. No experimental write buffer, browser memory flag, rollback-journal mode or application-specific code is included.

The C VFS bridge keeps signed 64-bit offsets in WASM, rejects offsets outside the exact nonnegative JavaScript range and passes high/low 32-bit words to the official synchronous callback. The loader reuses up to 4096 borrowed WASM views and one synchronous options object. It retains no file-content copies; heap growth clears the view cache. Reads, writes, short-read handling and flush ordering remain the official SAH implementation. Run `node --test test/sqlite-sahpool.checks.mjs` after rebuilding.

The asset minimum remains 4 MiB for embedding compatibility. The browser owner initializes a 16 MiB heap to avoid repeated transient growth during initial sync, with the same 32 MiB SQLite hard heap limit. This reserves WASM memory; it is not a browser heap flag.
