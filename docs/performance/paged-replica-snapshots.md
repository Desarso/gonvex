# Negotiated replica snapshot pages

The audit at `95da6c9` found that Replica Collections use one `replica.snapshot` JSON frame. See [the wire design](client-sync-engine.md#wire-and-server-changes), [TypeScript protocol](../../packages/protocol/src/index.ts), [Rust protocol](../../rust/crates/protocol/src/lib.rs), and [snapshot construction](../../rust/crates/runtime/src/replica.rs). Encoding and parsing that frame require a collection-sized string, and snapshot construction cloned the rows again after creating the subscription's row map.

## Wire behavior

Both ends must support `replicaSnapshotPages: 1`. The runtime advertises support in `session.ready`; the client requests it in `auth.capabilities`. A runtime sends pages only when that auth capability equals 1. A new client ignores unsolicited pages when the runtime has not advertised support. Omitting the capability, using another version, or connecting to an older runtime preserves monolithic JSON snapshots. No application contract version changes.

A `replica.snapshotPage` frame carries the subscription ID, snapshot ID, scope, visibility scope, epoch/revision cursor, zero-based sequence, `last`, key and collection metadata. Only the last page carries the digest. Version 1 targets 256 KiB per encoded JSON frame, including the envelope and final digest, with at most 512 rows. An indivisible row larger than the byte target travels alone. Empty collections have one final empty page.

Rows, visibility and cursor come from the existing PostgreSQL repeatable-read transaction. The runtime materializes that authorized generation once and moves its rows into a lazy page iterator. It does not serialize a monolithic snapshot or build a vector of page payloads for a negotiated connection. The subscription retains its existing row/hash map for change-feed routing. The socket sends every page and ready in order before it processes subsequent feed events. Concurrent commits remain in that feed; a lagged feed uses the existing reset/reconciliation path.

The client validates sequence, generation, collection metadata, scope, supported row keys, duplicate keys, row/byte budgets and final row-hash digest. Pages remain inactive. Final validation calls the existing atomic `replaceWindow` persistence operation for rows, membership, hashes and checkpoint; only successful persistence publishes a snapshot. Optimistic layers remain owned by LocalReplica. Public subscription consumers receive the existing consolidated snapshot shape rather than partial page events.

Paged activation preserves the validated server order and UTF-8 byte budget. It does not run legacy row bounding again. The legacy byte counter escapes U+2028/U+2029, which can otherwise discard rows from an already bounded server snapshot while retaining its integrity certificate. A focused Unicode/budget test covers this correction; monolithic fallback is unchanged.

A truncated progressive collection remains partial. Eager collections publish no staged rows before final validation. Interleaved deltas, authoritative transactions, ready messages and watermarks are deferred as whole operations until activation. That exceptional backlog is capped at 2 MiB or 1,024 frames. Overflow, malformed pages, missing pages, early ready or storage failure closes the connection and reopens from committed state. Socket, verification and visibility generations fence queued/async work. There is no resumable staging token and no partial checkpoint to recover after interruption.

## Memory scope

The current client is a resident LocalReplica, not the design's future worker-owned SQLite engine. Its storage contract has atomic window replacement but no inactive-generation page writer. Consequently decoded rows and row hashes still stage in RAM until activation, and activation uses the existing adapter transaction. This PR does **not** meet the future target of two pages plus SQLite cache for total snapshot staging memory. Adding that backend here would conflate wire paging with the earlier storage-engine phases and change the custom-adapter contract.

The runtime also retains the full authorized generation for its subscription. This change removes a redundant full-row clone and bounds individual JSON encoding/decoding work; it does not claim constant total heap, fewer disk writes, reduced battery use, or SQL streaming. A browser can queue already decoded pages while asynchronous validation is running.

The 20k-row Rust fixture reports exact UTF-8 frame sizes and the peak capacity of the single encoded Rust `String`, matching the socket serializer. Those capacities exclude retained rows, parsed client values, staging hashes, allocator overhead and transport queues. They are an allocation measurement for serialization, not a total-process heap benchmark.

The fixture has 20,000 rows with a nine-character ID and a 1,000-byte UTF-8 body. It ran with Rust 1.94.0, serde_json 1.0.151, one build job and debug information disabled.

| Metric | Monolithic | Paged |
| --- | ---: | ---: |
| Largest frame, UTF-8 bytes | 20,580,225 | 261,719 |
| Peak encoded String capacity, bytes | 35,225,600 | 296,448 |
| Total snapshot wire bytes | 20,580,225 | 20,607,952 |
| Snapshot frames | 1 | 79 |

That is a 98.73% smaller largest frame and a 99.16% smaller single serialization buffer, with 0.13% more wire bytes. The fixture measures JSON before transport compression. It does not measure wall-clock synchronization, process heap, battery or network transport buffering.

Focused verification covers protocol round trips and absent capabilities; byte/row limits and JSON escaping; materialized generation isolation from a concurrent writer; interruption, empty/oversized snapshots and legacy metadata; client activation, digest/generation/sequence failures, disconnect/visibility fences, rollback, and bounded interleaved feed replay. The concurrent-writer test exercises the owned materialized generation, not a live PostgreSQL stress fixture.

Verification passed: 14 client tests in `snapshot-pages.test.ts`, six runtime unit tests filtered by `snapshot_pages`, one protocol round-trip test filtered by `snapshot_pages`, protocol/client typechecks and builds, Rust protocol/runtime build, touched-crate clippy with `-D warnings`, and `cargo fmt --check`. Cargo used one build job. No full test suites ran.

## Adoption and rollback

Whagons needs no code change for normal adoption after the SDK and runtime are released. New SDK/new runtime connections negotiate paging automatically; either older end uses the existing path. Packages were not published and the app was not edited.

A deployment can disable the capability explicitly while retaining its other client options:

```ts
const client = new GonvexClient(runtimeUrl, {
  ...existingOptions,
  replicaSnapshotPages: false,
});
```

Disk-backed staging, progressive page rendering and the binary codec remain separate work. Existing synchronous LocalReplica reads and custom/native storage adapters keep their current behavior.
