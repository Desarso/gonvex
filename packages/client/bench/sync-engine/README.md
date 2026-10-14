# Sync engine storage benchmark

From the repository root, with frozen workspace dependencies and built protocol,
module-sdk and local-runtime dependencies:

```sh
export TMPDIR=/home/gabriel/.cache/sdk-wave1/tmp
mkdir -p "$TMPDIR"
pnpm exec playwright install chromium firefox webkit
pnpm --filter @gonvex/client bench:sync-engine:check
pnpm --filter @gonvex/client bench:sync-engine
node packages/client/bench/sync-engine/gates.mjs packages/client/bench/sync-engine/results.json
```

Install Playwright's OS dependencies on a supported host as needed. WebKit is an
approximation of Safari, not a physical Safari/iOS certification. Benchmarks are
manual and excluded from normal tests and CI. The Node statistical checks use an
explicit command and a filename outside Vitest discovery. The command builds the client,
then runs browsers sequentially with serial operations in separate adapter
profiles under `$TMPDIR`. Persistent profiles put writes in native disk-backed
IndexedDB. This does not promise a hardware fsync per transaction.

Options are `--browsers chromium,firefox,webkit`, `--cases single-row,startup-working-set,delete-100-windows,outbox`,
`--samples 20`, `--warmups 5`, `--cap 60`, `--cv 0.15`, `--load 0.5`, `--retries 10`,
and `--output path.json`. Pass options after the pnpm script name. A quick smoke
run can use `--samples 5 --warmups 1 --cap 5`. Missing cases produce NOISY gates,
not passes. Interrupted runs retain an incomplete JSON and cannot pass gates;
partial samples are saved every five rounds and on failure; profiles are removed
on interruption. The report records process scheduling priority.
The old `chromium.mjs` entry point delegates to this same runner.

## Cases and scope

The deterministic fixture contains 10k or 50k 50-column task rows, roughly 2 KiB
per row, four indexed scalar columns, five related tables, a 200 MiB/unlimited-row
working-set budget and fixture IDs for 250 visible rows and 1,000 exact IDs. These
IDs are not mounted subscriptions. The report records the seed, artifact hash,
encoded bytes, source commit, dirty flag, browser version and machine details.

Both legacy and engine adapters run the same cases:

- Middle-row delta with precomputed row hash, explicit membership edit and valid
  XOR certificate. Input decoding and hashing are outside the timed call.
- Startup working-set load after closing and relaunching the browser process into
  a populated profile. Adapter initialization and persisted decoding are timed.
  Process launch/navigation are excluded. OS disk caches are not evicted, but
  no in-process adapter/JS cache survives. This is not a first-paint measurement.
- Normalized delete with exactly 100 owners per victim, at both collection sizes.
  Each round deletes a different row so owner counts remain equal.
- 500-intent independent durable outbox transitions. Admission is
  prepared outside timing; timed operations are nextReady, markInflight and ack.
  Both modes use the existing DexieReducerOutbox, since replica adapters do not
  own an outbox. No network, reducer execution, authoritative replay or server
  visibility is simulated, so the design doc's 5-second/2x network drain gate
  does not apply. A dependent-chain replay needs that end-to-end fixture; repeating
  the same serial storage calls would not measure its scheduler. This storage
  measurement is not a full drain.

Every case has fresh isolated profiles. Seeding, bundling and input preparation
are excluded from timing. No runtime, worker, UI, subscriber or transport costs
are measured. Neither memory nor battery savings follow from storage timings.

## Statistics and gates

After warmups, adapter order alternates each round. Each report includes all raw
samples, median, nearest-rank p95, coefficient of variation and seeded 4,000-draw
bootstrap 95% intervals for median and p95. Ratios resample paired rounds for
candidate/baseline comparisons. Scaling resamples the two size runs independently.
Startup also collects one fresh-process diagnostic per adapter after timing: native requests, records read, cursor continuations and JSON parses. Chromium writes sibling CPU and 32 KiB sampled live-allocation profiles, plus top CPU frames and sampled bytes in the JSON. These include instrumentation overhead, are not timing samples, and sampled live bytes are not peak memory or an exact allocation count.

Five diagnostic engine samples after timing record native IndexedDB request counts
and latencies at each size. They include queue/instrumentation overhead and are
kept separate from performance samples. These are percentile bootstrap intervals, not distribution-free guarantees;
p95 uncertainty especially needs more samples than median uncertainty.

If either adapter's CV exceeds the configured limit, add ten rounds at a time
up to the cap, retaining every sample. Cases still above the limit are NOISY.
This adaptive sampling policy is recorded and should be considered when reading
CIs; it does not replace replication on reference hardware.

Before each browser, the runner waits/retries if 1-minute load exceeds half the
logical CPU count by default. It records load before/after and after every case.
An above-threshold observation prints NOT QUIESCENT and marks all that browser's
gates NOISY. After retries are exhausted the run continues visibly uncertified.

The gate script rejects emulator files. Storage gates cover 50k durable apply
p95 <= 10 ms, 50k/10k engine p95 <= 1.25x, and startup/delete p95 no slower than
legacy. PASS requires the CI upper bound at or below the threshold. FAIL requires
the lower bound above it. Engine-only gates use engine CV; comparisons use both
adapters. A noisy legacy case cannot invalidate a stable absolute engine measurement.
Overlap, excessive required-adapter CV, missing cases or non-quiescence
produce NOISY. FAIL exits nonzero; NOISY is not acceptance even though it does not
fail the command. Broader design-doc gates require later end-to-end fixtures.

## Why emulator numbers are excluded

fake-indexeddb is used only in correctness tests. Its RecordStore.deleteByValue
scans secondary-index entries on overwrite. The engine's membership indexes
therefore add collection-size emulator costs which native IndexedDB does not
share. The Node timing runner has been removed; Node now only orchestrates real
browsers. Archived emulator JSON is labeled as such and rejected by gates.
No emulator timings are presented as performance results in RESULTS.md.

## Engine startup index

Engine startup uses a compact generation snapshot and transactional per-member
edits in the existing metadata table. Membership rows remain authoritative.
Missing or stale indexes fall back; a full window replacement warms the index.
The fixture seeds a clean index. Patch-heavy startup and the extra metadata write
per member edit are correctness-tested but not timed by the startup cases.
Entity read-ahead keeps at most two 256-record pages and forecasts both resident
budgets before issuing the next request. Window reads run in batches of four
after entity loading. Scope discovery uses bounded primary-key getAllKeys pages,
including on WebKit, and never loads cache values merely to find retirement work.
