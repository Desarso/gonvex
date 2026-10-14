# Replica wire codec benchmark

Run from the repository root, outside CI:

```sh
pnpm install --frozen-lockfile
pnpm exec playwright install chromium
packages/client/bench/wire-codec/run.sh
```

The runner builds only the protocol package and a small optimized Rust protocol example, bundles the browser measurement with the installed esbuild, and runs one headless Chromium. It limits Cargo to two jobs. Node needs `--expose-gc`, which the runner supplies. No runtime, database or full test suite is started.

The deterministic Whagons-shaped fixture has 20,000 tasks with 50 mixed columns, divided into 157 snapshot pages of at most 128 rows, within today's 256 KiB page limit, a 1,000-row transaction, and a single-row delta. It includes nulls, numbers, booleans, unicode, nested JSON and arrays. The fixture is synthetic, not production tenant data.

Rust encodes the entire session, including the dictionary establishment frames. TypeScript decodes and deep-compares all 159 frames. Rust also decodes TypeScript-generated frames and deep-compares them in the other direction. Raw byte counts use the Rust output. Each isolated CPU group includes fresh dictionary establishment in both Node and Chromium. This makes a standalone delta conservative compared with a warmed session.

Decode CPU includes MessagePack parsing, dictionary expansion and structural validation. JSON uses today's `JSON.parse` baseline. Both return the same canonical frame and retain all results during each iteration. Integrity hashes and persistence are outside the timed region and remain in the client's existing application path. There are five warmups and 25 measured samples per group. Results report median, p95, and a deterministic 1,000-resample bootstrap 95% confidence interval for the median. The Rust encoder uses 15 samples, including serde conversion, dictionaries and serialization, excluding fixture parsing and stdout.

The runtime uses Axum's WebSocket implementation without a permessage-deflate extension. `hypotheticalDeflate*` numbers are independent zlib raw-deflate compression of each frame, for reference only. They are not runtime wire-byte measurements or a simulation of compression context takeover.

Peak JS allocation is the largest live heap delta sampled immediately after decoding the whole group, with GC before each iteration. The tiny delta repeats 1,000 times per sample to avoid Chromium timer quantization; CPU is divided by 1,000 and its heap number covers the whole repeated sample. Chromium uses `--enable-precise-memory-info`. It includes results and unreclaimed temporaries, excludes preallocated inputs, and is neither total allocated bytes nor OS RSS. Rust tracks peak live allocated bytes above the pre-encode baseline, including output buffers. Neither is a full browser memory profile.

`results.json` records the measured run and browser/Node versions. `output/` holds temporary browser assets and is ignored. Timing results vary by machine. Default activation requires at least 25% fewer raw replica bytes and at least 20% lower Chromium decode median for the snapshot workload. If CPU fails, leave `binaryReplica` off by default.

The v1 envelope is `[1, layoutOffset, addedLayouts, addedNames, payload]`. Layouts contain name IDs, so column names shared by different projections are sent once per authenticated session. Entity names and `changedColumns` also reference that dictionary. Strings in user JSON remain strings. Object layouts preserve key order. Arrays and objects have distinct tags, preserving empty arrays, absent fields and explicit null. Authentication refresh starts fresh dictionaries on both ends. Limits are 16 MiB per frame, 64 logical nesting levels, two million decoded values, 16,384 names/layouts, 65,536 layout entries and one million UTF-16 dictionary characters. A header scan enforces allocation/depth bounds before MessagePack decoding; `@msgpack/msgpack` handles decoding itself.

MessagePack carries full Rust u64 revisions without rounding. The current SDK's public `ReplicaCursor.revision` is a JavaScript number, so the runtime keeps frames beyond the exact safe-integer range in the existing JSON format. The TypeScript decoder rejects an unsafe integer if one arrives in binary before application. Supporting greater revisions end to end requires a separate cursor API change. It does not silently coerce them. No hashes or digest algorithms change.
