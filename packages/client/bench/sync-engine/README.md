# Sync engine storage benchmark

Run from `packages/client`, after building its workspace dependencies and client:

```sh
node --expose-gc bench/sync-engine/run.mjs bench/sync-engine/results.json
```

This serial Node benchmark uses fake-indexeddb. It compares the settled legacy adapter with the opt-in membership store, with five warmups and twenty measured samples per case, alternating which adapter runs first. Inputs and synthetic SHA-256 row hashes are prepared outside timed storage calls. The committed seed and artifact hash identify 50-column task rows with four indexed scalar columns averaging about 2 KiB, five related tables, 250 visible IDs and 1,000 exact IDs. Those IDs are fixture data, not mounted UI subscriptions.

Cases measure a middle-row delta at 10k and 50k memberships, durable deletion with 100 extra windows and three owners per victim, and persisted working-set decoding under the 200 MiB/unlimited-row budget. Samples include adapter transaction overhead. They exclude setup/import, transport, hashing, LocalReplica RAM materialization and subscriber delivery. Startup uses a populated store with no adapter query cache; it is not a browser navigation or a first paint measurement. Runtime and SDK source share the recorded repository commit, but no runtime is started or contacted.

The results file records raw samples, encoded bytes, machine details and source commit. fake-indexeddb timings establish storage scaling and regression fixtures, not native IndexedDB absolute latency or battery savings. Browser worker/DOM/transport benchmarks, network shaping, three-tab memory, Safari/device parity and kill/recovery of an engine owner belong to later engine phases. Run those gates before rollout.

## Native IndexedDB scaling check

```sh
node bench/sync-engine/chromium.mjs bench/sync-engine/chromium-results.json
```

This additional serial check bundles the built adapters once, opens an isolated local origin/profile in headless Chromium, seeds the same 10k/50k row fixtures, and compares single-row durable applies with five warmups and twenty alternating samples. Fetch, decoding, hashing and bundling finish outside timed calls. It does not start a runtime or measure worker/DOM/subscriber costs. It uses the workspace's installed Playwright and esbuild; install the matching Playwright Chromium browser if absent.

The Node harness uses fake-indexeddb 6.2.5, whose `RecordStore.deleteByValue` scans every index entry and whose `ObjectStore.storeRecord` invokes it for each secondary index on an overwrite. Membership/hash row updates therefore add emulator scans proportional to the corpus. Report those raw results, but assess native IndexedDB scaling separately. Never infer browser absolute latency from fake-indexeddb.
