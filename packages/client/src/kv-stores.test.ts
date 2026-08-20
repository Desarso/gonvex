import { describe, expect, it } from "vitest";
import { createKvOutboxStore, createMemoryGonvexKv, kvReducerOutboxTable } from "./kv-stores.js";

describe("KV outbox", () => {
  it("round-trips reducer entries", async () => {
    const kv = createMemoryGonvexKv();
    const store = createKvOutboxStore(kv);
    const entry = {
      id: 1, scope: "scope", path: "tasks.start", args: {}, idempotencyKey: "cmd",
      state: "pending" as const, attempts: 0, nextAttemptAt: 0, createdAt: 1,
    };
    await store.put(entry);
    expect(await store.load()).toEqual([entry]);
    expect((await kv.list(kvReducerOutboxTable))).toHaveLength(1);
    await store.delete(1);
    expect(await store.load()).toEqual([]);
  });
});
