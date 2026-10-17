import { afterEach, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import {
  SQLiteLocalReplicaStorage,
  type SQLiteReplicaDatabase,
} from "./sqlite-replica-storage";

const databases: DatabaseSync[] = [];
afterEach(() => {
  for (const db of databases.splice(0)) db.close();
});
function fixture() {
  const db = new DatabaseSync(":memory:");
  databases.push(db);
  const queries: Array<{ sql: string; params: unknown[] }> = [];
  const adapter: SQLiteReplicaDatabase = {
    async execAsync(sql) {
      db.exec(sql);
    },
    async runAsync(sql, ...params) {
      queries.push({ sql, params });
      return db.prepare(sql).run(...(params as any[]));
    },
    async getAllAsync<T>(sql, ...params) {
      queries.push({ sql, params });
      return db.prepare(sql).all(...(params as any[])) as T[];
    },
    async getFirstAsync<T>(sql, ...params) {
      queries.push({ sql, params });
      return (db.prepare(sql).get(...(params as any[])) as T) ?? null;
    },
    async withTransactionAsync(task) {
      db.exec("BEGIN");
      try {
        await task();
        db.exec("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  };
  return { storage: new SQLiteLocalReplicaStorage(adapter), queries };
}

it("loads a bounded working set and reads one cold row through the SQLite primary key", async () => {
  const { storage, queries } = fixture();
  const tasks = Object.fromEntries(
    Array.from({ length: 2000 }, (_, i) => [
      String(i),
      { _id: String(i), name: `Task ${i}`, count: i, note: "x".repeat(1000) },
    ]),
  );
  await storage.replaceSnapshot(
    { entities: { tasks }, liveQueries: {} },
    "tenant",
  );
  queries.length = 0;
  const snapshot = await storage.loadWorkingSet("tenant", {
    maxRows: 100,
    maxBytes: 3000,
  });
  expect(Object.keys(snapshot!.entities.tasks!)).toHaveLength(1);
  const selected = await storage.withReadView(
    "tenant",
    { tasks: { key: "_id", complete: true } },
    (view) =>
      view.select({
        table: "tasks",
        where: { column: "_id", op: "eq", value: "1999" },
      }),
  );
  expect(selected.rows).toEqual([tasks["1999"]]);
  expect(selected.complete).toBe(true);
  expect(
    queries.filter((query) => query.sql.includes("entity = ? AND id = ?")),
  ).toHaveLength(1);
  expect(
    (await storage.loadEntityRows("tenant", "tasks", ["1999"]))[0],
  ).toEqual({ id: "1999", row: tasks["1999"] });
});

it("combines bounded pages before projecting sorted results, without crossing tenant scope", async () => {
  const { storage } = fixture();
  const tasks = Object.fromEntries(
    Array.from({ length: 700 }, (_, i) => [
      String(i),
      { _id: String(i), name: `PR-${i}`, count: i },
    ]),
  );
  await storage.replaceSnapshot(
    { entities: { tasks }, liveQueries: {} },
    "tenant",
  );
  await storage.replaceSnapshot(
    {
      entities: {
        tasks: { secret: { _id: "secret", name: "PR-9999", count: 9999 } },
      },
      liveQueries: {},
    },
    "other",
  );
  const result = await storage.withReadView(
    "tenant",
    { tasks: { key: "_id", complete: true } },
    (view) =>
      view.select({
        table: "tasks",
        columns: ["name"],
        where: { column: "count", op: "gte", value: 500 },
        orderBy: [
          { column: "name", transform: "numericSuffix", direction: "desc" },
        ],
        limit: 3,
      }),
  );
  expect(result).toEqual({
    rows: [{ name: "PR-699" }, { name: "PR-698" }, { name: "PR-697" }],
    complete: true,
  });
});

it("persists only the incoming window projection and preserves other fields and cold rows", async () => {
  const { storage, queries } = fixture();
  const a = { _id: "a", name: "Old", status: "new" },
    b = { _id: "b", name: "Cold" };
  await storage.replaceSnapshot(
    { entities: { tasks: { a, b } }, liveQueries: {} },
    "tenant",
  );
  queries.length = 0;
  const window = {
    signature: "one",
    kind: "replica" as const,
    entity: "tasks",
    key: "_id",
    ids: ["a"],
    completeness: "complete" as const,
    source: "server" as const,
  };
  await storage.replaceWindow(
    window,
    { entities: { tasks: { a: { ...a, name: "stale" } } }, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "Changed" }],
  );
  expect((await storage.load("tenant"))!.entities.tasks).toEqual({
    a: { ...a, name: "Changed" },
    b,
  });
  const writes = queries.filter((query) =>
    query.sql.startsWith("INSERT INTO _gonvex_replica_entities"),
  );
  expect(writes).toHaveLength(1);
  expect(writes[0].params).toHaveLength(4);
});

it("uses secondary indexes for parent lookups and atomically maintains them after edits", async () => {
  const { storage, queries } = fixture();
  const links = Object.fromEntries(
    Array.from({ length: 1000 }, (_, i) => [
      String(i),
      { _id: String(i), parentId: `parent-${i % 100}` },
    ]),
  );
  await storage.replaceSnapshot(
    { entities: { links }, liveQueries: {} },
    "tenant",
  );
  queries.length = 0;
  const read = () =>
    storage.withReadView(
      "tenant",
      { links: { key: "_id", complete: true } },
      (view) =>
        view.select({
          table: "links",
          where: { column: "parentId", op: "eq", value: "parent-42" },
        }),
    );
  expect((await read()).rows).toHaveLength(10);
  expect(
    queries.some((query) => query.sql.includes("_gonvex_replica_lookup")),
  ).toBe(true);
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 2 },
      changes: [
        {
          entity: "links",
          id: "42",
          operation: "update",
          newValue: { _id: "42", parentId: "moved" },
        },
      ],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  expect((await read()).rows).toHaveLength(9);
});

it("merges projected authority and prevents stale snapshots from resurrecting deleted entities", async () => {
  const { storage } = fixture();
  const window = {
    signature: "one",
    kind: "replica" as const,
    entity: "tasks",
    key: "_id",
    ids: ["a"],
    completeness: "complete" as const,
    source: "server" as const,
    cursor: { epoch: "e", revision: 1 },
  };
  await storage.replaceWindow(
    window,
    { entities: {}, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "Old", status: "new" }],
  );
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 3 },
      changes: [
        {
          entity: "tasks",
          id: "a",
          operation: "update",
          newValue: { _id: "a", status: "working" },
        },
      ],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  await storage.replaceWindow(
    { ...window, signature: "other", cursor: { epoch: "e", revision: 2 } },
    { entities: {}, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "Renamed", status: "new" }],
  );
  expect(
    (await storage.loadEntityRows("tenant", "tasks", ["a"]))[0].row,
  ).toEqual({ _id: "a", name: "Renamed", status: "working" });
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 4 },
      changes: [{ entity: "tasks", id: "a", operation: "delete" }],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  await storage.replaceWindow(
    { ...window, signature: "late", cursor: { epoch: "e", revision: 3 } },
    { entities: {}, liveQueries: {} },
    "tenant",
    [{ _id: "a", name: "stale" }],
  );
  expect(await storage.loadEntityRows("tenant", "tasks", ["a"])).toEqual([]);
});

it("handles null predicates and only requires fields requested by the reducer", async () => {
  const { storage } = fixture();
  await storage.replaceSnapshot(
    {
      entities: {
        tasks: {
          a: { _id: "a", name: "A", deletedAt: null },
          b: { _id: "b", name: "B", deletedAt: 123 },
        },
      },
      liveQueries: {},
    },
    "tenant",
  );
  const coverage = {
    tasks: {
      key: "_id",
      complete: true,
      columns: ["_id", "name", "deletedAt", "unloadedDescription"],
    },
  };
  const selected = await storage.withReadView("tenant", coverage, (view) =>
    view.select({
      table: "tasks",
      columns: ["name"],
      where: { column: "deletedAt", op: "eq", value: null },
    }),
  );
  expect(selected).toEqual({ rows: [], complete: true });
  const nullable = await storage.withReadView("tenant", coverage, (view) =>
    view.select({
      table: "tasks",
      columns: ["name"],
      where: { column: "deletedAt", op: "isNull" },
    }),
  );
  expect(nullable).toEqual({ rows: [{ name: "A" }], complete: true });
  const mixed = await storage.withReadView("tenant", coverage, (view) =>
    view.select({
      table: "tasks",
      columns: ["name"],
      where: { column: "deletedAt", op: "in", values: [null, 123] },
    }),
  );
  expect(mixed.rows).toEqual([{ name: "B" }]);
});

it("never rewinds a cursor and clears obsolete row authority during a full replacement", async () => {
  const { storage } = fixture();
  await storage.applyTransaction(
    {
      cursor: { epoch: "e", revision: 10 },
      changes: [{ entity: "tasks", id: "a", operation: "delete" }],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  await storage.advanceWatermark([], { epoch: "e", revision: 5 }, "tenant");
  expect((await storage.load("tenant"))!.cursor).toEqual({
    epoch: "e",
    revision: 10,
  });
  await storage.replaceSnapshot(
    {
      entities: { tasks: { a: { _id: "a", name: "Restored" } } },
      liveQueries: {},
      cursor: { epoch: "fresh", revision: 1 },
    },
    "tenant",
  );
  await storage.applyTransaction(
    {
      cursor: { epoch: "fresh", revision: 2 },
      changes: [
        {
          entity: "tasks",
          id: "a",
          operation: "update",
          newValue: { _id: "a", name: "Updated" },
        },
      ],
    },
    { entities: {}, liveQueries: {} },
    "tenant",
  );
  expect(
    (await storage.loadEntityRows("tenant", "tasks", ["a"]))[0].row.name,
  ).toBe("Updated");
});

it("clears one replica scope for a cache reset without touching the offline session or other scopes", async () => {
  const { storage } = fixture();
  const identity = "identity\u0000ws://runtime\u0000account";
  await storage.saveSession(identity, { directive: { protocolVersion: 1, scope: "s", visibilityScope: "visible", epoch: "e" } } as any);
  await storage.replaceSnapshot({ entities: { tasks: { t1: { _id: "t1", name: "Cached" } } }, liveQueries: {} }, "visible");
  await storage.replaceSnapshot({ entities: { tasks: { t2: { _id: "t2", name: "Other" } } }, liveQueries: {} }, "other");
  await storage.clear("visible");
  expect((await storage.load("visible"))?.entities.tasks?.t1).toBeUndefined();
  expect((await storage.load("other"))?.entities.tasks?.t2).toMatchObject({ name: "Other" });
  expect(await storage.loadSession(identity)).toMatchObject({ directive: { visibilityScope: "visible" } });
});

it("keeps a row-limited warm replica partial when the collection is complete on disk", async () => {
  const { LocalReplica } = await import('./local-replica');
  const {storage}=fixture();
  const tasks=Object.fromEntries(Array.from({length:3},(_,i)=>['t'+i,{_id:'t'+i,name:'Task '+i}]));
  await storage.replaceSnapshot({entities:{tasks},liveQueries:{all:{signature:'all',kind:'replica',entity:'tasks',key:'_id',ids:Object.keys(tasks),completeness:'complete',source:'server'}}},'tenant');
  const replica=new LocalReplica(storage,{maxResidentRows:1,maxResidentBytes:100000});
  try{
    await replica.activateScope('tenant');
    expect(replica.cachedEntityRows('tasks')).toHaveLength(1);
    expect(replica.entityCompleteness('tasks')).toBe('partial');
  }finally{replica.dispose();}
});

it("preserves the existence of cold rows when no row fits the startup byte budget", async () => {
  const {storage}=fixture();
  await storage.replaceSnapshot({entities:{tasks:{a:{_id:'a',note:'x'.repeat(1000)}}},liveQueries:{}},'tenant');
  const snapshot=await storage.loadWorkingSet('tenant',{maxRows:10,maxBytes:1});
  expect(snapshot).toBeDefined();
  expect(snapshot!.entities).toEqual({});
  expect(snapshot!.residentPartialTables).toEqual(['tasks']);
});

it("marks only omitted live tables partial and leaves a full working set complete", async () => {
  const {storage}=fixture();
  await storage.replaceSnapshot({entities:{alpha:{a:{_id:'a'}},zeta:{z:{_id:'z'}}},liveQueries:{}},'tenant');
  const limited=await storage.loadWorkingSet('tenant',{maxRows:1,maxBytes:100000});
  expect(limited!.residentPartialTables).toEqual(['zeta']);
  const full=await storage.loadWorkingSet('tenant',{maxRows:10,maxBytes:100000});
  expect(full!.residentPartialTables??[]).toEqual([]);
  expect(await storage.loadWorkingSet('other',{maxRows:1,maxBytes:100000})).toBeUndefined();
});

it("skips a tombstone history without paging its payloads into a warm working set", async () => {
  const {storage,queries}=fixture();
  await storage.replaceSnapshot({entities:{tasks:{live:{_id:'live',name:'Still here'}}},liveQueries:{}},'tenant');
  await storage.applyTransaction({cursor:{epoch:'e',revision:1},changes:Array.from({length:1000},(_,i)=>({entity:'tasks',id:'gone_'+i,operation:'delete' as const}))},{entities:{},liveQueries:{}},'tenant');
  queries.length=0;
  const snapshot=await storage.loadWorkingSet('tenant',{maxRows:1,maxBytes:100000});
  expect(snapshot!.entities.tasks).toEqual({live:{_id:'live',name:'Still here'}});
  expect(snapshot!.residentPartialTables).toEqual([]);
  const pages=queries.filter(q=>q.sql.startsWith('SELECT scope, entity, id, value')&&q.sql.includes('LIMIT ?'));
  expect(pages).toHaveLength(1);
});
