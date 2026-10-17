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
  return { storage: new SQLiteLocalReplicaStorage(adapter), queries, db, adapter };
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

it("restores a snapshot through bounded batches instead of one SQL call per row", async () => {
  const {storage,queries}=fixture();
  const tasks=Object.fromEntries(Array.from({length:251},(_,i)=>['t'+i,{_id:'t'+i,note:'x'.repeat(3000)}]));
  await storage.replaceSnapshot({entities:{tasks},liveQueries:{}},'tenant');
  const writes=queries.filter(q=>q.sql.startsWith('INSERT INTO _gonvex_replica_entities'));
  expect(writes.length).toBeLessThan(10);
  for(const write of writes){
    expect(write.params.length).toBeLessThanOrEqual(400);
    expect(write.params.reduce((bytes,value)=>bytes+String(value).length*2,0)).toBeLessThanOrEqual(256*1024);
  }
  expect((await storage.loadEntityRows('tenant','tasks',['t250']))[0]!.row).toEqual(tasks.t250);
});

it("keeps the old snapshot atomic if a later restore batch fails", async () => {
  const {storage,db}=fixture();
  await storage.replaceSnapshot({entities:{tasks:{before:{_id:'before',name:'Keep me'}}},liveQueries:{}},'tenant');
  db.exec(`CREATE TRIGGER fail_restore BEFORE INSERT ON _gonvex_replica_entities WHEN NEW.id='t110' BEGIN SELECT RAISE(ABORT,'restore failure'); END;`);
  const tasks=Object.fromEntries(Array.from({length:251},(_,i)=>['t'+i,{_id:'t'+i,note:'x'.repeat(3000)}]));
  await expect(storage.replaceSnapshot({entities:{tasks},liveQueries:{}},'tenant')).rejects.toThrow('restore failure');
  expect(await storage.loadEntityRows('tenant','tasks',['before','t0','t109'])).toEqual([{id:'before',row:{_id:'before',name:'Keep me'}}]);
});

async function replicaWithNewerRowProjection(){
 const {storage,adapter}=fixture();const {LocalReplica}=await import('./local-replica');
 const replica=new LocalReplica(storage);await replica.activateScope('tenant');
 await replica.materializeWindow({signature:'references',kind:'replica',entity:'statuses',key:'_id',rows:[{_id:'s',name:'Status'}],cursor:{epoch:'e',revision:1},completeness:'complete',source:'server'});
 await replica.materializeWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'_id',rows:[{_id:'t',name:'Fresh',score:1}],cursor:{epoch:'e',revision:3},completeness:'complete',source:'server'});
 expect(replica.cursor()?.revision).toBe(1);
 return {storage,adapter,replica};
}
it('publishes committed authority when a delayed transaction is older than an existing row projection',async()=>{
 const {storage,replica}=await replicaWithNewerRowProjection();
 try{
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',name:'Stale'}}]},'tenant');
  expect((await storage.loadEntityRows('tenant','tasks',['t']))[0]?.row.name).toBe('Fresh');
  expect(replica.entity('tasks','t')?.name).toBe('Fresh');
 }finally{replica.dispose();}
});
it('publishes committed authority for a rejected stale delete without dropping its collection membership',async()=>{
 const {storage,replica}=await replicaWithNewerRowProjection();
 try{
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'t',operation:'delete'}]},'tenant');
  expect((await storage.loadEntityRows('tenant','tasks',['t']))[0]?.row.name).toBe('Fresh');
  expect(replica.entity('tasks','t')?.name).toBe('Fresh');
  expect(replica.liveQuery('tasks').rows.map(row=>row._id)).toEqual(['t']);
 }finally{replica.dispose();}
});
it('applies ordered row projections within one transaction without losing earlier fields',async()=>{
 const {storage}=fixture();await storage.replaceSnapshot({entities:{tasks:{t:{_id:'t',name:'Original',score:1}}},liveQueries:{}},'tenant');
 await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[
  {entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',name:'Changed'}},
  {entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',score:5}}]}, {entities:{},liveQueries:{}},'tenant');
 expect((await storage.loadEntityRows('tenant','tasks',['t']))[0]?.row).toEqual({_id:'t',name:'Changed',score:5});
});
it('reconciles peer publication before accepting its own later canonical commit',async()=>{
 const {storage,adapter,replica}=await replicaWithNewerRowProjection();
 try{
  const peer=new SQLiteLocalReplicaStorage(adapter);
  await peer.applyTransaction({cursor:{epoch:'e',revision:4},changes:[{entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',name:'Peer'}}]}, {entities:{},liveQueries:{}},'tenant');
  await replica.applyTransaction({cursor:{epoch:'e',revision:5},changes:[{entity:'tasks',id:'t',operation:'update',newValue:{_id:'t',score:9}}]},'tenant');
  expect((await storage.loadEntityRows('tenant','tasks',['t']))[0]?.row).toEqual({_id:'t',name:'Peer',score:9});
  expect(replica.entity('tasks','t')).toEqual({_id:'t',name:'Peer',score:9});
 }finally{replica.dispose();}
});

function epochSnapshot(epoch:string,revision:number,id:string) {
 const row={_id:id,name:epoch};
 const window={signature:'epoch-window',kind:'replica' as const,entity:'tasks',key:'_id',ids:[id],cursor:{epoch,revision},completeness:'complete' as const,source:'server' as const};
 return {cursor:{epoch,revision},entities:{tasks:{[id]:row}},liveQueries:{'epoch-window':window}};
}
it('rejects a retired transaction epoch without rewinding canonical rows or sequence',async()=>{
 const {storage}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 await storage.applyTransaction({cursor:next.cursor,changes:[{entity:'tasks',id:'b',operation:'insert',newValue:next.entities.tasks.b}]},next,'s');
 const before=await storage.loadWorkingSet('s',{maxRows:10,maxBytes:100000});
 await expect(storage.applyTransaction({cursor:{epoch:'old',revision:11},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:first.entities.tasks.a}]},first,'s')).rejects.toThrow('obsolete replica epoch');
 expect(await storage.loadWorkingSet('s',{maxRows:10,maxBytes:100000})).toEqual(before);
});
it('clears the preceding generation when a new-epoch collection window arrives first',async()=>{
 const {storage}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 await storage.replaceWindow(next.liveQueries['epoch-window'],next,'s');
 expect((await storage.load('s'))?.entities).toEqual(next.entities);
 await expect(storage.replaceWindow(first.liveQueries['epoch-window'],first,'s')).rejects.toThrow('obsolete replica epoch');
});
it('does not let an old watermark or removal change the new epoch',async()=>{
 const {storage}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');await storage.replaceSnapshot(next,'s');
 const before=await storage.loadWorkingSet('s',{maxRows:10,maxBytes:100000});
 await expect(storage.advanceWatermark([first.liveQueries['epoch-window']],first.cursor,'s')).rejects.toThrow('obsolete replica epoch');
 await expect(storage.removeWindow('epoch-window',first,'s')).rejects.toThrow('obsolete replica epoch');
 await expect(storage.replaceSnapshot(first,'s')).rejects.toThrow('obsolete replica epoch');
 expect(await storage.loadWorkingSet('s',{maxRows:10,maxBytes:100000})).toEqual(before);
});
it('requires watermarks to use the current epoch instead of creating a generation without rows',async()=>{
 const {storage}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 await expect(storage.advanceWatermark([next.liveQueries['epoch-window']],next.cursor,'s')).rejects.toThrow('obsolete replica epoch');
 expect((await storage.load('s'))?.entities).toEqual(first.entities);
});
it('rolls back epoch retirement along with a failed new-generation write',async()=>{
 const {storage,adapter}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 const original=adapter.runAsync,failed=Error('new generation failed');
 adapter.runAsync=async(sql,...args)=>{if(/INSERT INTO _gonvex_replica_entities/.test(sql))throw failed;return original(sql,...args);};
 await expect(storage.replaceWindow(next.liveQueries['epoch-window'],next,'s')).rejects.toBe(failed);
 adapter.runAsync=original;
 await expect(storage.replaceWindow(first.liveQueries['epoch-window'],first,'s')).resolves.toBeUndefined();
 expect((await storage.load('s'))?.entities).toEqual(first.entities);
});
it('keeps retired epoch history on disk and scope isolated',async()=>{
 const {storage,adapter}=fixture();
 for(let i=0;i<40;i++)await storage.replaceSnapshot(epochSnapshot('epoch-'+i,1,'t'+i),'s');
 const reopened=new SQLiteLocalReplicaStorage(adapter);
 await expect(reopened.replaceSnapshot(epochSnapshot('epoch-0',2,'old'),'s')).rejects.toThrow('obsolete replica epoch');
 await expect(storage.replaceSnapshot(epochSnapshot('epoch-0',1,'other'),'other')).resolves.toBeUndefined();
 expect(Object.keys((await storage.load('other'))!.entities.tasks!)).toEqual(['other']);
});

it('uses a legacy cursor as the current epoch without rehydrating rows',async()=>{
 const {storage,adapter,db}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 db.prepare("DELETE FROM _gonvex_replica_meta WHERE key='currentEpoch'").run();
 const reopened=new SQLiteLocalReplicaStorage(adapter);
 await reopened.replaceWindow(next.liveQueries['epoch-window'],next,'s');
 await expect(reopened.replaceSnapshot(first,'s')).rejects.toThrow('obsolete replica epoch');
 expect((await reopened.load('s'))?.entities).toEqual(next.entities);
});
it('rolls back a replacement whose memberships mix epochs',async()=>{
 const {storage}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 await expect(storage.replaceSnapshot({...next,liveQueries:first.liveQueries},'s')).rejects.toThrow('obsolete replica epoch');
 expect((await storage.load('s'))?.entities).toEqual(first.entities);
 await storage.replaceSnapshot(next,'s');
 await expect(storage.replaceSnapshot(first,'s')).rejects.toThrow('obsolete replica epoch');
});
it('fences retired epochs for a collection delta as well as its full replacement',async()=>{
 const {storage}=fixture(),first=epochSnapshot('old',10,'a'),next=epochSnapshot('new',1,'b');
 await storage.replaceSnapshot(first,'s');
 await storage.applyWindowDelta(next.liveQueries['epoch-window'],{upserts:[next.entities.tasks.b],deleted:[]},next,'s');
 expect((await storage.load('s'))?.entities).toEqual(next.entities);
 await expect(storage.applyWindowDelta(first.liveQueries['epoch-window'],{upserts:[first.entities.tasks.a],deleted:[]},first,'s')).rejects.toThrow('obsolete replica epoch');
});
