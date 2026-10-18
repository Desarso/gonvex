import type { BindableValue, Database, PreparedStatement, Sqlite3Static } from '@sqlite.org/sqlite-wasm';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { SQLiteBridgeBudgetError } from './wasm-sqlite-database.js';

export type OfficialSQLiteHost = {
  database: SQLiteReplicaDatabase;
  memoryBytes(): number;
  statementBytes(): number;
  memoryStats?(): {allocated:number;peakAllocated:number;schema:number;cache:number;statements:number};
  readonly nativeFastFold:boolean;
  readonly nativeHash:boolean;
  readonly nativeReceipt:boolean;
  readonly nativeSnapshot:boolean;
  takeProfile(): { sqlite?:{allocated:number;peakAllocated:number;schema:number;cache:number;statements:number}; vfs?:Record<string,number>; ingressFoldCalls:number; ingressFoldUTF16Bytes:number; execMs:number;execCalls:number;prepareMs: number; prepares: number; bindMs: number; boundTextUTF16Bytes: number; beginMs: number; callbackMs: number; commitMs: number; rollbackMs: number; commits: number; rollbacks: number };
  close(): void;
};

/** Lazy worker-only factory. The origin broker must hold its ownership lease
 * before invoking this. Followers never import WASM or acquire OPFS handles. */
export async function openOfficialSQLiteHost(): Promise<OfficialSQLiteHost> {
  if (typeof globalThis.postMessage !== 'function' || 'document' in globalThis || 'onconnect' in globalThis)
    throw new Error('SQLite OPFS must open in the elected dedicated worker');
  // Disable unused proxy VFS initialization and its helper workers. Configure
  // this before importing/initializing the pinned official module.
  (globalThis as typeof globalThis & { sqlite3ApiConfig?: unknown }).sqlite3ApiConfig = {
    disable: { vfs: { opfs: true, 'opfs-vfs': true, 'opfs-wl': true } },
  };
  const { default: initialize } = await import('@gonvex/client/sqlite-module');
  const initializePinned: (options?:{INITIAL_MEMORY:number})=>ReturnType<typeof initialize> = initialize;
  const sqlite3 = await initializePinned({ INITIAL_MEMORY: 16 * 1024 * 1024 });
  // On failed initialization preserve existing files and the unsent outbox.
  const pool = await sqlite3.installOpfsSAHPoolVfs({ directory: '.gonvex-sqlite-v1', initialCapacity: 6, preserveOnInitFailure: true } as Parameters<typeof sqlite3.installOpfsSAHPoolVfs>[0]);
  let db: Database | undefined;
  try {
    db = new pool.OpfsSAHPoolDb('/replica.sqlite');
    // Capability discovery may prepare a statement and read the schema. Set
    // single-owner WAL locking before constructing that bridge as well.
    db.exec('PRAGMA locking_mode=EXCLUSIVE');
    const host = officialSQLiteDatabase(sqlite3, db);
    // EXCLUSIVE must precede the very first database read for single-connection
    // WAL without shared-memory methods. A mode mismatch is a startup failure.
    await host.database.execAsync(`PRAGMA locking_mode=EXCLUSIVE;
PRAGMA page_size=4096; PRAGMA cache_size=-2048; PRAGMA mmap_size=0;
PRAGMA temp_store=FILE; PRAGMA synchronous=FULL; PRAGMA journal_mode=WAL;
PRAGMA hard_heap_limit=33554432; PRAGMA wal_autocheckpoint=4096;
PRAGMA recursive_triggers=ON; PRAGMA busy_timeout=3000;`);
    for (const [pragma, expected] of [['locking_mode', 'exclusive'], ['journal_mode', 'wal'], ['synchronous', 2], ['page_size', 4096], ['cache_size', -2048], ['temp_store', 1], ['hard_heap_limit', 33554432], ['wal_autocheckpoint', 4096]] as const) {
      const row = await host.database.getFirstAsync<Record<string, unknown>>('PRAGMA ' + pragma);
      if (!row || Object.values(row)[0] !== expected) throw new Error('SQLite storage mode invariant failed: ' + pragma);
    }
    return { ...host, close() { host.close(); pool.pauseVfs(); } };
  } catch (error) { db?.close(); pool.pauseVfs(); throw error; }
}

/** Official oo1 bridge shared by the owner and real-WASM tests. Statements are
 * complex read and write plans share one byte-capped LRU. Cursors and bound
 * inputs are released before any plan is retained. */
export function officialSQLiteDatabase(sqlite3: Sqlite3Static, db: Database,
  budget = { maxRows: 512, maxBytes: 262_144 }): OfficialSQLiteHost {
  const textDecoder=new TextDecoder('utf-8',{fatal:true});
  let transaction = false, closed = false, statementBytes = 0;
  const profile = { execMs:0,execCalls:0,ingressFoldCalls:0, ingressFoldUTF16Bytes:0, prepareMs: 0, prepares: 0, bindMs: 0, boundTextUTF16Bytes: 0, beginMs: 0, callbackMs: 0, commitMs: 0, rollbackMs: 0, commits: 0, rollbacks: 0 };
  const statements = new Map<string, { statement: PreparedStatement; bytes: number }>();
  const statementBudget = 262144, maxStatements = 32;
  function retire(sql: string) {
    const cached = statements.get(sql); if (!cached) return;
    statements.delete(sql); statementBytes -= cached.bytes; cached.statement.finalize();
  }
  function cachedPlan(sql: string) {
    const cached = statements.get(sql);
    if (cached) { statements.delete(sql); statements.set(sql, cached); }
    return cached;
  }
  function remember(sql: string, statement: PreparedStatement, cached: { statement: PreparedStatement; bytes: number } | undefined): boolean {
    statement.reset(true); // No cached plan owns a bound input or live cursor.
    const size = sql.length * 2 + 128 + sqlite3.capi.sqlite3_stmt_status(statement.pointer!, sqlite3.capi.SQLITE_STMTSTATUS_MEMUSED, 0);
    if (cached) {
      statementBytes += size - cached.bytes; cached.bytes = size;
      while (statementBytes > statementBudget) retire(statements.keys().next().value!);
      return true;
    }
    if (size > statementBudget) return false;
    while (statements.size >= maxStatements || statementBytes + size > statementBudget) retire(statements.keys().next().value!);
    statements.set(sql, { statement, bytes: size }); statementBytes += size; return true;
  }
  function prepare(sql: string) {
    const started = performance.now();
    try { return db.prepare(sql); }
    finally { profile.prepareMs += performance.now() - started; profile.prepares++; }
  }
  function check() { if (closed) throw new Error('SQLite host closed'); }
  function bind(statement: PreparedStatement, values: unknown[]) {
    for (const value of values) if (value !== null && typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean' && !(value instanceof Uint8Array)) throw new TypeError('Invalid SQLite binding');
    for (const value of values) if (typeof value === 'string') profile.boundTextUTF16Bytes += value.length * 2;
    const started = performance.now();
    try { if (values.length) statement.bind(values as BindableValue[]); }
    finally { profile.bindMs += performance.now() - started; }
  }
  function read<T>(sql: string, values: unknown[], first: boolean): T[] {
    check();
    // Metadata, validation and render-page reads share the same byte cap.
    // Retaining the small ingress cycle avoids allocating a statement per read.
    const cached = cachedPlan(sql);
    const statement = cached?.statement ?? prepare(sql); let retain = !!cached;
    try {
      bind(statement, values);
      const rows: T[] = []; let bytes = 0;
      const columns = statement.getColumnNames();
      while (statement.step()) {
        if (rows.length >= budget.maxRows) throw new SQLiteBridgeBudgetError('rows');
        const entries: [string, unknown][] = [];
        for (let index = 0; index < columns.length; index++) {
          const name = columns[index]!;
          const type = sqlite3.capi.sqlite3_column_type(statement.pointer!, index);
          const size = type === 3 || type === 4 ? sqlite3.capi.sqlite3_column_bytes(statement.pointer!, index) : 8;
          // Reserve conservatively before decoding UTF-8 or copying a blob.
          bytes += name.length * 2 + size * (type === 3 ? 2 : 1) + 32;
          if (bytes > budget.maxBytes) throw new SQLiteBridgeBudgetError('text');
          if(type===3){
            // oo1's C-string conversion stops at embedded NUL. Decode the
            // already-admitted byte span instead (also valid for private scope
            // names and ordinary projected text containing control characters).
            const pointer=Number(sqlite3.capi.sqlite3_column_blob(statement.pointer!,index));
            if(size&&(!pointer||pointer+size>sqlite3.wasm.heap8u().byteLength))throw new Error('Invalid SQLite text span');
            entries.push([name,textDecoder.decode(sqlite3.wasm.heap8u().subarray(pointer,pointer+size))]);
          }else entries.push([name, statement.get(index)]);
        }
        rows.push(Object.fromEntries(entries) as T);
        if (first) break;
      }
      retain = remember(sql, statement, cached);
      return rows;
    } catch (error) {
      if (cached) { retire(sql); retain = true; }
      throw error;
    } finally { if (!retain) statement.finalize(); }
  }
  let nativeFastFold=false;
  try{nativeFastFold=['ascii-latin1-cjk-v1','ascii-latin1-cjk-emoji-v2'].includes(String(db.selectValue('SELECT gv_ingress_fast_contract()')));}catch(error){if(!/no such function: gv_ingress_fast_contract/.test(String((error as Error).message)))throw error;}
  let nativeHash=false;
  try{nativeHash=db.selectValue('SELECT gv_ingress_hash_contract()')==='sha256-v1';}catch(error){if(!/no such function: gv_ingress_hash_contract/.test(String((error as Error).message)))throw error;}
  db.createFunction('gv_ingress_lower', (_context: unknown, value: unknown) => { const text=String(value??''); profile.ingressFoldCalls++; profile.ingressFoldUTF16Bytes+=text.length*2; return text.toLowerCase(); }, { arity: 1, deterministic: true, directOnly: true });
  let nativeReceipt=false;
  try{nativeReceipt=db.selectValue('SELECT gv_ingress_receipt_contract()')==='sum128-v1/receipt1';}catch(error){if(!/no such function: gv_ingress_receipt_contract/.test(String((error as Error).message)))throw error;}
  let nativeSnapshot=false;
  try{nativeSnapshot=nativeHash&&nativeReceipt&&db.selectValue('SELECT gv_ingress_sql_contract()')==='bounded-snapshot-v1';}catch(error){if(!/no such function: gv_ingress_sql_contract/.test(String((error as Error).message)))throw error;}
  const database: SQLiteReplicaDatabase = {
    ingressUnicodeFold: true,
    ...(nativeFastFold ? {ingressNativeFastFold:true as const} : {}),
    ...(nativeSnapshot ? {ingressNativeSnapshot:true as const} : {}),
    ...(nativeReceipt ? {ingressNativeReceipt:true as const} : {}),
    ...(nativeHash ? {ingressNativeHash:true as const} : {}),
    async execAsync(sql) { check();const began=performance.now();profile.execCalls++;try{db.exec(sql);}finally{profile.execMs+=performance.now()-began;} },
    async runAsync(sql, ...values) {
      check();
      const cached = cachedPlan(sql);
      const statement = cached?.statement ?? prepare(sql);
      let retain = !!cached;
      try {
        bind(statement, values); while (statement.step()) { /* No retained output. */ }
        retain = remember(sql, statement, cached);
      } catch (e) {
        if (cached) { retire(sql); retain = true; }
        throw e;
      } finally { if (!retain) statement.finalize(); }
    },
    async getAllAsync<T>(sql: string, ...values: unknown[]) { return read<T>(sql, values, false); },
    async getFirstAsync<T>(sql: string, ...values: unknown[]) { return read<T>(sql, values, true)[0] ?? null; },
    async withTransactionAsync(run) {
      check(); if (transaction) throw new Error('Overlapping SQLite operation outside owner FIFO');
      transaction = true; let began = false;
      try {
        let started = performance.now();
        try { db.exec('BEGIN IMMEDIATE'); began = true; } finally { profile.beginMs += performance.now() - started; }
        started = performance.now();
        try { await run(); } finally { profile.callbackMs += performance.now() - started; }
        started = performance.now();
        try { db.exec('COMMIT'); profile.commits++; } finally { profile.commitMs += performance.now() - started; }
      } catch (error) {
        if (began) {
          const started = performance.now();
          try { db.exec('ROLLBACK'); profile.rollbacks++; } catch { /* Preserve original failure. */ }
          finally { profile.rollbackMs += performance.now() - started; }
        }
        throw error;
      }
      finally { transaction = false; }
    },
  };
  function nativeMemory() {
    check();
    return sqlite3.wasm.scopedAllocCall(() => {
      const current=sqlite3.wasm.scopedAlloc(4), high=sqlite3.wasm.scopedAlloc(4);
      const {capi,wasm}=sqlite3;
      if(capi.sqlite3_status(capi.SQLITE_STATUS_MEMORY_USED,current,high,0))throw Error('SQLite memory status unavailable');
      const allocated=wasm.peek32(current),peakAllocated=wasm.peek32(high);
      const read=(kind:1|2|3)=>{if(capi.sqlite3_db_status(db.pointer!,kind,current,high,0))throw Error('SQLite database status unavailable');return wasm.peek32(current);};
      return {allocated,peakAllocated,schema:read(capi.SQLITE_DBSTATUS_SCHEMA_USED),cache:read(capi.SQLITE_DBSTATUS_CACHE_USED),statements:read(capi.SQLITE_DBSTATUS_STMT_USED)};
    });
  }
  return { database, nativeFastFold,nativeHash,nativeReceipt,nativeSnapshot, memoryBytes: () => sqlite3.wasm.heap8u().buffer.byteLength, statementBytes: () => statementBytes, memoryStats:nativeMemory,
    takeProfile() { const vfs=(sqlite3 as unknown as {__gonvexVFSProfile?:()=>Record<string,number>}).__gonvexVFSProfile?.();const result = { ...profile,sqlite:nativeMemory(),...(vfs?{vfs}:{}) }; for (const key of Object.keys(profile) as Array<keyof typeof profile>) profile[key] = 0; return result; },
    close() { if (!closed) { closed = true; for (const sql of statements.keys()) retire(sql); db.close(); } } };
}
