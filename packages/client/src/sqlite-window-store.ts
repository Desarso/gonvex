import type { ReplicaWindow } from './local-replica.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { sqlitePayloadBytes } from './sqlite-admission.js';

export type SQLiteWindowHeader = Omit<ReplicaWindow, 'ids' | 'hashes'>;
export type SQLiteWindowProgress = { generation: string; pages: number; rows: number; root: string };
export type SQLiteWindowPage = { generation: string; total: number; ids: string[]; hashes: Record<string, string>; nextOffset: number | null };
type Stage = { generation: string; header: string; pages: number; rows: number; root: string; unique_rows: number; discarded: number };
const schema = `CREATE TABLE IF NOT EXISTS gv_windows(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,PRIMARY KEY(scope,signature)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_window_generations(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,header TEXT NOT NULL,pages INTEGER NOT NULL,rows INTEGER NOT NULL,root TEXT NOT NULL,unique_rows INTEGER NOT NULL DEFAULT 0,discarded INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,signature,generation)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_window_members(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,position INTEGER NOT NULL,id TEXT NOT NULL,PRIMARY KEY(scope,signature,generation,position)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_window_member_identity ON gv_window_members(scope,signature,generation,id);
CREATE TABLE IF NOT EXISTS gv_window_hashes(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,id TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(scope,signature,generation,id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_window_frames(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,page INTEGER NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(scope,signature,generation,page)) WITHOUT ROWID;`;
async function digest(value: string) { return [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value)))].map(x => x.toString(16).padStart(2, '0')).join(''); }
function identity(scope: string, signature: string, generation?: string) {
  if (!scope || scope.length > 512 || !signature || signature.length > 4096 || generation !== undefined && (!generation || generation.length > 128)) throw new TypeError('Invalid native window identity');
}
function conflict(message: string): never { throw Object.assign(new Error(message), { code: 'PAGE_CHANGED' }); }
const progress = (stage: Stage): SQLiteWindowProgress => ({ generation: stage.generation, pages: stage.pages, rows: stage.rows, root: stage.root });

/** Durable membership and certificates never live in a window-sized JS array.
 * A replacement is staged in bounded frames and published by one pointer swap.
 * The owner must serialize this component with all other connection operations.
 * Each method owns one transaction; it must not run inside another transaction.
 * This stores memberships only, not canonical rows or optimistic predictions. */
export class SQLiteWindowStore {
  private initialized?: Promise<void>;
  constructor(private readonly db: SQLiteReplicaDatabase, private readonly fence: () => Promise<unknown> = async () => {}) {}
  private initialize() { return this.initialized ??= this.db.withTransactionAsync(async () => { await this.fence(); await this.db.execAsync(schema); }); }
  private async stage(scope: string, signature: string, generation: string): Promise<Stage> {
    const stage = await this.db.getFirstAsync<Stage>('SELECT generation,header,pages,rows,root,unique_rows,discarded FROM gv_window_generations WHERE scope=? AND signature=? AND generation=?', scope, signature, generation);
    if (!stage || stage.discarded) conflict('Window generation is not open');
    return stage!;
  }
  async begin(scope: string, header: SQLiteWindowHeader, generation: string): Promise<SQLiteWindowProgress> {
    identity(scope, header.signature, generation); sqlitePayloadBytes(header, 16384);
    if ('ids' in header || 'hashes' in header || !header.entity || !header.key) throw new TypeError('Native window header cannot contain memberships');
    await this.initialize();
    let output!: SQLiteWindowProgress;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const prior = await this.db.getFirstAsync<Stage>('SELECT generation,header,pages,rows,root,unique_rows,discarded FROM gv_window_generations WHERE scope=? AND signature=? AND generation=?', scope, header.signature, generation);
      const encoded = JSON.stringify(header);
      if (prior) { if (prior.discarded) conflict('Window generation is being collected'); if (prior.header !== encoded) conflict('Window generation reused with another header'); output = progress(prior); return; }
      const count = (await this.db.getFirstAsync<{ n: number }>('SELECT count(*) n FROM gv_window_generations WHERE scope=? AND signature=?', scope, header.signature))!.n;
      if (count >= 3) throw new RangeError('Window generation budget reached; collect obsolete generations first');
      const root = '0'.repeat(64);
      await this.db.runAsync('INSERT INTO gv_window_generations(scope,signature,generation,header,pages,rows,root) VALUES(?,?,?,?,0,0,?)', scope, header.signature, generation, encoded, root);
      output = { generation, pages: 0, rows: 0, root };
    }); return output;
  }
  async append(scope: string, signature: string, generation: string, page: number, encoded: string): Promise<SQLiteWindowProgress> {
    identity(scope, signature, generation);
    if (!Number.isSafeInteger(page) || page < 0 || typeof encoded !== 'string' || encoded.length * 2 > 65536) throw new RangeError('Native window frame exceeds budget');
    const frame = JSON.parse(encoded) as { ids: string[]; hashes?: Record<string, string> };
    sqlitePayloadBytes(frame, 65536);
    if (!Array.isArray(frame.ids) || frame.ids.length > 256 || frame.ids.some(id => typeof id !== 'string' || !id || id.length > 1024)
      || frame.hashes !== undefined && (!frame.hashes || Array.isArray(frame.hashes) || typeof frame.hashes !== 'object' || Object.keys(frame.hashes).length > 256 || Object.entries(frame.hashes).some(([id, hash]) => !id || id.length > 1024 || typeof hash !== 'string' || hash.length > 256))) throw new TypeError('Invalid native window frame');
    const hash = await digest(encoded);
    await this.initialize(); let output!: SQLiteWindowProgress;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const stage = await this.stage(scope, signature, generation);
      if (page < stage.pages) {
        const receipt = await this.db.getFirstAsync<{ hash: string }>('SELECT hash FROM gv_window_frames WHERE scope=? AND signature=? AND generation=? AND page=?', scope, signature, generation, page);
        if (receipt?.hash !== hash) conflict('Window frame replay changed its payload');
        output = progress(stage); return;
      }
      if (page !== stage.pages) conflict('Window frame arrived out of sequence');
      const active = await this.db.getFirstAsync<{ generation: string }>('SELECT generation FROM gv_windows WHERE scope=? AND signature=?', scope, signature);
      if (active?.generation === generation) conflict('Cannot append to a published window');
      if (!Number.isSafeInteger(stage.rows + frame.ids.length)) throw new RangeError('Native membership position exhausted');
      const unique = frame.ids.length ? (await this.db.getFirstAsync<{ n: number }>('SELECT count(DISTINCT value) n FROM json_each(?) j WHERE NOT EXISTS(SELECT 1 FROM gv_window_members m WHERE m.scope=? AND m.signature=? AND m.generation=? AND m.id=j.value)', JSON.stringify(frame.ids), scope, signature, generation))!.n : 0;
      if (frame.ids.length) await this.db.runAsync('INSERT INTO gv_window_members SELECT ?,?,?,?+CAST(key AS INTEGER),value FROM json_each(?)', scope, signature, generation, stage.rows, JSON.stringify(frame.ids));
      if (frame.hashes && Object.keys(frame.hashes).length) await this.db.runAsync('INSERT INTO gv_window_hashes SELECT ?,?,?,key,value FROM json_each(?) WHERE 1 ON CONFLICT(scope,signature,generation,id) DO UPDATE SET hash=excluded.hash', scope, signature, generation, JSON.stringify(frame.hashes));
      const root = await digest(stage.root + hash);
      await this.db.runAsync('INSERT INTO gv_window_frames VALUES(?,?,?,?,?)', scope, signature, generation, page, hash);
      await this.db.runAsync('UPDATE gv_window_generations SET pages=?,rows=?,root=?,unique_rows=unique_rows+? WHERE scope=? AND signature=? AND generation=?', page + 1, stage.rows + frame.ids.length, root, unique, scope, signature, generation);
      output = { generation, pages: page + 1, rows: stage.rows + frame.ids.length, root };
    }); return output;
  }
  async publish(scope: string, signature: string, expected: SQLiteWindowProgress): Promise<void> {
    identity(scope, signature, expected.generation); sqlitePayloadBytes(expected, 1024);
    await this.initialize();
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const stage = await this.stage(scope, signature, expected.generation);
      if (stage.pages !== expected.pages || stage.rows !== expected.rows || stage.root !== expected.root) conflict('Native window manifest mismatch');
      const old = await this.db.getFirstAsync<{ header: string }>('SELECT g.header FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) WHERE w.scope=? AND w.signature=?', scope, signature);
      const before = old ? JSON.parse(old.header) as SQLiteWindowHeader : undefined, next = JSON.parse(stage.header) as SQLiteWindowHeader;
      if (before?.cursor && (!next.cursor || before.cursor.epoch === next.cursor.epoch && before.cursor.revision > next.cursor.revision)) conflict('Cannot publish an obsolete window cursor');
      await this.db.runAsync('INSERT INTO gv_windows VALUES(?,?,?) ON CONFLICT(scope,signature) DO UPDATE SET generation=excluded.generation', scope, signature, expected.generation);
    });
  }
  async header(scope: string, signature: string): Promise<(SQLiteWindowProgress & { header: SQLiteWindowHeader; uniqueRows: number }) | undefined> {
    identity(scope, signature); await this.initialize(); let output: (SQLiteWindowProgress & { header: SQLiteWindowHeader; uniqueRows: number }) | undefined;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const stage = await this.db.getFirstAsync<Stage>('SELECT g.* FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) WHERE w.scope=? AND w.signature=?', scope, signature);
      if (!stage) return;
      output = { ...progress(stage), header: JSON.parse(stage.header) as SQLiteWindowHeader, uniqueRows: stage.unique_rows };
    }); return output;
  }
  async page(scope: string, signature: string, generation: string, offset: number, limit = 128): Promise<SQLiteWindowPage> {
    identity(scope, signature, generation);
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 256) throw new RangeError('Invalid native membership page');
    await this.initialize(); let output!: SQLiteWindowPage;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const active = await this.db.getFirstAsync<{ generation: string }>('SELECT generation FROM gv_windows WHERE scope=? AND signature=?', scope, signature);
      if (active?.generation !== generation) conflict('Native membership page generation changed');
      const stage = await this.stage(scope, signature, generation);
      let take = limit;
      for (;;) {
        try {
          const rows = await this.db.getAllAsync<{ id: string; hash: string | null }>('SELECT m.id,h.hash FROM gv_window_members m LEFT JOIN gv_window_hashes h USING(scope,signature,generation,id) WHERE m.scope=? AND m.signature=? AND m.generation=? AND m.position>=? AND m.position<? ORDER BY m.position', scope, signature, generation, offset, offset + take);
          output = { generation, total: stage.rows, ids: rows.map(row => row.id), hashes: Object.fromEntries(rows.filter(row => row.hash !== null).map(row => [row.id, row.hash!])), nextOffset: offset + rows.length < stage.rows ? offset + rows.length : null };
          sqlitePayloadBytes(output, 262144); break;
        } catch (error) {
          const code = (error as { code?: string }).code;
          if (take === 1 || code !== 'BUDGET' && code !== 'sqlite-bridge-budget') throw error;
          take = Math.max(1, Math.floor(take / 2));
        }
      }
    }); return output;
  }
  /** Every hash, including certificate-only keys, is available in bounded order. */
  async hashPage(scope: string, signature: string, generation: string, after = '', limit = 128): Promise<Array<{ id: string; hash: string }>> {
    identity(scope, signature, generation); if (after.length > 1024 || !Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw new RangeError('Invalid hash page');
    await this.initialize(); let rows!: Array<{ id: string; hash: string }>;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      await this.stage(scope, signature, generation);
      let take = limit;
      for (;;) {
        try {
          rows = await this.db.getAllAsync('SELECT id,hash FROM gv_window_hashes WHERE scope=? AND signature=? AND generation=? AND id>? ORDER BY id LIMIT ?', scope, signature, generation, after, take);
          sqlitePayloadBytes(rows, 262144);
          break;
        } catch (error) {
          const code = (error as { code?: string }).code;
          if (take === 1 || code !== 'BUDGET' && code !== 'sqlite-bridge-budget') throw error;
          take = Math.max(1, Math.floor(take / 2));
        }
      }
    }); return rows;
  }
  /** Bounded cleanup retires an unpublished/obsolete generation before deleting
   * any members. Interrupted cleanup can resume, but it can never republish a
   * partly collected generation or delete the active snapshot. */
  async collect(scope: string, signature: string, generation: string): Promise<{ remaining: boolean }> {
    identity(scope, signature, generation); await this.initialize(); let remaining = false;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const active = await this.db.getFirstAsync<{ generation: string }>('SELECT generation FROM gv_windows WHERE scope=? AND signature=?', scope, signature);
      if (active?.generation === generation) conflict('Cannot collect the active window generation');
      await this.db.runAsync('UPDATE gv_window_generations SET discarded=1 WHERE scope=? AND signature=? AND generation=?', scope, signature, generation);
      for (const [table, key, limit] of [['gv_window_members', 'position', 256], ['gv_window_hashes', 'id', 128], ['gv_window_frames', 'page', 256]] as const) {
        await this.db.runAsync(`DELETE FROM ${table} WHERE scope=? AND signature=? AND generation=? AND ${key} IN (SELECT ${key} FROM ${table} WHERE scope=? AND signature=? AND generation=? ORDER BY ${key} LIMIT ${limit})`, scope, signature, generation, scope, signature, generation);
        remaining ||= !!await this.db.getFirstAsync(`SELECT 1 FROM ${table} WHERE scope=? AND signature=? AND generation=? LIMIT 1`, scope, signature, generation);
      }
      if (!remaining) await this.db.runAsync('DELETE FROM gv_window_generations WHERE scope=? AND signature=? AND generation=?', scope, signature, generation);
    }); return { remaining };
  }

}
