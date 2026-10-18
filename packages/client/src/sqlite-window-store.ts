import {sqliteSnapshotWindowRows} from './sqlite-snapshot-window-rows.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import {sqliteSnapshotRowsSchema} from './sqlite-snapshot-pages.js';
import type { ReplicaWindow } from './local-replica.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import { sqlitePayloadBytes } from './sqlite-admission.js';

export type SQLiteWindowHeader = Omit<ReplicaWindow, 'ids' | 'hashes'>;
export type SQLiteWindowProgress = { generation: string; pages: number; rows: number; root: string };
export type SQLiteWindowPage = { generation: string; total: number; ids: string[]; hashes: Record<string, string>; nextOffset: number | null };
export type SQLiteWindowDelta={scope:string;signature:string;generation:string;cursor:{epoch:string;revision:number};upserts:Array<{id:string;hash:string}>;deleted:string[];digest:string};
type Stage = { generation: string; header: string; pages: number; rows: number; root: string; unique_rows: number; discarded: number };
export const sqliteSnapshotWindowAliasSchema=`CREATE TABLE IF NOT EXISTS gv_snapshot_window_alias(
 scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,
 PRIMARY KEY(scope,signature,generation)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_snapshot_window_source ON gv_snapshot_window_alias(scope,subscription,snapshot);`;
export const sqliteWindowAuxiliarySchema=`${sqliteSnapshotWindowAliasSchema}
CREATE TABLE IF NOT EXISTS gv_window_mutations(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,id TEXT NOT NULL,position INTEGER,hash TEXT,deleted INTEGER NOT NULL,PRIMARY KEY(scope,signature,generation,id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_window_mutation_position ON gv_window_mutations(scope,signature,generation,position);
CREATE TABLE IF NOT EXISTS gv_window_certificates(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,signature,generation)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_window_delta_receipts(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,revision INTEGER NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(scope,signature,generation,revision)) WITHOUT ROWID;
`;
const schema = `${sqliteWindowAuxiliarySchema}
${sqliteSnapshotRowsSchema}
CREATE TABLE IF NOT EXISTS gv_windows(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,PRIMARY KEY(scope,signature)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_window_generations(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,header TEXT NOT NULL,pages INTEGER NOT NULL,rows INTEGER NOT NULL,root TEXT NOT NULL,unique_rows INTEGER NOT NULL DEFAULT 0,discarded INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,signature,generation)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_window_members(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,position INTEGER NOT NULL,id TEXT NOT NULL,PRIMARY KEY(scope,signature,generation,position)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_window_member_identity ON gv_window_members(scope,signature,generation,id);
CREATE TABLE IF NOT EXISTS gv_window_hashes(scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,id TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(scope,signature,generation,id)) WITHOUT ROWID;
CREATE VIEW IF NOT EXISTS gv_all_window_members AS SELECT scope,signature,generation,position,id FROM gv_window_members
 UNION ALL SELECT a.scope,a.signature,a.generation,s.position,s.id FROM gv_snapshot_window_alias a JOIN gv_snapshot_rows s ON s.scope=a.scope AND s.subscription=a.subscription AND s.snapshot=a.snapshot
 LEFT JOIN gv_window_mutations d ON d.scope=a.scope AND d.signature=a.signature AND d.generation=a.generation AND d.id=s.id WHERE d.id IS NULL OR(d.deleted=0 AND d.position IS NULL)
 UNION ALL SELECT scope,signature,generation,position,id FROM gv_window_mutations WHERE deleted=0 AND position IS NOT NULL;
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
  private initialize() { return this.initialized ??= this.db.withTransactionAsync(async () => { await this.fence(); const old=await this.db.getFirstAsync<{sql:string}>("SELECT sql FROM sqlite_master WHERE type='view' AND name='gv_all_window_members'");if(old&&!old.sql.includes('gv_window_mutations'))await this.db.execAsync('DROP VIEW gv_all_window_members');await this.db.execAsync(schema); }); }
  ready(){return this.initialize();}
  /** Publish an already verified disk stage without decoding any membership or
   * hash corpus. Typed stages reuse their compact membership/hash relation. The caller composes this with canonical rows in one owner TX. */
  async publishSnapshot(scope:string,header:SQLiteWindowHeader,subscription:string,snapshot:string,reuseStage=false):Promise<SQLiteWindowProgress>{
    identity(scope,header.signature,snapshot);sqlitePayloadBytes(header,16384);
    if('ids'in header||'hashes'in header||!header.entity||!header.key||header.source!=='server')throw new TypeError('Invalid native snapshot header');
    await this.initialize();let output!:SQLiteWindowProgress;
    await this.db.withTransactionAsync(async()=>{
      await this.fence();
      const source=await this.db.getFirstAsync<{metadata:string;next_page:number;rows:number;complete:number;discarded:number;digest:string;receipt:Uint8Array}>('SELECT metadata,next_page,rows,complete,discarded,digest,receipt FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);
      if(!source?.complete||source.discarded)conflict('Snapshot is not verified');
      const metadata=JSON.parse(source.metadata) as {key:string;cursor:unknown};
      if(metadata.key!==header.key||JSON.stringify(metadata.cursor)!==JSON.stringify(header.cursor)||header.aggregateDigest!==source.digest)conflict('Snapshot header does not match its certificate');
      const count=await this.db.getFirstAsync<{n:number;last:number|null}>('SELECT count(*) n,max(position) last FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);
      if(count?.n!==source.rows||(count?.last??-1)!==source.rows-1)conflict('Verified snapshot lost rows');
      const oldRecord=await this.db.getFirstAsync<{header:string}>('SELECT g.header FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) WHERE w.scope=? AND w.signature=?',scope,header.signature);
      const old=oldRecord?{header:JSON.parse(oldRecord.header) as SQLiteWindowHeader}:undefined;
      if(old?.header.cursor&&header.cursor&&old.header.cursor.epoch===header.cursor.epoch&&old.header.cursor.revision>header.cursor.revision)conflict('Cannot publish an obsolete snapshot');
      const encoded=JSON.stringify(header),root=await digest('native-snapshot-v1:'+source.metadata+':'+source.digest+':'+source.rows+':'+source.next_page);
      const previous=await this.db.getFirstAsync<Stage>('SELECT * FROM gv_window_generations WHERE scope=? AND signature=? AND generation=?',scope,header.signature,snapshot);
      if(previous){if(previous.discarded||previous.header!==encoded||previous.rows!==source.rows||previous.root!==root)conflict('Snapshot generation reused with another manifest');}
      else{
        const generations=await this.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_window_generations WHERE scope=? AND signature=?',scope,header.signature);
        if((generations?.n??0)>=3)throw new RangeError('Window generation budget reached; collect obsolete generations first');
        await this.db.runAsync('INSERT INTO gv_window_generations(scope,signature,generation,header,pages,rows,root,unique_rows) VALUES(?,?,?,?,?,?,?,?)',scope,header.signature,snapshot,encoded,source.next_page,source.rows,root,source.rows);
        await this.db.runAsync('INSERT INTO gv_window_certificates VALUES(?,?,?,?)',scope,header.signature,snapshot,source.receipt);
        if(reuseStage){
         if(!await this.db.getFirstAsync('SELECT 1 FROM gv_snapshot_typed s JOIN gv_entity_alias a ON a.scope=s.scope AND a.entity=s.entity AND a.physical=s.physical WHERE s.scope=? AND s.subscription=? AND s.snapshot=? AND s.entity=?',scope,subscription,snapshot,header.entity))conflict('Snapshot rows are not active typed state');
         await this.db.runAsync('INSERT INTO gv_snapshot_window_alias VALUES(?,?,?,?,?)',scope,header.signature,snapshot,subscription,snapshot);
        }else{
        await this.db.runAsync('INSERT INTO gv_window_members SELECT scope,?,?,position,id FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=?',header.signature,snapshot,scope,subscription,snapshot);
        await this.db.runAsync('INSERT INTO gv_window_hashes SELECT scope,?,?,id,hash FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=?',header.signature,snapshot,scope,subscription,snapshot);
        }
      }
      await this.db.runAsync('INSERT INTO gv_windows VALUES(?,?,?) ON CONFLICT(scope,signature) DO UPDATE SET generation=excluded.generation',scope,header.signature,snapshot);
      output={generation:snapshot,pages:source.next_page,rows:source.rows,root};
    });return output;
  }
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
      if (active?.generation === generation||await this.db.getFirstAsync('SELECT 1 FROM gv_snapshot_window_alias WHERE scope=? AND signature=? AND generation=?',scope,signature,generation)) conflict('Cannot append to a published window');
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
  /** A server ready can certify unchanged membership at a newer cursor. It
   * cannot change a digest, epoch, completeness or generation without rows. */
  async confirmReady(scope:string,signature:string,cursor:{epoch:string;revision:number},digest:string,truncated=false):Promise<void>{
    identity(scope,signature);sqlitePayloadBytes({cursor,digest,truncated},16384);
    if(!cursor||!cursor.epoch||!Number.isSafeInteger(cursor.revision)||cursor.revision<0||typeof digest!=='string'||typeof truncated!=='boolean')throw new TypeError('Invalid native ready certificate');
    await this.initialize();await this.db.withTransactionAsync(async()=>{
      await this.fence();const current=await this.header(scope,signature),header=current?.header;
      if(!current||!header?.cursor||header.cursor.epoch!==cursor.epoch||header.cursor.revision>cursor.revision||header.aggregateDigest!==digest||header.completeness!==(truncated?'partial':'complete'))conflict('Ready differs from verified native membership');
      await this.db.runAsync('UPDATE gv_window_generations SET header=? WHERE scope=? AND signature=? AND generation=?',JSON.stringify({...header,cursor}),scope,signature,current.generation);
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
      const source=await this.db.getFirstAsync<{subscription:string;snapshot:string}>('SELECT subscription,snapshot FROM gv_snapshot_window_alias WHERE scope=? AND signature=? AND generation=?',scope,signature,generation);
      const native=source?sqliteSnapshotWindowRows(scope,signature,generation,source):undefined;
      let take = limit;
      for (;;) {
        try {
          const rows = source?await this.db.getAllAsync<{id:string;hash:string}>(native!.sql+' ORDER BY position LIMIT ? OFFSET ?',...native!.bindings,take,offset):await this.db.getAllAsync<{ id: string; hash: string | null }>('SELECT m.id,h.hash FROM gv_window_members m LEFT JOIN gv_window_hashes h USING(scope,signature,generation,id) WHERE m.scope=? AND m.signature=? AND m.generation=? ORDER BY m.position LIMIT ? OFFSET ?', scope, signature, generation, take, offset);
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
      const source=await this.db.getFirstAsync<{subscription:string;snapshot:string}>('SELECT subscription,snapshot FROM gv_snapshot_window_alias WHERE scope=? AND signature=? AND generation=?',scope,signature,generation);
      const native=source?sqliteSnapshotWindowRows(scope,signature,generation,source,{after}):undefined;
      let take = limit;
      for (;;) {
        try {
          rows = source?await this.db.getAllAsync('SELECT id,hash FROM ('+native!.sql+') ORDER BY id LIMIT ?',...native!.bindings,take):await this.db.getAllAsync('SELECT id,hash FROM gv_window_hashes WHERE scope=? AND signature=? AND generation=? AND id>? ORDER BY id LIMIT ?', scope, signature, generation, after, take);
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
  /** Source certificates stay immutable. A disk overlay records only changed
   * identities without copying or decoding the published membership corpus.
   * The trusted callback composes canonical rows in this same owner TX. */
  async applyDelta(input:SQLiteWindowDelta,mutate:()=>Promise<void>=async()=>{}):Promise<{changed:boolean;rows:number}>{
    identity(input.scope,input.signature,input.generation);sqlitePayloadBytes(input,131072);
    if(!input.cursor||typeof input.cursor.epoch!=='string'||!input.cursor.epoch||input.cursor.epoch.length>256||!Number.isSafeInteger(input.cursor.revision)||input.cursor.revision<0||!Array.isArray(input.upserts)||!Array.isArray(input.deleted)||input.upserts.length>128||input.deleted.length>128||typeof input.digest!=='string')throw new TypeError('Invalid native window delta');
    const owned=JSON.parse(JSON.stringify(input)) as SQLiteWindowDelta,hash=await digest(JSON.stringify(input));
    const changed=new Map<string,string>();
    for(const row of owned.upserts){if(!row||typeof row.id!=='string'||!row.id||row.id.length>1024||typeof row.hash!=='string'||!/^[0-9a-f]{64}$/.test(row.hash)||changed.has(row.id))throw new TypeError('Invalid native delta hash');changed.set(row.id,row.hash);}
    const deleted=new Set(owned.deleted);for(const id of deleted)if(typeof id!=='string'||!id||id.length>1024)throw new TypeError('Invalid native delta deletion');
    await this.initialize();let output!:{changed:boolean;rows:number};
    await this.db.withTransactionAsync(async()=>{
      await this.fence();
      const active=await this.db.getFirstAsync<{generation:string}>('SELECT generation FROM gv_windows WHERE scope=? AND signature=?',owned.scope,owned.signature);
      if(active?.generation!==owned.generation)conflict('Native delta generation changed');
      const stage=await this.stage(owned.scope,owned.signature,owned.generation),header=JSON.parse(stage.header) as SQLiteWindowHeader;
      if(header.kind!=='replica'||header.source!=='server'||header.cursor?.epoch!==owned.cursor.epoch||stage.rows!==stage.unique_rows)conflict('Native delta requires a unique authoritative collection');
      const previous=await this.db.getFirstAsync<{hash:string}>('SELECT hash FROM gv_window_delta_receipts WHERE scope=? AND signature=? AND generation=? AND revision=?',owned.scope,owned.signature,owned.generation,owned.cursor.revision);
      if(previous){if(previous.hash!==hash)conflict('Native delta replay changed its payload');output={changed:false,rows:stage.rows};return;}
      if(header.cursor.revision>=owned.cursor.revision){if(header.cursor.revision===owned.cursor.revision&&!changed.size&&!deleted.size&&owned.digest===header.aggregateDigest){output={changed:false,rows:stage.rows};return;}conflict('Native delta cursor is obsolete');}
      const certificate=await this.db.getFirstAsync<{receipt:Uint8Array}>('SELECT receipt FROM gv_window_certificates WHERE scope=? AND signature=? AND generation=?',owned.scope,owned.signature,owned.generation);
      const source=await this.db.getFirstAsync<{subscription:string;snapshot:string}>('SELECT subscription,snapshot FROM gv_snapshot_window_alias WHERE scope=? AND signature=? AND generation=?',owned.scope,owned.signature,owned.generation);
      const state=certificate?ReplicaSetDigest.decode(certificate.receipt):new ReplicaSetDigest();
      if(!certificate){
        let after='';for(;;){const native=source?sqliteSnapshotWindowRows(owned.scope,owned.signature,owned.generation,source,{after}):undefined;const hashes=source?await this.db.getAllAsync<{id:string;hash:string}>('SELECT id,hash FROM ('+native!.sql+') ORDER BY id LIMIT 128',...native!.bindings):await this.db.getAllAsync<{id:string;hash:string}>('SELECT id,hash FROM gv_window_hashes WHERE scope=? AND signature=? AND generation=? AND id>? ORDER BY id LIMIT 128',owned.scope,owned.signature,owned.generation,after);for(const row of hashes)state.add(row.hash);if(hashes.length<128)break;after=hashes.at(-1)!.id;}
      }
      if(state.count!==stage.rows)conflict('Native window certificate count differs');
      if(header.aggregateDigest&&(header.aggregateDigest.startsWith('sum128-v1:')?state.digest():header.aggregateDigest.startsWith('xor1:')?state.legacyDigest():undefined)!==header.aggregateDigest)conflict('Native window certificate digest differs');
      const affected=[...new Set([...changed.keys(),...deleted])];
      for(let offset=0;offset<affected.length;offset+=128){
        const ids=affected.slice(offset,offset+128),native=source?sqliteSnapshotWindowRows(owned.scope,owned.signature,owned.generation,source,{ids}):undefined;
        const old=source?await this.db.getAllAsync<{id:string;hash:string}>('SELECT id,hash FROM ('+native!.sql+')',...native!.bindings):await this.db.getAllAsync<{id:string;hash:string}>('SELECT id,hash FROM gv_window_hashes WHERE scope=? AND signature=? AND generation=? AND id IN (SELECT value FROM json_each(?))',owned.scope,owned.signature,owned.generation,JSON.stringify(ids));for(const row of old)state.remove(row.hash);
      }
      for(const value of changed.values())state.add(value);
      const actual=owned.digest.startsWith('sum128-v1:')?state.digest():owned.digest.startsWith('xor1:')?state.legacyDigest():undefined;
      if(!actual||actual!==owned.digest)throw new TypeError('native-delta-integrity-mismatch');
      if(header.maxRows!==undefined&&state.count>header.maxRows)throw new RangeError('Native collection row budget exceeded');
      if(source){
        let position=Math.max((await this.db.getFirstAsync<{n:number|null}>('SELECT max(position) n FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=?',owned.scope,source.subscription,source.snapshot))!.n??-1,(await this.db.getFirstAsync<{n:number|null}>('SELECT max(position) n FROM gv_window_mutations WHERE scope=? AND signature=? AND generation=?',owned.scope,owned.signature,owned.generation))!.n??-1);
        for(const id of deleted)if(!changed.has(id))await this.db.runAsync('INSERT INTO gv_window_mutations VALUES(?,?,?,?,NULL,NULL,1) ON CONFLICT(scope,signature,generation,id) DO UPDATE SET hash=NULL,deleted=1',owned.scope,owned.signature,owned.generation,id);
        for(const [id,value]of changed){
          const old=await this.db.getFirstAsync<{position:number|null;deleted:number}>('SELECT position,deleted FROM gv_window_mutations WHERE scope=? AND signature=? AND generation=? AND id=?',owned.scope,owned.signature,owned.generation,id);
          const existing=old?.deleted!==1&&await this.db.getFirstAsync('SELECT 1 FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND id=?',owned.scope,source.subscription,source.snapshot,id);
          let slot=old?.deleted!==1&&old?.position!==undefined?old.position:existing?null:++position;
          if(slot!==null&&!Number.isSafeInteger(slot))throw new RangeError('Native membership position exhausted');
          await this.db.runAsync('INSERT INTO gv_window_mutations VALUES(?,?,?,?,?,?,0) ON CONFLICT(scope,signature,generation,id) DO UPDATE SET position=excluded.position,hash=excluded.hash,deleted=0',owned.scope,owned.signature,owned.generation,id,slot,value);
        }
      }else{
      const removed=[...deleted].filter(id=>!changed.has(id));
      if(removed.length){const encoded=JSON.stringify(removed);for(const table of ['gv_window_members','gv_window_hashes'])await this.db.runAsync('DELETE FROM '+table+' WHERE scope=? AND signature=? AND generation=? AND id IN (SELECT value FROM json_each(?))',owned.scope,owned.signature,owned.generation,encoded);}
      let position=(await this.db.getFirstAsync<{n:number|null}>('SELECT max(position) n FROM gv_window_members WHERE scope=? AND signature=? AND generation=?',owned.scope,owned.signature,owned.generation))!.n??-1;
      for(const [id,value]of changed){
        if(!await this.db.getFirstAsync('SELECT 1 FROM gv_window_members WHERE scope=? AND signature=? AND generation=? AND id=?',owned.scope,owned.signature,owned.generation,id)){
          if(position>=Number.MAX_SAFE_INTEGER)throw new RangeError('Native membership position exhausted');await this.db.runAsync('INSERT INTO gv_window_members VALUES(?,?,?,?,?)',owned.scope,owned.signature,owned.generation,++position,id);
        }
        await this.db.runAsync('INSERT INTO gv_window_hashes VALUES(?,?,?,?,?) ON CONFLICT(scope,signature,generation,id) DO UPDATE SET hash=excluded.hash',owned.scope,owned.signature,owned.generation,id,value);
      }
      }
      await mutate();
      await this.db.runAsync('UPDATE gv_window_generations SET header=?,rows=?,unique_rows=?,root=? WHERE scope=? AND signature=? AND generation=?',JSON.stringify({...header,cursor:owned.cursor,aggregateDigest:owned.digest}),state.count,state.count,await digest(stage.root+hash),owned.scope,owned.signature,owned.generation);
      await this.db.runAsync('INSERT INTO gv_window_certificates VALUES(?,?,?,?) ON CONFLICT(scope,signature,generation) DO UPDATE SET receipt=excluded.receipt',owned.scope,owned.signature,owned.generation,state.encode());
      await this.db.runAsync('INSERT INTO gv_window_delta_receipts VALUES(?,?,?,?,?)',owned.scope,owned.signature,owned.generation,owned.cursor.revision,hash);
      output={changed:true,rows:state.count};
    });return output;
  }
  async collect(scope: string, signature: string, generation: string): Promise<{ remaining: boolean }> {
    identity(scope, signature, generation); await this.initialize(); let remaining = false;
    await this.db.withTransactionAsync(async () => {
      await this.fence();
      const active = await this.db.getFirstAsync<{ generation: string }>('SELECT generation FROM gv_windows WHERE scope=? AND signature=?', scope, signature);
      if (active?.generation === generation) conflict('Cannot collect the active window generation');
      await this.db.runAsync('UPDATE gv_window_generations SET discarded=1 WHERE scope=? AND signature=? AND generation=?', scope, signature, generation);
      const source=await this.db.getFirstAsync<{subscription:string;snapshot:string}>('SELECT subscription,snapshot FROM gv_snapshot_window_alias WHERE scope=? AND signature=? AND generation=?',scope,signature,generation);
      if(source){
       const shared=await this.db.getFirstAsync('SELECT 1 FROM gv_snapshot_window_alias WHERE scope=? AND subscription=? AND snapshot=? AND (signature<>? OR generation<>?) LIMIT 1',scope,source.subscription,source.snapshot,signature,generation);
       if(!shared){
        await this.db.runAsync('UPDATE gv_snapshot_stages SET discarded=1 WHERE scope=? AND subscription=? AND snapshot=?',scope,source.subscription,source.snapshot);
        await this.db.runAsync('DELETE FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND id IN (SELECT id FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? ORDER BY position LIMIT 128)',scope,source.subscription,source.snapshot,scope,source.subscription,source.snapshot);
        remaining=!!await this.db.getFirstAsync('SELECT 1 FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? LIMIT 1',scope,source.subscription,source.snapshot);
       }
       if(!remaining)await this.db.runAsync('DELETE FROM gv_snapshot_window_alias WHERE scope=? AND signature=? AND generation=?',scope,signature,generation);
      }
      for (const [table, key, limit] of [['gv_window_members', 'position', 256], ['gv_window_hashes', 'id', 128], ['gv_window_frames', 'page', 256],['gv_window_delta_receipts','revision',128],['gv_window_mutations','id',128]] as const) {
        await this.db.runAsync(`DELETE FROM ${table} WHERE scope=? AND signature=? AND generation=? AND ${key} IN (SELECT ${key} FROM ${table} WHERE scope=? AND signature=? AND generation=? ORDER BY ${key} LIMIT ${limit})`, scope, signature, generation, scope, signature, generation);
        remaining ||= !!await this.db.getFirstAsync(`SELECT 1 FROM ${table} WHERE scope=? AND signature=? AND generation=? LIMIT 1`, scope, signature, generation);
      }
      if (!remaining) {await this.db.runAsync('DELETE FROM gv_window_certificates WHERE scope=? AND signature=? AND generation=?',scope,signature,generation);await this.db.runAsync('DELETE FROM gv_window_generations WHERE scope=? AND signature=? AND generation=?', scope, signature, generation);}
    }); return { remaining };
  }

}
