import type {ServerMessage} from '@gonvex/protocol';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes} from './sqlite-admission.js';

export type SQLiteFeedGroupFrame=Extract<ServerMessage,{type:'replica.transactionEncoded'|'replica.delta'|'replica.ready'}>;
const schema=`CREATE TABLE IF NOT EXISTS gv_feed_group_frames(stream TEXT NOT NULL,sequence INTEGER NOT NULL,revision INTEGER NOT NULL,body TEXT NOT NULL,hash TEXT NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(stream,sequence)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_feed_group_usage(id INTEGER PRIMARY KEY CHECK(id=1),bytes INTEGER NOT NULL CHECK(bytes>=0),frames INTEGER NOT NULL CHECK(frames>=0));
CREATE TABLE IF NOT EXISTS gv_feed_group_retired(stream TEXT PRIMARY KEY) WITHOUT ROWID;
INSERT OR IGNORE INTO gv_feed_group_usage VALUES(1,0,0);`;
async function digest(body:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(body))),byte=>byte.toString(16).padStart(2,'0')).join('');}
/** Unfinished authority groups are disk data, never partially published rows.
 * The owner drains one bounded frame at a time inside the terminal transaction.
 * Old incarnations can be discarded: no receipt or canonical state uses them. */
export class SQLiteFeedGroups {
 private initialized?:Promise<void>;
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly fence:()=>Promise<unknown>){}
 ready(){return this.initialized??=this.db.withTransactionAsync(async()=>{await this.fence();await this.db.execAsync(schema);}).catch(error=>{this.initialized=undefined;throw error;});}
 async append(stream:string,frame:SQLiteFeedGroupFrame){
  if(!stream||stream.length>1024)throw new TypeError('Invalid feed stream');sqlitePayloadBytes(frame,262144);
  const body=JSON.stringify(frame),bytes=body.length*2+stream.length*2+512,hash=await digest(body);await this.ready();
  await this.db.withTransactionAsync(async()=>{await this.fence();
   const usage=await this.db.getFirstAsync<{bytes:number;frames:number}>('SELECT bytes,frames FROM gv_feed_group_usage WHERE id=1');
   if(!usage||usage.bytes+bytes>67108864||usage.frames>=200000)fail('BUDGET','Unfinished feed groups exhausted their disk budget');
   await this.db.runAsync('INSERT INTO gv_feed_group_frames SELECT ?,coalesce(max(sequence),0)+1,?,?,?,? FROM gv_feed_group_frames WHERE stream=?',stream,frame.cursor.revision,body,hash,bytes,stream);
   await this.db.runAsync('UPDATE gv_feed_group_usage SET bytes=bytes+?,frames=frames+1 WHERE id=1',bytes);
  });
 }
 async pending(stream:string){await this.ready();await this.fence();return !!await this.db.getFirstAsync('SELECT 1 FROM gv_feed_group_frames WHERE stream=? LIMIT 1',stream);}
 async recover(){await this.ready();await this.fence();await this.db.runAsync('INSERT OR IGNORE INTO gv_feed_group_retired SELECT DISTINCT stream FROM gv_feed_group_frames');}
 async retire(stream:string){await this.ready();await this.fence();await this.db.runAsync('INSERT OR IGNORE INTO gv_feed_group_retired VALUES(?)',stream);}
 async collect(){await this.ready();await this.fence();const row=await this.db.getFirstAsync<{stream:string}>('SELECT stream FROM gv_feed_group_retired ORDER BY stream LIMIT 1');if(!row)return false;
  if(!await this.discard(row.stream))await this.db.runAsync('DELETE FROM gv_feed_group_retired WHERE stream=?',row.stream);return true;
 }
 async drain(stream:string,revision:number,apply:(frame:SQLiteFeedGroupFrame)=>Promise<void>){
  await this.ready();await this.fence();
  // Reject a watermark that would split a later transaction from its earlier
  // membership frames. No part of that group may escape the outer transaction.
  if(await this.db.getFirstAsync('SELECT 1 FROM gv_feed_group_frames WHERE stream=? AND revision>? LIMIT 1',stream,revision))fail('INVALID_ROW','Terminal watermark precedes its feed group');
  for(;;){
   const row=await this.db.getFirstAsync<{sequence:number;body:string;hash:string;bytes:number}>('SELECT sequence,body,hash,bytes FROM gv_feed_group_frames WHERE stream=? ORDER BY sequence LIMIT 1',stream);
   if(!row)return;
   decodedJSONBytes(row.body,524288);if(await digest(row.body)!==row.hash)fail('INTENT_CORRUPT','Unfinished feed frame hash differs');
   const frame=JSON.parse(row.body) as SQLiteFeedGroupFrame;sqlitePayloadBytes(frame,262144);await apply(frame);
   await this.db.runAsync('DELETE FROM gv_feed_group_frames WHERE stream=? AND sequence=?',stream,row.sequence);
   if(!await this.db.getFirstAsync('UPDATE gv_feed_group_usage SET bytes=bytes-?,frames=frames-1 WHERE id=1 AND bytes>=? AND frames>=1 RETURNING id',row.bytes,row.bytes))fail('INTENT_CORRUPT','Unfinished feed accounting differs');
  }
 }
 /** Bounded retirement for canceled/replaced admissions, without publication. */
 async discard(stream:string){await this.ready();await this.fence();let remaining=false;
  await this.db.withTransactionAsync(async()=>{
   const size=await this.db.getFirstAsync<{bytes:number;frames:number}>('SELECT coalesce(sum(bytes),0) bytes,count(*) frames FROM (SELECT bytes FROM gv_feed_group_frames WHERE stream=? ORDER BY sequence LIMIT 128)',stream);
   if(size?.frames){await this.db.runAsync('DELETE FROM gv_feed_group_frames WHERE stream=? AND sequence IN (SELECT sequence FROM gv_feed_group_frames WHERE stream=? ORDER BY sequence LIMIT 128)',stream,stream);if(!await this.db.getFirstAsync('UPDATE gv_feed_group_usage SET bytes=bytes-?,frames=frames-? WHERE id=1 AND bytes>=? AND frames>=? RETURNING id',size.bytes,size.frames,size.bytes,size.frames))fail('INTENT_CORRUPT','Unfinished feed accounting differs');}
   remaining=await this.pending(stream);
  });return remaining;
 }
}
