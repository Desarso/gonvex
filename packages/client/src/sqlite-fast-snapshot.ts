import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSnapshotMetadata,SQLiteSnapshotReceipt} from './sqlite-snapshot-pages.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
const f=(name:string,source='NEW.value')=>`json_extract(${source},'$.${name}')`;
const t=(name:string,source='NEW.value')=>`json_type(${source},'$.${name}')`;
const id=f('id'),snapshot=f('snapshotId'),sequence=f('sequence'),last=`${t('last')}='true'`;
const selected=`scope=NEW.scope AND subscription=${id} AND snapshot=${snapshot}`;
const stage=`(SELECT next_page FROM gv_snapshot_stages WHERE ${selected})`;
const slot=(name:string)=>`(SELECT ${name} FROM gv_snapshot_fast_state WHERE id=1)`;
const error=(code:string,message:string)=>`RAISE(ABORT,'GV_SNAPSHOT_${code}:${message}')`;
const guard=(condition:string,code:string,message:string)=>`SELECT CASE WHEN ${condition} THEN ${error(code,message)} END;`;
const safeInteger=(name:string)=>`(${t(name)} IN ('integer','real') AND ${f(name)}>=0 AND ${f(name)}<=9007199254740991 AND ${f(name)}=CAST(${f(name)} AS INTEGER))`;
const optionalNames=['mode','maxRows','maxBytes','truncated','orderBy','orderDirection','path'];
const metadataNames=['type','id','snapshotId','scope','visibilityScope','key','sequence','last','cursor',...optionalNames];
const comparable=['id','snapshotId','scope','visibilityScope','key','mode','maxRows','maxBytes','truncated','orderBy','orderDirection','path','cursor.epoch','cursor.revision'];
const tuple=(source:string)=>`json_array(${comparable.map(name=>['maxRows','maxBytes','cursor.revision'].includes(name)?`CAST(${f(name,source)} AS INTEGER)`:f(name,source)).join(',')})`;
const rowPath=`'$.'||json_quote(${f('key')})`;
const key=(body:string)=>`CASE WHEN json_type(${body},${rowPath})='text' THEN json_extract(${body},${rowPath}) ELSE ${body} -> (${rowPath}) END`;
const rows=`json_each(NEW.value,'$.canonicalRows')`;
const newFrame=`${slot('replay')}=0`;
const metadata=`(SELECT json_group_object(key,CASE
 WHEN key='type' THEN 'replica.snapshotPage'
 WHEN key='cursor' THEN json_object('epoch',${f('cursor.epoch')},'revision',${f('cursor.revision')})
 WHEN type IN ('array','object') THEN json(value) WHEN type='true' THEN json('true') WHEN type='false' THEN json('false')
 ELSE value END) FROM json_each(NEW.value) WHERE key IN (${metadataNames.map(name=>`'${name}'`).join(',')}))`;
const required=['id','snapshotId','scope','visibilityScope','key','cursor.epoch'].map(name=>`(${t(name)}='text' AND ${f(name)}<>'')`).join(' AND ');

/** All statements execute inside one bounded INSERT trigger. The input schema
 * is SDK-private; this SQL never accepts application SQL or entity table names.
 * Pure native hash/receipt functions must be explicitly marked innocuous by
 * the pinned host. JS callbacks remain DIRECTONLY and are never called here. */
export const fastSnapshotSchema=`CREATE TABLE IF NOT EXISTS gv_snapshot_fast_input(
 id INTEGER PRIMARY KEY CHECK(id=1),scope TEXT NOT NULL,value BLOB NOT NULL,hash TEXT NOT NULL,
 zero BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS gv_snapshot_fast_state(id INTEGER PRIMARY KEY CHECK(id=1),metadata TEXT NOT NULL,n INTEGER NOT NULL,bytes INTEGER NOT NULL,replay INTEGER NOT NULL,before_rows INTEGER NOT NULL);
DROP TRIGGER IF EXISTS gv_snapshot_fast_append;
DROP TRIGGER IF EXISTS gv_snapshot_fast_cleanup;
CREATE TRIGGER gv_snapshot_fast_cleanup AFTER DELETE ON gv_snapshot_fast_input BEGIN DELETE FROM gv_snapshot_fast_state; END;
CREATE TRIGGER gv_snapshot_fast_append AFTER INSERT ON gv_snapshot_fast_input BEGIN
 ${guard(`NOT coalesce(json_type(NEW.value)='object' AND ${f('type')}='replica.snapshotEncodedPage' AND ${t('canonicalRows')}='array' AND (${required}) AND ${f('visibilityScope')}=NEW.scope AND ${safeInteger('sequence')} AND ${safeInteger('cursor.revision')} AND ${t('last')} IN ('true','false'),0)`,'INVALID_ROW','Invalid native snapshot header')}
 ${guard(`${t('result')} IS NOT NULL OR ${t('hashes')} IS NOT NULL`,'INVALID_ROW','Unexpected snapshot row representation')}
 ${['maxRows','maxBytes'].map(name=>guard(`${t(name)} IS NOT NULL AND NOT coalesce(${safeInteger(name)},0)`,'INVALID_ROW','Invalid snapshot budget')).join('\n')}
 ${['mode','orderBy','orderDirection','path','digest'].map(name=>guard(`${t(name)} IS NOT NULL AND ${t(name)}<>'text'`,'INVALID_ROW','Invalid snapshot metadata')).join('\n')}
 ${guard(`${t('truncated')} IS NOT NULL AND ${t('truncated')} NOT IN ('true','false')`,'INVALID_ROW','Invalid snapshot truncation')}
 ${guard(`${t('mode')} IS NOT NULL AND ${f('mode')} NOT IN ('eager','progressive') OR ${t('orderDirection')} IS NOT NULL AND ${f('orderDirection')} NOT IN ('asc','desc')`,'INVALID_ROW','Invalid snapshot ordering')}
 ${guard(`NOT (${last}) AND ${t('digest')} IS NOT NULL OR (${last}) AND ${t('digest')} IS NOT 'text'`,'INVALID_ROW','Invalid final snapshot certificate')}
 DELETE FROM gv_snapshot_fast_state;
 INSERT INTO gv_snapshot_fast_state SELECT 1,${metadata},count(*),coalesce(sum(length(CAST(value AS BLOB))),0),0,0 FROM ${rows};
 ${guard(`${slot('n')}>512 OR NOT (${last}) AND ${slot('n')}=0`,'INVALID_ROW','Invalid canonical snapshot rows')}
 ${guard(`EXISTS(SELECT 1 FROM ${rows} WHERE CASE WHEN type<>'text' OR NOT json_valid(value) THEN 1 WHEN json_type(value)<>'object' THEN 1 WHEN NOT coalesce(json_type(value,${rowPath}) IN ('text','integer','real'),0) THEN 1 ELSE 0 END)`,'INVALID_ROW','Invalid canonical snapshot rows')}
 ${guard(`EXISTS(SELECT 1 FROM ${rows} WHERE ${key('value')} IS NULL OR ${key('value')}='')`,'INVALID_ROW','Snapshot row requires an identity')}
 ${guard(`EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected} AND discarded<>0)`,'PAGE_CHANGED','Snapshot is being discarded')}
 ${guard(`EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected} AND ${tuple('metadata')}<>${tuple('NEW.value')})`,'INTENT_CONFLICT','Snapshot metadata changed')}
 ${guard(`${sequence}<${stage} AND NOT EXISTS(SELECT 1 FROM gv_snapshot_frames WHERE ${selected} AND page=${sequence} AND hash=NEW.hash)`,'INTENT_CONFLICT','Snapshot page replay changed its payload')}
 ${guard(`${sequence}>coalesce(${stage},0)`,'PAGE_CHANGED','Snapshot page arrived out of sequence')}
 ${guard(`EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected} AND complete<>0 AND next_page=${sequence})`,'PAGE_CHANGED','Snapshot is already verified')}
 ${guard(`NOT EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected}) AND (SELECT count(*) FROM gv_snapshot_stages WHERE scope=NEW.scope AND subscription=${id})>=2`,'BUDGET','Collect obsolete snapshot stages first')}
 INSERT INTO gv_snapshot_stages SELECT NEW.scope,${id},${snapshot},${slot('metadata')},0,0,0,NEW.zero,0,NULL,0 WHERE NOT EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected});
 UPDATE gv_snapshot_fast_state SET replay=${sequence}<${stage},before_rows=(SELECT rows FROM gv_snapshot_stages WHERE ${selected}) WHERE id=1;
 INSERT INTO gv_snapshot_rows SELECT NEW.scope,${id},${snapshot},${key('value')},${slot('before_rows')}+CAST(key AS INTEGER),value,gv_ingress_sha256(value) FROM ${rows} WHERE ${newFrame};
 UPDATE gv_snapshot_stages SET receipt=coalesce((SELECT gv_ingress_receipt_step(gv_snapshot_stages.receipt,hash,1) FROM gv_snapshot_rows WHERE ${selected} AND position>=${slot('before_rows')} AND position<${slot('before_rows')}+${slot('n')}),receipt),rows=rows+${slot('n')},bytes=bytes+${slot('bytes')} WHERE ${selected} AND ${newFrame};
 ${guard(`EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected} AND (rows<>gv_ingress_receipt_count(receipt) OR rows>9007199254740991 OR bytes>9007199254740991 OR coalesce(${f('maxRows')},0)>0 AND rows>${f('maxRows')} OR coalesce(${f('maxBytes')},0)>0 AND bytes>${f('maxBytes')}))`,'BUDGET','snapshot-page-budget-exceeded')}
 ${guard(`${newFrame} AND (${last}) AND EXISTS(SELECT 1 FROM gv_snapshot_stages WHERE ${selected} AND CASE WHEN ${f('digest')} LIKE 'sum128-v1:%' THEN gv_ingress_receipt_digest(receipt) WHEN ${f('digest')} LIKE 'xor1:%' THEN gv_ingress_receipt_xor(receipt) ELSE NULL END IS NOT ${f('digest')})`,'INVALID_ROW','snapshot-page-integrity-mismatch')}
 INSERT INTO gv_snapshot_frames SELECT NEW.scope,${id},${snapshot},${sequence},NEW.hash WHERE ${newFrame};
 UPDATE gv_snapshot_stages SET next_page=${sequence}+1,complete=(${last}),digest=CASE WHEN (${last}) THEN ${f('digest')} ELSE NULL END WHERE ${selected} AND ${newFrame};
END;`;

function fastFailure(error:unknown):never{
 const message=String((error as Error).message),known=message.match(/GV_SNAPSHOT_(BUDGET|INVALID_ROW|PAGE_CHANGED|INTENT_CONFLICT):([^\n]*)/);
 if(known)fail(known[1] as 'BUDGET'|'INVALID_ROW'|'PAGE_CHANGED'|'INTENT_CONFLICT',known[2]!);
 if(/UNIQUE constraint failed: gv_snapshot_rows\./.test(message))fail('INVALID_ROW','duplicate-snapshot-key');
 throw error;
}
const zero=new ReplicaSetDigest().encode();
export async function appendFastSnapshot(db:SQLiteReplicaDatabase,fence:()=>Promise<unknown>,scope:string,bytes:Uint8Array,join:boolean):Promise<{receipt:SQLiteSnapshotReceipt;rows:number;newFrame:boolean}>{
 let output!:{receipt:SQLiteSnapshotReceipt;rows:number;newFrame:boolean};
 const run=async()=>{
  await fence();await db.runAsync('DELETE FROM gv_snapshot_fast_input');
  try{
   await db.runAsync(`WITH wire AS MATERIALIZED(SELECT CAST(? AS TEXT) raw)
    INSERT INTO gv_snapshot_fast_input(id,scope,value,hash,zero) SELECT 1,?,jsonb(raw),gv_ingress_sha256(raw),? FROM wire WHERE json_valid(raw)`,bytes,scope,zero);
   const row=await db.getFirstAsync<{metadata:string;n:number;replay:number;next_page:number;rows:number;complete:number;digest:string|null;sequence:number}>(`SELECT c.metadata,c.n,c.replay,s.next_page,s.rows,s.complete,s.digest,json_extract(i.value,'$.sequence') sequence FROM gv_snapshot_fast_input i JOIN gv_snapshot_fast_state c USING(id)
    JOIN gv_snapshot_stages s ON s.scope=i.scope AND s.subscription=json_extract(i.value,'$.id') AND s.snapshot=json_extract(i.value,'$.snapshotId') WHERE i.id=1`);
   if(!row)fail('INVALID_ROW','Invalid native snapshot JSON');
   const metadata=JSON.parse(row.metadata) as SQLiteSnapshotMetadata;sqlitePayloadBytes(metadata,16384);
   if(metadata.id.length>256||metadata.snapshotId.length>128||scope.length>512)fail('INVALID_ROW','Invalid snapshot identity');
   output={receipt:{ack:{type:'replica.snapshotAck',id:metadata.id,snapshotId:metadata.snapshotId,sequence:row.sequence},progress:{snapshotId:metadata.snapshotId,nextPage:row.next_page,rows:row.rows,complete:!!row.complete,...(row.digest?{digest:row.digest}:{})}},rows:row.replay?0:row.n,newFrame:!row.replay};
  }catch(error){fastFailure(error);}finally{await db.runAsync('DELETE FROM gv_snapshot_fast_input');}
 };
 if(join)await run();else await db.withTransactionAsync(run);
 return output;
}
