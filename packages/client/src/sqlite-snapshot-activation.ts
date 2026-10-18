import {sqlitePhysicalEntityScope} from './sqlite-entity-scope.js';
import {sqliteIdentifier,sqliteTableName} from './sqlite-typed-schema.js';
import {activateSQLiteTypedSnapshot} from './sqlite-typed-snapshot.js';
import {insertFreshSnapshotRows,snapshotOverlapSchema} from './sqlite-snapshot-native-rows.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {ReplicaRow,ReplicaStorageCommit} from './local-replica.js';
import type {SQLiteLocalReplicaStorage,SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {SQLiteWindowStore,type SQLiteWindowHeader} from './sqlite-window-store.js';
import {decodedJSONBytes,fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';
import type {SQLiteSnapshotMetadata} from './sqlite-snapshot-pages.js';

type Input={scope:string;subscription:string;snapshot:string;header:SQLiteWindowHeader};
const receiptSchema=`CREATE TABLE IF NOT EXISTS gv_snapshot_activation_receipts(
 scope TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,input TEXT NOT NULL,receipt TEXT NOT NULL,
 PRIMARY KEY(scope,subscription,snapshot)) WITHOUT ROWID;`;

/** Owner-only publication of an already verified disk stage. Canonical authority
 * merges and the normalized membership pointer share one durable transaction.
 * This never grants coverage from an incomplete stage or retains a corpus-sized
 * commit receipt. The default client must consume native headers before using it. */
export class SQLiteSnapshotActivator {
 private initialized=false;
 private readonly windows:SQLiteWindowStore;
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly canonical:SQLiteLocalReplicaStorage,private readonly fence:()=>Promise<unknown>,private readonly schema?:SQLiteTypedSchema){
  this.windows=new SQLiteWindowStore(db,fence);
 }
 private async initialize(){
  if(this.initialized)return;
  if(this.db.ownerTransactionDepth)fail('FENCED','Prepare snapshot activation before ingress transactions');
  await this.canonical.ready();await this.windows.ready();
  await this.db.withTransactionAsync(async()=>{await this.fence();await this.db.execAsync(receiptSchema);await this.db.execAsync(snapshotOverlapSchema);});this.initialized=true;
 }
 ready(){return this.initialize();}
 async activate(input:Input):Promise<ReplicaStorageCommit>{
  sqlitePayloadBytes(input,16384);
  if(!input.scope||input.scope.length>512||!input.subscription||input.subscription.length>256||!input.snapshot||input.snapshot.length>128||input.header.kind!=='replica'
   ||input.header.entityCoverage!==undefined&&(input.header.entityCoverage!=='complete'||input.header.completeness!=='complete')
   ||!input.header.cursor||input.header.source!=='server'||'ids'in input.header||'hashes'in input.header)throw new TypeError('Invalid snapshot activation');
  const encoded=JSON.stringify(input),owned=JSON.parse(encoded) as Input;
  await this.initialize();let receipt!:ReplicaStorageCommit;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();
   const prior=await this.db.getFirstAsync<{input:string;receipt:string}>('SELECT input,receipt FROM gv_snapshot_activation_receipts WHERE scope=? AND subscription=? AND snapshot=?',owned.scope,owned.subscription,owned.snapshot);
   if(prior){
    const epoch=await this.db.getFirstAsync<{value:string}>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='currentEpoch'",owned.scope);
    if(epoch&&epoch.value!==owned.header.cursor!.epoch)fail('FENCED','Snapshot activation belongs to a retired server epoch');
    if(prior.input!==encoded)fail('PAGE_CHANGED','Snapshot activation replay changed its header');receipt=JSON.parse(prior.receipt);return;}
   const stage=await this.db.getFirstAsync<{metadata:string;complete:number;discarded:number;rows:number;digest:string}>('SELECT metadata,complete,discarded,rows,digest FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=?',owned.scope,owned.subscription,owned.snapshot);
   if(!stage?.complete||stage.discarded)fail('PAGE_CHANGED','Snapshot has not been verified');
   const metadata=JSON.parse(stage.metadata) as SQLiteSnapshotMetadata;
   if(metadata.visibilityScope!==owned.scope||metadata.key!==owned.header.key||JSON.stringify(metadata.cursor)!==JSON.stringify(owned.header.cursor)
    ||owned.header.aggregateDigest!==stage.digest||owned.header.completeness!==(metadata.truncated?'partial':'complete'))fail('INVALID_ROW','Snapshot activation does not match its certificate');
   receipt=await this.canonical.commitSnapshotActivation(owned.scope,metadata.cursor,async(merge,sequence)=>{
    const table=this.schema&&Object.hasOwn(this.schema,owned.header.entity)?this.schema[owned.header.entity]:undefined;
    const typed=this.schema?await activateSQLiteTypedSnapshot(this.db,this.schema,{...owned,entity:owned.header.entity,sequence,rows:stage.rows}):undefined;
    const fresh=typed??(table?await insertFreshSnapshotRows(this.db,table,{...owned,entity:owned.header.entity,revision:metadata.cursor.revision,sequence}):undefined);
    const body=typed?.body??'body';
    const overlap=fresh===undefined?'':' AND EXISTS(SELECT 1 FROM temp.gv_snapshot_overlap o WHERE o.id=gv_snapshot_rows.id)';
    let after=-1,seen=fresh?.freshRows??0;
    for(;;){
     // Size-only probes prevent decoding 100 individually large rows at once.
     const sizes=await this.db.getAllAsync<{position:number;bytes:number}>('SELECT position,length(CAST('+body+' AS BLOB))*2+256 bytes FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND position>?'+overlap+' ORDER BY position LIMIT 100',owned.scope,owned.subscription,owned.snapshot,after);
     if(!sizes.length)break;
     let limit=0,bytes=0;
     for(const size of sizes){if(bytes+size.bytes>196608)break;bytes+=size.bytes;limit++;}
     if(!limit)fail('BUDGET','Snapshot row exceeds canonical merge budget');
     const entries=await this.db.getAllAsync<{id:string;body:string;position:number}>('SELECT id,'+body+(typed?' body': '')+',position FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND position>?'+overlap+' ORDER BY position LIMIT ?',owned.scope,owned.subscription,owned.snapshot,after,limit);
     let decoded=64;
     for(let i=0;i<entries.length;i++){
      const entry=entries[i]!;
      try{const next=decoded+decodedJSONBytes(entry.body,262144-decoded)+entry.id.length*2+128;if(next>262144)fail('BUDGET','Snapshot authority page exceeds decoded budget');decoded=next;}
      catch(error){if(!pageBudgetError(error)||i===0)throw error;entries.splice(i);break;}
     }
     const rows=entries.map(entry=>({id:entry.id,row:JSON.parse(entry.body) as ReplicaRow}));
     await merge(owned.header.entity,rows);
     seen+=rows.length;after=entries.at(-1)!.position;
    }
    if(seen!==stage.rows)fail('INVALID_ROW','Verified snapshot lost canonical rows');
    // Retire removed members only when no other native window retains them.
    // The shared authority merge preserves newer values and newer tombstones.
    let removedAfter=-1;
    for(;;){
     const removed=await this.db.getAllAsync<{id:string;position:number}>(`SELECT m.id,m.position FROM gv_windows w
      JOIN gv_all_window_members m USING(scope,signature,generation)
      WHERE w.scope=? AND w.signature=? AND m.position>?
       AND NOT EXISTS(SELECT 1 FROM gv_snapshot_rows s WHERE s.scope=w.scope AND s.subscription=? AND s.snapshot=? AND s.id=m.id)
       AND NOT EXISTS(SELECT 1 FROM gv_windows other JOIN gv_window_generations g USING(scope,signature,generation)
        JOIN gv_all_window_members kept USING(scope,signature,generation)
        WHERE other.scope=w.scope AND other.signature<>w.signature AND kept.id=m.id AND json_extract(g.header,'$.entity')=?)
      ORDER BY m.position LIMIT 100`,owned.scope,owned.header.signature,removedAfter,owned.subscription,owned.snapshot,owned.header.entity);
     if(!removed.length)break;
     await merge(owned.header.entity,removed.map(entry=>({id:entry.id,row:null})));
     removedAfter=removed.at(-1)!.position;
    }
    if(owned.header.entityCoverage==='complete'){
     // A row received between an older source certificate and disconnect may
     // never have belonged to that old membership. A complete generated source
     // must reconcile the canonical corpus too, rather than only old members.
     // Authority merge keeps rows/fields newer than this snapshot's cursor.
     const typedTable=this.schema&&Object.hasOwn(this.schema,owned.header.entity)?this.schema[owned.header.entity]:undefined;
     const physical=typedTable?await sqlitePhysicalEntityScope(this.db,owned.scope,owned.header.entity):owned.scope;
     const relation=typedTable?sqliteIdentifier(sqliteTableName(owned.header.entity)):'_gonvex_replica_entities';
     const restriction=typedTable?'':' AND t.entity=? AND t.value<>\'null\'';
     let after='';for(;;){
      const missing=await this.db.getAllAsync<{id:string}>(`SELECT t.id FROM ${relation} t WHERE t.scope=? AND t.id>?${restriction}
       AND NOT EXISTS(SELECT 1 FROM gv_snapshot_rows s WHERE s.scope=? AND s.subscription=? AND s.snapshot=? AND s.id=t.id)
       ORDER BY t.id LIMIT 100`,physical,after,...(typedTable?[]:[owned.header.entity]),owned.scope,owned.subscription,owned.snapshot);
      if(!missing.length)break;await merge(owned.header.entity,missing.map(({id})=>({id,row:null})));after=missing.at(-1)!.id;
     }
    }
    await fresh?.restoreIndexes();
    await this.db.runAsync('DELETE FROM temp.gv_snapshot_overlap');
    // Membership and hash certificates move entirely inside SQLite.
    await this.windows.publishSnapshot(owned.scope,owned.header,owned.subscription,owned.snapshot,!!typed);
   });
   await this.db.runAsync('INSERT INTO gv_snapshot_activation_receipts VALUES(?,?,?,?,?)',owned.scope,owned.subscription,owned.snapshot,encoded,JSON.stringify(receipt));
  });return receipt;
 }
}
