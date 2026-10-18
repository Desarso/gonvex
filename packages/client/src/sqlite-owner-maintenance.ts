import {SQLiteFeedGroups} from './sqlite-feed-groups.js';
import {sqlitePayloadBytes} from './sqlite-admission.js';
import {collectSQLiteRetiredEntityRows} from './sqlite-typed-snapshot.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
/** Created once before this owner's first connection. Work is metadata-only
 * until one bounded128-row cleanup step enters the same fenced SQL FIFO.
 * No corpus, stale-generation list or cleanup promise queue lives in JS. */
export class SQLiteOwnerMaintenance {
 private readonly initialized:Promise<void>;private readonly windows:SQLiteWindowStore;
 private readonly groups:SQLiteFeedGroups;private phase=0;private timer?:ReturnType<typeof setTimeout>;private active?:Promise<{more:boolean}>;private closed=false;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly onError:(error:unknown)=>void){
  this.windows=new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner());this.groups=new SQLiteFeedGroups(owner.database,()=>owner.engine.verifyOwner());
  this.initialized=owner.runOwner(0,async()=>{
   await this.groups.ready();await this.groups.recover();await owner.snapshots.ready();await owner.activation.ready();await this.windows.ready();
   await owner.database.withTransactionAsync(async()=>{await owner.engine.verifyOwner();
    await owner.database.execAsync('CREATE TABLE IF NOT EXISTS gv_snapshot_cleanup_queue(scope TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,PRIMARY KEY(scope,subscription,snapshot)) WITHOUT ROWID');
    await owner.database.runAsync(`INSERT OR IGNORE INTO gv_snapshot_cleanup_queue SELECT b.scope,b.subscription,b.snapshot FROM gv_snapshot_typed b WHERE NOT EXISTS(SELECT 1 FROM gv_snapshot_stages s WHERE s.scope=b.scope AND s.subscription=b.subscription AND s.snapshot=b.snapshot)`);
    // The old worker no longer owns handles. Unpublished stages left by it
    // cannot grant coverage and must not exhaust the next owner's stage cap.
    await owner.database.runAsync(`UPDATE gv_snapshot_stages AS s SET discarded=1 WHERE NOT EXISTS(
     SELECT 1 FROM gv_snapshot_window_alias a JOIN gv_windows w USING(scope,signature,generation)
     WHERE a.scope=s.scope AND a.subscription=s.subscription AND a.snapshot=s.snapshot)`);
   });
  });void this.initialized.catch(()=>{});
 }
 async retire(scope:string,subscriptions:readonly string[]){
  sqlitePayloadBytes({scope,subscriptions},131072);if(!scope||scope.length>512||subscriptions.length>128||subscriptions.some(id=>typeof id!=='string'||!id||id.length>256))throw new TypeError('Invalid retired source identities');const owned=[...subscriptions];await this.initialized;if(this.closed)return;
  await this.owner.runOwner(131072,()=>this.owner.database.withTransactionAsync(async()=>{
   if(this.closed)return;await this.owner.engine.verifyOwner();const ids=JSON.stringify(owned);
   await this.owner.database.runAsync('INSERT OR IGNORE INTO gv_snapshot_cleanup_queue SELECT scope,subscription,snapshot FROM gv_snapshot_typed WHERE scope=? AND subscription IN (SELECT value FROM json_each(?)) UNION SELECT scope,subscription,snapshot FROM gv_snapshot_stages WHERE scope=? AND subscription IN (SELECT value FROM json_each(?))',scope,ids,scope,ids);
   await this.owner.database.runAsync('UPDATE gv_snapshot_stages SET discarded=1 WHERE scope=? AND subscription IN (SELECT value FROM json_each(?))',scope,ids);
  }));this.schedule(0);
 }
 ready(){return this.initialized;}
 start(){void this.initialized.then(()=>this.schedule(0),error=>{if(!this.closed)this.onError(error);});}
 private schedule(delay:number){if(this.closed)return;clearTimeout(this.timer);this.timer=setTimeout(()=>{this.timer=undefined;void this.pulse().then(result=>this.schedule(result.more?5:5000),error=>{if(!this.closed){this.onError(error);this.schedule(5000);}});},delay);}
 pulse():Promise<{more:boolean}>{if(this.active)return this.active;const operation=this.collect();this.active=operation;void operation.then(()=>{this.active=undefined;},()=>{this.active=undefined;});return operation;}
 private async collect(){
  await this.initialized;if(this.closed)return {more:false};
  return this.owner.runOwner(16384,async()=>{
   if(this.closed)return {more:false};let more=false;
   await this.owner.database.withTransactionAsync(async()=>{
    await this.owner.engine.verifyOwner();
    for(let attempt=0;attempt<4;attempt++){
     const phase=this.phase;this.phase=(phase+1)%4;
     if(phase===0){
      const window=await this.owner.database.getFirstAsync<{scope:string;signature:string;generation:string}>(`SELECT g.scope,g.signature,g.generation FROM gv_window_generations g WHERE NOT EXISTS(SELECT 1 FROM gv_windows w WHERE w.scope=g.scope AND w.signature=g.signature AND w.generation=g.generation) ORDER BY g.scope,g.signature,g.generation LIMIT 1`);
      if(window){await this.windows.collect(window.scope,window.signature,window.generation);more=true;return;}
     }else if(phase===1){
      const queued=await this.owner.database.getFirstAsync<{scope:string;subscription:string;snapshot:string}>('SELECT scope,subscription,snapshot FROM gv_snapshot_cleanup_queue ORDER BY scope,subscription,snapshot LIMIT 1');
      const stage=queued??await this.owner.database.getFirstAsync<{scope:string;subscription:string;snapshot:string}>('SELECT scope,subscription,snapshot FROM gv_snapshot_stages WHERE discarded=1 ORDER BY scope,subscription,snapshot LIMIT 1');
      if(stage){if(await this.owner.snapshots.discard(stage.scope,stage.subscription,stage.snapshot))await this.owner.database.runAsync('DELETE FROM gv_snapshot_cleanup_queue WHERE scope=? AND subscription=? AND snapshot=?',stage.scope,stage.subscription,stage.snapshot);more=true;return;}
     }else if(phase===2&&await this.groups.collect()){more=true;return;}else if(phase===3&&await this.owner.database.getFirstAsync('SELECT 1 FROM gv_entity_retired LIMIT 1')){
      await collectSQLiteRetiredEntityRows(this.owner.database,this.owner.engine.schema,128);more=true;return;
     }
    }
   });return {more};
  });
 }
 close(){this.closed=true;clearTimeout(this.timer);}
}
