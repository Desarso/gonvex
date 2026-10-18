import type {IndexedDBReplicaEngine} from './indexeddb-replica-engine.js';
import type {IndexedDBReducerCore} from './indexeddb-reducer-core.js';
import type {SQLiteSafetyEntry,SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {fail} from './sqlite-admission.js';
type Options={engine:IndexedDBReplicaEngine;core:IndexedDBReducerCore;safety:Pick<SQLiteSafetyJournal,'page'|'settlements'>;scope:string;intentScope:string;assertCurrent():void;recoverLegacy?(entry:SQLiteSafetyEntry):Promise<void>};
/** Runs before fallback reads/delivery are admitted. Retire stale commands
 * already settled by another adapter, then restore original prepared replies.
 * Eight commands at a time; no memory-only recovered or settlement corpus. */
export class IndexedDBSafetyRecovery {
 private active?:Promise<void>;private closed=false;
 constructor(private readonly options:Options){if(!options.scope||options.scope.length>512||!options.intentScope||options.intentScope.length>512)throw new TypeError('Invalid fallback recovery scope');}
 private check(){if(this.closed)fail('FENCED','Fallback recovery closed');this.options.assertCurrent();}
 recover(){if(this.active)return this.active;const operation=this.run();this.active=operation;void operation.then(()=>{this.active=undefined;},()=>{this.active=undefined;});return operation;}
 private async run(){
  const {engine,core,safety,scope,intentScope}=this.options;this.check();let after=0;
  for(;;){
   const page=await engine.deliveryPage(intentScope,after);this.check();const outcomes=await safety.settlements(intentScope,page.records.map(record=>record.command));this.check();
   const terminal=outcomes.filter(value=>value.settled);if(terminal.some(value=>!value.outcome))fail('FENCED','Settled fallback command lacks its authoritative outcome');
   if(terminal.length)await engine.atomic(undefined,async tx=>{this.check();for(const value of terminal){await engine.settleInTransaction(tx,intentScope,value.command,value.outcome!.status,value.outcome!.error);this.check();}});
   if(page.nextSequence===null)break;after=page.nextSequence;
  }
  after=0;for(;;){const page=await safety.page(intentScope,after,8);this.check();for(const entry of page.entries){if(entry.prepared)await core.recover(entry.invocation,entry.prepared);else if(this.options.recoverLegacy)await this.options.recoverLegacy(entry);else fail('FENCED','Fallback legacy recovery requires its original envelope importer');this.check();}if(page.nextSequence===null)break;after=page.nextSequence;}
  await core.rebase(scope);this.check();
 }
 close(){this.closed=true;}
}
