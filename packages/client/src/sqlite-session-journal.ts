import type {LocalReplicaSession} from './local-replica.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';

/** Admission and revocation share the replica database and its owner FIFO.
 * Memory mode consequently keeps no admission or commands across reloads. */
export class SQLiteSessionJournal {
 private ready:Promise<void>;
 constructor(private readonly owner:SQLiteOwnerProgramContext){
  this.ready=owner.runOwner(0,()=>owner.database.execAsync('CREATE TABLE IF NOT EXISTS gv_session_admission(scope TEXT PRIMARY KEY,value TEXT,revoked INTEGER NOT NULL) WITHOUT ROWID'));
  void this.ready.catch(()=>{});
 }
 private run<T>(scope:string,work:()=>Promise<T>){
  if(typeof scope!=='string'||!scope||scope.length>512)throw new TypeError('Invalid session scope');
  return this.owner.runOwner(32768,async()=>{await this.ready;await this.owner.engine.verifyOwner();return work();});
 }
 async session(scope:string):Promise<LocalReplicaSession|undefined>{return this.run(scope,async()=>{
  const row=await this.owner.database.getFirstAsync<{value:string|null;revoked:number}>('SELECT value,revoked FROM gv_session_admission WHERE scope=?',scope);
  if(!row||row.revoked||!row.value)return;const session=JSON.parse(row.value) as LocalReplicaSession;sqlitePayloadBytes(session,32768);return session;
 });}
 sessionRevoked(scope:string){return this.run(scope,async()=>!!(await this.owner.database.getFirstAsync<{revoked:number}>('SELECT revoked FROM gv_session_admission WHERE scope=?',scope))?.revoked);}
 saveSession(scope:string,session:LocalReplicaSession,authoritative=false,current:()=>boolean=()=>true){
  sqlitePayloadBytes(session,32768);const value=JSON.stringify(session);
  return this.run(scope,()=>this.owner.database.withTransactionAsync(async()=>{
   if(!current())fail('FENCED','Session admission changed');
   const row=await this.owner.database.getFirstAsync<{revoked:number}>('SELECT revoked FROM gv_session_admission WHERE scope=?',scope);
   if(row?.revoked&&!authoritative)fail('FENCED','Cached session was revoked');
   await this.owner.database.runAsync('INSERT INTO gv_session_admission VALUES(?,?,0) ON CONFLICT(scope) DO UPDATE SET value=excluded.value,revoked=0',scope,value);
   if(!current())fail('FENCED','Session admission changed');
  }));
 }
 revokeSession(scope:string){return this.run(scope,()=>this.owner.database.withTransactionAsync(async()=>{
  await this.owner.database.runAsync('INSERT INTO gv_session_admission VALUES(?,NULL,1) ON CONFLICT(scope) DO UPDATE SET value=NULL,revoked=1',scope);
  await this.owner.canonical.saveSession(scope,undefined);
 }));}
}
