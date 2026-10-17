import { afterEach, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteReplicaCommits } from './sqlite-replica-commits';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage';
import type { ReplicaWindow } from './local-replica';
const databases:DatabaseSync[]=[];
afterEach(()=>{for(const db of databases.splice(0))db.close();});
function fixture(){
 const native=new DatabaseSync(':memory:');databases.push(native);const queries:Array<{sql:string;args:unknown[]}>=[];
 native.exec('CREATE TABLE _gonvex_replica_meta(scope TEXT,key TEXT,value TEXT,PRIMARY KEY(scope,key));CREATE TABLE _gonvex_replica_entities(scope TEXT,entity TEXT,id TEXT,value TEXT,PRIMARY KEY(scope,entity,id));CREATE TABLE _gonvex_replica_queries(scope TEXT,signature TEXT,value TEXT,PRIMARY KEY(scope,signature));');
 const database:SQLiteReplicaDatabase={
  async execAsync(sql){native.exec(sql);},async runAsync(sql,...args){queries.push({sql,args});return native.prepare(sql).run(...args as any[]);},
  async getAllAsync<T>(sql,...args){queries.push({sql,args});return native.prepare(sql).all(...args as any[]) as T[];},
  async getFirstAsync<T>(sql,...args){queries.push({sql,args});return native.prepare(sql).get(...args as any[]) as T??null;},
  async withTransactionAsync(run){native.exec('BEGIN');try{await run();native.exec('COMMIT');}catch(error){native.exec('ROLLBACK');throw error;}}
 };
 const notifications:string[]=[],log=new SQLiteReplicaCommits(database,scope=>{notifications.push(scope);});
 async function row(scope:string,id:string,value:Record<string,any>|null){
  return log.commit(scope,async()=>{await database.runAsync('INSERT OR REPLACE INTO _gonvex_replica_entities VALUES(?,?,?,?)',scope,'tasks',id,JSON.stringify(value));await log.rows(scope,'tasks',[{id,row:value}]);});
 }
 return {native,database,log,row,queries,notifications};
}
const interest={rows:{tasks:['hot']},windows:[],availableRows:0,availableBytes:0,maxRows:1,maxBytes:10000};
it('returns the actual committed rows and an adjacent scope sequence',async()=>{
 const f=fixture(),first=await f.row('s','hot',{_id:'hot',name:'First'}),second=await f.row('s','hot',{_id:'hot',name:'Second'});
 expect(first).toMatchObject({scope:'s',previousSequence:0,sequence:1,changes:{sequence:1,entities:{tasks:{hot:{name:'First'}}}}});
 expect(second).toMatchObject({previousSequence:1,sequence:2});
 expect((await f.log.readChanges('s',1,interest)).entities.tasks).toEqual({hot:{_id:'hot',name:'Second'}});
 expect(f.notifications).toEqual(['s','s']);
});
it('rolls back rows, change keys and sequence together, then permits another commit',async()=>{
 const f=fixture();await f.row('s','hot',{_id:'hot',name:'Keep'});const error=Error('abort');
 await expect(f.log.commit('s',async()=>{await f.database.runAsync("UPDATE _gonvex_replica_entities SET value=? WHERE scope='s'",JSON.stringify({_id:'hot',name:'Wrong'}));await f.log.rows('s','tasks',[{id:'hot',row:{_id:'hot',name:'Wrong'}}]);throw error;})).rejects.toBe(error);
 expect(await f.log.sequence('s')).toBe(1);expect((await f.log.readChanges('s',0,interest)).entities.tasks!.hot!.name).toBe('Keep');
 expect(f.notifications).toEqual(['s']);expect((await f.row('s','hot',{_id:'hot',name:'Later'})).sequence).toBe(2);
});
it('stores one latest-change key instead of retaining the transaction history',async()=>{
 const f=fixture();for(let i=0;i<10;i++)await f.row('s','hot',{_id:'hot',i});
 expect((f.native.prepare('SELECT count(*) AS n FROM _gonvex_replica_changes').get() as any).n).toBe(1);
 expect((await f.log.readChanges('s',0,interest)).entities.tasks!.hot!.i).toBe(9);
});
it('does not read cold row JSON just to filter changes by resident interest',async()=>{
 const f=fixture();await f.row('s','cold',{_id:'cold',note:'x'.repeat(1000000)});await f.row('s','hot',{_id:'hot'});f.queries.length=0;
 expect((await f.log.readChanges('s',0,interest)).entities.tasks).toEqual({hot:{_id:'hot'}});
 const bodies=f.queries.filter(q=>q.sql.startsWith('SELECT value FROM _gonvex_replica_entities'));
 expect(bodies).toHaveLength(1);expect(bodies[0]!.args.at(-1)).toBe('hot');
});
it('keeps scopes isolated and detects a peer commit through the durable sequence',async()=>{
 const f=fixture(),peer=new SQLiteReplicaCommits(f.database);
 await f.row('s','hot',{_id:'hot',name:'Visible'});await f.row('other','hot',{_id:'hot',name:'Secret'});
 const receipt=await peer.commit('s',async()=>{await f.database.runAsync("UPDATE _gonvex_replica_entities SET value=? WHERE scope='s'",JSON.stringify({_id:'hot',name:'Peer'}));await peer.rows('s','tasks',[{id:'hot',row:{_id:'hot',name:'Peer'}}]);});
 expect(receipt.previousSequence).toBe(1);expect(receipt.sequence).toBe(2);
 expect((await f.log.readChanges('s',0,interest)).entities.tasks!.hot!.name).toBe('Peer');
 expect((await f.log.readChanges('other',0,interest)).entities.tasks!.hot!.name).toBe('Secret');
});
it('resets old state and indexes a replacement without decoding a duplicate receipt',async()=>{
 const f=fixture();await f.row('s','old',{_id:'old'});
 const receipt=await f.log.commit('s',async()=>{
  await f.database.runAsync("DELETE FROM _gonvex_replica_entities WHERE scope='s'");await f.log.reset('s');
  await f.database.runAsync("INSERT INTO _gonvex_replica_entities VALUES('s','tasks','fresh',?)",JSON.stringify({_id:'fresh'}));await f.log.indexReplacement('s');
 });
 expect(receipt.changes).toMatchObject({reset:true,entities:{}});
 const changes=await f.log.readChanges('s',1,{...interest,rows:{},maxRows:2,maxBytes:10000});
 expect(changes.reset).toBe(true);expect(changes.entities.tasks).toEqual({fresh:{_id:'fresh'}});
});
it('does not truncate an oversized requested change and remains readable after the error',async()=>{
 const f=fixture();await f.row('s','hot',{_id:'hot',note:'x'.repeat(1000)});
 await expect(f.log.readChanges('s',0,{...interest,maxBytes:1})).rejects.toThrow('budget');
 expect((await f.log.readChanges('s',0,interest)).entities.tasks!.hot!.note).toHaveLength(1000);
});
it('preserves a window change and hydrates newly selected IDs despite zero cold-admission capacity',async()=>{
 const f=fixture();await f.row('s','new',{_id:'new'});
 const window:ReplicaWindow={signature:'w',kind:'live',entity:'tasks',key:'_id',ids:['new'],source:'server',completeness:'complete'};
 await f.log.commit('s',async()=>{await f.database.runAsync('INSERT INTO _gonvex_replica_queries VALUES(?,?,?)','s','w',JSON.stringify(window));await f.log.window('s','w',window);});
 const changes=await f.log.readChanges('s',0,{...interest,rows:{},windows:['w']});
 expect(changes.windows.w).toEqual(window);expect(changes.entities.tasks).toEqual({new:{_id:'new'}});
});
it('does not skip a changed Replica Collection coverage certificate when rows are not mounted',async()=>{
 const f=fixture();const window:ReplicaWindow={signature:'background',kind:'replica',entity:'tasks',key:'_id',ids:[],source:'server',completeness:'partial',truncated:true};
 await f.log.commit('s',async()=>{await f.database.runAsync('INSERT INTO _gonvex_replica_queries VALUES(?,?,?)','s',window.signature,JSON.stringify(window));await f.log.window('s',window.signature,window);});
 expect((await f.log.readChanges('s',0,interest)).windows.background).toEqual(window);
});
it('keeps tombstones and removed windows visible to interested readers',async()=>{
 const f=fixture();await f.row('s','hot',null);
 await f.log.commit('s',async()=>{await f.log.window('s','gone',null);});
 const changes=await f.log.readChanges('s',0,interest);expect(changes.entities.tasks).toEqual({hot:null});expect(changes.windows).toEqual({gone:null});
});
it('does not turn a successful commit into a rejection if peer notification fails',async()=>{
 const f=fixture(),log=new SQLiteReplicaCommits(f.database,()=>{throw Error('notification failed');});
 expect((await log.commit('s',async()=>{})).sequence).toBe(1);expect(await log.sequence('s')).toBe(1);
});
