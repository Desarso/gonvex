import {DatabaseSync} from 'node:sqlite';
import {afterEach,expect,it} from 'vitest';
import {SQLiteDeferredReplicaFrames} from './sqlite-deferred-replica-frames.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {ServerMessage} from '@gonvex/protocol';
const databases:DatabaseSync[]=[];afterEach(()=>{for(const db of databases.splice(0))db.close();});
function fixture(limits?:{maxBytes:number;maxFrames:number}){
 const sql=new DatabaseSync(':memory:');databases.push(sql);const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 let current=true;const fence=async()=>{if(!current)throw Object.assign(Error('replaced owner'),{code:'FENCED'});};return {sql,db,fence,store:new SQLiteDeferredReplicaFrames(db,'artifact',fence,limits),replace(){current=false;}};
}
const delta=(revision:number):Extract<ServerMessage,{type:'replica.delta'}>=>({type:'replica.delta',id:'wire-id',path:'tasks/all',cursor:{epoch:'e',revision},upserts:[{id:'row-'+revision,text:'Synthetic café 東京'}],deleted:[],digest:'sum128-v1:'+'0'.repeat(32)});
it('persists scoped frames through reopen, deduplicates subscription rebinding and detects changed payloads',async()=>{
 const f=fixture(),input=delta(11),pending=f.store.append('s','signature',input);(input.upserts![0] as any).text='caller mutation';expect(await pending).toEqual({inserted:true,revision:11});expect(await f.store.append('s','signature',{...delta(11),id:'rebound',path:'other-display-path'})).toEqual({inserted:false,revision:11});
 const next=new SQLiteDeferredReplicaFrames(f.db,'artifact',f.fence);expect((await next.read('s','signature','e',10))[0].frame).toEqual(delta(11));expect(await next.read('other','signature','e')).toEqual([]);expect(await next.read('s','other','e')).toEqual([]);expect(await next.read('s','signature','other-epoch')).toEqual([]);expect(await new SQLiteDeferredReplicaFrames(f.db,'new-artifact',f.fence).read('s','signature','e')).toEqual([]);
 await expect(next.append('s','signature',{...delta(11),deleted:['changed']})).rejects.toMatchObject({code:'INTENT_CONFLICT'});expect((await next.summary('s')).frames).toBe(1);f.replace();await expect(next.read('s','signature','e')).rejects.toMatchObject({code:'FENCED'});
});
it('retires only verified earlier revisions in bounded chunks and keeps usage atomic on late failure',async()=>{
 const f=fixture();await f.store.ready();await f.db.withTransactionAsync(async()=>{for(let n=0;n<260;n++)await f.store.append('s','signature',delta(n));});await f.store.append('other','signature',delta(5));await f.store.append('s','other',delta(5));
 expect(await f.store.discardThrough('s','signature','e',258)).toEqual({removed:128,remaining:true});expect(await f.store.discardThrough('s','signature','e',258)).toEqual({removed:128,remaining:true});expect(await f.store.discardThrough('s','signature','e',258)).toEqual({removed:3,remaining:false});expect((await f.store.read('s','signature','e')).map(entry=>entry.revision)).toEqual([259]);expect((await f.store.summary('s')).frames).toBe(2);expect((await f.store.summary('other')).frames).toBe(1);
 const before=await f.store.summary('s');await expect(f.db.withTransactionAsync(async()=>{await f.store.append('s','signature',delta(300));throw Error('late quota surrogate');})).rejects.toThrow('late quota surrogate');expect(await f.store.summary('s')).toEqual(before);expect((await f.store.read('s','signature','e',259))).toEqual([]);
});
it('bounds read pages and disk admission and detects corrupted durable frame bodies',async()=>{
 const f=fixture({maxBytes:1000000,maxFrames:3});for(let n=1;n<=3;n++)await f.store.append('s','signature',delta(n));await expect(f.store.append('s','signature',delta(4))).rejects.toMatchObject({code:'BUDGET'});expect((await f.store.summary('s')).frames).toBe(3);expect((await f.store.read('s','signature','e',0,2)).map(entry=>entry.revision)).toEqual([1,2]);await expect(f.store.read('s','signature','e',0,9)).rejects.toThrow('page');
 const corrupt={...delta(1),upserts:[{id:'tampered'}]};await f.db.runAsync('UPDATE gv_deferred_replica_frames SET body=? WHERE revision=1',new TextEncoder().encode(JSON.stringify(corrupt)));await expect(f.store.read('s','signature','e')).rejects.toMatchObject({code:'INTENT_CORRUPT'});
 const small=fixture({maxBytes:1,maxFrames:100});await expect(small.store.append('s','signature',delta(1))).rejects.toMatchObject({code:'BUDGET'});expect(await small.store.summary('s')).toEqual({bytes:0,frames:0});
});

it('reclaims canceled incarnations in128-record pages while preserving a replacement source and its accounting',async()=>{
 const f=fixture();await f.store.ready();await f.db.withTransactionAsync(async()=>{for(let n=0;n<260;n++)await f.store.append('s','old-incarnation',delta(n));});await f.store.recoverCanceled();await f.store.append('s','replacement-incarnation',delta(20));await expect(f.store.append('s','old-incarnation',delta(300))).rejects.toMatchObject({code:'FENCED'});
 for(const remaining of [133,5,1]){expect(await f.store.collectCanceled()).toBe(true);expect((await f.store.summary('s')).frames).toBe(remaining);expect((await f.store.read('s','replacement-incarnation','e')).map(frame=>frame.revision)).toEqual([20]);}expect(await f.store.collectCanceled()).toBe(false);
 await f.store.cancel('s','replacement-incarnation');expect(await f.store.collectCanceled()).toBe(true);expect(await f.store.summary('s')).toEqual({frames:0,bytes:0});
});
