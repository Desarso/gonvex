import { expect, it } from 'vitest';
import { wasmSQLiteDatabase, type WasmSQLiteAPI } from './wasm-sqlite-database';

function fixture(result: Array<Record<string,unknown>>, gate?:Promise<void>) {
 const calls:string[]=[],bound:unknown[][]=[];let row=-1,finalized=0,active=0;
 const api:WasmSQLiteAPI={
  async *statements(_db,sql){calls.push(sql);active++;expect(active).toBe(1);try{row=-1;yield 1;}finally{active--;finalized++;}},
  bind_collection(_stmt,values){bound.push(values);},
  async step(){if(gate)await gate;return ++row<result.length?100:101;},
  column_names(){return Object.keys(result[row]!);},
  column(_stmt,index){return Object.values(result[row]!)[index];},
 };
 return {api,calls,bound,finalized:()=>finalized};
}
it('finalizes a native statement after selecting only its first row',async()=>{
 const f=fixture([{value:'a'},{value:'b'}]);const db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:100});
 expect(await db.getFirstAsync('read',true,false,'false',null)).toEqual({value:'a'});
 expect(f.bound).toEqual([[1,0,'false',null]]);expect(f.finalized()).toBe(1);
});
it('returns null for an empty first-row query',async()=>{
 const f=fixture([]),db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:100});
 expect(await db.getFirstAsync('empty')).toBeNull();expect(f.finalized()).toBe(1);
});
it.each([['rows',{maxRows:1,maxTextBytes:100}],['text',{maxRows:3,maxTextBytes:1}]] as const)('finalizes on %s budget failure and keeps the statement lane usable',async(limit,budget)=>{
 const f=fixture([{v:'one'},{v:'two'}]),db=wasmSQLiteDatabase(f.api,1,budget);
 await expect(db.getAllAsync('too much')).rejects.toMatchObject({code:'sqlite-bridge-budget',limit});expect(f.finalized()).toBe(1);
 await db.execAsync('next');expect(f.finalized()).toBe(2);
});
it('serializes parallel initialization reads without reentering a suspended VFS',async()=>{
 let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
 const f=fixture([{v:1}],gate),db=wasmSQLiteDatabase(f.api,1,{maxRows:2,maxTextBytes:100});
 const a=db.getAllAsync('first'),b=db.getAllAsync('second');await Promise.resolve();expect(f.calls).toEqual(['first']);
 release();await Promise.all([a,b]);expect(f.calls).toEqual(['first','second']);expect(f.finalized()).toBe(2);
});
it('rolls back a failed callback, preserves its error and allows a later transaction',async()=>{
 const f=fixture([]),db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:100}),error=new Error('failed write');
 await expect(db.withTransactionAsync(async()=>{await db.runAsync('write',1);throw error;})).rejects.toBe(error);
 await db.withTransactionAsync(async()=>{await db.execAsync('other write');});
 expect(f.calls).toEqual(['BEGIN','write','ROLLBACK','BEGIN','other write','COMMIT']);
});
it('rejects nested transactions instead of deadlocking the statement lane',async()=>{
 const f=fixture([]),db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:100});
 await expect(db.withTransactionAsync(()=>db.withTransactionAsync(async()=>{}))).rejects.toThrow('nested');
 expect(f.calls).toEqual(['BEGIN','ROLLBACK']);
});
it.each([0,-1,NaN,Infinity,1.1])('refuses an unbounded or invalid bridge budget %s',maxRows=>{
 const f=fixture([]);expect(()=>wasmSQLiteDatabase(f.api,1,{maxRows,maxTextBytes:100})).toThrow(RangeError);
});
it('uses own properties for unusual result column names',async()=>{
 const row=Object.fromEntries([['__proto__','value'],['constructor','column']]),f=fixture([row]);
 const db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:1000});
 expect(await db.getFirstAsync('read')).toEqual(row);expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
});

it('rejects an oversized native text value before copying it into JS',async()=>{
 const f=fixture([{v:'large'}]);let copied=false;
 f.api.column_type=()=>3;f.api.column_bytes=()=>1000000;f.api.column=()=>{copied=true;throw Error('must not copy');};
 const db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:100});
 await expect(db.getAllAsync('large')).rejects.toMatchObject({code:'sqlite-bridge-budget'});expect(copied).toBe(false);expect(f.finalized()).toBe(1);
});
it('does not reject multi-byte text merely because UTF-8 is larger than UTF-16',async()=>{
 const f=fixture([{v:'漢字'}]);f.api.column_type=()=>3;f.api.column_bytes=()=>6;
 const db=wasmSQLiteDatabase(f.api,1,{maxRows:1,maxTextBytes:6});
 expect(await db.getAllAsync('text')).toEqual([{v:'漢字'}]);
});
