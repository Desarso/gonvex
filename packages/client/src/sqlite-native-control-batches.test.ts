import {expect,it} from 'vitest';
import {decodeSQLiteNativeTransaction,decodeSQLiteNativeTransactionBatches,decodeSQLiteNativeDeltaBatches} from './sqlite-native-control.js';
import {sqlitePayloadBytes} from './sqlite-admission.js';
const rows=Array.from({length:80},(_,n)=>({id:String(n),name:'Search after create '+n,...Object.fromEntries(Array.from({length:52},(_,i)=>['field'+i,null]))}));
it('applies a wire-bounded wide-row update in decoded-budget chunks without dropping its searched new row',async()=>{
 const canonicalChanges=rows.map(row=>JSON.stringify({entity:'tasks',id:row.id,operation:'insert',newValue:row}));
 const frame={type:'replica.transactionEncoded' as const,cursor:{epoch:'e',revision:2},canonicalChanges};
 expect(sqlitePayloadBytes(frame,262144)).toBeLessThan(262144);
 expect(()=>decodeSQLiteNativeTransaction(frame,{tasks:'id'})).toThrow('Decoded SQLite page');
 const chunks=[...decodeSQLiteNativeTransactionBatches(frame,{tasks:'id'})];expect(chunks.length).toBeGreaterThan(1);expect(chunks.flat().map(c=>c.newValue)).toEqual(rows);
 const delta={type:'replica.delta' as const,id:'tasks',cursor:frame.cursor,canonicalUpserts:rows.map(row=>JSON.stringify(row)),deleted:[],digest:''};
 const result=[];for await(const chunk of decodeSQLiteNativeDeltaBatches(delta,'id'))result.push(...chunk);
 expect(result.map(r=>r.row)).toEqual(rows);expect(result.find(r=>r.row.name==='Search after create 79')?.id).toBe('79');
});
it('still refuses a single oversized decoded row and detects duplicate IDs across chunks',async()=>{
 const frame={type:'replica.delta' as const,id:'tasks',cursor:{epoch:'e',revision:2},canonicalUpserts:rows.map(row=>JSON.stringify(row)),deleted:[],digest:''};
 const consume=async(value:typeof frame)=>{for await(const _ of decodeSQLiteNativeDeltaBatches(value,'id')){};};
 await expect(consume({...frame,canonicalUpserts:[JSON.stringify({id:'huge',text:'x'.repeat(140000)})]})).rejects.toMatchObject({code:'BUDGET'});
 await expect(consume({...frame,canonicalUpserts:[...frame.canonicalUpserts,frame.canonicalUpserts[0]]})).rejects.toThrow('Duplicate');
});
