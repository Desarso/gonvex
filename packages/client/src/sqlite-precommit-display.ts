import {sqlitePayloadBytes} from './sqlite-admission.js';
import type {SQLitePendingSnapshotPage} from './sqlite-pending-snapshot-reader.js';
export type SQLitePrecommitDisplay={id:string;signature:string;admissionKey:string;page:SQLitePendingSnapshotPage|null};
/** Validate the optional display envelope independently of committed changes. */
export function validSQLitePrecommitDisplay(value:unknown):value is SQLitePrecommitDisplay{
 try{
  sqlitePayloadBytes(value,65536);const v=value as SQLitePrecommitDisplay;
  return !!v&&typeof v.id==='string'&&v.id.length<=128&&typeof v.signature==='string'&&v.signature.length<=128&&typeof v.admissionKey==='string'&&v.admissionKey.length<=128&&(v.page===null||v.page.source==='pending'&&v.page.complete===false&&v.page.serverVerified===false&&Array.isArray(v.page.rows)&&v.page.rows.length<=24&&v.page.rows.every(r=>typeof r.id==='string'&&r.id.length<=512&&r.value!==undefined));
 }catch{return false;}
}
