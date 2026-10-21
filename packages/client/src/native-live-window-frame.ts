import type {JsonValue} from '@gonvex/protocol';
import type {FunctionReference} from './index.js';
import type {SQLiteLiveQueryUpdate} from './sqlite-native-transport.js';
import type {SQLiteWindowHeader} from './sqlite-window-store.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import {fail,sqlitePayloadBytes,decodedJSONBytes} from './sqlite-admission.js';
// The wire/parsed frame remains capped at 128 KiB. Decoding its persisted
// JSON uses a separate 256 KiB portion of the existing 512 KiB owner turn;
// decodedJSONBytes includes conservative container/key/token overhead.
export const NATIVE_LIVE_DECODE_BYTES=262144;
export const nativeLiveHash=async(value:string)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');
const record=(value:JsonValue):value is Record<string,JsonValue>=>value!==null&&typeof value==='object'&&!Array.isArray(value);
function projection(result:JsonValue,path:readonly string[]){let current=result;for(const part of path){if(!record(current)||!Object.hasOwn(current,part))throw new TypeError('Live result path is missing');current=current[part]!;}const scalar=!Array.isArray(current),rows=Array.isArray(current)?current:current===null?[]:[current];if(rows.length>256||rows.some(row=>!record(row)))throw new RangeError('Live windows must contain at most 256 entity rows');
 const replace=(value:JsonValue,depth:number):JsonValue=>depth===path.length?(scalar?null:[]):{...(value as Record<string,JsonValue>),[path[depth]!]:replace((value as Record<string,JsonValue>)[path[depth]!]!,depth+1)};
 return {rows:rows as Array<Record<string,JsonValue>>,scalar,skeleton:replace(result,0)};
}
/** Validate the generated projection and hash one bounded server result before
 * either storage engine admits it. Limited live windows grant no coverage. */
export async function prepareNativeLiveWindow(schema:SQLiteTypedSchema,artifactHash:string,stream:string,scope:string,signature:string,ref:FunctionReference,frame:Exclude<SQLiteLiveQueryUpdate,{error:string}>){
  sqlitePayloadBytes({stream,scope,signature},4096);sqlitePayloadBytes(frame,131072);if(!stream||stream.length>128||!scope||scope.length>512||!signature||signature.length>128||ref.kind!=='query'||ref.delivery!=='live'||!ref.live)throw new TypeError('Invalid generated live source');
  const live=ref.live,table=schema[live.entity],cursor=frame.subscriptionRevision;if(!table||table.key!==live.key||!cursor||!cursor.epoch||cursor.epoch.length>256||!Number.isSafeInteger(cursor.sequence)||cursor.sequence<0||frame.replicaScope!==undefined&&frame.replicaScope!==scope)fail('FENCED','Live window differs from its admitted replica');
  const path=live.resultPath??[];if(path.length>16||path.some(part=>typeof part!=='string'||!part||part.length>256||['__proto__','constructor','prototype'].includes(part)))throw new TypeError('Invalid generated live result path');
  const projected=projection(frame.result,path),columns=new Set(live.plan.columns??Object.keys(table.fields));columns.add(live.key);
  const identities=new Map<string,string>();for(const row of projected.rows){const id=row[live.key];if(typeof id!=='string'||!id||id.length>1024||Object.keys(row).some(column=>!columns.has(column)||!Object.hasOwn(table.fields,column)))throw new TypeError('Live window contains an undeclared entity projection');const body=JSON.stringify(row);if(identities.has(id)&&identities.get(id)!==body)fail('INTENT_CONFLICT','Live window repeats an entity with different values');identities.set(id,body);}
  const rows=JSON.stringify(projected.rows);if(rows.length*2>131072)fail('BUDGET','Encoded live window exceeds budget');for(const row of projected.rows)decodedJSONBytes(JSON.stringify(row),NATIVE_LIVE_DECODE_BYTES);const hash=await nativeLiveHash(rows);
  const header:SQLiteWindowHeader={signature,kind:'live',entity:live.entity,key:live.key,source:'server',completeness:'complete',cursor:{epoch:cursor.epoch,revision:cursor.sequence},subscriptionRevision:cursor,columns:[...columns],artifactHash:artifactHash,resultPath:[...path],resultSkeleton:projected.skeleton,scalar:projected.scalar,aggregateDigest:hash,...(frame.windowRevision!==undefined?{windowRevision:frame.windowRevision}:{})};sqlitePayloadBytes(header,16384);
  return {header,rows,values:projected.rows,hash,cursor};
}
