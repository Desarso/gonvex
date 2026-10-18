import type {FunctionReference} from './index.js';
import type {JsonValue} from '@gonvex/protocol';
import type {SQLiteCollectionPageOptions} from './sqlite-collection-plan.js';
import {compileSQLiteTypedPage,type SQLitePredicate,type SQLiteBinding} from './sqlite-typed-query.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {SQLiteTypedPage} from './sqlite-typed-engine.js';
import {sqliteCollectionPagePlan} from './sqlite-collection-plan.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
import {IndexedDBLiveWindows} from './indexeddb-live-windows.js';

type Candidate={id:string;row:Record<string,unknown>};
type Input={scope:string;epoch:string;artifactHash:string;signature:string;ref:FunctionReference;args:JsonValue;plan:SQLiteCollectionPageOptions;generation?:string};
const scalar=(value:unknown):string|number|boolean|null=>value===undefined?null:value as string|number|boolean|null;
function binary(a:string,b:string){const left=new TextEncoder().encode(a),right=new TextEncoder().encode(b);for(let i=0;i<Math.min(left.length,right.length);i++)if(left[i]!==right[i])return left[i]!<right[i]!?-1:1;return Math.sign(left.length-right.length);}
function compare(a:unknown,b:unknown){const left=scalar(a),right=scalar(b);if(left===null||right===null)return left===right?0:left===null?1:-1;if(typeof left==='string'&&typeof right==='string')return binary(left,right);if(typeof left==='boolean'||typeof right==='boolean')return Number(left)-Number(right);return Number(left)-Number(right);}
function predicate(row:Record<string,unknown>,input:SQLitePredicate):boolean|null{
 if(input.op==='and'){let unknown=false;for(const child of input.children){const result=predicate(row,child);if(result===false)return false;if(result===null)unknown=true;}return unknown?null:true;}
 if(input.op==='or'){let unknown=false;for(const child of input.children){const result=predicate(row,child);if(result===true)return true;if(result===null)unknown=true;}return unknown?null:false;}
 if(input.op==='not'){const result=predicate(row,input.child);return result===null?null:!result;}
 if(input.op==='relation')throw Object.assign(Error('Fallback related collection page requires an indexed relation plan'),{code:'UNSUPPORTED'});
 if(input.op==='in'){const value=scalar(row[input.field]);if(value===null)return null;return input.values.some(candidate=>candidate!==null&&compare(value,candidate)===0);}
 if(!('field' in input))throw new TypeError('Invalid fallback collection predicate');const left=scalar(row[input.field]),right=input.value;if(input.op==='eq'&&right===null)return left===null;if(input.op==='ne'&&right===null)return left!==null;if(left===null||right===null)return null;
 const order=compare(left,right);switch(input.op){case 'eq':return order===0;case 'ne':return order!==0;case 'lt':return order<0;case 'lte':return order<=0;case 'gt':return order>0;case 'gte':return order>=0;}
}
/** Fallback collection pages keep only winning rows while streaming the
 * durable membership in native 128-row pages. No full collection enters JS. */
export class IndexedDBCollectionPages {
 constructor(private readonly engine:IndexedDBLiveWindows,private readonly schema:SQLiteTypedSchema){}
 async page(input:Input,completeEntities:ReadonlySet<string>):Promise<(SQLiteTypedPage&{generation:string})|null>{
  sqlitePayloadBytes(input,32768);const first=await this.engine.windowPage({scope:input.scope,signature:input.signature,epoch:input.epoch,artifactHash:input.artifactHash,offset:0,limit:128});if(!first)return null;
  const windowGeneration=first.window.generation;if(input.generation!==undefined&&input.generation!==windowGeneration)fail('PAGE_CHANGED','Fallback collection membership changed');
  const {plan,allowLocalInserts}=sqliteCollectionPagePlan(input.ref,input.args,input.plan,{signature:input.signature,generation:windowGeneration},completeEntities),table=this.schema[plan.table];if(!table)throw new TypeError('Unknown fallback collection table');compileSQLiteTypedPage(this.schema,input.scope,plan);
  if(plan.after&&input.generation===undefined)throw new TypeError('Fallback keyset requires its window generation');if(plan.after&&plan.after.sequence!==first.generation.sequence)fail('PAGE_CHANGED','Fallback keyset belongs to an older replica snapshot');
  if(!plan.order.length||plan.order.length>8||plan.order.some(order=>!Object.hasOwn(table.fields,order.field)||table.fields[order.field]?.kind==='json'||table.fields[order.field]?.kind==='text'&&plan.textOrder!=='binary'))throw new TypeError('Unsupported fallback collection sort');
  if(plan.search&&(!table.search?.length||plan.search.length>512||table.search.some(field=>input.ref.replica?.columns&&!input.ref.replica.columns.includes(field))))fail('INVALID_ROW','Fallback collection search needs declared columns');
  const selected=new Map<string,Candidate>(),compareCandidate=(a:Candidate,b:Candidate)=>{for(const order of plan.order){const left=scalar(a.row[order.field]),right=scalar(b.row[order.field]);if(left===right)continue;if(left===null)return order.direction==='asc'?1:-1;if(right===null)return order.direction==='asc'?-1:1;const difference=compare(left,right);if(difference)return order.direction==='asc'?difference:-difference;}return binary(a.id,b.id);};
  const after=plan.after?{id:plan.after.id,row:Object.fromEntries(plan.order.map((order,index)=>[order.field,plan.after!.values[index]!]))}:undefined;
  const add=(candidate:Candidate)=>{if(plan.where&&predicate(candidate.row,plan.where)!==true)return;if(plan.search&&!table.search!.some(field=>String(candidate.row[field]??'').toLowerCase().includes(plan.search!.toLowerCase())))return;if(after&&compareCandidate(candidate,after)<=0)return;const previous=selected.get(candidate.id);if(previous)selected.delete(candidate.id);selected.set(candidate.id,candidate);if(selected.size>plan.limit){let worst:Candidate|undefined;for(const value of selected.values())if(!worst||compareCandidate(value,worst)>0)worst=value;selected.delete(worst!.id);}};
  let current=first;for(;;){for(const value of current.rows)if(value.value)add({id:value.id,row:value.value});if(current.nextOffset===null)break;current=await this.engine.windowPage({scope:input.scope,signature:input.signature,epoch:input.epoch,artifactHash:input.artifactHash,offset:current.nextOffset,limit:128,generation:windowGeneration,expected:first.generation})??fail('PAGE_CHANGED','Fallback collection window disappeared');}
  if(allowLocalInserts){let cursor:string|undefined;for(;;){const page=await this.engine.predictedPage(input.scope,plan.table,cursor,first.generation);for(const value of page.rows)add({id:value.id,row:value.row});if(page.ids.length<16)break;cursor=page.ids.at(-1);}}
  const winners=[...selected.values()].sort(compareCandidate),rows=winners.map(value=>plan.select?Object.fromEntries([...new Set([table.key,...plan.select])].filter(column=>Object.hasOwn(value.row,column)).map(column=>[column,value.row[column]])):value.row),last=winners.at(-1),cursor=last?{id:last.id,sequence:first.generation.sequence,values:plan.order.map(order=>scalar(last.row[order.field]) as SQLiteBinding)}:null;
  const result={rows,cursor,bytes:sqlitePayloadBytes(rows,131072),sequence:first.generation.sequence,generation:windowGeneration};sqlitePayloadBytes(result,131072);return result;
 }
}
