import type {FunctionReference} from './index.js';
import type {JsonValue} from '@gonvex/protocol';
import type {SQLitePagePlan,SQLitePredicate,SQLiteBinding} from './sqlite-typed-query.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
export type SQLiteCollectionPageOptions=Omit<SQLitePagePlan,'table'|'membership'>;
/** The caller chooses a bounded view, never its authority table/membership.
 * Generated source filters are mandatory, including for optimistic inserts. */
export function sqliteCollectionPagePlan(ref:FunctionReference,args:JsonValue,input:SQLiteCollectionPageOptions,membership:{signature:string;generation:string},completeEntities:ReadonlySet<string>){
 sqlitePayloadBytes({input,args},16384);if(!ref.replica||ref.delivery!=='replica')throw new TypeError('Expected a generated Replica Collection');if(!input||Object.keys(input).some(key=>!['where','order','after','search','limit','select','textOrder'].includes(key)))throw new TypeError('Unsupported bounded collection plan option');
 if(!Number.isSafeInteger(input.limit)||input.limit<1||input.limit>128)throw new RangeError('Collection pages are bounded to 128 rows');
 const columns=ref.replica.columns?new Set(ref.replica.columns):undefined;
 const allowed=(field:string)=>{if(typeof field!=='string'||!field||field.length>256||columns&&!columns.has(field))fail('INVALID_ROW','Collection plan requires an undeclared projection column');};
 let nodes=0;const visit=(predicate:SQLitePredicate,depth=0,root=true):void=>{if(++nodes>128||depth>16)fail('BUDGET','Collection predicate exceeds its structural budget');if(!predicate||typeof predicate!=='object')throw new TypeError('Invalid collection predicate');if('field'in predicate&&root)allowed(predicate.field);if(predicate.op==='relation'){if(!completeEntities.has(predicate.table))fail('PAGE_CHANGED','Related entity needs complete certified coverage');visit(predicate.where??{op:'and',children:[]},depth+1,false);}else if(predicate.op==='and'||predicate.op==='or'){if(!Array.isArray(predicate.children))throw new TypeError('Invalid collection predicate group');for(const child of predicate.children)visit(child,depth+1,root);}else if(predicate.op==='not')visit(predicate.child,depth+1,root);};
 if(input.where)visit(input.where);if(!Array.isArray(input.order))throw new TypeError('Collection pages need an order');for(const order of input.order)allowed(order.field);for(const column of input.select??[])allowed(column);
 let allowLocalInserts=true;const terms:SQLitePredicate[]=[];if(input.where)terms.push(structuredClone(input.where));
 for(const [argument,column] of Object.entries(ref.replica.equalFilters??{})){const value=args&&typeof args==='object'&&!Array.isArray(args)?args[argument]:undefined;if(value===undefined){allowLocalInserts=false;continue;}allowed(column);if(value!==null&&typeof value!=='string'&&typeof value!=='number'&&typeof value!=='boolean')throw new TypeError('Generated collection filter requires a scalar argument');terms.push({field:column,op:'eq',value:value as SQLiteBinding|boolean});}
 for(const column of ref.replica.excludeWhenSet??[]){allowed(column);terms.push({field:column,op:'eq',value:null});}
 const plan={...structuredClone(input),...(!input.select&&ref.replica.columns?{select:[...ref.replica.columns]}:{}),table:ref.replica.table,membership:{...membership,requireComplete:true},...(terms.length?{where:terms.length===1?terms[0]:{op:'and' as const,children:terms}}:{})} satisfies SQLitePagePlan;return {plan,allowLocalInserts};
}
