import {sqlitePhysicalEntityScope} from './sqlite-entity-scope.js';
import type { DataPredicate, DataRead, JsonObject } from '@gonvex/module-sdk';
import type { ReplicaReadPage } from './paged-read-view.js';
import { executionReadColumns, primaryReadKeys } from '@gonvex/local-runtime/read-view';
import { decodedJSONBytes, decodedPageBytes, pageBudgetError, sqlitePayloadBytes } from './sqlite-admission.js';
import { sqliteRowBodySQL } from './sqlite-typed-body.js';
import { sqliteFieldName, sqliteIdentifier as q, sqliteTableName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

/** Native candidate page for portable reducers. Only proved restrictions are
 * pushed down. The portable reader always applies the complete predicate and
 * exact ordering, so unsupported transforms retain their existing semantics. */
export async function readSQLitePortablePage(db: SQLiteReplicaDatabase, schema: SQLiteTypedSchema,
  scope: string, read: DataRead, after?: string, excluded: readonly string[] = [], coverageColumns?: readonly string[], predictionScope?:string): Promise<ReplicaReadPage> {
  sqlitePayloadBytes({scope,read,excluded,coverageColumns:coverageColumns??null},65536);
  const table = Object.hasOwn(schema,read.table) && schema[read.table];
  if (!table) throw new TypeError('Portable read references an undeclared SQLite table');
  const physical = await sqlitePhysicalEntityScope(db,scope,read.table);
  // Predictions use the same generated typed table and indexes in a private
  // physical scope. Markers exclude replaced/deleted canonical rows on disk.
  // Keep the original indexed source when this table has no predictions.
  const typed=q(sqliteTableName(read.table));let source=typed,sourceBindings:unknown[]=[];
  if(predictionScope&&await db.getFirstAsync('SELECT 1 FROM gv_prediction_members WHERE scope=? AND entity=? LIMIT 1',scope,read.table)){
    // UNION columns must have exactly the typed table's original order.
    source=`(SELECT b.* FROM ${typed} b WHERE b.scope=? AND NOT EXISTS(SELECT 1 FROM gv_prediction_members p WHERE p.scope=? AND p.entity=? AND p.id=b.id)
     UNION ALL SELECT rid,? AS scope,id,body,version,${[...Object.keys(table.fields).map(sqliteFieldName),...(table.search??[]).map((_,i)=>'s'+i)].map(q).join(',')} FROM ${typed} WHERE scope=?)`;
    sourceBindings=[physical,scope,read.table,physical,predictionScope];
  }
  let fieldsComplete = true;
  if (after === undefined) {
    const required = new Set(read.columns ?? coverageColumns ?? []);
    const collect = (predicate: DataPredicate): void => {
      if ('and' in predicate) predicate.and.forEach(collect);
      else if ('or' in predicate) predicate.or.forEach(collect);
      else required.add(predicate.column);
    };
    if (read.where) collect(read.where);
    for (const order of read.orderBy ?? []) required.add(order.column);
    required.delete(table.key); // Identity is required by every admitted write.
    const absent = [...required].map(name => {
      const path = "'"+('$.'+JSON.stringify(name)).replaceAll("'","''")+"'";
      const shape = table.fields[name];
      return '('+(shape ? `t.${q(sqliteFieldName(name))} IS NULL AND ` : '')+`json_type(t.body,${path}) IS NULL)`;
    });
    if (absent.length) {
      const keys = primaryReadKeys(read,table.key), params:unknown[]=[physical];
      let restriction='';
      if (keys) { restriction+=' AND t.id IN (SELECT value FROM json_each(?))'; params.push(JSON.stringify(keys)); }
      if (excluded.length) { restriction+=' AND t.id NOT IN (SELECT value FROM json_each(?))'; params.push(JSON.stringify(excluded)); }
      const incomplete=await db.getFirstAsync<{missing:number}>(`SELECT EXISTS(SELECT 1 FROM ${source} t WHERE t.scope=?${restriction} AND (${absent.join(' OR ')})) missing`,...sourceBindings,...params);
      fieldsComplete=!incomplete?.missing;
    }
  }
  const bindings: unknown[] = [physical,after ?? ''];
  const bind = (value: unknown) => { bindings.push(typeof value === 'boolean' ? Number(value) : value); return '?'; };
  let residualOnly = false;
  const fallback = () => { residualOnly = true; return '1'; };
  const restrict = (input?: DataPredicate): string => {
    if (!input) return '1';
    if ('and' in input || 'or' in input) {
      const and = 'and' in input, children = and ? input.and : input.or;
      return '('+(children.map(restrict).join(and?' AND ':' OR ') || (and?'1':'0'))+')';
    }
    const shape = table.fields[input.column];
    if (!shape || input.transform) return fallback();
    const column = 't.'+q(sqliteFieldName(input.column));
    if (input.op==='isNull' || input.op==='notNull') return column+(input.op==='isNull'?' IS NULL':' IS NOT NULL');
    if (shape.kind==='json') return fallback();
    const kind = shape.kind === 'text' ? 'string' : shape.kind === 'number' ? 'number' : 'boolean';
    if (input.op==='in') {
      if (input.values.length>256) return fallback();
      const values = input.values.filter(value=>value!==null && typeof value===kind);
      return values.length ? column+' IN ('+values.map(bind).join(',')+')' : '0';
    }
    if (!('value' in input) || !['eq','ne','lt','lte','gt','gte'].includes(input.op)) return fallback();
    if (input.value===null) return '0';
    // A mismatched comparison must reach the portable type error. UTF-16 text
    // inequalities differ from SQLite UTF-8 BINARY for supplementary scalars.
    if (typeof input.value!==kind || shape.kind==='text' && !['eq','ne'].includes(input.op)) return fallback();
    const op = {eq:'=',ne:'<>',lt:'<',lte:'<=',gt:'>',gte:'>='}[input.op as 'eq'];
    return column+op+bind(input.value);
  };
  let where = restrict(read.where);
  // Do not suppress an earlier residual type/transform error by pushing down
  // a later AND term. Preserve portable short-circuit/error semantics.
  if (residualOnly) { where='1'; bindings.length=2; }
  const requested = executionReadColumns(read,table.key);
  const fields = requested?.every(field=>Object.hasOwn(table.fields,field)) ? requested : undefined;
  const selected = fields ? fields.map((name,i)=>{
    const path = ('$.'+JSON.stringify(name)).replaceAll("'","''");
    return `t.${q(sqliteFieldName(name))} p${i},json_type(t.body,'${path}') h${i}`;
  }).join(',') : sqliteRowBodySQL(table)+' body';
  let limit=256, records: Record<string,unknown>[];
  for (;;) {
    try {
      records=await db.getAllAsync(`SELECT t.id,${selected} FROM ${source} t WHERE t.scope=? AND t.id>? AND (${where}) ORDER BY t.id LIMIT ?`,...sourceBindings,...bindings,limit);
      sqlitePayloadBytes(records,262144);
      if (fields) decodedPageBytes(records,table,fields,262144);
      else { let bytes=0; for (const row of records) bytes+=decodedJSONBytes(String(row.body),262144-bytes); }
      break;
    } catch(error) { if (!pageBudgetError(error)||limit===1) throw error; limit=Math.max(1,Math.floor(limit/2)); }
  }
  const result:ReplicaReadPage = records.map(record=>({id:String(record.id),row:!fields ? JSON.parse(String(record.body)) as JsonObject : Object.fromEntries(fields.flatMap((name,i)=>{
    if (record['h'+i]===null) return [];
    const value=record['p'+i],kind=table.fields[name]!.kind;
    return [[name,value===null?null:kind==='json'?JSON.parse(String(value)):kind==='boolean'?Boolean(value):value]];
  }))}));
  result.fieldsComplete=fieldsComplete; return result;
}
