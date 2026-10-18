import type {DataRead} from '@gonvex/module-sdk';
import type {ReadCoverage} from '@gonvex/local-runtime/read-view';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
import type {SQLiteReplicaDatabase,SQLiteLocalReplicaStorage} from './sqlite-replica-storage.js';
import type {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteCanonicalScopeArguments,sqliteCanonicalWrites} from './sqlite-worker-protocol.js';
export type SQLiteCanonicalGeneration={epoch:number;sequence:number};
export type SQLiteCanonicalReply={value:unknown;sequence:number};

/** Only the dedicated owner's FIFO invokes this dispatcher. Canonical calls
 * share its connection, epoch fence and durable retry ledger with native pages. */
export class SQLiteCanonicalDispatcher {
  constructor(private readonly db:SQLiteReplicaDatabase,private readonly storage:SQLiteLocalReplicaStorage,private readonly engine:SQLiteTypedReplicaEngine){}
  async generation(scope:string,expected?:SQLiteCanonicalGeneration):Promise<SQLiteCanonicalGeneration>{
    this.validateScope(scope);
    if(expected!==undefined&&(!expected||!Number.isSafeInteger(expected.epoch)||expected.epoch<0||!Number.isSafeInteger(expected.sequence)||expected.sequence<0))throw new TypeError('Invalid canonical read generation');
    await this.storage.ready();
    const generation=await this.db.getFirstAsync<SQLiteCanonicalGeneration>('SELECT epoch,sequence FROM gv_storage_meta WHERE id=1');
    if(!generation)fail('FENCED','Storage owner is not initialized');
    if(expected&&(expected.epoch!==generation.epoch||expected.sequence!==generation.sequence))fail('PAGE_CHANGED','Canonical read generation changed');
    return generation;
  }
  private validateScope(scope:unknown):asserts scope is string{
    if(typeof scope!=='string'||!scope||scope.length>512)throw new TypeError('Invalid canonical storage scope');
  }
  async call(requestID:string,input:{scope:string;method:string;args:unknown[]}):Promise<SQLiteCanonicalReply>{
    sqlitePayloadBytes(input,262144);this.validateScope(input.scope);
    if(!Object.hasOwn(sqliteCanonicalScopeArguments,input.method)||!Array.isArray(input.args))throw Object.assign(new Error('Unsupported canonical method'),{code:'OPERATION'});
    const index=sqliteCanonicalScopeArguments[input.method as keyof typeof sqliteCanonicalScopeArguments];
    if(input.args[index]!==input.scope)throw new TypeError('Canonical method scope does not match its envelope');
    await this.storage.ready();
    const invoke=async(admitted:unknown):Promise<SQLiteCanonicalReply>=>{
      const call=admitted as typeof input;
      // Null encodes an omitted optional positional argument on this internal
      // JSON transport. Nested application data retains its ordinary nulls.
      const args=call.args.map(value=>value===null?undefined:value);
      if(call.method==='loadWorkingSet'){
        const budget=args[1] as {maxRows:number;maxBytes:number};
        if(!budget||!Number.isSafeInteger(budget.maxRows)||budget.maxRows<0||!Number.isSafeInteger(budget.maxBytes)||budget.maxBytes<1)throw new TypeError('Invalid canonical working-set budget');
        args[1]={...budget,maxRows:Math.min(budget.maxRows,256),maxBytes:Math.min(budget.maxBytes,131072)};
      }
      if(call.method==='loadEntityRows'&&(!Array.isArray(args[2])||args[2].length>256))fail('BUDGET','Canonical entity request exceeds row budget');
      if(call.method==='readChanges'){
        const supplied=args[2] as Record<string,unknown>|undefined;
        for(const name of ['availableRows','availableBytes','maxRows','maxBytes'])if(supplied?.[name]!==undefined&&(!Number.isSafeInteger(supplied[name])||Number(supplied[name])<0))throw new TypeError('Invalid canonical change budget');
        args[2]={...supplied,rows:supplied?.rows??{},windows:supplied?.windows??[],availableRows:Math.min(Number(supplied?.availableRows??256),256),availableBytes:Math.min(Number(supplied?.availableBytes??131072),131072),maxRows:Math.min(Number(supplied?.maxRows??256),256),maxBytes:Math.min(Number(supplied?.maxBytes??131072),131072)};
      }
      if(call.method==='loadWindowRows'){
        const header=await this.db.getFirstAsync<{n:number}>("SELECT json_array_length(value,'$.ids') n FROM _gonvex_replica_queries WHERE scope=? AND signature=?",call.scope,args[1]);
        if(header&&header.n>256)fail('BUDGET','Legacy window exceeds bounded hydration; use native pages');
      }
      const method=this.storage[call.method as keyof typeof sqliteCanonicalScopeArguments] as (...args:unknown[])=>Promise<unknown>;
      const value=await method.apply(this.storage,args);
      const sequence=Number((await this.db.getFirstAsync<{value:string}>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='sequence'",call.scope))?.value??0);
      if(!Number.isSafeInteger(sequence)||sequence<0)fail('FENCED','Invalid canonical commit sequence');
      const result={value:value??null,sequence};sqlitePayloadBytes(result,262144);return result;
    };
    return sqliteCanonicalWrites.has(input.method)?this.engine.executeStorageOnce(input.scope,requestID,input,invoke):invoke(input);
  }
  async read(input:{scope:string;coverage:ReadCoverage;read:DataRead;excluded?:readonly string[];expected:SQLiteCanonicalGeneration}){
    sqlitePayloadBytes(input,262144);
    if(input.expected===undefined)throw new TypeError('Canonical reads require a captured generation');
    const generation=await this.generation(input.scope,input.expected);
    const value=await this.storage.withReadView(input.scope,input.coverage,view=>view.select(input.read,input.excluded));
    sqlitePayloadBytes(value,262144);return {generation,value};
  }
}
